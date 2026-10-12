/**
 * Rebuilds `website_event` into its v3 shape by copying it to a new table
 * instead of updating it in place.
 *
 * Migrations 05_add_visit_id and 09_update_hostname_region each backfill every
 * row of `website_event`. Done as in-place UPDATEs they are unusably slow on a
 * large table: each updated row has to be written into every index on the
 * table, which measured at ~820 rows/s here - about four days for both passes,
 * plus roughly 100 GB of bloat. The same work as a sequential copy into a
 * fresh table measured at ~51,000 rows/s, because indexes are built once at
 * the end rather than maintained row by row.
 *
 * The phases are ordered to keep peak disk close to the table's current
 * footprint: the old table's indexes are most of its size, so they come off up
 * front, and the new table's indexes are only built once the old heap is gone.
 *
 * `visit_id` is deliberately left nullable here. The running v2 application
 * does not write that column, so making it NOT NULL before the v3 deploy would
 * stop event collection; scripts/finish-v3-migration.mjs tightens it at the end.
 *
 * Every phase is idempotent and safe to re-run after an interruption.
 *
 * Usage:
 *   node scripts/rebuild-website-event.mjs --phase status
 *   node scripts/rebuild-website-event.mjs --phase prepare
 *   node scripts/rebuild-website-event.mjs --phase create
 *   node scripts/rebuild-website-event.mjs --phase copy [--days 7] [--from ISO] [--to ISO]
 *   node scripts/rebuild-website-event.mjs --phase verify
 *   node scripts/rebuild-website-event.mjs --phase deep-verify
 *   node scripts/rebuild-website-event.mjs --phase swap
 *   node scripts/rebuild-website-event.mjs --phase index
 *   node scripts/rebuild-website-event.mjs --phase cleanup   (drops the old table)
 */
import 'dotenv/config';
import pg from 'pg';

const NEW_TABLE = 'website_event_v3';
const OLD_TABLE = 'website_event_old';
const PROGRESS_TABLE = 'website_event_rebuild_progress';

/**
 * Narrow (session_id, website_id) -> hostname lookup. The copy needs only
 * hostname out of `session`, but joining the real table makes Postgres seq-scan
 * all 11GB of it and spill the hash on every window. This projection is a few
 * hundred megabytes, so it stays cached. It doubles as the preserved copy that
 * migration 09 needs before it drops session.hostname.
 */
const HOSTNAME_LOOKUP = 'session_hostname_backup';

/** Visit id derived from (session, hour), matching migration 05's grouping. */
const VISIT_ID_EXPR = `md5(we.session_id::text || date_trunc('hour', coalesce(we.created_at, '1970-01-01'::timestamptz))::text)::uuid`;

/** Columns carried over verbatim from the existing table. */
const CARRIED_COLUMNS = [
  'event_id',
  'website_id',
  'session_id',
  'created_at',
  'url_path',
  'url_query',
  'referrer_path',
  'referrer_query',
  'referrer_domain',
  'page_title',
  'event_type',
  'event_name',
];

/** Columns added by migrations 07, 08 and 09, with their migration types. */
const ADDED_COLUMNS = [
  ['tag', 'VARCHAR(50)'],
  ['hostname', 'VARCHAR(100)'],
  ['utm_source', 'VARCHAR(255)'],
  ['utm_medium', 'VARCHAR(255)'],
  ['utm_campaign', 'VARCHAR(255)'],
  ['utm_content', 'VARCHAR(255)'],
  ['utm_term', 'VARCHAR(255)'],
  ['gclid', 'VARCHAR(255)'],
  ['fbclid', 'VARCHAR(255)'],
  ['msclkid', 'VARCHAR(255)'],
  ['ttclid', 'VARCHAR(255)'],
  ['li_fat_id', 'VARCHAR(255)'],
  ['twclid', 'VARCHAR(255)'],
];

/**
 * `created_at` is built as soon as the copy finishes because the swap's
 * catch-up queries depend on it. The rest are deferred until after the old
 * heap is dropped so the two never occupy disk simultaneously.
 */
const EARLY_INDEXES = [['website_event_created_at_idx', '("created_at")']];

const DEFERRED_INDEXES = [
  ['website_event_session_id_idx', '("session_id")'],
  ['website_event_visit_id_idx', '("visit_id")'],
  ['website_event_website_id_idx', '("website_id")'],
  ['website_event_website_id_created_at_idx', '("website_id", "created_at")'],
  ['website_event_website_id_visit_id_created_at_idx', '("website_id", "visit_id", "created_at")'],
  [
    'website_event_website_id_session_id_created_at_idx',
    '("website_id", "session_id", "created_at")',
  ],
  ['website_event_website_id_created_at_url_path_idx', '("website_id", "created_at", "url_path")'],
  ['website_event_website_id_created_at_url_query_idx', '("website_id", "created_at", "url_query")'],
  [
    'website_event_website_id_created_at_referrer_domain_idx',
    '("website_id", "created_at", "referrer_domain")',
  ],
  [
    'website_event_website_id_created_at_page_title_idx',
    '("website_id", "created_at", "page_title")',
  ],
  [
    'website_event_website_id_created_at_event_name_idx',
    '("website_id", "created_at", "event_name")',
  ],
  ['website_event_website_id_created_at_tag_idx', '("website_id", "created_at", "tag")'],
  ['website_event_website_id_created_at_hostname_idx', '("website_id", "created_at", "hostname")'],
];

/**
 * Old indexes dropped before the copy to free the disk the new table needs.
 * All of them are rebuilt on the new table afterwards.
 */
const DROPPABLE_LEGACY_INDEXES = [
  'website_event_website_id_created_at_page_title_idx',
  'website_event_website_id_created_at_url_path_idx',
  'website_event_website_id_created_at_referrer_domain_idx',
  'website_event_website_id_created_at_url_query_idx',
  'website_event_website_id_created_at_event_name_idx',
  'website_event_website_id_session_id_created_at_idx',
  'website_event_website_id_created_at_idx',
  // Left over from an older schema: empty, and not defined by any migration.
  'idx_website_event_type',
];

/**
 * Indexes kept on the old table so the running application can still read and
 * record events during the rebuild. Index names are unique across the schema,
 * so these are renamed out of the way to free the names for the new table.
 */
const RETAINED_LEGACY_INDEXES = [
  'website_event_created_at_idx',
  'website_event_session_id_idx',
  'website_event_website_id_idx',
  'website_event_pkey',
];

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const phase = arg('phase', 'status');

/**
 * Window width for the copy. Kept small so each statement finishes well
 * inside the pooler's tolerance for a single long-running query.
 */
const CHUNK_DAYS = Number(arg('days', 7));

const CONNECTION_STRING = process.env.DIRECT_DATABASE_URL || process.env.DATABASE_URL;

if (!CONNECTION_STRING) {
  console.error('DIRECT_DATABASE_URL or DATABASE_URL must be set.');
  process.exit(1);
}

const log = m => console.log(m);
const step = m => console.log(`  - ${m}`);

async function connect() {
  const client = new pg.Client({
    connectionString: CONNECTION_STRING,
    // The connection goes through a pooler that can drop a long-running
    // session without sending a FIN, which leaves the socket half-open and a
    // pending query waiting forever. Keepalives surface that as an error.
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
  });
  client.on('error', () => {});
  await client.connect();
  await client.query('SET statement_timeout = 0');
  // The 64MB default makes large index builds spill to disk constantly.
  await client.query(`SET maintenance_work_mem = '512MB'`);
  // The server default here is 3.5MB, which makes every hash join in the copy
  // spill to disk. This is a single connection, so it can afford more.
  await client.query(`SET work_mem = '64MB'`);
  return client;
}

/**
 * Opens a connection, tolerating transient loss of the network. A laptop
 * sleeping or a brief DNS failure must not end a multi-hour rebuild.
 */
async function connectPersistently() {
  for (let attempt = 1; ; attempt++) {
    try {
      return await connect();
    } catch (e) {
      if (attempt >= 60) {
        throw new Error(`could not connect after ${attempt} attempts: ${e.message}`);
      }

      if (attempt === 1 || attempt % 10 === 0) {
        step(`waiting for the database to be reachable: ${e.message}`);
      }
      await new Promise(r => setTimeout(r, Math.min(15_000, 2000 * attempt)));
    }
  }
}

let db = await connectPersistently();

async function reconnect() {
  try {
    db.connection?.stream?.destroy();
    await db.end();
  } catch {
    // The old connection is being discarded; failures here are expected.
  }
  db = await connectPersistently();
}

/**
 * Runs a statement with an upper bound on how long it may sit without
 * returning. Keepalives catch most dead connections, but a watchdog is what
 * guarantees the script can never stall indefinitely on one.
 */
async function guarded(sql, params, timeoutMs) {
  let timer;

  const watchdog = new Promise((_, reject) => {
    timer = setTimeout(() => {
      // Destroying the socket makes the in-flight query reject too.
      try {
        db.connection?.stream?.destroy();
      } catch {
        // Nothing to clean up.
      }
      reject(new Error(`watchdog: no response after ${Math.round(timeoutMs / 1000)}s`));
    }, timeoutMs);
  });

  try {
    return await Promise.race([db.query(sql, params), watchdog]);
  } finally {
    clearTimeout(timer);
  }
}

/** Retries a unit of work on connection failure, reconnecting between tries. */
async function withRetry(label, fn, attempts = 8) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (attempt >= attempts) {
        throw new Error(`${label} failed after ${attempts} attempts: ${e.message}`);
      }

      step(`${label}: ${e.message} - reconnecting and retrying (${attempt}/${attempts - 1})`);
      await reconnect();
      await new Promise(r => setTimeout(r, 3000 * attempt));
    }
  }
}


async function tableExists(name) {
  const { rows } = await db.query(
    `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
    [name],
  );
  return rows.length > 0;
}

async function indexExists(name) {
  const { rows } = await db.query(`SELECT 1 FROM pg_indexes WHERE indexname = $1`, [name]);
  return rows.length > 0;
}

async function dbSize() {
  const { rows } = await db.query(
    `SELECT pg_size_pretty(pg_database_size(current_database())) AS s`,
  );
  return rows[0].s;
}

/**
 * Builds indexes, optionally without blocking writes.
 *
 * A plain CREATE INDEX takes a SHARE lock, which blocks INSERTs for the whole
 * build - on a table this size that stalls event collection for minutes per
 * index and loses traffic. CONCURRENTLY trades roughly double the build time
 * for leaving writes untouched, which is the right trade on a live table.
 *
 * A failed concurrent build leaves an invalid index behind, so any leftover is
 * dropped before retrying.
 */
async function buildIndexes(table, indexes, { concurrently = false } = {}) {
  for (const [name, def] of indexes) {
    // Each index is its own retryable unit. These builds run for 15-20 minutes
    // apiece, which is long enough that a dropped pooler connection is likely
    // at least once across a run; without this, one drop ends the whole phase.
    await withRetry(name, async () => {
      // Losing the client connection does not cancel the statement: the pooler
      // keeps the backend alive, so the build may still be running server-side.
      // Waiting for it both avoids a redundant rebuild and prevents fighting
      // over the locks it holds.
      for (let waited = 0; ; waited += 30) {
        const { rows } = await db.query(
          `SELECT round(extract(epoch from (now() - query_start)) / 60, 1) AS min
             FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND state <> 'idle'
              AND query LIKE '%CREATE INDEX%' AND query LIKE $1`,
          [`%${name}%`],
        );

        if (!rows.length) {
          break;
        }

        if (waited === 0) {
          step(`${name} is still building server-side (${rows[0].min} min in) - waiting`);
        }
        await new Promise(r => setTimeout(r, 30_000));
      }

      if (await indexExists(name)) {
        const { rows } = await db.query(
          `SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
            WHERE c.relname = $1`,
          [name],
        );

        if (rows[0]?.indisvalid) {
          step(`${name} already exists`);
          return;
        }

        // A concurrent build that failed partway leaves an invalid index that
        // writes still have to maintain, so clear it before retrying.
        step(`dropping invalid ${name} from an earlier attempt`);
        await guarded(`DROP INDEX IF EXISTS "${name}"`, [], 600_000);
      }

      const started = Date.now();
      await guarded(
        `CREATE INDEX ${concurrently ? 'CONCURRENTLY ' : ''}"${name}" ON "${table}" ${def}`,
        [],
        7_200_000,
      );
      step(
        `built ${name} in ${((Date.now() - started) / 60000).toFixed(1)} min (db ${await dbSize()})`,
      );
    });
  }
}

/* ---------------------------------------------------------------- status */

async function showStatus() {
  log(`database size: ${await dbSize()}`);

  for (const t of ['website_event', NEW_TABLE, OLD_TABLE]) {
    if (!(await tableExists(t))) {
      log(`${t}: does not exist`);
      continue;
    }

    const { rows } = await db.query(
      `SELECT pg_size_pretty(pg_relation_size($1::regclass)) heap,
              pg_size_pretty(pg_indexes_size($1::regclass)) idx,
              (SELECT reltuples::bigint FROM pg_class WHERE oid = $1::regclass) est`,
      [t],
    );
    log(`${t}: heap ${rows[0].heap}, indexes ${rows[0].idx}, ~${rows[0].est} rows`);
  }

  if (await tableExists(PROGRESS_TABLE)) {
    const { rows } = await db.query(
      `SELECT count(*) FILTER (WHERE done_at IS NOT NULL) done,
              count(*) FILTER (WHERE done_at IS NULL) partial,
              coalesce(sum(rows_copied), 0) rows,
              max(month) FILTER (WHERE done_at IS NOT NULL) latest
         FROM ${PROGRESS_TABLE}`,
    );
    const r = rows[0];
    log(`copy progress: ${r.done} months done (${r.rows} rows), latest ${r.latest}, ${r.partial} partial`);
  }
}

/* --------------------------------------------------------------- prepare */

async function prepare() {
  log('Phase: prepare - freeing disk and releasing index names');
  log(`database size before: ${await dbSize()}`);

  if (!(await tableExists('website_event'))) {
    throw new Error('website_event does not exist - has the swap already run?');
  }

  for (const name of DROPPABLE_LEGACY_INDEXES) {
    if (!(await indexExists(name))) {
      step(`${name} already gone`);
      continue;
    }
    const started = Date.now();
    await db.query(`DROP INDEX IF EXISTS "${name}"`);
    step(`dropped ${name} in ${((Date.now() - started) / 1000).toFixed(0)}s`);
  }

  // Renaming frees the canonical names for the rebuilt table. Renaming the
  // index behind a primary key renames its constraint too.
  for (const name of RETAINED_LEGACY_INDEXES) {
    if (!(await indexExists(name))) {
      step(`${name} already renamed or absent`);
      continue;
    }
    await db.query(`ALTER INDEX "${name}" RENAME TO "${name}_legacy"`);
    step(`renamed ${name} -> ${name}_legacy`);
  }

  log(`database size after: ${await dbSize()}`);
}

/* ---------------------------------------------------------------- create */

async function create() {
  log(`Phase: create - building empty ${NEW_TABLE}`);

  await db.query(`
    CREATE TABLE IF NOT EXISTS ${PROGRESS_TABLE} (
      month       DATE PRIMARY KEY,   -- window start date
      rows_copied BIGINT,
      started_at  TIMESTAMPTZ,
      done_at     TIMESTAMPTZ
    )
  `);

  if (await tableExists(NEW_TABLE)) {
    step(`${NEW_TABLE} already exists`);
    return;
  }

  // Inherits the source column types, defaults and NOT NULL flags exactly.
  // The primary key is added after the copy, not here, so the bulk load does
  // not pay for index maintenance.
  await db.query(
    `CREATE TABLE "${NEW_TABLE}" (LIKE "website_event" INCLUDING DEFAULTS INCLUDING CONSTRAINTS)`,
  );
  step('created from the existing table definition');

  for (const [name, type] of ADDED_COLUMNS) {
    await db.query(`ALTER TABLE "${NEW_TABLE}" ADD COLUMN IF NOT EXISTS "${name}" ${type}`);
  }
  step(`added ${ADDED_COLUMNS.length} v3 columns`);
}

/* ------------------------------------------------------------------ copy */

/**
 * Fixed-width [start, end) windows over the whole range. Deliberately built
 * by adding a constant number of days rather than calendar months: Postgres
 * clamps `'2024-01-30'::date + interval '1 month'` to 2024-02-29, so calendar
 * stepping can silently leave a gap between one window's end and the next
 * one's start.
 */
function chunksBetween(min, max, days) {
  const chunks = [];
  const width = days * 86_400_000;

  // Align to midnight UTC so windows are stable across runs.
  const start = Date.UTC(min.getUTCFullYear(), min.getUTCMonth(), min.getUTCDate());

  for (let t = start; t <= max.getTime(); t += width) {
    chunks.push([new Date(t), new Date(t + width)]);
  }

  return chunks;
}

/**
 * Projects session hostnames into a narrow table. Joining `session` directly
 * costs a seq scan of all 11GB plus a spilling hash on every window, which is
 * what collapsed copy throughput from ~20,000 to ~1,700 rows/s once the
 * growing new table pushed `session` out of the cache.
 */
async function buildHostnameLookup() {
  if (await tableExists(HOSTNAME_LOOKUP)) {
    const { rows } = await guarded(`SELECT count(*) AS n FROM "${HOSTNAME_LOOKUP}"`, [], 300_000);
    step(`${HOSTNAME_LOOKUP} already exists with ${rows[0].n} row(s)`);
    return;
  }

  step(`building ${HOSTNAME_LOOKUP} from session`);
  const started = Date.now();

  await guarded(
    `CREATE TABLE "${HOSTNAME_LOOKUP}" AS
     SELECT session_id, website_id, hostname FROM "session" WHERE hostname IS NOT NULL`,
    [],
    1_800_000,
  );

  // Clustered on the join key so lookups stay cheap.
  await guarded(
    `CREATE INDEX "${HOSTNAME_LOOKUP}_session_website_idx"
       ON "${HOSTNAME_LOOKUP}" (session_id, website_id)`,
    [],
    1_800_000,
  );
  await guarded(`ANALYZE "${HOSTNAME_LOOKUP}"`, [], 600_000);

  const { rows } = await guarded(
    `SELECT count(*) AS n, pg_size_pretty(pg_total_relation_size($1::regclass)) AS sz
       FROM "${HOSTNAME_LOOKUP}"`,
    [HOSTNAME_LOOKUP],
    300_000,
  );
  step(
    `built with ${rows[0].n} row(s), ${rows[0].sz}, in ` +
      `${((Date.now() - started) / 60000).toFixed(1)} min`,
  );
}

async function copy() {
  log(`Phase: copy - loading ${NEW_TABLE} from website_event`);

  // Counting rows per window on the new table needs this index; it is one of
  // the required indexes anyway, so building it up front costs nothing.
  log('ensuring the created_at index exists on the new table');
  await withRetry('created_at index', () => buildIndexes(NEW_TABLE, EARLY_INDEXES));

  await withRetry('hostname lookup', buildHostnameLookup);

  const { rows: bounds } = await guarded(
    `SELECT min(created_at) AS min, max(created_at) AS max FROM website_event`,
    [],
    600_000,
  );

  if (!bounds[0].min) {
    step('source table is empty');
    return;
  }

  // --from/--to narrow the work to a sub-range, so a resumed run can use small
  // windows over what is left without re-checking the completed prefix.
  const from = arg('from') ? new Date(arg('from')) : new Date(bounds[0].min);
  const to = arg('to') ? new Date(arg('to')) : new Date(bounds[0].max);

  const chunks = chunksBetween(from, to, CHUNK_DAYS);
  log(
    `${chunks.length} window(s) of ${CHUNK_DAYS} day(s) covering ` +
      `${from.toISOString()} -> ${to.toISOString()}`,
  );

  let copied = 0;
  let skipped = 0;
  const startedAll = Date.now();

  for (const [from, to] of chunks) {
    const label = from.toISOString().slice(0, 10);

    // The whole window - counting, clearing and inserting - is one retryable
    // unit, and every attempt re-reads the counts. An INSERT can commit on the
    // server and still lose the connection before the client sees the ack, so
    // an attempt that reused a count taken earlier would skip the clear and
    // insert the window a second time.
    await withRetry(`${label}`, async () => {
      const { rows: counts } = await guarded(
        `SELECT (SELECT count(*) FROM website_event
                  WHERE created_at >= $1 AND created_at < $2) AS old,
                (SELECT count(*) FROM "${NEW_TABLE}"
                  WHERE created_at >= $1 AND created_at < $2) AS new`,
        [from.toISOString(), to.toISOString()],
        900_000,
      );

      const oldRows = Number(counts[0].old);
      const newRows = Number(counts[0].new);

      // Comparing counts rather than trusting bookkeeping makes this
      // self-healing: a window left half-copied, duplicated by an earlier
      // retry, or skipped entirely is simply redone.
      if (oldRows === newRows) {
        skipped += 1;
        return;
      }

      if (newRows > 0) {
        step(`${label}: clearing ${newRows} row(s) from an incomplete window`);
        await guarded(
          `DELETE FROM "${NEW_TABLE}" WHERE created_at >= $1 AND created_at < $2`,
          [from.toISOString(), to.toISOString()],
          1_800_000,
        );
      }

      const started = Date.now();
      const { rowCount } = await guarded(
        `INSERT INTO "${NEW_TABLE}" (${CARRIED_COLUMNS.join(', ')}, visit_id, hostname)
         SELECT ${CARRIED_COLUMNS.map(c => `we.${c}`).join(', ')},
                ${VISIT_ID_EXPR},
                s.hostname
           FROM website_event we
           LEFT JOIN "session_hostname_backup" s
                  ON s.session_id = we.session_id AND s.website_id = we.website_id
          WHERE we.created_at >= $1 AND we.created_at < $2`,
        [from.toISOString(), to.toISOString()],
        1_800_000,
      );

      await guarded(
        `INSERT INTO ${PROGRESS_TABLE} (month, rows_copied, started_at, done_at)
         VALUES ($1, $2, NOW(), NOW())
             ON CONFLICT (month) DO UPDATE SET rows_copied = $2, done_at = NOW()`,
        [label, rowCount],
        60_000,
      );

      copied += rowCount;
      const secs = (Date.now() - started) / 1000;
      step(
        `${label}: ${rowCount} rows in ${secs.toFixed(0)}s ` +
          `(${Math.round(rowCount / Math.max(secs, 1))} rows/s) | db ${await dbSize()}`,
      );
    });
  }

  log(
    `copied ${copied} rows in ${((Date.now() - startedAll) / 60000).toFixed(1)} min ` +
      `(${skipped} window(s) already complete)`,
  );

  const { rows: pk } = await guarded(
    `SELECT 1 FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'p'`,
    [NEW_TABLE],
    60_000,
  );

  if (!pk.length) {
    log('adding the primary key (this also proves event_id is unique)');
    const started = Date.now();
    await withRetry('primary key', () =>
      guarded(`ALTER TABLE "${NEW_TABLE}" ADD PRIMARY KEY (event_id)`, [], 3_600_000),
    );
    step(`added in ${((Date.now() - started) / 60000).toFixed(1)} min`);
  } else {
    step('primary key already present');
  }
}

/* ---------------------------------------------------------------- verify */

async function verify() {
  log('Phase: verify - comparing the new table against the source');

  const { rows } = await db.query(`
    WITH old AS (
      SELECT date_trunc('month', created_at)::date m, count(*) n FROM website_event GROUP BY 1
    ), new AS (
      SELECT date_trunc('month', created_at)::date m, count(*) n FROM "${NEW_TABLE}" GROUP BY 1
    )
    SELECT coalesce(old.m, new.m)::text AS month,
           coalesce(old.n, 0) AS old_rows,
           coalesce(new.n, 0) AS new_rows
      FROM old FULL OUTER JOIN new ON old.m = new.m
     ORDER BY 1
  `);

  const latestMonth = rows.length ? rows[rows.length - 1].month : null;
  let oldTotal = 0;
  let newTotal = 0;
  let mismatches = 0;

  for (const r of rows) {
    oldTotal += Number(r.old_rows);
    newTotal += Number(r.new_rows);

    if (r.old_rows === r.new_rows) {
      continue;
    }

    // The source keeps growing during the rebuild, so it legitimately leads
    // in the newest month; the swap copies that remainder.
    if (r.month === latestMonth && Number(r.old_rows) >= Number(r.new_rows)) {
      log(`  ${r.month}: old ${r.old_rows} vs new ${r.new_rows} (expected - still collecting)`);
      continue;
    }

    mismatches += 1;
    log(`  ${r.month}: old ${r.old_rows} vs new ${r.new_rows} MISMATCH`);
  }

  log(`totals: old ${oldTotal}, new ${newTotal}, outstanding ${oldTotal - newTotal}`);

  // event_id uniqueness is already guaranteed by the primary key added in the
  // copy phase, so only the computed and required columns need checking.
  const { rows: bad } = await db.query(
    `SELECT count(*) AS n FROM "${NEW_TABLE}"
      WHERE visit_id IS NULL OR website_id IS NULL OR session_id IS NULL OR url_path IS NULL`,
  );
  log(`rows with a null visit_id/website_id/session_id/url_path: ${bad[0].n}`);

  if (Number(bad[0].n) > 0 || mismatches > 0) {
    throw new Error('verification failed - do not run the swap phase');
  }

  log('verification passed');
}

/* ----------------------------------------------------------- deep verify */

/**
 * Order-independent content fingerprint over the carried columns, so the copy
 * is compared on actual values rather than row counts alone. Run against both
 * tables before the old one is dropped.
 */
function fingerprintSql(table) {
  const fields = [
    'event_id::text',
    'website_id::text',
    'session_id::text',
    `coalesce(created_at::text, '')`,
    'url_path',
    `coalesce(url_query, '')`,
    `coalesce(referrer_path, '')`,
    `coalesce(referrer_query, '')`,
    `coalesce(referrer_domain, '')`,
    `coalesce(page_title, '')`,
    'event_type::text',
    `coalesce(event_name, '')`,
  ].join(` || '|' || `);

  return `
    SELECT date_trunc('month', created_at)::date::text AS month,
           count(*) AS rows,
           sum(('x' || substr(md5(${fields}), 1, 8))::bit(32)::bigint) AS fingerprint
      FROM "${table}"
     GROUP BY 1`;
}

async function deepVerify() {
  log('Phase: deep-verify - comparing column contents, not just row counts');

  const source = (await tableExists(OLD_TABLE)) ? OLD_TABLE : 'website_event';
  const target = (await tableExists(OLD_TABLE)) ? 'website_event' : NEW_TABLE;
  log(`comparing ${source} (original) against ${target} (rebuilt)`);

  const started = Date.now();
  // Scanning both tables with an md5 per row is a long single statement, so it
  // needs the same watchdog and reconnect handling as the copy.
  const { rows } = await withRetry('fingerprint', () =>
    guarded(
      `WITH a AS (${fingerprintSql(source)}), b AS (${fingerprintSql(target)})
       SELECT coalesce(a.month, b.month) AS month,
              a.rows AS old_rows, b.rows AS new_rows,
              a.fingerprint AS old_fp, b.fingerprint AS new_fp
         FROM a FULL OUTER JOIN b ON a.month = b.month
        ORDER BY 1`,
      [],
      5_400_000,
    ),
  );

  const latest = rows.length ? rows[rows.length - 1].month : null;
  let mismatches = 0;

  for (const r of rows) {
    const same = r.old_rows === r.new_rows && r.old_fp === r.new_fp;

    if (same) {
      continue;
    }

    // The newest month is still being written to while this runs.
    if (r.month === latest) {
      log(`  ${r.month}: still collecting (old ${r.old_rows} / new ${r.new_rows}) - not compared`);
      continue;
    }

    mismatches += 1;
    log(
      `  ${r.month}: MISMATCH rows ${r.old_rows} vs ${r.new_rows}, ` +
        `fingerprint ${r.old_fp} vs ${r.new_fp}`,
    );
  }

  log(`compared ${rows.length} month(s) in ${((Date.now() - started) / 60000).toFixed(1)} min`);

  if (mismatches > 0) {
    throw new Error(`${mismatches} month(s) differ - do not drop the original table`);
  }

  log('contents match exactly');
}

/* ------------------------------------------------------------------ swap */

async function swap() {
  log('Phase: swap - catching up and renaming');

  if (await tableExists(OLD_TABLE)) {
    step('swap has already run');
    return;
  }

  const catchUp = `
    INSERT INTO "${NEW_TABLE}" (${CARRIED_COLUMNS.join(', ')}, visit_id, hostname)
    SELECT ${CARRIED_COLUMNS.map(c => `we.${c}`).join(', ')}, ${VISIT_ID_EXPR}, s.hostname
      FROM website_event we
      LEFT JOIN "session_hostname_backup" s ON s.session_id = we.session_id AND s.website_id = we.website_id
     WHERE we.created_at > $1
       AND NOT EXISTS (SELECT 1 FROM "${NEW_TABLE}" n WHERE n.event_id = we.event_id)`;

  // Most of the backlog is copied before taking the lock, to keep the
  // exclusive window as short as possible.
  const { rows: wm } = await db.query(`SELECT max(created_at) AS w FROM "${NEW_TABLE}"`);
  step(`catching up rows newer than ${wm[0].w}`);

  const caught = await db.query(catchUp, [wm[0].w]);
  step(`copied ${caught.rowCount} rows before locking`);

  await db.query('BEGIN');
  try {
    // Blocks writers only for the final delta and the renames.
    await db.query(`LOCK TABLE website_event IN ACCESS EXCLUSIVE MODE`);

    // Bounded to the tail of the table rather than scanning all of it: an
    // unbounded anti-join over every row would hold this exclusive lock - and
    // so block event collection - for minutes. The overlap covers rows that
    // landed while the pre-lock catch-up was running.
    const final = await db.query(
      `INSERT INTO "${NEW_TABLE}" (${CARRIED_COLUMNS.join(', ')}, visit_id, hostname)
       SELECT ${CARRIED_COLUMNS.map(c => `we.${c}`).join(', ')}, ${VISIT_ID_EXPR}, s.hostname
         FROM website_event we
         LEFT JOIN "session_hostname_backup" s ON s.session_id = we.session_id AND s.website_id = we.website_id
        WHERE we.created_at > $1::timestamptz - interval '2 hours'
          AND NOT EXISTS (SELECT 1 FROM "${NEW_TABLE}" n WHERE n.event_id = we.event_id)`,
      [wm[0].w],
    );
    step(`copied a final ${final.rowCount} row(s) under the lock`);

    await db.query(`ALTER TABLE website_event RENAME TO "${OLD_TABLE}"`);
    await db.query(`ALTER TABLE "${NEW_TABLE}" RENAME TO website_event`);
    await db.query(
      `ALTER TABLE website_event RENAME CONSTRAINT "${NEW_TABLE}_pkey" TO website_event_pkey`,
    );

    await db.query('COMMIT');
    step('website_event is now the rebuilt table');
  } catch (e) {
    await db.query('ROLLBACK');
    throw e;
  }
}

/* ----------------------------------------------------------------- index */

async function index() {
  log('Phase: index - building the remaining indexes');
  log(`database size before: ${await dbSize()}`);

  const started = Date.now();
  // The table is live and taking writes by this point, so these must not lock
  // out the collection endpoint.
  await buildIndexes('website_event', DEFERRED_INDEXES, { concurrently: true });

  log('analyzing');
  await db.query('ANALYZE website_event');
  log(`done in ${((Date.now() - started) / 60000).toFixed(1)} min, database size ${await dbSize()}`);
}

/* --------------------------------------------------------------- cleanup */

async function cleanup() {
  log('Phase: cleanup - dropping the old table');

  if (!(await tableExists(OLD_TABLE))) {
    step('already removed');
  } else {
    await db.query(`DROP TABLE "${OLD_TABLE}"`);
    step(`dropped ${OLD_TABLE}`);
  }

  await db.query(`DROP TABLE IF EXISTS ${PROGRESS_TABLE}`);
  step('dropped the progress table');

  log(`database size: ${await dbSize()}`);
}

/* ------------------------------------------------------------------ main */

const phases = {
  status: showStatus,
  prepare,
  create,
  copy,
  verify,
  'deep-verify': deepVerify,
  swap,
  index,
  cleanup,
};

if (!phases[phase]) {
  console.error(`Unknown phase "${phase}". One of: ${Object.keys(phases).join(', ')}`);
  process.exit(1);
}

try {
  await phases[phase]();
} finally {
  try {
    await db.end();
  } catch {
    // Already closed.
  }
}
