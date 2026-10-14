/**
 * Adds a covering index so the dashboard's time-series queries can be answered
 * from the index alone, without reading the 31GB table.
 *
 * Those queries reference only website_id, created_at, event_type and
 * session_id:
 *
 *   select to_char(date_trunc(..., created_at at time zone ...), ...) x,
 *          count(*) | count(distinct session_id) y
 *     from website_event
 *    where website_id = $1 and created_at between $2 and $3
 *      and event_type != $4
 *    group by 1
 *
 * Measured on a 30-day window (7.8M rows), the index scan spent ~12.5s of its
 * ~19s touching 1.86M heap buffers. Carrying event_type and session_id in the
 * index removes those heap reads entirely.
 *
 * event_type and session_id are INCLUDE columns rather than key columns: they
 * only need to be readable, not sortable, and keeping them out of the key
 * leaves the index's internal pages small.
 *
 * Index-only scans also require pages to be marked all-visible, which is what
 * the VACUUM below is for - without it Postgres still visits the heap to check
 * row visibility and the index buys much less.
 *
 * This index is additional to the schema umami defines; it is not part of any
 * migration, and nothing breaks if it is dropped.
 *
 * Usage:
 *   node scripts/add-dashboard-covering-index.mjs [--website <uuid>] [--skip-vacuum]
 */
import 'dotenv/config';
import pg from 'pg';

const INDEX = 'website_event_dashboard_covering_idx';
const DEFINITION = '(website_id, created_at) INCLUDE (event_type, session_id)';

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const CONNECTION_STRING = process.env.DIRECT_DATABASE_URL || process.env.DATABASE_URL;

async function connect() {
  const client = new pg.Client({
    connectionString: CONNECTION_STRING,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    query_timeout: 7_200_000,
  });
  client.on('error', () => {});
  await client.connect();
  await client.query('SET statement_timeout = 0');
  await client.query(`SET maintenance_work_mem = '512MB'`);
  return client;
}

const log = m => console.log(m);
const step = m => console.log(`  - ${m}`);

/**
 * Opens a connection, tolerating transient network loss. Reconnecting must
 * never throw out of a retry handler - that turns a recoverable blip into an
 * uncaught exception that ends the run.
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

/** Reconnects and retries, since these statements run for many minutes. */
async function withRetry(label, fn, attempts = 5) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (attempt >= attempts) {
        throw new Error(`${label} failed after ${attempts} attempts: ${e.message}`);
      }
      step(`${label}: ${e.message} - reconnecting`);
      try {
        await db.end();
      } catch {
        // Discarding a dead connection.
      }
      await new Promise(r => setTimeout(r, 5000 * attempt));
      db = await connectPersistently();
    }
  }
}

/** The busiest website, unless one is named explicitly. */
async function pickWebsite() {
  const explicit = arg('website');

  if (explicit) {
    return explicit;
  }

  const { rows } = await db.query(
    `SELECT website_id, name FROM website ORDER BY name LIMIT 1`,
  );
  return rows[0]?.website_id;
}

const website = await pickWebsite();

const CHART_QUERY = `
  select to_char(date_trunc('day', website_event.created_at at time zone 'UTC'), 'YYYY-MM-DD') x,
         count(*) y
    from website_event
   where website_event.website_id = $1::uuid
     and website_event.created_at between now() - interval '30 days' and now()
     and website_event.event_type != 2
   group by 1 order by 1`;

async function timeChartQuery(label) {
  const started = Date.now();
  const { rows } = await db.query(CHART_QUERY, [website]);
  const secs = (Date.now() - started) / 1000;
  log(`${label}: ${secs.toFixed(1)}s (${rows.length} buckets)`);
  return secs;
}

async function planSummary() {
  const { rows } = await db.query(`EXPLAIN (ANALYZE, BUFFERS) ${CHART_QUERY}`, [website]);
  const text = rows.map(r => r['QUERY PLAN']).join('\n');
  const scan = text.split('\n').find(l => /Scan/.test(l)) ?? '';
  const buffers = text.split('\n').find(l => /Buffers/.test(l)) ?? '';
  step(`plan: ${scan.trim()}`);
  step(`${buffers.trim()}`);
}

log(`Tuning dashboard queries for website ${website}\n`);

log('Before:');
await withRetry('baseline', () => timeChartQuery('  elapsed'));
await withRetry('baseline plan', planSummary);

log(`\nBuilding ${INDEX} ${DEFINITION}`);
const { rows: existing } = await db.query(
  `SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = $1`,
  [INDEX],
);

if (existing[0]?.indisvalid) {
  step('already exists');
} else {
  if (existing.length) {
    step('dropping an invalid index from an earlier attempt');
    await db.query(`DROP INDEX IF EXISTS "${INDEX}"`);
  }

  const started = Date.now();
  // Concurrently: the table is live and must keep accepting events.
  await withRetry('index build', () =>
    db.query(`CREATE INDEX CONCURRENTLY "${INDEX}" ON website_event ${DEFINITION}`),
  );
  step(`built in ${((Date.now() - started) / 60000).toFixed(1)} min`);
}

if (!process.argv.includes('--skip-vacuum')) {
  log('\nVacuuming so pages are marked all-visible (required for index-only scans)');
  const started = Date.now();
  await withRetry('vacuum', () => db.query('VACUUM (ANALYZE) website_event'));
  step(`done in ${((Date.now() - started) / 60000).toFixed(1)} min`);

  const { rows } = await db.query(
    `SELECT relallvisible, relpages FROM pg_class WHERE relname = 'website_event'`,
  );
  const pct = (100 * rows[0].relallvisible) / Math.max(rows[0].relpages, 1);
  step(`${pct.toFixed(1)}% of pages all-visible`);
}

log('\nAfter:');
await withRetry('result', () => timeChartQuery('  elapsed'));
await withRetry('result plan', planSummary);

const { rows: size } = await db.query(
  `SELECT pg_size_pretty(pg_relation_size($1::regclass)) sz,
          pg_size_pretty(pg_database_size(current_database())) db`,
  [INDEX],
);
log(`\nIndex size ${size[0].sz}; database now ${size[0].db}`);

await db.end().catch(() => {});
