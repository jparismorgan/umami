/**
 * Applies migrations 05-14 to a large Postgres database without tripping
 * `statement_timeout`, then records them in `_prisma_migrations` so that
 * `prisma migrate deploy` (run by `check-db` during the build) becomes a no-op.
 *
 * Why this exists: `prisma migrate deploy` runs each migration as one
 * server-side batch under the role's `statement_timeout`. Two migrations in
 * this range rewrite every row of `website_event`, and two more build indexes
 * on it, so on a table of this size the deploy fails with P3018 and blocks all
 * later migrations.
 *
 * Every step is idempotent and checked before it runs, so the script can be
 * re-run safely after an interruption.
 *
 * Prerequisite: `node scripts/rebuild-website-event.mjs` has completed through
 * its swap and index phases.
 *
 * Usage:
 *   node scripts/finish-v3-migration.mjs [--with-hostname-backfill]
 */
import 'dotenv/config';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import pg from 'pg';

const MIGRATIONS_DIR = new URL('../prisma/migrations/', import.meta.url);
const CONNECTION_STRING = process.env.DIRECT_DATABASE_URL || process.env.DATABASE_URL;
const WITH_HOSTNAME_BACKFILL = process.argv.includes('--with-hostname-backfill');

if (!CONNECTION_STRING) {
  console.error('DIRECT_DATABASE_URL or DATABASE_URL must be set.');
  process.exit(1);
}

async function connect() {
  const client = new pg.Client({
    connectionString: CONNECTION_STRING,
    // Without keepalives, a connection the pooler has dropped looks alive
    // forever and a pending query never settles - the statement finishes on
    // the server while the client waits indefinitely for a reply that is
    // never coming.
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    // Backstop in case keepalives miss it. Generous, because validating a
    // constraint or building an index here legitimately takes many minutes.
    query_timeout: 3_600_000,
  });
  client.on('error', () => {});
  await client.connect();
  // These steps are meant to run long; the server default would kill them.
  await client.query('SET statement_timeout = 0');
  await client.query(`SET maintenance_work_mem = '512MB'`);
  return client;
}

const db = await connect();

const log = msg => console.log(msg);
const step = msg => console.log(`  - ${msg}`);

/* ------------------------------------------------------------------ helpers */

async function isApplied(name) {
  const { rows } = await db.query(
    `SELECT 1 FROM _prisma_migrations
      WHERE migration_name = $1 AND finished_at IS NOT NULL AND rolled_back_at IS NULL`,
    [name],
  );
  return rows.length > 0;
}

function migrationSql(name) {
  return readFileSync(new URL(`${name}/migration.sql`, MIGRATIONS_DIR), 'utf8');
}

/**
 * Prisma stores a checksum of the migration file; compute the same value so a
 * later `migrate deploy` sees the migration as applied and unmodified.
 */
function checksum(name) {
  return createHash('sha256').update(migrationSql(name)).digest('hex');
}

/**
 * Records a migration as successfully applied. Any failed or rolled-back row
 * for the same migration is cleared first, since Prisma refuses to proceed
 * while a failed migration is present.
 */
async function markApplied(name) {
  await db.query(`DELETE FROM _prisma_migrations WHERE migration_name = $1`, [name]);
  await db.query(
    `INSERT INTO _prisma_migrations (id, checksum, migration_name, started_at, finished_at, applied_steps_count)
     VALUES (gen_random_uuid(), $1, $2, NOW(), NOW(), 1)`,
    [checksum(name), name],
  );
  step(`recorded ${name} as applied`);
}

async function columnExists(table, column) {
  const { rows } = await db.query(
    `SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
    [table, column],
  );
  return rows.length > 0;
}

async function indexExists(name) {
  const { rows } = await db.query(`SELECT 1 FROM pg_indexes WHERE indexname = $1`, [name]);
  return rows.length > 0;
}

/**
 * Builds an index without blocking writes. A CONCURRENTLY build cannot run
 * inside a transaction and leaves an invalid index behind if it fails, so any
 * leftover invalid index is dropped before retrying.
 */
async function createIndexConcurrently(name, table, definition) {
  if (await indexExists(name)) {
    const { rows } = await db.query(
      `SELECT i.indisvalid FROM pg_class c
         JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = $1`,
      [name],
    );

    if (rows[0]?.indisvalid) {
      step(`index ${name} already exists`);
      return;
    }

    step(`dropping invalid index ${name} from an earlier attempt`);
    await db.query(`DROP INDEX IF EXISTS "${name}"`);
  }

  step(`building index ${name} (concurrently, may take a while)`);
  const started = Date.now();
  await db.query(`CREATE INDEX CONCURRENTLY "${name}" ON "${table}" ${definition}`);
  step(`built ${name} in ${((Date.now() - started) / 60000).toFixed(1)} min`);
}

/**
 * Adds a NOT NULL constraint without the full-table scan that
 * `SET NOT NULL` normally takes under an ACCESS EXCLUSIVE lock. A CHECK
 * constraint can be validated under a weaker lock that allows concurrent
 * reads and writes, and Postgres 12+ then uses it to skip the scan.
 */
async function setNotNullWithoutLongLock(table, column) {
  const { rows } = await db.query(
    `SELECT is_nullable FROM information_schema.columns
      WHERE table_name = $1 AND column_name = $2`,
    [table, column],
  );

  if (rows[0]?.is_nullable === 'NO') {
    step(`${table}.${column} is already NOT NULL`);
    return;
  }

  const constraint = `${table}_${column}_not_null_chk`;

  const existing = await db.query(`SELECT convalidated FROM pg_constraint WHERE conname = $1`, [
    constraint,
  ]);

  if (existing.rows.length === 0) {
    step(`adding CHECK constraint ${constraint} (NOT VALID)`);
    await db.query(
      `ALTER TABLE "${table}" ADD CONSTRAINT "${constraint}" CHECK ("${column}" IS NOT NULL) NOT VALID`,
    );
  }

  if (!existing.rows[0]?.convalidated) {
    step('validating constraint (concurrent reads and writes continue)');
    await db.query(`ALTER TABLE "${table}" VALIDATE CONSTRAINT "${constraint}"`);
  }

  step(`setting ${column} NOT NULL`);
  await db.query(`ALTER TABLE "${table}" ALTER COLUMN "${column}" SET NOT NULL`);

  await db.query(`ALTER TABLE "${table}" DROP CONSTRAINT "${constraint}"`);
  step('dropped the temporary constraint');
}

/** Runs a migration file verbatim with no statement timeout. */
async function runVerbatim(name) {
  if (await isApplied(name)) {
    log(`${name}: already applied`);
    return;
  }

  log(`${name}: applying`);
  await db.query('BEGIN');
  try {
    await db.query(migrationSql(name));
    await db.query('COMMIT');
  } catch (e) {
    await db.query('ROLLBACK');
    throw e;
  }
  await markApplied(name);
}

/* ------------------------------------------------------------- 05 visit_id */

async function migration05() {
  const name = '05_add_visit_id';

  if (await isApplied(name)) {
    log(`${name}: already applied`);
    return;
  }

  log(`${name}: applying`);

  if (!(await columnExists('website_event', 'visit_id'))) {
    throw new Error(
      'visit_id column is missing - run scripts/rebuild-website-event.mjs first.',
    );
  }

  // The outgoing v2 application does not write visit_id, so it keeps inserting
  // nulls while this runs - faster than they can be backfilled, which makes
  // the NOT NULL validation below fail on rows that appeared mid-backfill.
  // A default stops new nulls at the source; it is dropped again at the end so
  // the schema matches what the migration defines. v3 always sets visit_id
  // explicitly, so the default is never used once deployed.
  step('setting a temporary default so new rows cannot be null');
  await db.query(
    `ALTER TABLE website_event ALTER COLUMN visit_id SET DEFAULT gen_random_uuid()`,
  );

  step('counting rows with a null visit_id');
  const { rows } = await db.query(`SELECT count(*) AS n FROM website_event WHERE visit_id IS NULL`);
  const pending = Number(rows[0].n);

  // The rebuild leaves visit_id nullable so the outgoing application can keep
  // recording events, which means anything it wrote since the swap still
  // needs a visit id. That is a small, recent set.
  const MAX_INLINE_BACKFILL = 10_000_000;

  if (pending > MAX_INLINE_BACKFILL) {
    throw new Error(
      `${pending} rows still have a null visit_id - too many to backfill inline. ` +
        'Run scripts/rebuild-website-event.mjs instead.',
    );
  }

  if (pending > 0) {
    step(`backfilling ${pending} row(s) written since the rebuild`);
    const started = Date.now();
    await db.query(`
      UPDATE website_event
         SET visit_id = md5(session_id::text || date_trunc('hour', created_at)::text)::uuid
       WHERE visit_id IS NULL
    `);
    step(`backfilled in ${((Date.now() - started) / 1000).toFixed(0)}s`);
  } else {
    step('every row already has a visit id');
  }

  await createIndexConcurrently('website_event_visit_id_idx', 'website_event', '("visit_id")');
  await createIndexConcurrently(
    'website_event_website_id_visit_id_created_at_idx',
    'website_event',
    '("website_id", "visit_id", "created_at")',
  );

  await setNotNullWithoutLongLock('website_event', 'visit_id');

  // Restore the column to exactly what the migration defines.
  step('dropping the temporary default');
  await db.query(`ALTER TABLE website_event ALTER COLUMN visit_id DROP DEFAULT`);

  await markApplied(name);
}

/* ----------------------------------------------------------------- 07 tag */

async function migration07() {
  const name = '07_add_tag';

  if (await isApplied(name)) {
    log(`${name}: already applied`);
    return;
  }

  log(`${name}: applying`);

  if (!(await columnExists('website_event', 'tag'))) {
    step('adding website_event.tag');
    await db.query(`ALTER TABLE "website_event" ADD COLUMN "tag" VARCHAR(50)`);
  }

  // Built concurrently rather than inline: a plain CREATE INDEX on a table
  // this size exceeds statement_timeout and locks out writes meanwhile.
  await createIndexConcurrently(
    'website_event_website_id_created_at_tag_idx',
    'website_event',
    '("website_id", "created_at", "tag")',
  );

  await markApplied(name);
}

/* ------------------------------------------------------ 08 utm and click ids */

async function migration08() {
  const name = '08_add_utm_clid';

  if (await isApplied(name)) {
    log(`${name}: already applied`);
    return;
  }

  log(`${name}: applying`);

  // The rebuild created these columns up front, so running the migration file
  // verbatim would fail on the first one that already exists.
  const columns = [
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

  let added = 0;

  for (const [column, type] of columns) {
    if (!(await columnExists('website_event', column))) {
      await db.query(`ALTER TABLE "website_event" ADD COLUMN "${column}" ${type}`);
      added += 1;
    }
  }

  step(added === 0 ? 'all columns already present' : `added ${added} missing column(s)`);
  await markApplied(name);
}

/* -------------------------------------------------- 09 hostname and region */

async function migration09() {
  const name = '09_update_hostname_region';

  if (await isApplied(name)) {
    log(`${name}: already applied`);
    return;
  }

  log(`${name}: applying`);

  if (!(await columnExists('website_event', 'hostname'))) {
    step('adding website_event.hostname');
    await db.query(`ALTER TABLE "website_event" ADD COLUMN "hostname" VARCHAR(100)`);
  }

  // The rebuild populates hostname during its copy, so the backfill below is
  // usually already done by the time this runs.
  const { rows: filled } = await db.query(
    `SELECT EXISTS(SELECT 1 FROM "website_event" WHERE hostname IS NOT NULL) AS any`,
  );
  const alreadyBackfilled = filled[0].any;

  if (alreadyBackfilled) {
    step('website_event.hostname is already populated - nothing to backfill');
  } else if (await columnExists('session', 'hostname')) {
    // This migration drops session.hostname, the only source for the
    // historical values. Keep a copy so the backfill stays possible after the
    // schema change, whether it runs now or later.
    step('preserving session hostnames in session_hostname_backup');
    await db.query(`
      CREATE TABLE IF NOT EXISTS "session_hostname_backup" AS
      SELECT session_id, website_id, hostname FROM "session" WHERE hostname IS NOT NULL
    `);
    await db.query(`
      CREATE INDEX IF NOT EXISTS "session_hostname_backup_session_id_idx"
        ON "session_hostname_backup"(session_id)
    `);
    const { rows } = await db.query(`SELECT count(*) AS n FROM "session_hostname_backup"`);
    step(`preserved ${rows[0].n} session hostnames`);
  }

  if (!alreadyBackfilled && WITH_HOSTNAME_BACKFILL) {
    step('backfilling website_event.hostname (this rewrites every row)');
    const started = Date.now();
    await db.query(`
      UPDATE "website_event" w
         SET hostname = s.hostname
        FROM "session" s
       WHERE s.website_id = w.website_id AND s.session_id = w.session_id
    `);
    step(`backfilled in ${((Date.now() - started) / 60000).toFixed(1)} min`);
  } else if (!alreadyBackfilled) {
    step('skipping the historical hostname backfill (hostname is nullable)');
    step('session hostnames are preserved in session_hostname_backup for later');
  }

  step('reshaping the session table');
  await db.query(`DROP INDEX IF EXISTS "session_website_id_created_at_hostname_idx"`);
  await db.query(`DROP INDEX IF EXISTS "session_website_id_created_at_subdivision1_idx"`);

  if (await columnExists('session', 'subdivision1')) {
    await db.query(`ALTER TABLE "session" RENAME COLUMN "subdivision1" TO "region"`);
  }
  if (await columnExists('session', 'subdivision2')) {
    await db.query(`ALTER TABLE "session" DROP COLUMN "subdivision2"`);
  }
  if (await columnExists('session', 'hostname')) {
    await db.query(`ALTER TABLE "session" DROP COLUMN "hostname"`);
  }

  await createIndexConcurrently(
    'website_event_website_id_created_at_hostname_idx',
    'website_event',
    '("website_id", "created_at", "hostname")',
  );
  await createIndexConcurrently(
    'session_website_id_created_at_region_idx',
    'session',
    '("website_id", "created_at", "region")',
  );

  await markApplied(name);
}

/* ------------------------------------------------------------------- main */

try {
  await migration05();
  await runVerbatim('06_session_data');
  await migration07();
  await migration08();
  await migration09();
  await runVerbatim('10_add_distinct_id');
  await runVerbatim('11_add_segment');
  await runVerbatim('12_update_report_parameter');
  await runVerbatim('13_add_revenue');
  await runVerbatim('14_add_link_and_pixel');

  log('\nAll migrations applied. `prisma migrate deploy` should now be a no-op.');
} finally {
  try {
    await db.end();
  } catch {
    // Already closed.
  }
}
