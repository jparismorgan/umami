/**
 * Permanently removes websites and everything belonging to them.
 *
 * Intended for handing an instance over: the websites named here disappear
 * entirely, including their names and domains, so they leave no trace in the
 * dashboard.
 *
 * Export first - scripts/export-websites.mjs - because this cannot be undone.
 *
 * Events are deleted in time-boxed batches rather than one statement, so no
 * single delete runs long enough to be killed, and so the table never holds a
 * huge number of row locks at once while the site is live.
 *
 * Usage:
 *   node scripts/delete-websites.mjs --all-except <id>,<id>,...  [--dry-run]
 *   node scripts/delete-websites.mjs --website <id> [--website <id>]... [--dry-run]
 */
import 'dotenv/config';
import pg from 'pg';

const BATCH = 200_000;
const DRY_RUN = process.argv.includes('--dry-run');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

function argAll(name) {
  const values = [];
  process.argv.forEach((a, i) => {
    if (a === `--${name}`) {
      values.push(process.argv[i + 1]);
    }
  });
  return values;
}

const db = new pg.Client({
  connectionString: process.env.DIRECT_DATABASE_URL || process.env.DATABASE_URL,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10_000,
  query_timeout: 1_800_000,
});
db.on('error', () => {});
await db.connect();
await db.query('SET statement_timeout = 0');

const log = m => console.log(m);
const step = m => console.log(`  - ${m}`);

async function tableExists(name) {
  const { rows } = await db.query(
    `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
    [name],
  );
  return rows.length > 0;
}

async function resolveTargets() {
  const explicit = argAll('website').filter(Boolean);

  if (explicit.length) {
    return explicit;
  }

  const keep = (arg('all-except') ?? '').split(',').filter(Boolean);

  if (!keep.length) {
    throw new Error('Pass --website <id> (repeatable) or --all-except <id>,<id>,...');
  }

  const { rows } = await db.query(
    `SELECT website_id FROM website WHERE website_id <> ALL($1::uuid[])`,
    [keep],
  );
  return rows.map(r => r.website_id);
}

const targets = await resolveTargets();

if (!targets.length) {
  log('Nothing matches - no websites to delete.');
  await db.end().catch(() => {});
  process.exit(0);
}

const { rows: named } = await db.query(
  `SELECT website_id, name, domain FROM website WHERE website_id = ANY($1::uuid[])`,
  [targets],
);

log(`${DRY_RUN ? 'Would delete' : 'Deleting'} ${named.length} website(s):`);
named.forEach(w => step(`${w.name} (${w.domain})`));

const { rows: kept } = await db.query(
  `SELECT name FROM website WHERE website_id <> ALL($1::uuid[]) ORDER BY name`,
  [targets],
);
log(`\nKeeping ${kept.length}: ${kept.map(k => k.name).join(', ')}`);

if (DRY_RUN) {
  const { rows } = await db.query(
    `SELECT count(*) AS n FROM website_event WHERE website_id = ANY($1::uuid[])`,
    [targets],
  );
  log(`\nDry run: ${rows[0].n} event(s) would be removed. Nothing changed.`);
  await db.end().catch(() => {});
  process.exit(0);
}

/** Deletes in batches so no single statement runs unboundedly long. */
async function deleteBatched(table) {
  if (!(await tableExists(table))) {
    step(`${table}: not present, skipped`);
    return 0;
  }

  let total = 0;

  for (;;) {
    const { rowCount } = await db.query(
      `DELETE FROM "${table}" WHERE ctid IN (
         SELECT ctid FROM "${table}" WHERE website_id = ANY($1::uuid[]) LIMIT ${BATCH}
       )`,
      [targets],
    );

    total += rowCount;

    if (rowCount === 0) {
      break;
    }
    step(`${table}: ${total} row(s) deleted so far`);
  }

  step(`${table}: ${total} row(s) deleted`);
  return total;
}

// Child rows first, then the website records themselves.
for (const table of ['event_data', 'session_data', 'revenue', 'report', 'segment', 'website_event', 'session']) {
  await deleteBatched(table);
}

const { rowCount } = await db.query(`DELETE FROM website WHERE website_id = ANY($1::uuid[])`, [
  targets,
]);
step(`website: ${rowCount} record(s) deleted`);

const { rows: remaining } = await db.query(
  `SELECT name, domain FROM website ORDER BY name`,
);
log(`\nRemaining websites (${remaining.length}):`);
remaining.forEach(w => step(`${w.name} (${w.domain})`));

await db.end().catch(() => {});
