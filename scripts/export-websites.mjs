/**
 * Exports everything belonging to a set of websites to local files, so the
 * data is retained outside the database before it is deleted.
 *
 * Writes newline-delimited JSON - one row per line - which is readable, easy
 * to grep, and can be loaded back row by row without parsing a whole file.
 *
 * Rows are read through a server-side cursor in batches rather than all at
 * once, so memory stays flat regardless of table size.
 *
 * Usage:
 *   node scripts/export-websites.mjs --out ./export --website <id> [--website <id>]...
 *   node scripts/export-websites.mjs --out ./export --all-except <id>,<id>,...
 */
import 'dotenv/config';
import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { once } from 'node:events';
import pg from 'pg';

const BATCH = 10_000;

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

const OUT_DIR = arg('out', './export');

const db = new pg.Client({
  connectionString: process.env.DIRECT_DATABASE_URL || process.env.DATABASE_URL,
  keepAlive: true,
});
db.on('error', () => {});
await db.connect();
await db.query('SET statement_timeout = 0');
await db.query(`SET work_mem = '64MB'`);

const log = m => console.log(m);
const step = m => console.log(`  - ${m}`);

async function tableExists(name) {
  const { rows } = await db.query(
    `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
    [name],
  );
  return rows.length > 0;
}

/** Resolves which websites to export, either listed explicitly or by exclusion. */
async function resolveWebsites() {
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

/**
 * Streams one query to a .jsonl file through a server-side cursor, respecting
 * stream backpressure so a large table cannot outrun the disk.
 */
async function exportQuery(table, sql, params) {
  const path = `${OUT_DIR}/${table}.jsonl`;
  const out = createWriteStream(path);
  const started = Date.now();
  let count = 0;

  await db.query('BEGIN');
  try {
    await db.query(`DECLARE export_cur NO SCROLL CURSOR FOR ${sql}`, params);

    for (;;) {
      const { rows } = await db.query(`FETCH ${BATCH} FROM export_cur`);

      if (!rows.length) {
        break;
      }

      count += rows.length;

      const chunk = `${rows.map(r => JSON.stringify(r)).join('\n')}\n`;
      if (!out.write(chunk)) {
        await once(out, 'drain');
      }
    }

    await db.query('CLOSE export_cur');
    await db.query('COMMIT');
  } catch (e) {
    await db.query('ROLLBACK').catch(() => {});
    throw e;
  }

  out.end();
  await once(out, 'finish');

  step(`${table}: ${count} row(s) -> ${path} (${((Date.now() - started) / 1000).toFixed(0)}s)`);
  return count;
}

const websites = await resolveWebsites();

if (!websites.length) {
  log('Nothing to export.');
  await db.end().catch(() => {});
  process.exit(0);
}

const { rows: named } = await db.query(
  `SELECT website_id, name, domain FROM website WHERE website_id = ANY($1::uuid[])`,
  [websites],
);

log(`Exporting ${named.length} website(s) to ${OUT_DIR}:`);
named.forEach(w => step(`${w.name} (${w.domain}) ${w.website_id}`));

await mkdir(OUT_DIR, { recursive: true });

/**
 * Every table that holds rows belonging to a website. `event_data` and
 * `session_data` are scoped through website_id directly in this schema.
 */
const EXPORTS = [
  ['website', `SELECT * FROM website WHERE website_id = ANY($1::uuid[])`],
  ['website_event', `SELECT * FROM website_event WHERE website_id = ANY($1::uuid[])`],
  ['session', `SELECT * FROM session WHERE website_id = ANY($1::uuid[])`],
  ['event_data', `SELECT * FROM event_data WHERE website_id = ANY($1::uuid[])`],
  ['session_data', `SELECT * FROM session_data WHERE website_id = ANY($1::uuid[])`],
  ['report', `SELECT * FROM report WHERE website_id = ANY($1::uuid[])`],
  ['segment', `SELECT * FROM segment WHERE website_id = ANY($1::uuid[])`],
  ['revenue', `SELECT * FROM revenue WHERE website_id = ANY($1::uuid[])`],
];

log('');
let grandTotal = 0;

for (const [table, sql] of EXPORTS) {
  // Tables introduced by migrations that have not been applied yet.
  if (!(await tableExists(table))) {
    step(`${table}: not present in this schema, skipped`);
    continue;
  }

  grandTotal += await exportQuery(table, sql, [websites]);
}

log(`\nExported ${grandTotal} row(s) across ${EXPORTS.length} table(s).`);
await db.end().catch(() => {});
