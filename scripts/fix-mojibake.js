const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const DB_FILE = process.env.DB_FILE || path.join(process.cwd(), 'data.db');
const APPLY = process.argv.includes('--apply');

const MARKERS = ['Ã', 'Â', 'â', 'ðŸ', 'ï¸', '�'];

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) return reject(err);
      resolve({ changes: this.changes, lastID: this.lastID });
    });
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows || []);
    });
  });
}

function mojibakePenalty(value) {
  const text = String(value || '');
  let penalty = 0;
  for (const marker of MARKERS) {
    penalty += (text.split(marker).length - 1) * 4;
  }
  return penalty;
}

function decodePotentialMojibake(value) {
  if (typeof value !== 'string' || !value) return value;
  if (!MARKERS.some(marker => value.includes(marker))) return value;

  try {
    const decoded = Buffer.from(value, 'latin1').toString('utf8');
    if (!decoded) return value;
    return mojibakePenalty(decoded) < mojibakePenalty(value) ? decoded : value;
  } catch (_error) {
    return value;
  }
}

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

async function main() {
  const db = new sqlite3.Database(DB_FILE);

  try {
    const tables = await all(
      db,
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );

    let detected = 0;
    let updated = 0;

    for (const tableRow of tables) {
      const table = String(tableRow.name || '');
      if (!table) continue;

      const columns = await all(db, `PRAGMA table_info(${quoteIdent(table)})`);
      const textColumns = columns
        .filter(col => /char|text|clob/i.test(String(col.type || '')))
        .map(col => String(col.name || ''));

      if (!textColumns.length) continue;

      const hasId = columns.some(col => String(col.name || '') === 'id');
      const idExpr = hasId ? 'id' : 'rowid';

      for (const column of textColumns) {
        const whereClause = MARKERS.map(() => `${quoteIdent(column)} LIKE ?`).join(' OR ');
        const likeParams = MARKERS.map(marker => `%${marker}%`);
        const rows = await all(
          db,
          `SELECT ${idExpr} AS _pk, ${quoteIdent(column)} AS _value FROM ${quoteIdent(table)} WHERE ${quoteIdent(column)} IS NOT NULL AND (${whereClause})`,
          likeParams
        );

        for (const row of rows) {
          const id = row._pk;
          const current = String(row._value || '');
          const fixed = decodePotentialMojibake(current);

          if (fixed === current) continue;

          detected += 1;
          if (APPLY) {
            await run(
              db,
              `UPDATE ${quoteIdent(table)} SET ${quoteIdent(column)} = ? WHERE ${idExpr} = ?`,
              [fixed, id]
            );
            updated += 1;
          }

          console.log(`${APPLY ? 'FIX' : 'CANDIDATE'} table=${table} column=${column} id=${id}`);
          console.log(`  before: ${current.slice(0, 180)}`);
          console.log(`  after : ${fixed.slice(0, 180)}`);
        }
      }
    }

    console.log(`SUMMARY detected=${detected} updated=${updated} mode=${APPLY ? 'apply' : 'dry-run'}`);
  } finally {
    db.close();
  }
}

main().catch(error => {
  console.error('fix-mojibake failed:', error.message || error);
  process.exit(1);
});
