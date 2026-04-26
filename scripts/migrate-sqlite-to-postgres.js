#!/usr/bin/env node

const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const { Client } = require('pg');

const SQLITE_FILE = process.env.DB_FILE || path.join(process.cwd(), 'data.db');
const DATABASE_URL = process.env.DATABASE_URL;
const PGSSL_DISABLE = process.env.PGSSL_DISABLE === '1';
const BATCH_SIZE = Number(process.env.MIGRATION_BATCH_SIZE || 500);
const TRUNCATE_TARGET = process.env.MIGRATION_TRUNCATE_TARGET === '1';

function quoteIdentifier(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function mapSqliteTypeToPostgres(sqliteType) {
  const t = String(sqliteType || '').toUpperCase();
  if (t.includes('INT')) return 'BIGINT';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB')) return 'DOUBLE PRECISION';
  if (t.includes('NUM') || t.includes('DEC')) return 'NUMERIC';
  if (t.includes('BLOB')) return 'BYTEA';
  if (t.includes('BOOL')) return 'BOOLEAN';
  if (t.includes('DATE') || t.includes('TIME')) return 'TIMESTAMPTZ';
  return 'TEXT';
}

function sqliteAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

function sqliteGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function sqliteClose(db) {
  return new Promise((resolve, reject) => {
    db.close(err => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function ensureTable(pg, tableName, columns) {
  const pkColumns = columns.filter(col => Number(col.pk) > 0);

  const columnSql = columns.map(col => {
    const colName = quoteIdentifier(col.name);
    const mappedType = mapSqliteTypeToPostgres(col.type);
    const isPk = pkColumns.length === 1 && pkColumns[0].name === col.name;
    const isIntegerPk = isPk && String(col.type || '').toUpperCase().includes('INT');

    if (isIntegerPk) {
      return `${colName} BIGSERIAL PRIMARY KEY`;
    }

    const nullability = Number(col.notnull) === 1 ? ' NOT NULL' : '';
    return `${colName} ${mappedType}${nullability}`;
  });

  if (pkColumns.length > 1) {
    const compositePk = pkColumns
      .sort((a, b) => Number(a.pk) - Number(b.pk))
      .map(col => quoteIdentifier(col.name))
      .join(', ');
    columnSql.push(`PRIMARY KEY (${compositePk})`);
  }

  const createSql = `CREATE TABLE IF NOT EXISTS ${quoteIdentifier(tableName)} (${columnSql.join(', ')})`;
  await pg.query(createSql);
}

async function syncTable(pg, sqliteDb, tableName) {
  const columns = await sqliteAll(sqliteDb, `PRAGMA table_info(${quoteIdentifier(tableName)})`);
  if (!columns.length) {
    return { tableName, migratedRows: 0 };
  }

  await ensureTable(pg, tableName, columns);

  if (TRUNCATE_TARGET) {
    await pg.query(`TRUNCATE TABLE ${quoteIdentifier(tableName)} RESTART IDENTITY CASCADE`);
  }

  const totalRow = await sqliteGet(sqliteDb, `SELECT COUNT(*) AS count FROM ${quoteIdentifier(tableName)}`);
  const totalRows = Number(totalRow && totalRow.count ? totalRow.count : 0);
  if (!totalRows) {
    return { tableName, migratedRows: 0 };
  }

  const columnNames = columns.map(col => col.name);
  const columnIdentifiers = columnNames.map(name => quoteIdentifier(name)).join(', ');
  const pkColumns = columns.filter(col => Number(col.pk) > 0);

  for (let offset = 0; offset < totalRows; offset += BATCH_SIZE) {
    const rows = await sqliteAll(
      sqliteDb,
      `SELECT ${columnIdentifiers} FROM ${quoteIdentifier(tableName)} LIMIT ? OFFSET ?`,
      [BATCH_SIZE, offset]
    );

    if (!rows.length) continue;

    const values = [];
    const valuePlaceholders = rows.map((row, rowIndex) => {
      const rowPlaceholders = columnNames.map((colName, colIndex) => {
        values.push(row[colName]);
        return `$${rowIndex * columnNames.length + colIndex + 1}`;
      });
      return `(${rowPlaceholders.join(', ')})`;
    });

    const conflictClause = pkColumns.length
      ? ` ON CONFLICT (${pkColumns
          .sort((a, b) => Number(a.pk) - Number(b.pk))
          .map(col => quoteIdentifier(col.name))
          .join(', ')}) DO NOTHING`
      : '';

    const insertSql = `INSERT INTO ${quoteIdentifier(tableName)} (${columnIdentifiers}) VALUES ${valuePlaceholders.join(', ')}${conflictClause}`;
    await pg.query(insertSql, values);
  }

  if (pkColumns.length === 1) {
    const pk = pkColumns[0];
    const isIntegerPk = String(pk.type || '').toUpperCase().includes('INT');
    if (isIntegerPk) {
      const seqSql = `SELECT pg_get_serial_sequence($1, $2) AS seq`;
      const seqRes = await pg.query(seqSql, [tableName, pk.name]);
      const seqName = seqRes.rows[0] && seqRes.rows[0].seq;
      if (seqName) {
        await pg.query(
          `SELECT setval($1, COALESCE((SELECT MAX(${quoteIdentifier(pk.name)}) FROM ${quoteIdentifier(tableName)}), 1), true)`,
          [seqName]
        );
      }
    }
  }

  return { tableName, migratedRows: totalRows };
}

async function main() {
  if (!DATABASE_URL) {
    console.error('DATABASE_URL est obligatoire pour la migration vers PostgreSQL.');
    process.exit(1);
    return;
  }

  const sqliteDb = new sqlite3.Database(SQLITE_FILE);
  const pg = new Client({
    connectionString: DATABASE_URL,
    ssl: PGSSL_DISABLE ? false : { rejectUnauthorized: false },
  });

  await pg.connect();

  try {
    const tables = await sqliteAll(
      sqliteDb,
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );

    const results = [];
    for (const row of tables) {
      const tableName = row.name;
      const result = await syncTable(pg, sqliteDb, tableName);
      results.push(result);
      console.log(`Table ${tableName}: ${result.migratedRows} lignes migrees`);
    }

    const totalRows = results.reduce((sum, item) => sum + item.migratedRows, 0);
    console.log('\nMigration terminee.');
    console.log(`Tables migrees: ${results.length}`);
    console.log(`Total lignes migrees: ${totalRows}`);
  } finally {
    await pg.end();
    await sqliteClose(sqliteDb);
  }
}

main().catch(err => {
  console.error('Echec migration SQLite -> PostgreSQL:', err.message);
  process.exit(1);
});
