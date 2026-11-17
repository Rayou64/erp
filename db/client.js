const fs = require('fs');
const path = require('path');
const { AsyncLocalStorage } = require('node:async_hooks');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function isTransientPgError(error) {
  const message = String(error?.message || '').toLowerCase();
  const code = String(error?.code || '').toLowerCase();

  if (code === '57p01' || code === '57p02' || code === '57p03') {
    return true;
  }

  return (
    message.includes('connection terminated') ||
    message.includes('connection timeout') ||
    message.includes('timeout expired') ||
    message.includes('terminating connection') ||
    message.includes('remaining connection slots are reserved') ||
    message.includes('sorry, too many clients already')
  );
}

function normalizeDriver(rawDriver, databaseUrl) {
  const normalized = String(rawDriver || '').trim().toLowerCase();
  if (normalized === 'postgres' || normalized === 'postgresql' || normalized === 'pg') {
    return 'postgres';
  }
  if (normalized === 'sqlite') {
    return 'sqlite';
  }
  return databaseUrl ? 'postgres' : 'sqlite';
}

function toPgPlaceholders(sql) {
  let index = 0;
  return String(sql).replace(/\?/g, () => {
    index += 1;
    return `$${index}`;
  });
}

function quoteCamelCaseIdentifiers(sql) {
  const source = String(sql);
  return source.replace(/("(?:[^"]|"")*"|'(?:[^']|'')*')|\b[a-z_]+[A-Z][A-Za-z0-9_]*\b/g,
    (token, quoted) => quoted ? token : `"${token}"`);
}

function createSqliteClient(dbFile) {
  const sqlite3 = require('sqlite3').verbose();
  fs.mkdirSync(path.dirname(dbFile), { recursive: true });

  const db = new sqlite3.Database(dbFile, err => {
    if (err) {
      throw err;
    }
  });

  db.serialize(() => {
    db.run('PRAGMA foreign_keys = ON');
    db.run('PRAGMA legacy_alter_table = ON');
    db.run('PRAGMA journal_mode = WAL');
    db.run('PRAGMA synchronous = NORMAL');
    db.run('PRAGMA busy_timeout = 5000');
    db.run('PRAGMA temp_store = MEMORY');
  });

  async function run(sql, params = []) {
    return await new Promise((resolve, reject) => {
      db.run(sql, params, function onRun(err) {
        if (err) {
          reject(err);
        } else {
          resolve({
            lastID: this.lastID,
            changes: this.changes,
            rowCount: this.changes,
          });
        }
      });
    });
  }

  async function get(sql, params = []) {
    return await new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => {
        if (err) {
          reject(err);
        } else {
          resolve(row);
        }
      });
    });
  }

  async function all(sql, params = []) {
    return await new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }

  async function close() {
    await new Promise((resolve, reject) => {
      db.close(err => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  const transactionContext = new AsyncLocalStorage();
  let pending = Promise.resolve();
  async function exclusive(operation) {
    if (transactionContext.getStore()) return operation();
    const previous = pending;
    let release;
    pending = new Promise(resolve => { release = resolve; });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }
  const client = {
    driver: 'sqlite',
    run: (...args) => exclusive(() => run(...args)),
    get: (...args) => exclusive(() => get(...args)),
    all: (...args) => exclusive(() => all(...args)),
    close: () => exclusive(close),
    transaction: operation => exclusive(() => transactionContext.run(true, async () => {
      await run('BEGIN IMMEDIATE');
      try {
        const result = await operation(client);
        await run('COMMIT');
        return result;
      } catch (error) {
        try {
          await run('ROLLBACK');
        } catch (rollbackError) {
          throw new AggregateError([error, rollbackError], 'SQLite transaction and rollback failed');
        }
        throw error;
      }
    })),
  };
  return client;
}

function createPostgresClient(databaseUrl) {
  if (!databaseUrl) {
    throw new Error('DATABASE_URL est obligatoire avec DATABASE_DRIVER=postgres');
  }

  let effectiveDatabaseUrl = databaseUrl;
  try {
    const parsedUrl = new URL(databaseUrl);
    const disableSsl = process.env.PGSSL_DISABLE === '1';
    const sslMode = (parsedUrl.searchParams.get('sslmode') || '').toLowerCase();

    if (disableSsl) {
      parsedUrl.searchParams.delete('sslmode');
      parsedUrl.searchParams.delete('sslrootcert');
      parsedUrl.searchParams.delete('sslcert');
      parsedUrl.searchParams.delete('sslkey');
    } else if (sslMode && !parsedUrl.searchParams.has('uselibpqcompat')) {
      // Keep TLS while avoiding strict verify-full behavior with managed DB self-signed chains.
      parsedUrl.searchParams.set('uselibpqcompat', 'true');
    }

    effectiveDatabaseUrl = parsedUrl.toString();
  } catch (_error) {
    effectiveDatabaseUrl = databaseUrl;
  }

  const { Pool } = require('pg');
  const queryRetryCount = Math.max(0, Number(process.env.PG_QUERY_RETRY_COUNT || 1));
  const queryRetryDelayMs = Math.max(0, Number(process.env.PG_QUERY_RETRY_DELAY_MS || 250));
  const pool = new Pool({
    connectionString: effectiveDatabaseUrl,
    ssl: process.env.PGSSL_DISABLE === '1' ? false : { rejectUnauthorized: false },
    max: Number(process.env.PG_POOL_MAX || 20),
    idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30_000),
    connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS || 30_000),
  });

  async function queryWithRetry(sql, params = []) {
    let attempt = 0;
    let lastError = null;

    while (attempt <= queryRetryCount) {
      try {
        return await pool.query(sql, params);
      } catch (error) {
        lastError = error;
        if (attempt >= queryRetryCount || !isTransientPgError(error)) {
          throw error;
        }

        const waitMs = queryRetryDelayMs * (attempt + 1);
        await sleep(waitMs);
        attempt += 1;
      }
    }

    throw lastError;
  }

  async function run(sql, params = []) {
    const preparedSql = toPgPlaceholders(quoteCamelCaseIdentifiers(sql));
    const isInsert = /^\s*insert\s+/i.test(preparedSql);
    const hasReturning = /\breturning\b/i.test(preparedSql);

    let result;
    if (isInsert && !hasReturning) {
      try {
        result = await queryWithRetry(`${preparedSql} RETURNING "id"`, params);
      } catch (_error) {
        result = await queryWithRetry(preparedSql, params);
      }
    } else {
      result = await queryWithRetry(preparedSql, params);
    }

    return {
      lastID: result.rows[0] && result.rows[0].id ? result.rows[0].id : null,
      changes: result.rowCount,
      rowCount: result.rowCount,
    };
  }

  async function get(sql, params = []) {
    const result = await queryWithRetry(toPgPlaceholders(quoteCamelCaseIdentifiers(sql)), params);
    return result.rows[0];
  }

  async function all(sql, params = []) {
    const result = await queryWithRetry(toPgPlaceholders(quoteCamelCaseIdentifiers(sql)), params);
    return result.rows;
  }

  async function close() {
    await pool.end();
  }

  async function transaction(operation) {
    const connection = await pool.connect();
    const query = (sql, params = []) => connection.query(toPgPlaceholders(quoteCamelCaseIdentifiers(sql)), params);
    try {
      await connection.query('BEGIN');
      const result = await operation({
        driver: 'postgres',
        run: async (sql, params) => {
          const response = await query(sql, params);
          return { lastID: response.rows[0]?.id ?? null, changes: response.rowCount, rowCount: response.rowCount };
        },
        get: async (sql, params) => (await query(sql, params)).rows[0],
        all: async (sql, params) => (await query(sql, params)).rows,
      });
      await connection.query('COMMIT');
      return result;
    } catch (error) {
      try {
        await connection.query('ROLLBACK');
      } catch (rollbackError) {
        throw new AggregateError([error, rollbackError], 'PostgreSQL transaction and rollback failed');
      }
      throw error;
    } finally {
      connection.release();
    }
  }

  return {
    driver: 'postgres',
    run,
    get,
    all,
    close,
    transaction,
  };
}

function createDbClient(options = {}) {
  const databaseUrl = options.databaseUrl || process.env.DATABASE_URL || '';
  const driver = normalizeDriver(options.driver || process.env.DATABASE_DRIVER, databaseUrl);
  const dbFile = options.dbFile || process.env.DB_FILE || path.join(process.cwd(), 'data.db');

  if (driver === 'postgres') {
    return createPostgresClient(databaseUrl);
  }

  return createSqliteClient(dbFile);
}

module.exports = {
  createDbClient,
};
