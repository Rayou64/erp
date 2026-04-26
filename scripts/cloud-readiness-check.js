#!/usr/bin/env node

const path = require('path');

const DEFAULT_JWT = 'erp-secret-2026';
const cwd = process.cwd();

function isTruthy(value) {
  if (value == null) return false;
  const normalized = String(value).trim().toLowerCase();
  return normalized !== '' && normalized !== '0' && normalized !== 'false' && normalized !== 'no';
}

function isUnderWorkspace(targetPath) {
  const resolvedWorkspace = path.resolve(cwd);
  const resolvedTarget = path.resolve(targetPath);
  return resolvedTarget.startsWith(resolvedWorkspace);
}

const checks = [];

function addCheck(category, label, status, details) {
  checks.push({ category, label, status, details });
}

const env = process.env;
const nodeEnv = env.NODE_ENV || 'development';
const jwtSecret = env.JWT_SECRET || DEFAULT_JWT;
const dbFile = env.DB_FILE || path.join(cwd, 'data.db');
const archiveRoot = env.ARCHIVE_ROOT || path.join(cwd, 'archives');
const hasDatabaseUrl = isTruthy(env.DATABASE_URL);
const hasRedisUrl = isTruthy(env.REDIS_URL);
const hasErrorTracking = isTruthy(env.SENTRY_DSN) || isTruthy(env.APPLICATIONINSIGHTS_CONNECTION_STRING);

addCheck(
  'Runtime',
  'NODE_ENV production',
  nodeEnv === 'production' ? 'PASS' : 'WARN',
  nodeEnv === 'production' ? 'Execution en mode production.' : `NODE_ENV=${nodeEnv}. Pour un deploiement entreprise, utiliser NODE_ENV=production.`
);

if (!jwtSecret || jwtSecret === DEFAULT_JWT || jwtSecret.length < 32) {
  addCheck(
    'Security',
    'JWT secret robuste',
    'FAIL',
    'JWT_SECRET est absent, faible ou par defaut. Utiliser une cle d au moins 32 caracteres en secret manager.'
  );
} else {
  addCheck('Security', 'JWT secret robuste', 'PASS', 'JWT secret non defaut et longueur acceptable.');
}

if (/\.db$/i.test(String(dbFile))) {
  addCheck(
    'Database',
    'Base compatible grande echelle',
    'FAIL',
    `DB_FILE=${dbFile}. SQLite est limitee pour la scalabilite horizontale multi-instance. Migrer vers PostgreSQL manag\u00e9.`
  );
} else if (hasDatabaseUrl) {
  addCheck('Database', 'Base compatible grande echelle', 'PASS', 'DATABASE_URL detectee.');
} else {
  addCheck(
    'Database',
    'Base compatible grande echelle',
    'WARN',
    'Impossible de confirmer la base cible. Fournir DATABASE_URL (PostgreSQL manag\u00e9 recommande).'
  );
}

if (isUnderWorkspace(archiveRoot)) {
  addCheck(
    'Storage',
    'Stockage documents externe',
    'WARN',
    `ARCHIVE_ROOT=${archiveRoot}. Pour grande entreprise, utiliser object storage (S3, Blob, GCS) plutot que disque applicatif.`
  );
} else {
  addCheck('Storage', 'Stockage documents externe', 'PASS', `ARCHIVE_ROOT=${archiveRoot}`);
}

addCheck(
  'Reliability',
  'Cache/session distribue',
  hasRedisUrl ? 'PASS' : 'WARN',
  hasRedisUrl ? 'REDIS_URL detectee.' : 'REDIS_URL non configuree. Recommande pour files, cache distribue et anti-thundering-herd.'
);

addCheck(
  'Observability',
  'Error tracking centralise',
  hasErrorTracking ? 'PASS' : 'WARN',
  hasErrorTracking ? 'Solution de tracking detectee.' : 'Configurer SENTRY_DSN ou APPLICATIONINSIGHTS_CONNECTION_STRING.'
);

const counts = checks.reduce((acc, check) => {
  acc[check.status] = (acc[check.status] || 0) + 1;
  return acc;
}, {});

const score = Math.round(((counts.PASS || 0) / checks.length) * 100);
const enterpriseReady = (counts.FAIL || 0) === 0 && score >= 80;

const report = {
  timestamp: new Date().toISOString(),
  enterpriseReady,
  score,
  summary: {
    pass: counts.PASS || 0,
    warn: counts.WARN || 0,
    fail: counts.FAIL || 0,
    total: checks.length,
  },
  checks,
  nextActions: [
    'Migrer de SQLite vers PostgreSQL manag\u00e9 avec haute disponibilite.',
    'Basculer les archives PDF vers un object storage cloud.',
    'Mettre tous les secrets en secret manager et supprimer toute valeur par defaut.',
    'Configurer observabilite centralisee (logs, metriques, traces, alertes).',
    'Valider avec tests de charge et tests de reprise apres incident.'
  ]
};

console.log(JSON.stringify(report, null, 2));

if (!enterpriseReady) {
  process.exitCode = 1;
}
