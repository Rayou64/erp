

// --- ROUTES DYNAMIQUES ---
// Retourne dynamiquement tous les entrepôts (projets ZONE_STOCK)
// À placer après la création de app et la config des middlewares

// app.js
// API Construction & Logistique avec une base de données SQLite et authentification

const express = require('express');
const path = require('path');
const fs = require('fs');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const PDFDocument = require('pdfkit');
const { PDFDocument: PdfLibDocument, StandardFonts: PdfLibStandardFonts, rgb: pdfRgb } = require('pdf-lib');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');
const archiver = require('archiver');
const nodemailer = require('nodemailer');
const { spawn } = require('child_process');
const { createDbClient } = require('./db/client');

const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || '1mb' }));
app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginResourcePolicy: { policy: 'cross-origin' },
}));

// Forcer explicitement UTF-8 sur les fichiers texte statiques pour eviter les lectures CP-1252/ISO-8859-1.
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    const lower = String(filePath || '').toLowerCase();
    if (lower.endsWith('.html')) {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
      res.setHeader('Pragma', 'no-cache');
      return;
    }
    if (lower.endsWith('.js')) {
      res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
      return;
    }
    if (lower.endsWith('.css')) {
      res.setHeader('Content-Type', 'text/css; charset=utf-8');
      return;
    }
    if (lower.endsWith('.json')) {
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      return;
    }
    if (lower.endsWith('.webmanifest')) {
      res.setHeader('Content-Type', 'application/manifest+json; charset=utf-8');
    }
  },
}));

// Middleware de secours pour les routes HTML non statiques.
app.use((req, res, next) => {
  if (req.path.endsWith('.html')) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
    res.setHeader('Pragma', 'no-cache');
  }
  next();
});

// Autoriser les appels API depuis l'application mobile (Capacitor/file://) et repondre aux preflights OPTIONS.
app.use('/api', (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, X-Device-Token, X-Tracking-Token');
  res.setHeader('Access-Control-Max-Age', '86400');

  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }

  next();
});

// --- ROUTES DYNAMIQUES ---
// Retourne dynamiquement tous les entrepôts (projets ZONE_STOCK)
app.get('/api/warehouses', async (req, res) => {
  try {
    const rows = await all(`SELECT id, nomProjet, prefecture FROM projects WHERE UPPER(typeMaison) = 'ZONE_STOCK' ORDER BY prefecture ASC, nomProjet ASC`);
    const warehouses = rows.map(row => ({
      id: row.id,
      name: row.prefecture || row.nomProjet,
      project: row.nomProjet,
      prefecture: row.prefecture
    }));
    res.json({ warehouses });
  } catch (e) {
    res.status(500).json({ error: 'Erreur chargement entrepôts', details: String(e) });
  }
});

function normalizeCustomWarehouseRow(row) {
  if (!row) return null;
  const id = String(row.id || '').trim();
  const name = String(row.name || '').trim();
  if (!id || !name) return null;

  return {
    id,
    name,
    linkedProjectId: Number(row.linkedProjectId || 0) || null,
    linkedProjectName: String(row.linkedProjectName || '').trim(),
    linkedZoneId: String(row.linkedZoneId || '').trim(),
    linkedZoneName: String(row.linkedZoneName || '').trim(),
    prefecture: String(row.prefecture || '').trim(),
    custom: true,
    isHidden: Number(row.isHidden || 0) === 1,
    createdAt: String(row.createdAt || '').trim(),
    updatedAt: String(row.updatedAt || '').trim(),
  };
}

async function ensureHistoricalCustomWarehouse() {
  const now = new Date().toISOString();
  await run(`INSERT INTO custom_stock_warehouses (
    id,
    name,
    linkedProjectId,
    linkedProjectName,
    linkedZoneId,
    linkedZoneName,
    prefecture,
    isHidden,
    createdAt,
    updatedAt
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT (id) DO NOTHING`, [
    'entrepot-songon-1',
    'Songon',
    null,
    '',
    '',
    'Songon',
    'Songon',
    0,
    now,
    now,
  ]);
}

app.get('/api/custom-warehouses', authenticateToken, async (_req, res) => {
  try {
    await ensureHistoricalCustomWarehouse();
    const rows = await all(`
      SELECT id, name, linkedProjectId, linkedProjectName, linkedZoneId, linkedZoneName, prefecture, isHidden, createdAt, updatedAt
      FROM custom_stock_warehouses
      ORDER BY updatedAt DESC, createdAt DESC, name ASC
    `);
    res.json({ warehouses: rows.map(normalizeCustomWarehouseRow).filter(Boolean) });
  } catch (error) {
    res.status(500).json({ error: 'Erreur chargement entrepôts personnalisés', details: String(error) });
  }
});

app.post('/api/custom-warehouses', authenticateToken, async (req, res) => {
  try {
    const id = String(req.body?.id || '').trim();
    const name = String(req.body?.name || '').trim();
    const linkedProjectId = Number(req.body?.linkedProjectId || 0) || null;
    const linkedProjectName = String(req.body?.linkedProjectName || '').trim();
    const linkedZoneId = String(req.body?.linkedZoneId || '').trim();
    const linkedZoneName = String(req.body?.linkedZoneName || '').trim();
    const prefecture = String(req.body?.prefecture || '').trim();

    if (!id || !name) {
      return res.status(400).json({ error: 'id et name sont obligatoires' });
    }

    const existing = await get('SELECT id FROM custom_stock_warehouses WHERE id = ?', [id]);
    const timestamp = new Date().toISOString();

    await run(`
      INSERT INTO custom_stock_warehouses (
        id, name, linkedProjectId, linkedProjectName, linkedZoneId, linkedZoneName, prefecture, isHidden, createdAt, updatedAt
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        name = excluded.name,
        linkedProjectId = excluded.linkedProjectId,
        linkedProjectName = excluded.linkedProjectName,
        linkedZoneId = excluded.linkedZoneId,
        linkedZoneName = excluded.linkedZoneName,
        prefecture = excluded.prefecture,
        isHidden = excluded.isHidden,
        updatedAt = excluded.updatedAt
    `, [id, name, linkedProjectId, linkedProjectName, linkedZoneId, linkedZoneName, prefecture, timestamp, timestamp]);

    const row = await get(
      'SELECT id, name, linkedProjectId, linkedProjectName, linkedZoneId, linkedZoneName, prefecture, isHidden, createdAt, updatedAt FROM custom_stock_warehouses WHERE id = ?',
      [id]
    );

    res.status(existing ? 200 : 201).json(normalizeCustomWarehouseRow(row));
  } catch (error) {
    res.status(500).json({ error: 'Erreur creation entrepôt personnalisé', details: String(error) });
  }
});

app.delete('/api/custom-warehouses/:id', authenticateToken, async (req, res) => {
  try {
    const id = String(req.params.id || '').trim();
    if (!id) {
      return res.status(400).json({ error: 'id manquant' });
    }

    const existing = await get('SELECT id FROM custom_stock_warehouses WHERE id = ?', [id]);
    if (!existing) {
      return res.status(404).json({ error: 'Entrepôt introuvable' });
    }

    await run('DELETE FROM custom_stock_warehouses WHERE id = ?', [id]);
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ error: 'Erreur suppression entrepôt personnalisé', details: String(error) });
  }
});

const DATA_PURGE_TABLES = [
  'stock_issue_authorization_items',
  'stock_issues',
  'stock_issue_authorizations',
  'purchase_order_items',
  'purchase_orders',
  'material_requests',
  'project_assignments',
  'expenses',
  'revenues',
  'materials',
];

async function purgeBusinessData() {
  const now = new Date().toISOString();

  if (process.env.DATABASE_URL) {
    if (DATA_PURGE_TABLES.length) {
      await run(`TRUNCATE TABLE ${DATA_PURGE_TABLES.map(name => `"${name}"`).join(', ')} RESTART IDENTITY CASCADE`);
    }
  } else {
    try { await run('PRAGMA foreign_keys = OFF'); } catch (_error) {}
    for (const tableName of DATA_PURGE_TABLES) {
      try {
        await run(`DELETE FROM ${tableName}`);
      } catch (_error) {}
    }
    try {
      await run(`DELETE FROM sqlite_sequence WHERE name IN (${DATA_PURGE_TABLES.map(() => '?').join(', ')})`, DATA_PURGE_TABLES);
    } catch (_error) {}
    try { await run('PRAGMA foreign_keys = ON'); } catch (_error) {}
  }

  return {
    purgedAt: now,
    tables: DATA_PURGE_TABLES,
    archivesCleared: false,
  };
}

app.post('/api/admin/purge-business-data', authenticateToken, async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin' && role !== 'dirigeant') {
      return res.status(403).json({ error: 'Acces reserve a l\'administrateur ou au dirigeant' });
    }

    const confirm = String(req.body?.confirm || '').trim();
    if (confirm !== 'PURGE_ALL_DATA') {
      return res.status(400).json({ error: 'Confirmation manquante', details: 'Envoyer confirm=PURGE_ALL_DATA' });
    }

    const result = await purgeBusinessData();
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ error: 'Erreur purge donnees metier', details: String(error) });
  }
});

const APP_DATA_DIR = process.env.APP_DATA_DIR || (process.env.RAILWAY_ENVIRONMENT ? '/data' : __dirname);
const DB_FILE = process.env.DB_FILE || path.join(APP_DATA_DIR, 'data.db');
const ARCHIVE_ROOT = process.env.ARCHIVE_ROOT || path.join(APP_DATA_DIR, 'archives');
const DESKTOP_ZIP_FILE = process.env.DESKTOP_ZIP_FILE || 'RyanERP-win32-x64.zip';
const DESKTOP_ZIP_PATH = path.resolve(__dirname, 'electron', 'dist', DESKTOP_ZIP_FILE);
const JWT_SECRET = process.env.JWT_SECRET || 'erp-secret-2026';
const NODE_ENV = String(process.env.NODE_ENV || 'development').trim().toLowerCase();
const ALLOW_INSECURE_JWT_SECRET = String(process.env.ALLOW_INSECURE_JWT_SECRET || '0').trim() === '1';
const PORT = Number(process.env.PORT || 4000);
const LOCALHOST_FALLBACK_PORT = Number(process.env.LOCALHOST_FALLBACK_PORT || (PORT === 4000 ? 3000 : 0));
const COMMIS_STOCK_USERNAME = process.env.COMMIS_STOCK_USERNAME || 'commis_stock';
const COMMIS_STOCK_PASSWORD = process.env.COMMIS_STOCK_PASSWORD || 'stock123';

// Entrepôts masqués par projet (PINUT)
const HIDDEN_WAREHOUSES = {
  'entrepot-plateau': true,    // Adzopé
  'entrepot-yopougon': true,   // Akoupé
  'entrepot-bingerville': true, // Alepe
  'entrepot-port-bouet': true  // Yakassé
};

function isWarehouseHidden(warehouseId) {
  return HIDDEN_WAREHOUSES[String(warehouseId || '').trim()] === true;
}

const GEST_STOCK_USERNAME = process.env.GEST_STOCK_USERNAME || 'gestionnaire_stock';
const GEST_STOCK_PASSWORD = process.env.GEST_STOCK_PASSWORD || 'geststock123';
const EXECUTIVE_USERNAME = process.env.EXECUTIVE_USERNAME || 'dirigeant';
const EXECUTIVE_PASSWORD = process.env.EXECUTIVE_PASSWORD || 'dirigeant123';
const HR_DIRECTOR_USERNAME = process.env.HR_DIRECTOR_USERNAME || 'directeur_rh';
const HR_DIRECTOR_PASSWORD = process.env.HR_DIRECTOR_PASSWORD || 'rh123';
const PROCUREMENT_REVIEWER_USERNAME = process.env.PROCUREMENT_REVIEWER_USERNAME || 'controle_achat_global';
const PROCUREMENT_REVIEWER_PASSWORD = process.env.PROCUREMENT_REVIEWER_PASSWORD || 'achatglobal123';
const ACHAT_USERNAME = process.env.ACHAT_USERNAME || 'achat';
const ACHAT_PASSWORD = process.env.ACHAT_PASSWORD || 'achat123';
const KOKAN_USERNAME = process.env.KOKAN_USERNAME || 'Kokan_SK';
const KOKAN_PASSWORD = process.env.KOKAN_PASSWORD || 'Stock_SK123';
const SMTP_HOST = String(process.env.SMTP_HOST || '').trim();
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_SECURE = String(process.env.SMTP_SECURE || '0').trim() === '1';
const SMTP_USER = String(process.env.SMTP_USER || '').trim();
const SMTP_PASS = String(process.env.SMTP_PASS || '').trim();
const MAIL_FROM = String(process.env.MAIL_FROM || SMTP_USER || '').trim();
let mailTransport = null;
const IDENTITY_ROSTER = [
  { fullName: 'AGBODJRO BEUGRE AWO ELFRIED JOSEPH', hrUsername: 'AGBODJRO123', userUsername: 'AGBODJRO123', role: 'employe_standard', password: 'AGBODJRO123@2026' },
  { fullName: 'APPIA ROBERT NICAISE', hrUsername: 'APPIA123', userUsername: 'APPIA123', role: 'employe_standard', password: 'APPIA123@2026' },
  { fullName: 'ARMEL KOUDOU RODRIGUE', hrUsername: '', userUsername: '', role: '', password: '' },
  { fullName: 'ATTA BINDE GREGOIRE MARC', hrUsername: 'ATTA123', userUsername: 'ATTA123', role: 'employe_standard', password: 'ATTA123@2026' },
  { fullName: 'BADO NARCISSE BONDIALI', hrUsername: 'BADO123', userUsername: 'BADO123', role: 'employe_standard', password: 'BADO123@2026' },
  { fullName: 'BALMA MOHAMED', hrUsername: 'BALMA123', userUsername: 'BALMA123', role: 'employe_standard', password: 'BALMA123@2026' },
  { fullName: 'BEAKA DAVY WILFRIED', hrUsername: 'BEAKA123', userUsername: 'BEAKA123', role: 'employe_standard', password: 'BEAKA123@2026' },
  { fullName: 'BOSSIO KEHATON JEAN MARC', hrUsername: 'BOSSIO123', userUsername: 'BOSSIO123', role: 'employe_standard', password: 'BOSSIO123@2026' },
  { fullName: 'Chef chantier SK', hrUsername: 'Chef_chantier_SK', userUsername: 'Chef_chantier_SK', role: 'chef_chantier_site', password: 'chefsite15@123' },
  { fullName: 'Contrôle Achat', hrUsername: 'controle_achat_global', userUsername: 'controle_achat_global', role: 'controle_achat', password: 'achatglobal123' },
  { fullName: 'COULIBALY MALICK', hrUsername: 'COULIBALY123', userUsername: 'COULIBALY123', role: 'employe_standard', password: 'COULIBALY123@2026' },
  { fullName: 'DABIE VALENTINE EPSE DJINA', hrUsername: 'DABIE123', userUsername: 'DABIE123', role: 'employe_standard', password: 'DABIE123@2026' },
  { fullName: 'DJE BI IRIE JEAN CLAUDE', hrUsername: 'DJE123', userUsername: 'DJE123', role: 'employe_standard', password: 'DJE123@2026' },
  { fullName: 'DOUDOU ALEXANDRE', hrUsername: 'DOUDOU123', userUsername: 'DOUDOU123', role: 'employe_standard', password: 'DOUDOU123@2026' },
  { fullName: 'DOUMBIA BRAHIM', hrUsername: 'DOUMBIA123', userUsername: 'DOUMBIA123', role: 'employe_standard', password: 'DOUMBIA123@2026' },
  { fullName: 'GNEKPO AKOULA YANNICK ZEGBEHI', hrUsername: 'GNEKPO123', userUsername: 'GNEKPO123', role: 'employe_standard', password: 'GNEKPO123@2026' },
  { fullName: 'KENDREBEOGO EMILE', hrUsername: 'KENDREBEOGO123', userUsername: 'KENDREBEOGO123', role: 'employe_standard', password: 'KENDREBEOGO123@2026' },
  { fullName: 'KOAUSSI YAO MAURICE', hrUsername: 'KOAUSSI123', userUsername: 'KOAUSSI123', role: 'employe_standard', password: 'KOAUSSI123@2026' },
  { fullName: 'KOFFI KOUAKOU KRA', hrUsername: 'KOFFI123', userUsername: 'KOFFI123', role: 'employe_standard', password: 'KOFFI123@2026' },
  { fullName: 'KOKAN', hrUsername: 'Kokan_SK', userUsername: 'Kokan_SK', role: 'gestionnaire_stock_songon', password: 'Stock_SK123' },
  { fullName: 'KONE ABOUBACAR', hrUsername: 'KONE123', userUsername: 'KONE123', role: 'employe_standard', password: 'KONE123@2026' },
  { fullName: 'KONE YAYA', hrUsername: 'KONE124', userUsername: 'KONE124', role: 'employe_standard', password: 'KONE124@2026' },
  { fullName: 'KOUAME ROLAND', hrUsername: 'KOUAME123', userUsername: 'KOUAME123', role: 'employe_standard', password: 'KOUAME123@2026' },
  { fullName: 'KOUAME YAFFI BROU FELIX', hrUsername: 'KOUAME124', userUsername: 'KOUAME124', role: 'employe_standard', password: 'KOUAME124@2026' },
  { fullName: 'KOUASSI KOUAME ANDERSON FRANCK O.', hrUsername: 'KOUASSI123', userUsername: 'KOUASSI123', role: 'employe_standard', password: 'KOUASSI123@2026' },
  { fullName: 'KPANKOUN ACONASSOU Bienvenu Bernabé', hrUsername: 'KPANKOUN123', userUsername: 'KPANKOUN123', role: 'employe_standard', password: 'KPANKOUN123@2026' },
  { fullName: 'MME GUIEGUIE EPSE MAKOUBI NADIA', hrUsername: 'MME123', userUsername: 'MME123', role: 'employe_standard', password: 'MME123@2026' },
  { fullName: 'N’GUESSAN KOUASSI CELESTIN', hrUsername: 'NGUESSAN123', userUsername: 'NGUESSAN123', role: 'employe_standard', password: 'NGUESSAN123@2026' },
  { fullName: 'NDJIE ABOMO ELI', hrUsername: 'NDJIE123', userUsername: 'NDJIE123', role: 'employe_standard', password: 'NDJIE123@2026' },
  { fullName: 'NOELLE AGNELLA', hrUsername: 'controle_achat_global', userUsername: 'controle_achat_global', role: 'controle_achat', password: 'achatglobal123' },
  { fullName: 'OGOU MARIE DANIELLE', hrUsername: 'dirigeant', userUsername: 'dirigeant', role: 'dirigeant', password: 'dirigeant123' },
  { fullName: 'OULAI OSWALD', hrUsername: 'OULAI123', userUsername: 'OULAI123', role: 'employe_standard', password: 'OULAI123@2026' },
  { fullName: "RUBEN N'DAH", hrUsername: 'directeur_rh', userUsername: 'directeur_rh', role: 'directeur_rh', password: 'rh123' },
  { fullName: 'SAI JEAN CLAUDE HILAIRE', hrUsername: 'SAI123', userUsername: 'SAI123', role: 'employe_standard', password: 'SAI123@2026' },
  { fullName: 'SORO KARNA PRI CI', hrUsername: 'SORO123', userUsername: 'SORO123', role: 'employe_standard', password: 'SORO123@2026' },
  { fullName: 'SORO ZANA FRANCOIS', hrUsername: 'SORO124', userUsername: 'SORO124', role: 'employe_standard', password: 'SORO124@2026' },
  { fullName: 'YAO FOFFIE', hrUsername: 'Conducteur_de_travaux', userUsername: 'Conducteur_de_travaux', role: 'chef_chantier_site', password: 'Yaofoffie_SK' },
  { fullName: 'YEO YARDJOUMA', hrUsername: 'YEO123', userUsername: 'YEO123', role: 'employe_standard', password: 'YEO123@2026' },
  { fullName: 'ZRAN GUE FABRICE', hrUsername: 'ZRAN123', userUsername: 'ZRAN123', role: 'employe_standard', password: 'ZRAN123@2026' },
];
const IDENTITY_KEEP_ALWAYS = new Set(['admin', 'dirigeant']);
const IDENTITY_PASSWORD_HINTS = new Map(
  IDENTITY_ROSTER
    .filter(row => String(row.userUsername || '').trim() && String(row.password || '').trim())
    .map(row => [String(row.userUsername || '').trim().toLowerCase(), String(row.password || '').trim()])
);

function resolveKnownUserPasswordHint(username, role, fallback = '-') {
  const key = String(username || '').trim().toLowerCase();
  if (key && IDENTITY_PASSWORD_HINTS.has(key)) {
    return IDENTITY_PASSWORD_HINTS.get(key);
  }
  if (String(role || '').trim().toLowerCase() === 'employe_standard' && key) {
    return `${String(username || '').trim()}@2026`;
  }
  return String(fallback || '-').trim() || '-';
}

async function reconcileIdentityDirectory() {
  const now = new Date().toISOString();
  const desiredUsers = new Map();
  for (const row of IDENTITY_ROSTER) {
    const username = String(row.userUsername || '').trim();
    const role = String(row.role || '').trim();
    const password = String(row.password || '').trim();
    if (!username || !role || !password) continue;
    desiredUsers.set(username.toLowerCase(), {
      username,
      role,
      password,
    });
  }

  let createdUsers = 0;
  let updatedUsers = 0;
  for (const { username, role, password } of desiredUsers.values()) {
    const existing = await get('SELECT id, role FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) LIMIT 1', [username]);
    const hashedPassword = await bcrypt.hash(password, 10);
    if (!existing) {
      const nextUserId = await getNextTableId('users');
      await run(
        'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
        [nextUserId, username, hashedPassword, role, now]
      );
      createdUsers += 1;
    } else {
      await run(
        'UPDATE users SET username = ?, password = ?, role = ? WHERE id = ?',
        [username, hashedPassword, role, Number(existing.id)]
      );
      updatedUsers += 1;
    }
  }

  const keepUsernames = new Set(Array.from(IDENTITY_KEEP_ALWAYS));
  for (const row of desiredUsers.values()) {
    keepUsernames.add(String(row.username || '').trim().toLowerCase());
  }

  const allUsersRows = await all('SELECT id, username FROM users');
  let deletedUsers = 0;
  for (const userRow of allUsersRows) {
    const username = String(userRow?.username || '').trim();
    const key = username.toLowerCase();
    if (!username || keepUsernames.has(key)) continue;
    await run('DELETE FROM project_assignments WHERE userId = ?', [Number(userRow.id)]).catch(() => {});
    await run('DELETE FROM user_access_profiles WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))', [username]).catch(() => {});
    await run('DELETE FROM user_access_profile_audit WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))', [username]).catch(() => {});
    await run('DELETE FROM users WHERE id = ?', [Number(userRow.id)]);
    deletedUsers += 1;
  }

  let createdHr = 0;
  let updatedHr = 0;
  for (const row of IDENTITY_ROSTER) {
    const fullName = String(row.fullName || '').trim();
    if (!fullName) continue;
    const hrUsername = String(row.hrUsername || '').trim();
    const role = String(row.role || '').trim();
    const jobTitle = getRoleDefaultJobTitle(role || 'employe_standard');
    const createdBy = String(hrUsername || row.userUsername || 'admin').trim() || 'admin';

    const existing = await get('SELECT id FROM hr_employees WHERE LOWER(TRIM(fullName)) = LOWER(TRIM(?)) LIMIT 1', [fullName]);
    if (!existing) {
      const nextHrId = await getNextTableId('hr_employees');
      await run(
        'INSERT INTO hr_employees (id, fullName, jobTitle, username, createdBy, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?)',
        [nextHrId, fullName, jobTitle, hrUsername, createdBy, now, now]
      );
      createdHr += 1;
    } else {
      await run(
        'UPDATE hr_employees SET username = ?, jobTitle = ?, createdBy = ?, updatedAt = ? WHERE id = ?',
        [hrUsername, jobTitle, createdBy, now, Number(existing.id)]
      );
      updatedHr += 1;
    }
  }

  const keepHrNames = new Set(IDENTITY_ROSTER.map(row => normalizeUserKey(row.fullName || '')).filter(Boolean));
  const keepHrUsernames = new Set(IDENTITY_ROSTER.map(row => String(row.hrUsername || '').trim().toLowerCase()).filter(Boolean));
  const hrRows = await all('SELECT id, fullName, username FROM hr_employees');
  let deletedHr = 0;
  for (const hrRow of hrRows) {
    const hrNameKey = normalizeUserKey(hrRow?.fullName || '');
    const hrUsernameKey = String(hrRow?.username || '').trim().toLowerCase();
    if (keepHrNames.has(hrNameKey) || (hrUsernameKey && keepHrUsernames.has(hrUsernameKey))) {
      continue;
    }
    await run('DELETE FROM hr_employees WHERE id = ?', [Number(hrRow.id)]);
    deletedHr += 1;
  }

  return { createdUsers, updatedUsers, deletedUsers, createdHr, updatedHr, deletedHr };
}
const API_RATE_WINDOW_MS = Number(process.env.API_RATE_WINDOW_MS || 60_000);
const API_RATE_MAX = Number(process.env.API_RATE_MAX || 600);
const AUTH_RATE_WINDOW_MS = Number(process.env.AUTH_RATE_WINDOW_MS || 15 * 60_000);
const AUTH_RATE_MAX = Number(process.env.AUTH_RATE_MAX || 25);
const AUTO_BACKUP_ENABLED = String(process.env.AUTO_BACKUP_ENABLED || '1').trim() !== '0';
const AUTO_BACKUP_ON_MUTATION = String(process.env.AUTO_BACKUP_ON_MUTATION || '1').trim() !== '0';
const AUTO_BACKUP_DEBOUNCE_MS = Number(process.env.AUTO_BACKUP_DEBOUNCE_MS || 10_000);

let isReady = false;
let isShuttingDown = false;
let server = null;
let localhostFallbackServer = null;
let autoBackupTimer = null;
let autoBackupInProgress = false;
let autoBackupPending = false;
const profileStreamClients = new Set();

fs.mkdirSync(ARCHIVE_ROOT, { recursive: true });
app.use('/archives', express.static(ARCHIVE_ROOT));

if (JWT_SECRET === 'erp-secret-2026') {
  console.warn('Avertissement securite: JWT_SECRET par defaut detecte. Configure une valeur forte en production.');
}

if (NODE_ENV === 'production' && !ALLOW_INSECURE_JWT_SECRET) {
  if (JWT_SECRET === 'erp-secret-2026' || JWT_SECRET.length < 32) {
    throw new Error('Configuration invalide: JWT_SECRET doit etre robuste (>= 32 caracteres) en production.');
  }
}

function shouldTriggerMutationBackup(method, originalUrl, statusCode) {
  if (!AUTO_BACKUP_ENABLED || !AUTO_BACKUP_ON_MUTATION) return false;
  if (statusCode >= 400) return false;
  const verb = String(method || '').toUpperCase();
  if (!['POST', 'PATCH', 'PUT', 'DELETE'].includes(verb)) return false;

  const pathName = String(originalUrl || '').split('?')[0];
  if (!pathName.startsWith('/api/')) return false;
  if (pathName.startsWith('/api/auth/')) return false;
  return true;
}

function runAutomaticBackup(reason = 'api-mutation') {
  if (!AUTO_BACKUP_ENABLED) return;
  if (autoBackupInProgress) {
    autoBackupPending = true;
    return;
  }

  const backupScript = path.join(__dirname, 'scripts', 'backup-local.js');
  if (!fs.existsSync(backupScript)) {
    console.warn('Sauvegarde auto ignoree: script introuvable', backupScript);
    return;
  }

  autoBackupInProgress = true;
  const child = spawn(process.execPath, [backupScript], {
    cwd: __dirname,
    env: {
      ...process.env,
      BACKUP_DIR: process.env.BACKUP_DIR || path.join(APP_DATA_DIR, 'backups'),
    },
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stderrOutput = '';
  child.stderr.on('data', chunk => {
    stderrOutput += String(chunk || '');
    if (stderrOutput.length > 4_000) {
      stderrOutput = stderrOutput.slice(-4_000);
    }
  });

  child.on('close', code => {
    autoBackupInProgress = false;
    if (code === 0) {
      console.log(JSON.stringify({
        ts: new Date().toISOString(),
        event: 'auto-backup',
        status: 'ok',
        reason,
      }));
    } else {
      console.error(JSON.stringify({
        ts: new Date().toISOString(),
        event: 'auto-backup',
        status: 'failed',
        reason,
        exitCode: code,
        stderr: stderrOutput.trim(),
      }));
    }

    if (autoBackupPending) {
      autoBackupPending = false;
      runAutomaticBackup('queued-mutation');
    }
  });
}

function scheduleAutomaticBackup(reason = 'api-mutation') {
  if (!AUTO_BACKUP_ENABLED || !AUTO_BACKUP_ON_MUTATION) return;
  autoBackupPending = true;

  if (autoBackupTimer) {
    clearTimeout(autoBackupTimer);
  }

  autoBackupTimer = setTimeout(() => {
    autoBackupTimer = null;
    if (!autoBackupPending) return;
    autoBackupPending = false;
    runAutomaticBackup(reason);
  }, Number.isFinite(AUTO_BACKUP_DEBOUNCE_MS) && AUTO_BACKUP_DEBOUNCE_MS > 0 ? AUTO_BACKUP_DEBOUNCE_MS : 10_000);
}

app.use((req, res, next) => {
  const requestId = req.headers['x-request-id'] || crypto.randomUUID();
  const startedAt = Date.now();
  req.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);

  res.on('finish', () => {
    const logLine = {
      ts: new Date().toISOString(),
      requestId,
      method: req.method,
      path: req.originalUrl,
      status: res.statusCode,
      durationMs: Date.now() - startedAt,
      user: req.user ? req.user.username : null,
      ip: req.ip,
    };
    console.log(JSON.stringify(logLine));

    if (shouldTriggerMutationBackup(req.method, req.originalUrl, res.statusCode)) {
      scheduleAutomaticBackup(`${req.method} ${req.originalUrl}`);
    }
  });

  next();
});

const apiRateLimiter = rateLimit({
  windowMs: API_RATE_WINDOW_MS,
  max: API_RATE_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Trop de requetes, reessaie dans quelques instants.' },
});

const authRateLimiter = rateLimit({
  windowMs: AUTH_RATE_WINDOW_MS,
  max: AUTH_RATE_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Trop de tentatives de connexion. Reessaie plus tard.' },
});



app.use('/api', apiRateLimiter);

const dbClient = createDbClient({
  dbFile: DB_FILE,
  databaseUrl: process.env.DATABASE_URL,
  driver: process.env.DATABASE_DRIVER,
});
const { run, get, all } = dbClient;
const DB_DRIVER = dbClient.driver;

if (DB_DRIVER === 'postgres') {
  console.log('Mode base de donnees: PostgreSQL');
} else {
  console.log(`Mode base de donnees: SQLite (${DB_FILE})`);
}

async function getTableColumns(tableName) {
  if (DB_DRIVER === 'postgres') {
    const rows = await all(
      'SELECT column_name AS name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?',
      ['public', tableName]
    );
    return new Set(rows.map(row => row.name));
  }

  const rows = await all(`PRAGMA table_info(${tableName})`);
  return new Set(rows.map(row => row.name));
}

async function getNextTableId(tableName) {
  const row = await get(`SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM ${tableName}`);
  return Number(row?.nextId || row?.nextid || 1);
}

function normalizeNumericIdList(value) {
  const asArray = Array.isArray(value)
    ? value
    : String(value || '')
      .split(',')
      .map(item => item.trim())
      .filter(Boolean);

  const seen = new Set();
  const normalized = [];
  for (const item of asArray) {
    const numeric = Number(item);
    if (!Number.isInteger(numeric) || numeric <= 0) continue;
    if (seen.has(numeric)) continue;
    seen.add(numeric);
    normalized.push(numeric);
  }
  return normalized;
}

function hasTableColumn(columns, name) {
  if (!columns || typeof columns.has !== 'function') return false;
  if (columns.has(name)) return true;
  const lower = String(name || '').toLowerCase();
  for (const entry of columns) {
    if (String(entry || '').toLowerCase() === lower) return true;
  }
  return false;
}

async function ensureGuideDocumentAudienceColumns() {
  try { await run("ALTER TABLE guide_documents ADD COLUMN audienceScope TEXT NOT NULL DEFAULT 'all'"); } catch (_e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN recipientEmployeeIds TEXT NOT NULL DEFAULT ''"); } catch (_e) {}
  return getTableColumns('guide_documents');
}

app.get('/healthz', (_req, res) => {
  if (isShuttingDown) {
    return res.status(503).json({ status: 'shutting-down' });
  }
  res.json({ status: 'ok' });
});

app.get('/api/push/public-key', (_req, res) => {
  const publicKey = String(
    process.env.PUSH_VAPID_PUBLIC_KEY
      || process.env.VAPID_PUBLIC_KEY
      || process.env.PUSH_PUBLIC_KEY
      || ''
  ).trim();

  res.json({
    publicKey,
    enabled: Boolean(publicKey),
  });
});

app.post('/api/push/subscribe', (_req, res) => {
  // Localhost fallback: accept subscription payload even if push is not configured.
  return res.status(204).end();
});

app.get('/readyz', async (_req, res) => {
  if (!isReady || isShuttingDown) {
    return res.status(503).json({ status: 'not-ready' });
  }

  try {
    await get('SELECT 1 as ok');
    res.json({ status: 'ready' });
  } catch (error) {
    res.status(503).json({ status: 'not-ready', error: 'database-unreachable' });
  }
});

app.get('/download/desktop', (req, res) => {
  if (!fs.existsSync(DESKTOP_ZIP_PATH)) {
    return res.status(404).json({ error: 'Archive desktop introuvable' });
  }

  const downloadName = path.basename(DESKTOP_ZIP_PATH);
  res.setHeader('Cache-Control', 'no-store');
  return res.download(DESKTOP_ZIP_PATH, downloadName);
});

app.get('/download/desktop/windows', (req, res) => {
  if (!fs.existsSync(DESKTOP_ZIP_PATH)) {
    return res.status(404).json({ error: 'Archive desktop introuvable' });
  }

  const downloadName = path.basename(DESKTOP_ZIP_PATH);
  res.setHeader('Cache-Control', 'no-store');
  return res.download(DESKTOP_ZIP_PATH, downloadName);
});

app.get('/download/desktop/app', (req, res) => {
  const exists = fs.existsSync(DESKTOP_ZIP_PATH);
  const host = req.get('host');
  const protocol = req.headers['x-forwarded-proto'] || req.protocol || 'https';
  const downloadUrl = `${protocol}://${host}/download/desktop/windows`;

  if (!exists) {
    return res.status(404).send(`<!doctype html><html lang="fr"><head><meta charset="utf-8"><title>Téléchargement RyanERP</title></head><body style="font-family:Arial,sans-serif;padding:24px;background:#f8fafc;color:#0f172a;"><h1>RyanERP Desktop</h1><p>Archive desktop introuvable sur ce serveur.</p></body></html>`);
  }

  return res.send(`<!doctype html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Téléchargement RyanERP Desktop</title></head><body style="font-family:Arial,sans-serif;padding:24px;background:#f1f5f9;color:#0f172a;"><div style="max-width:760px;margin:0 auto;background:#fff;border:1px solid #dbeafe;border-radius:14px;padding:22px;box-shadow:0 8px 24px rgba(15,23,42,.08)"><h1 style="margin:0 0 8px">RyanERP Desktop (Windows)</h1><p style="margin:0 0 16px;color:#475569">Télécharge l'application puis décompresse le ZIP et lance l'exécutable RyanERP.</p><a href="${downloadUrl}" style="display:inline-block;background:#2563eb;color:#fff;text-decoration:none;padding:12px 16px;border-radius:10px;font-weight:700">Télécharger RyanERP pour ordinateur</a><p style="margin:14px 0 0;color:#64748b;font-size:13px">Lien direct: <a href="${downloadUrl}">${downloadUrl}</a></p></div></body></html>`);
});

async function insertExpenseRecord({
  materialId = null,
  projetId = null,
  purchaseOrderId = null,
  description = '',
  quantite,
  prixUnitaire,
  fournisseur = '',
  categorie,
  statut = 'EN_ATTENTE',
  createdBy = 'system',
  dateExpense = new Date().toISOString(),
}) {
  const expenseDescription = String(description || '').trim();
  const expenseCategory = String(categorie || '').trim();
  const expenseQuantity = Number(quantite);
  const expenseUnitPrice = Number(prixUnitaire);
  const expenseProjectId = projetId || null;

  if (!expenseDescription || !expenseCategory || Number.isNaN(expenseQuantity) || expenseQuantity <= 0 || Number.isNaN(expenseUnitPrice) || expenseUnitPrice < 0) {
    throw new Error('Données de dépense invalides');
  }

  const montantTotal = expenseQuantity * expenseUnitPrice;
  const availableColumns = await getTableColumns('expenses');
  const insertColumns = [];
  const insertValues = [];

  let nextExpenseId = null;
  if (availableColumns.has('id')) {
    const nextExpenseIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM expenses');
    nextExpenseId = Number(nextExpenseIdRow?.nextId || nextExpenseIdRow?.nextid || 1);
  }

  function pushColumnIfExists(columnName, value) {
    if (availableColumns.has(columnName)) {
      insertColumns.push(columnName);
      insertValues.push(value);
    }
  }

  pushColumnIfExists('id', nextExpenseId);
  pushColumnIfExists('materialId', materialId || null);
  pushColumnIfExists('projetId', expenseProjectId);
  pushColumnIfExists('projectId', expenseProjectId);
  pushColumnIfExists('purchaseOrderId', purchaseOrderId || null);
  pushColumnIfExists('description', expenseDescription);
  pushColumnIfExists('item', expenseDescription);
  pushColumnIfExists('quantite', expenseQuantity);
  pushColumnIfExists('quantity', expenseQuantity);
  pushColumnIfExists('prixUnitaire', expenseUnitPrice);
  pushColumnIfExists('unitPrice', expenseUnitPrice);
  pushColumnIfExists('montantTotal', montantTotal);
  pushColumnIfExists('totalPrice', montantTotal);
  pushColumnIfExists('dateExpense', dateExpense);
  pushColumnIfExists('date', dateExpense);
  pushColumnIfExists('fournisseur', fournisseur);
  pushColumnIfExists('supplier', fournisseur);
  pushColumnIfExists('categorie', expenseCategory);
  pushColumnIfExists('category', expenseCategory);
  pushColumnIfExists('statut', statut);
  pushColumnIfExists('status', statut);
  pushColumnIfExists('createdBy', createdBy);

  const placeholders = insertColumns.map(() => '?').join(', ');
  const sql = `INSERT INTO expenses (${insertColumns.join(', ')}) VALUES (${placeholders})`;
  await run(sql, insertValues);

  if (nextExpenseId !== null) {
    return get('SELECT * FROM expenses WHERE id = ?', [nextExpenseId]);
  }
  return null;
}

function resolveSafariLogoPath() {
  const candidates = [
    path.join(__dirname, 'public', 'logo_safari_constructions.png'),
    path.join(__dirname, 'public', 'logo_safari_constructions.jpg'),
    path.join(__dirname, 'public', 'logo_safari_constructions.jpeg'),
    path.join(__dirname, 'public', 'logo_safari_constructions.webp'),
    path.join(__dirname, 'logo_safari_constructions.png'),
    path.join(__dirname, 'logo_safari_constructions.jpg'),
    path.join(__dirname, 'logo_safari_constructions.jpeg'),
    path.join(__dirname, 'logo_safari_constructions.webp'),
  ];

  for (const filePath of candidates) {
    if (fs.existsSync(filePath)) {
      return filePath;
    }
  }

  const publicDir = path.join(__dirname, 'public');
  if (fs.existsSync(publicDir)) {
    const match = fs.readdirSync(publicDir).find(name =>
      /safari.*construct|construct.*safari/i.test(name) && /\.(png|jpg|jpeg|webp)$/i.test(name)
    );
    if (match) {
      return path.join(publicDir, match);
    }
  }

  return null;
}

function resolveSignatureFontPath() {
  const candidates = [
    path.join(__dirname, 'public', 'fonts', 'GreatVibes-Regular.ttf'),
    path.join(__dirname, 'public', 'fonts', 'PinyonScript-Regular.ttf'),
    path.join(__dirname, 'fonts', 'GreatVibes-Regular.ttf'),
    path.join(__dirname, 'fonts', 'PinyonScript-Regular.ttf'),
    'C:\\Windows\\Fonts\\PinyonScript-Regular.ttf',
    'C:\\Windows\\Fonts\\BRUSHSCI.TTF',
    'C:\\Windows\\Fonts\\SEGOESCR.TTF',
  ];

  for (const fontPath of candidates) {
    try {
      if (fs.existsSync(fontPath)) {
        return fontPath;
      }
    } catch (_err) {
      // Ignore inaccessible paths and continue with fallbacks.
    }
  }

  return null;
}

async function buildSignedHrDocumentPdfBuffer({ sourceFilePath, signatureName, signatureRole = '', signedAt = '', signedBy = '' }) {
  const pdfBytes = await fs.promises.readFile(sourceFilePath);
  const pdfDoc = await PdfLibDocument.load(pdfBytes);
  const pages = pdfDoc.getPages();
  if (!pages.length) {
    throw new Error('Document PDF vide');
  }

  const lastPage = pages[pages.length - 1];
  const { width, height } = lastPage.getSize();
  const regularFont = await pdfDoc.embedFont(PdfLibStandardFonts.Helvetica);
  let signatureFont = await pdfDoc.embedFont(PdfLibStandardFonts.HelveticaOblique);

  const signatureFontPath = resolveSignatureFontPath();
  if (signatureFontPath) {
    try {
      signatureFont = await pdfDoc.embedFont(await fs.promises.readFile(signatureFontPath));
    } catch (_err) {
      signatureFont = await pdfDoc.embedFont(PdfLibStandardFonts.HelveticaOblique);
    }
  }

  const footerWidth = Math.min(220, Math.max(160, width - 80));
  const footerX = Math.max(40, width - footerWidth - 40);
  const footerY = Math.max(36, 42);
  const signatureLabel = 'Signature autorisee';
  const signerName = String(signatureName || '').trim() || 'Signature';
  const signerRole = String(signatureRole || '').trim();
  const signerDate = String(signedAt || '').trim() ? new Date(signedAt).toLocaleString('fr-FR') : new Date().toLocaleString('fr-FR');
  const signedByValue = String(signedBy || '').trim();

  lastPage.drawRectangle({
    x: footerX - 10,
    y: footerY - 4,
    width: footerWidth,
    height: 84,
    borderWidth: 1,
    borderColor: pdfRgb(0.81, 0.85, 0.92),
    color: pdfRgb(1, 1, 1),
    opacity: 0.94,
  });

  lastPage.drawText(signatureLabel, {
    x: footerX,
    y: footerY + 56,
    size: 9,
    font: regularFont,
    color: pdfRgb(0.05, 0.11, 0.22),
  });

  lastPage.drawText(signerName, {
    x: footerX,
    y: footerY + 25,
    size: 24,
    font: signatureFont,
    color: pdfRgb(0.07, 0.09, 0.15),
  });

  if (signerRole) {
    lastPage.drawText(signerRole, {
      x: footerX,
      y: footerY + 10,
      size: 9,
      font: regularFont,
      color: pdfRgb(0.2, 0.29, 0.4),
    });
  }

  lastPage.drawText(`Date: ${signerDate}`, {
    x: footerX,
    y: footerY - 1,
    size: 8,
    font: regularFont,
    color: pdfRgb(0.34, 0.41, 0.51),
  });

  if (signedByValue) {
    lastPage.drawText(`Par: ${signedByValue}`, {
      x: footerX + 112,
      y: footerY - 1,
      size: 8,
      font: regularFont,
      color: pdfRgb(0.34, 0.41, 0.51),
    });
  }

  return Buffer.from(await pdfDoc.save());
}

async function archiveSignedHrDocument({ documentRow, signatureRequest, signatureName, signatureRole = '', signedAt = '' }) {
  const sourcePath = path.join(ARCHIVE_ROOT, String(documentRow.relativePath || ''));
  if (!String(documentRow.relativePath || '').trim() || !fs.existsSync(sourcePath)) {
    throw new Error('Fichier source introuvable');
  }

  const isPdf = /pdf$/i.test(String(documentRow.mimeType || '')) || /\.pdf$/i.test(String(documentRow.fileName || ''));
  if (!isPdf) {
    throw new Error('Seuls les documents PDF peuvent être signes');
  }

  const signedFileName = `signe-${String(signatureRequest.id || documentRow.id || Date.now())}-${sanitizeFileName(documentRow.fileName || 'document.pdf')}`;
  const signedRelativePath = path.join('construction', 'hr-employees', `employee-${Number(documentRow.employeeId || 0) || 'shared'}`, 'signed', signedFileName);
  const signedAbsolutePath = path.join(ARCHIVE_ROOT, signedRelativePath);
  fs.mkdirSync(path.dirname(signedAbsolutePath), { recursive: true });

  const buffer = await buildSignedHrDocumentPdfBuffer({
    sourceFilePath: sourcePath,
    signatureName,
    signatureRole,
    signedAt,
    signedBy: signatureRequest.signedBy || signatureRequest.employeeUsername || '',
  });
  await fs.promises.writeFile(signedAbsolutePath, buffer);

  return {
    fileName: signedFileName,
    relativePath: signedRelativePath,
    fileUrl: `/archives/${signedRelativePath.replace(/\\/g, '/')}`,
  };
}

async function enrichPurchaseOrders(orders) {
  if (!orders.length) {
    return [];
  }

  const ids = orders.map(order => Number(order.id)).filter(id => Number.isInteger(id));
  if (!ids.length) {
    return orders;
  }

  const placeholders = ids.map(() => '?').join(',');
  const itemRows = await all(`
    SELECT poi.*, mr.itemName as requestItemName, mr.etapeApprovisionnement, p.nomProjet, p.numeroMaison, p.typeMaison, p.prefecture
    FROM purchase_order_items poi
    LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
    LEFT JOIN projects p ON p.id = mr.projetId
    WHERE poi.purchaseOrderId IN (${placeholders})
    ORDER BY poi.id ASC
  `, ids);

  const itemsByOrderId = {};
  itemRows.forEach(row => {
    const orderId = Number(row.purchaseOrderId);
    if (!itemsByOrderId[orderId]) {
      itemsByOrderId[orderId] = [];
    }
    itemsByOrderId[orderId].push({
      id: row.id,
      materialRequestId: row.materialRequestId,
      article: row.article,
      details: row.details,
      quantite: Number(row.quantite || 0),
      prixUnitaire: Number(row.prixUnitaire || 0),
      totalLigne: Number(row.totalLigne || 0),
      projetNom: row.nomProjet || null,
      numeroMaison: row.numeroMaison || null,
      typeMaison: row.typeMaison || null,
      prefecture: row.prefecture || null,
      etapeApprovisionnement: row.etapeApprovisionnement || null,
      requestItemName: row.requestItemName || null,
    });
  });

  const fallbackRequestIds = Array.from(new Set(
    orders
      .map(order => Number(order.materialRequestId || 0))
      .filter(id => Number.isInteger(id) && id > 0)
  ));
  const fallbackStageByRequestId = new Map();
  if (fallbackRequestIds.length) {
    const fallbackPlaceholders = fallbackRequestIds.map(() => '?').join(',');
    const fallbackRows = await all(
      `SELECT id, etapeApprovisionnement FROM material_requests WHERE id IN (${fallbackPlaceholders})`,
      fallbackRequestIds
    );
    fallbackRows.forEach(row => {
      fallbackStageByRequestId.set(Number(row.id), String(row.etapeApprovisionnement || '').trim());
    });
  }

  return orders.map(order => {
    const items = itemsByOrderId[Number(order.id)] || [];
    const firstItem = items[0] || null;
    const totalFromItems = items.reduce((sum, item) => sum + Number(item.totalLigne || 0), 0);
    const projectNames = Array.from(new Set(items.map(item => item.projetNom).filter(Boolean)));
    const houseNumbers = Array.from(new Set(items.map(item => String(item.numeroMaison || '').trim()).filter(Boolean)));
    const stages = Array.from(new Set(items.map(item => String(item.etapeApprovisionnement || '').trim()).filter(Boolean)));
    const manualSiteRaw = String(order.nomSiteManuel || '').trim();
    const zoneFromManualSite = manualSiteRaw.replace(/^zone\s*/i, '').trim();
    const zoneFromItems = items.reduce((acc, item) => acc || (String(item.typeMaison || '').toUpperCase() === 'ZONE_STOCK' ? String(item.prefecture || '').trim() : ''), '');
    const isZoneOrder = items.some(item => String(item.typeMaison || '').toUpperCase() === 'ZONE_STOCK') || /^zone\s+/i.test(manualSiteRaw);

    const resolvedNomProjet = (isZoneOrder && String(order.nomProjetManuel || '').trim())
      ? String(order.nomProjetManuel || '').trim()
      : (projectNames.length
      ? projectNames.join(', ')
      : (order.nomProjetManuel || order.nomProjet || '-'));
    const resolvedSite = houseNumbers.length
      ? houseNumbers.join(', ')
      : (order.nomSiteManuel || '-');
    return {
      ...order,
      items,
      itemName: firstItem ? firstItem.article : (order.itemName || 'Article'),
      quantiteCommandee: firstItem ? Number(firstItem.quantite || 0) : Number(order.quantiteCommandee || 0),
      prixUnitaire: firstItem ? Number(firstItem.prixUnitaire || 0) : Number(order.prixUnitaire || 0),
      montantTotal: Number(order.montantTotal || totalFromItems || 0),
      nomProjet: resolvedNomProjet,
      numeroMaison: resolvedSite,
      etapeApprovisionnement: String(
        order.etapeApprovisionnement
          || stages[0]
          || fallbackStageByRequestId.get(Number(order.materialRequestId || 0))
          || ''
      ).trim(),
      projetId: order.projetId || null,
      isZoneOrder,
      zoneName: zoneFromItems || zoneFromManualSite || null,
    };
  });
}

async function getPurchaseOrderById(id) {
  const row = await get('SELECT * FROM purchase_orders WHERE id = ?', [id]);
  if (!row) {
    return null;
  }
  const [enriched] = await enrichPurchaseOrders([row]);
  return enriched;
}

function isZoneStockProjectRow(project) {
  return String(project?.typeMaison || '').trim().toUpperCase() === 'ZONE_STOCK';
}

async function ensureZoneStockProject(projectName, zoneName) {
  const normalizedProjectName = String(projectName || '').trim();
  const normalizedZoneName = String(zoneName || '').trim();
  if (!normalizedProjectName || !normalizedZoneName) {
    return null;
  }

  const existing = await get(
    `SELECT *
     FROM projects
     WHERE TRIM(nomProjet) = ?
       AND TRIM(prefecture) = ?
       AND UPPER(COALESCE(typeMaison, '')) = 'ZONE_STOCK'
     ORDER BY id ASC
     LIMIT 1`,
    [normalizedProjectName, normalizedZoneName]
  );
  if (existing) {
    return existing;
  }

  const createdAt = new Date().toISOString();
  const nextIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM projects');
  const nextId = Number(nextIdRow?.nextId || nextIdRow?.nextid || 1);
  const result = await run(
    `INSERT INTO projects
      (id, nomProjet, prefecture, nomSite, typeMaison, numeroMaison, description, etapeConstruction, statutConstruction, createdAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextId,
      normalizedProjectName,
      normalizedZoneName,
      `Zone ${normalizedZoneName}`,
      'ZONE_STOCK',
      `ZONE-${normalizedZoneName.toUpperCase().replace(/[^A-Z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'STOCK'}`,
      'Stock tampon de zone',
      'STOCK',
      'ACTIF',
      createdAt,
    ]
  );

  return get('SELECT * FROM projects WHERE id = ?', [Number(result.lastID || 0)]);
}

function renderPurchaseOrderPdf(doc, order) {
  const total = Number(order.montantTotal || 0);
  const purchaseOrderTitle = buildPurchaseOrderDocumentTitle(order);
  const supplierLabel = String(order.fournisseur || '').trim() || 'Fournisseur non renseigne';
  const siteValueRaw = String(order.numeroMaison || order.nomSiteManuel || '').trim();
  const siteLabel = siteValueRaw
    ? (siteValueRaw.toLowerCase().includes('lot') ? siteValueRaw : `Lot Numero ${siteValueRaw}`)
    : 'Lot non renseigne';
  const orderDate = new Date(order.dateCommande || Date.now());
  const orderDateLabel = Number.isNaN(orderDate.getTime()) ? new Date().toLocaleDateString('fr-FR') : orderDate.toLocaleDateString('fr-FR');
  const signatureFontPath = resolveSignatureFontPath();
  const signatureFontName = signatureFontPath ? 'SignatureScript' : 'Helvetica-Oblique';

  if (signatureFontPath) {
    try {
      doc.registerFont(signatureFontName, signatureFontPath);
    } catch (_err) {
      // Keep built-in fallback font.
    }
  }

  // Header style aligned with authorization PDF, but with more spacing for the purchase order look.
  doc.font('Helvetica-Bold').fontSize(18).fillColor('#000000').text('BON DE COMMANDE', 40, 36, { width: 515, align: 'center' });

  doc.font('Helvetica-Bold').fontSize(10).fillColor('#000000').text('Numero :', 40, 68);
  doc.font('Helvetica').fontSize(10).text(`#${Number(order.id || 0) || '-'}`, 110, 68);
  doc.font('Helvetica-Bold').fontSize(10).text('Date :', 300, 68);
  doc.font('Helvetica').fontSize(10).text(orderDateLabel, 345, 68);

  doc.font('Helvetica-Bold').fontSize(10).text('Fournisseur :', 40, 86);
  doc.font('Helvetica').fontSize(10).text(supplierLabel, 110, 86, { width: 430 });

  doc.font('Helvetica-Bold').fontSize(10).text('Titre :', 40, 104);
  doc.font('Helvetica').fontSize(10).text(purchaseOrderTitle, 110, 104, { width: 430 });

  doc.font('Helvetica-Bold').fontSize(10).text('Lot :', 40, 122);
  doc.font('Helvetica').fontSize(10).text(siteLabel, 110, 122, { width: 430 });

  doc.moveTo(40, 142).lineTo(555, 142).lineWidth(1).strokeColor('#d1d5db').stroke();

  doc.font('Helvetica-Bold').fontSize(11).fillColor('#000000').text('Details de la commande', 40, 152);

  const tableTop = 172;
  const startX = 40;
  const widths = [250, 90, 90, 85];
  const headers = ['Article', 'Prix unitaire', 'Quantite', 'Total ligne'];
  const rowHeight = 22;

  function drawGridRow(y, values, isHeader = false) {
    let x = startX;
    values.forEach((value, index) => {
      if (isHeader) {
        doc.save();
        doc.rect(x, y, widths[index], rowHeight).fillAndStroke('#f3f4f6', '#9ca3af');
        doc.restore();
      } else {
        doc.rect(x, y, widths[index], rowHeight).lineWidth(0.5).strokeColor('#cbd5e1').stroke();
      }
      doc.font(isHeader ? 'Helvetica-Bold' : 'Helvetica').fontSize(9.5).fillColor('#111827').text(String(value), x + 6, y + 6, {
        width: widths[index] - 12,
        ellipsis: true,
      });
      x += widths[index];
    });
  }

  drawGridRow(tableTop, headers, true);

  const itemRows = (order.items && order.items.length ? order.items : [
    {
      article: order.itemName || 'Article',
      quantite: Number(order.quantiteCommandee || 0),
      prixUnitaire: Number(order.prixUnitaire || 0),
      totalLigne: Number(order.montantTotal || 0),
    },
  ]).map(item => [
    item.article || 'Article',
    `${Number(item.prixUnitaire || 0).toFixed(2)} EUR`,
    Number(item.quantite || 0).toFixed(2),
    `${Number(item.totalLigne || 0).toFixed(2)} EUR`,
  ]);

  const minRows = 8;
  const maxRows = 18;
  const displayRows = itemRows.slice(0, maxRows);
  while (displayRows.length < minRows) {
    displayRows.push(['', '', '', '']);
  }

  displayRows.forEach((row, idx) => drawGridRow(tableTop + rowHeight * (idx + 1), row, false));

  const totalBoxY = tableTop + rowHeight * (displayRows.length + 1) + 14;
  doc.save();
  doc.roundedRect(365, totalBoxY, 190, 30, 6).lineWidth(1).strokeColor('#94a3b8').stroke();
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#0f172a').text(`Total : ${total.toFixed(2)} EUR`, 378, totalBoxY + 9, {
    width: 165,
    align: 'right',
  });
  doc.restore();

  // Footer: signatures and decision status.
  const footerY = 730;
  doc.moveTo(40, footerY).lineTo(555, footerY).lineWidth(1).strokeColor('#94a3b8').stroke();
  doc.font('Helvetica').fontSize(9).fillColor('#475569').text(`Edite le : ${new Date().toLocaleString('fr-FR')}`, 40, footerY + 10);

  const purchaseSignatureName = String(order.signatureName || order.creePar || order.createdBy || '').trim() || 'Signataire';
  const purchaseSignatureRole = String(order.signatureRole || '').trim();

  doc.font('Helvetica-Bold').fontSize(10).fillColor('#0f172a').text('Validé par', 48, 744, { width: 190, align: 'left' });
  doc.font('Helvetica').fontSize(9).fillColor('#334155').text(purchaseSignatureRole || '____________________', 48, 760, { width: 190, align: 'left' });

  doc.font('Helvetica').fontSize(9).fillColor('#0f172a').text("Signature de l'achat", 335, 742, { width: 220, align: 'right' });
  doc.font(signatureFontName).fontSize(26).fillColor('#111827').text(purchaseSignatureName, 335, 754, { width: 220, align: 'right' });
}

async function generatePurchaseOrderPdfBuffer(order) {
  return await new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({ size: 'A4', margin: 40 });
    doc.on('data', chunk => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
    renderPurchaseOrderPdf(doc, order);
    doc.end();
  });
}

async function archivePurchaseOrderPdf(purchaseOrderId) {
  const order = await getPurchaseOrderById(purchaseOrderId);
  if (!order) {
    return null;
  }

  const sectionCode = 'achats';
  const supplierSlug = String(order.fournisseur || 'fournisseur')
    .replace(/[^a-z0-9]+/gi, '-')
    .replace(/^-+|-+$/g, '')
    .toLowerCase() || 'fournisseur';
  const fileName = `bon-commande-${purchaseOrderId}-${supplierSlug}.pdf`;
  const relativePath = path.join(sectionCode, fileName);
  const directory = path.join(ARCHIVE_ROOT, sectionCode);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(directory, { recursive: true });

  const buffer = await generatePurchaseOrderPdfBuffer(order);
  await fs.promises.writeFile(absolutePath, buffer);

  await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ? AND sectionCode = ?', ['purchase_order', purchaseOrderId, sectionCode]);
  await run(
    'INSERT INTO generated_documents (sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [sectionCode, 'Achats', 'purchase_order', purchaseOrderId, `Bon de commande #${purchaseOrderId}`, fileName, relativePath, new Date().toISOString(), new Date().toISOString()]
  );

  return {
    title: `Bon de commande #${purchaseOrderId}`,
    fileName,
    fileUrl: `/archives/${relativePath.replace(/\\/g, '/')}`,
  };
}

function getArchiveRelativePath(sectionCode, filename) {
  return path.join(sectionCode, filename);
}

function getArchiveSectionLabel(sectionCode) {
  const labels = {
    achats: 'Achats',
    construction: 'Construction',
    comptabilite: 'Comptabilite',
    hr_presence: 'Presence RH',
  };
  return labels[sectionCode] || sectionCode;
}

function sanitizeFileName(fileName) {
  return String(fileName || 'document')
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '-')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 120) || 'document';
}

function resolveExistingGuideAbsolutePath(row) {
  const relativePath = String(row?.relativePath || '').trim();
  if (relativePath) {
    const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
    try {
      if (fs.existsSync(absolutePath) && fs.statSync(absolutePath).isFile() && fs.statSync(absolutePath).size > 0) {
        return {
          absolutePath,
          relativePath,
        };
      }
    } catch (e) {
      console.warn(`Guide file validation failed for ${absolutePath}:`, e.message);
    }
  }

  const blobBuffer = row?.contentBlob ? Buffer.from(row.contentBlob) : null;
  if (blobBuffer && blobBuffer.length > 0) {
    const fallbackFileName = sanitizeFileName(String(row?.fileName || '').trim() || 'guide-document');
    const persistedRelative = relativePath || getArchiveRelativePath('guides', fallbackFileName);
    const persistedAbsolute = path.join(ARCHIVE_ROOT, persistedRelative);
    try {
      fs.mkdirSync(path.dirname(persistedAbsolute), { recursive: true });
      fs.writeFileSync(persistedAbsolute, blobBuffer);
      if (fs.existsSync(persistedAbsolute) && fs.statSync(persistedAbsolute).isFile() && fs.statSync(persistedAbsolute).size > 0) {
        return {
          absolutePath: persistedAbsolute,
          relativePath: persistedRelative,
        };
      }
    } catch (e) {
      console.warn(`Guide blob restore failed for ${persistedAbsolute}:`, e.message);
    }
  }

  const guideDir = path.join(ARCHIVE_ROOT, 'guides');
  const fallbackFileName = sanitizeFileName(String(row?.fileName || '').trim());
  const relativeBaseName = sanitizeFileName(path.basename(relativePath || ''));
  const candidateNames = Array.from(new Set([fallbackFileName, relativeBaseName].filter(Boolean)));

  for (const candidateName of candidateNames) {
    try {
      const fallbackRelative = getArchiveRelativePath('guides', candidateName);
      const fallbackAbsolute = path.join(ARCHIVE_ROOT, fallbackRelative);
      if (fs.existsSync(fallbackAbsolute) && fs.statSync(fallbackAbsolute).isFile() && fs.statSync(fallbackAbsolute).size > 0) {
        return {
          absolutePath: fallbackAbsolute,
          relativePath: fallbackRelative,
        };
      }
    } catch (e) {
      console.warn(`Guide file validation failed for ${candidateName}:`, e.message);
      continue;
    }
  }

  if (fs.existsSync(guideDir)) {
    try {
      const guideFileNames = fs.readdirSync(guideDir, { withFileTypes: true })
        .filter(entry => {
          try {
            return entry.isFile() && fs.statSync(path.join(guideDir, entry.name)).size > 0;
          } catch (e) {
            return false;
          }
        })
        .map(entry => entry.name);

      const matchedName = guideFileNames.find(name => candidateNames.some(candidateName => name === candidateName || name.endsWith(`-${candidateName}`)));
      if (matchedName) {
        return {
          absolutePath: path.join(guideDir, matchedName),
          relativePath: getArchiveRelativePath('guides', matchedName),
        };
      }
    } catch (e) {
      console.warn(`Guide directory validation failed for ${guideDir}:`, e.message);
    }
  }

  return null;
}

function buildProjectDocumentTitle(projectName, numberValue) {
  const safeProjectName = String(projectName || '').trim() || 'Projet';
  const safeNumber = Number(numberValue) || 0;
  return `${safeProjectName}-${safeNumber}`;
}

function normalizeStageLabel(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/^etape\s*:\s*/i, '')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/s$/, '');
}

function parseCatalogStageLabels(value) {
  const raw = String(value || '').trim().replace(/^etape\s*:\s*/i, '');
  if (!raw) return [];
  return raw
    .split(/[;,|/]+/)
    .flatMap(label => String(label || '').split(/::|>|\|/))
    .map(label => String(label || '').trim())
    .filter(Boolean);
}

function isCatalogStageMatching(catalogNotes, requestedStage) {
  const requestedParts = parseCatalogStageLabels(requestedStage);
  const requestedMainKey = normalizeStageLabel(requestedParts[0] || requestedStage);
  const requestedSubKey = normalizeStageLabel(requestedParts.slice(1).join('::') || '');
  if (!requestedMainKey) return true;

  const catalogParts = parseCatalogStageLabels(catalogNotes);
  if (!catalogParts.length) return true;

  const catalogMainKey = normalizeStageLabel(catalogParts[0] || catalogNotes);
  const catalogSubKey = normalizeStageLabel(catalogParts.slice(1).join('::') || '');
  if (!catalogMainKey) return false;
  if (requestedSubKey) {
    return catalogMainKey === requestedMainKey && catalogSubKey === requestedSubKey;
  }
  return catalogMainKey === requestedMainKey;
}

function resolvePurchaseOrderStageDisplay(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const normalized = normalizeStageLabel(raw);
  if (!normalized) return '';
  if (normalized.includes('bon commande') || normalized.includes('bon_commande')) {
    return '';
  }
  return raw;
}

function extractSiteNumberLabel(value) {
  const raw = String(value || '').trim();
  if (!raw || /^(-|non\s*renseigne)$/i.test(raw)) {
    return '-';
  }
  const stripped = raw.replace(/^lot\s*(numero|n°|no)?\s*/i, '').trim();
  const numberMatch = stripped.match(/\d+/);
  if (numberMatch && numberMatch[0]) {
    return numberMatch[0];
  }
  return stripped || raw;
}

function shortenDocumentStageLabel(value, maxLen = 72) {
  const normalized = String(value || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  if (normalized.length <= maxLen) return normalized;
  return `${normalized.slice(0, Math.max(8, maxLen - 1)).trim()}…`;
}

function buildPurchaseOrderDocumentTitle(order) {
  const stageRaw = resolvePurchaseOrderStageDisplay(order?.etapeApprovisionnement)
    || resolvePurchaseOrderStageDisplay(order?.items?.[0]?.etapeApprovisionnement)
    || '';
  const stageLabel = shortenDocumentStageLabel(stageRaw || 'Étape', 72);

  const siteRaw = String(order?.numeroMaison || order?.nomSiteManuel || '').trim();
  const siteLabel = extractSiteNumberLabel(siteRaw);

  return `${stageLabel}-${siteLabel}`;
}

function buildStageSiteTitle(stageValue, siteValue) {
  const stageLabel = shortenDocumentStageLabel(String(stageValue || '').trim() || 'Etape', 72);
  const siteLabel = extractSiteNumberLabel(siteValue);
  return `${stageLabel}-${siteLabel}`;
}

async function archivePurchaseOrderPdf(purchaseOrderId) {
  const order = await getPurchaseOrderById(purchaseOrderId);
  if (!order) {
    return null;
  }

  const sectionCode = 'achats';
  const safeSupplier = String(order.fournisseur || 'fournisseur').replace(/[^a-z0-9]+/gi, '-').replace(/^-+|-+$/g, '').toLowerCase() || 'fournisseur';
  const fileName = `bon-commande-${purchaseOrderId}-${safeSupplier}.pdf`;
  const documentTitle = buildPurchaseOrderDocumentTitle(order);
  const relativePath = getArchiveRelativePath(sectionCode, fileName);
  const absoluteDir = path.join(ARCHIVE_ROOT, sectionCode);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(absoluteDir, { recursive: true });

  const buffer = await generatePurchaseOrderPdfBuffer(order);
  await fs.promises.writeFile(absolutePath, buffer);

  await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ? AND sectionCode = ?', ['purchase_order', purchaseOrderId, sectionCode]);
  await run(
    'INSERT INTO generated_documents (sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [sectionCode, 'Achats', 'purchase_order', purchaseOrderId, documentTitle, fileName, relativePath, new Date().toISOString(), new Date().toISOString()]
  );

  return {
    title: documentTitle,
    fileName,
    relativePath,
    fileUrl: `/archives/${relativePath.replace(/\\/g, '/')}`,
  };
}

function resolveWarehouseLabel(warehouseId) {
  const labels = {
    'entrepot-plateau': 'Adzope',
    'entrepot-yopougon': 'Akoupe',
    'entrepot-bingerville': 'Alepe',
    'entrepot-port-bouet': 'Yakasse',
  };
  return labels[String(warehouseId || '').trim()] || String(warehouseId || '').trim() || 'Entrepot non defini';
}

function renderMaterialAuthorizationPdf(doc, payload) {
  const { order, request, items, signatureName, signatureRole, signedAt, decisionStatus = 'VALIDEE', documentKind = 'authorization' } = payload;
  const normalizedDocumentKind = String(documentKind || 'authorization').trim().toLowerCase();
  const isRequestDocument = normalizedDocumentKind === 'request';
  const projectTitle = String(request?.nomProjet || request?.projetNom || order?.nomProjet || '').trim() || 'Projet';
  const siteValueRaw = String(request.numeroMaison || request.nomSite || '').trim();
  const siteLabel = siteValueRaw
    ? (siteValueRaw.toLowerCase().includes('lot') ? siteValueRaw : `Lot Numero ${siteValueRaw}`)
    : 'Lot non renseigne';
  const normalizedDecision = String(decisionStatus || 'VALIDEE').toUpperCase();
  const isRejected = !isRequestDocument && (normalizedDecision === 'REJETEE' || normalizedDecision === 'ANNULEE');
  const stampLabel = isRequestDocument ? 'Demande' : (isRejected ? 'Rejet\u00e9' : 'Valid\u00e9');
  const stampColor = isRequestDocument ? '#1d4ed8' : (isRejected ? '#b91c1c' : '#166534');
  const signatureFontPath = resolveSignatureFontPath();
  const signatureFontName = signatureFontPath ? 'SignatureScript' : 'Helvetica-Oblique';
  const documentDate = new Date((isRequestDocument ? request?.dateDemande : signedAt) || signedAt || request?.dateDemande || Date.now());
  const titleText = isRequestDocument ? 'DEMANDE D\'APPROVISIONNEMENT DE MATERIEL' : 'AUTORISATION DE RETRAIT DE MATERIEL';
  const referenceLabel = isRequestDocument ? 'Reference :' : 'Bon de commande :';
  const referenceValue = isRequestDocument ? `DEM-${request.id}` : `#${order.id}`;
  const materialSectionTitle = isRequestDocument ? 'Materiel demande' : 'Mat\u00e9riel autoris\u00e9';
  const signatureLegend = isRequestDocument ? 'Signature du demandeur' : 'Signature autoris\u00e9e';
  const signedAtLabel = isRequestDocument ? 'Emis le :' : 'Sign\u00e9 le :';
  const requestStatusLabel = isRequestDocument ? 'DEMANDEE' : normalizedDecision;
  const requestStageLabel = String(request.etapeApprovisionnement || '-').trim() || '-';
  const requestStageLabelSingleLine = requestStageLabel.replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim() || '-';
  const warehouseLabel = resolveWarehouseLabel(request.warehouseId);

  if (signatureFontPath) {
    try {
      doc.registerFont(signatureFontName, signatureFontPath);
    } catch (_err) {
      // Keep built-in fallback font.
    }
  }

  // === HEADER (compact, absolute positions) ===
  doc.font('Helvetica-Bold').fontSize(17).fillColor('#000000').text(titleText, 40, 36, { width: 515, align: 'center' });

  // Info line: project / site / date / BC# in two columns
  doc.font('Helvetica-Bold').fontSize(10).fillColor('#000000').text('Projet :', 40, 66);
  doc.font('Helvetica').fontSize(10).text(projectTitle, 105, 66);
  doc.font('Helvetica-Bold').fontSize(10).text('Lot :', 300, 66);
  doc.font('Helvetica').fontSize(10).text(siteLabel, 340, 66);

  doc.font('Helvetica-Bold').fontSize(10).text('Date :', 40, 82);
  doc.font('Helvetica').fontSize(10).text(documentDate.toLocaleDateString('fr-FR'), 105, 82);
  doc.font('Helvetica-Bold').fontSize(10).text(referenceLabel, 300, 82);
  doc.font('Helvetica').fontSize(10).text(referenceValue, 440, 82);

  // Separator
  doc.moveTo(40, 100).lineTo(555, 100).lineWidth(1).strokeColor('#cccccc').stroke();

  // === INFORMATIONS DEMANDE ===
  const requestInfoTitleY = 110;
  const requestInfoRow1Y = 128;
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#000000').text('Informations de la demande', 40, requestInfoTitleY);

  doc.font('Helvetica-Bold').fontSize(10).text('Demandeur :', 40, requestInfoRow1Y);
  doc.font('Helvetica').fontSize(10).text(String(request.demandeur || '-').trim() || '-', 130, requestInfoRow1Y);
  doc.font('Helvetica-Bold').fontSize(10).text('\u00c9tape :', 300, requestInfoRow1Y);
  doc.font('Helvetica').fontSize(10).text(requestStageLabelSingleLine, 345, requestInfoRow1Y, {
    width: 205,
  });

  const stageTextHeight = Math.max(12, doc.heightOfString(requestStageLabelSingleLine, { width: 205 }));
  const requestInfoRow2Y = requestInfoRow1Y + Math.max(16, stageTextHeight + 4);

  doc.font('Helvetica-Bold').fontSize(10).text('Entrepot :', 40, requestInfoRow2Y);
  doc.font('Helvetica').fontSize(10).text(warehouseLabel, 130, requestInfoRow2Y);
  doc.font('Helvetica-Bold').fontSize(10).text('Statut :', 300, requestInfoRow2Y);
  doc.font('Helvetica').fontSize(10).text(requestStatusLabel, 345, requestInfoRow2Y);

  const requestInfoSeparatorY = requestInfoRow2Y + 18;

  // Separator
  doc.moveTo(40, requestInfoSeparatorY).lineTo(555, requestInfoSeparatorY).lineWidth(1).strokeColor('#cccccc').stroke();

  // === TABLE MATERIEL AUTORISE ===
  const materialSectionTitleY = requestInfoSeparatorY + 10;
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#000000').text(materialSectionTitle, 40, materialSectionTitleY);

  const startX = 40;
  const tableTop = materialSectionTitleY + 18;
  const widths = [375, 140];
  const rowHeight = 20;

  function drawRow(y, values, isHeader = false) {
    let x = startX;
    values.forEach((value, index) => {
      doc.rect(x, y, widths[index], rowHeight).lineWidth(0.5).strokeColor('#888888').stroke();
      doc.font(isHeader ? 'Helvetica-Bold' : 'Helvetica').fontSize(9).fillColor('#000000').text(String(value), x + 5, y + 5, {
        width: widths[index] - 10,
        ellipsis: true,
      });
      x += widths[index];
    });
  }

  drawRow(tableTop, ['Article', 'Quantite'], true);
  const rows = (items || []).map(item => ([
    String(item.article || request.itemName || 'Article'),
    Number(item.quantite || 0).toFixed(2),
  ]));
  if (!rows.length) {
    rows.push([
      String(request.itemName || 'Article'),
      Number(request.quantiteDemandee || 0).toFixed(2),
    ]);
  }
  // Dynamic cap to keep content above footer even when stage text spans multiple lines.
  const footerY = 730;
  const firstDataRowY = tableTop + rowHeight;
  const maxDataBottomY = footerY - 24;
  const availableRowsHeight = Math.max(rowHeight, maxDataBottomY - firstDataRowY);
  const maxRowsPerPage = Math.max(1, Math.min(18, Math.floor(availableRowsHeight / rowHeight)));
  const displayRows = rows.slice(0, maxRowsPerPage);
  displayRows.forEach((row, idx) => drawRow(tableTop + rowHeight * (idx + 1), row, false));

  // === FOOTER: signature + tampon (fixed at bottom of page) ===
  doc.moveTo(40, footerY).lineTo(555, footerY).lineWidth(1).strokeColor('#999999').stroke();

  // Date/heure de signature (left)
  doc.font('Helvetica').fontSize(9).fillColor('#555555').text(`${signedAtLabel} ${documentDate.toLocaleString('fr-FR')}`, 40, footerY + 10);

  // Signature (right)
  const sigName = String(signatureName || '').trim() || (isRequestDocument ? (String(request.demandeur || '').trim() || 'Demandeur') : 'Signature');
  doc.font('Helvetica').fontSize(9).fillColor('#0f172a').text(signatureLegend, 335, footerY + 10, { width: 220, align: 'right' });
  doc.font(signatureFontName).fontSize(26).fillColor('#111827').text(sigName, 335, footerY + 22, { width: 220, align: 'right' });
  if (String(signatureRole || '').trim() || isRequestDocument) {
    doc.font('Helvetica').fontSize(9).fillColor('#334155').text(String(signatureRole || '').trim() || 'Demandeur', 335, footerY + 54, { width: 220, align: 'right' });
  }

  // Tampon (left)
  doc.save();
  doc.lineWidth(2).strokeColor(stampColor).fillColor(stampColor);
  doc.roundedRect(40, footerY + 10, 160, 50, 8).stroke();
  doc.font('Helvetica-Bold').fontSize(8).text('TAMPON', 48, footerY + 16, { width: 144, align: 'left' });
  doc.font('Helvetica-Bold').fontSize(18).text(stampLabel, 48, footerY + 28, { width: 144, align: 'left' });
  doc.restore();
}

async function generateMaterialAuthorizationPdfBuffer(payload) {
  return await new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({ size: 'A4', margin: 40 });
    doc.on('data', chunk => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
    renderMaterialAuthorizationPdf(doc, payload);
    doc.end();
  });
}

async function archiveMaterialRequestAuthorizationPdf({ order, request, items, signatureName, signatureRole, signedAt, decisionStatus = 'VALIDEE' }) {
  const sectionCode = 'achats';
  const safeSupplier = sanitizeFileName(order.fournisseur || 'fournisseur').toLowerCase();
  const decisionSlug = String(decisionStatus || 'VALIDEE').toLowerCase();
  const fileName = `autorisation-retrait-${decisionSlug}-demande-${request.id}-bc-${order.id}-${safeSupplier}.pdf`;
  const relativePath = getArchiveRelativePath(sectionCode, fileName);
  const absoluteDir = path.join(ARCHIVE_ROOT, sectionCode);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(absoluteDir, { recursive: true });

  const buffer = await generateMaterialAuthorizationPdfBuffer({
    order,
    request,
    items,
    signatureName,
    signatureRole,
    signedAt,
    decisionStatus,
  });
  await fs.promises.writeFile(absolutePath, buffer);

  const now = new Date().toISOString();
  const formTitle = buildStageSiteTitle(
    request?.etapeApprovisionnement,
    request?.numeroMaison || request?.nomSite || '-'
  );
  await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ?', ['material_request_authorization', request.id]);
  const result = await run(
    'INSERT INTO generated_documents (sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      sectionCode,
      getArchiveSectionLabel(sectionCode),
      'material_request_authorization',
      request.id,
      formTitle,
      fileName,
      relativePath,
      now,
      now,
    ]
  );

  return {
    id: result.lastID,
    fileName,
    fileUrl: `/archives/${relativePath.replace(/\\/g, '/')}`,
  };
}

async function archiveStockIssueAuthorizationPdf({ authorization, request, signatureName, signatureRole, signedAt, decisionStatus = 'VALIDEE' }) {
  const safeDecision = String(decisionStatus || 'VALIDEE').toUpperCase();
  const pseudoOrder = {
    id: `SORTIE-${authorization.id}`,
    fournisseur: 'Sortie stock',
    montantTotal: 0,
    quantiteCommandee: Number(authorization.quantiteSortie || 0),
    prixUnitaire: 0,
    dateCommande: signedAt,
    warehouseId: authorization.warehouseId,
  };
  const pseudoItems = Array.isArray(authorization?.items) && authorization.items.length
    ? authorization.items.map(item => ({
        article: item.itemName || item.article || 'Article',
        quantite: Number(item.quantiteSortie || item.quantite || 0),
        prixUnitaire: 0,
        totalLigne: 0,
      }))
    : [{
        article: request.itemName || authorization.itemName || 'Article',
        quantite: Number(authorization.quantiteSortie || 0),
        prixUnitaire: 0,
        totalLigne: 0,
      }];

  const payloadRequest = {
    ...request,
    demandeur: authorization.requestedBy,
    dateDemande: authorization.requestedAt,
    statut: safeDecision,
  };

  const buffer = await generateMaterialAuthorizationPdfBuffer({
    order: pseudoOrder,
    request: payloadRequest,
    items: pseudoItems,
    signatureName,
    signatureRole,
    signedAt,
    decisionStatus: safeDecision,
  });

  const sectionCode = 'construction';
  const sectionLabel = getArchiveSectionLabel(sectionCode);
  const stageLabel = String(request?.etapeApprovisionnement || '').trim() || 'Etape';
  const siteLabel = extractSiteNumberLabel(request?.numeroMaison || '-');
  const title = `${stageLabel}-${siteLabel}`;
  const fileName = sanitizeFileName(`autorisation-sortie-stock-${authorization.id}-${safeDecision.toLowerCase()}.pdf`);
  const relativePath = getArchiveRelativePath(sectionCode, fileName);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(path.dirname(absolutePath), { recursive: true });
  await fs.promises.writeFile(absolutePath, buffer);

  await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ?', ['stock_issue_authorization', authorization.id]);
  await run(
    'INSERT INTO generated_documents (sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [sectionCode, sectionLabel, 'stock_issue_authorization', authorization.id, title, fileName, relativePath, new Date().toISOString(), new Date().toISOString()]
  );

  return {
    title,
    fileName,
    relativePath,
    fileUrl: `/archives/${relativePath.replace(/\\/g, '/')}`,
  };
}

async function archiveAuthorizationsForValidatedOrder(orderId, signatureName, signatureRole, signedAt, decisionStatus = 'VALIDEE') {
  const order = await getPurchaseOrderById(orderId);
  if (!order) {
    return [];
  }

  const itemList = Array.isArray(order.items) ? order.items : [];
  const requestIds = Array.from(new Set(itemList.map(item => Number(item.materialRequestId)).filter(id => Number.isInteger(id) && id > 0)));
  const fallbackRequestId = Number(order.materialRequestId || 0);
  if (!requestIds.length && Number.isInteger(fallbackRequestId) && fallbackRequestId > 0) {
    requestIds.push(fallbackRequestId);
  }

  if (!requestIds.length) {
    return [];
  }

  const placeholders = requestIds.map(() => '?').join(',');
  const requests = await all(
    `SELECT mr.*, p.nomProjet, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id IN (${placeholders})`,
    requestIds
  );

  const docs = [];
  for (const request of requests) {
    let items = itemList.filter(item => Number(item.materialRequestId) === Number(request.id));
    if (!items.length) {
      // Legacy fallback: order linked via purchase_orders.materialRequestId without item link rows.
      items = [{
        article: String(request.itemName || order.itemName || 'Article'),
        quantite: Number(order.quantiteCommandee || request.quantiteDemandee || 0),
        prixUnitaire: Number(order.prixUnitaire || 0),
        totalLigne: Number(order.montantTotal || 0),
      }];
    }

    const doc = await archiveMaterialRequestAuthorizationPdf({
      order,
      request,
      items,
      signatureName,
      signatureRole,
      signedAt,
      decisionStatus,
    });
    docs.push(doc);
  }
  return docs;
}

function renderRevenueInvoicePdf(doc, revenue, projectName) {
  doc.font('Helvetica-Bold').fontSize(22).text('FACTURE DE REVENU', 40, 40);
  doc.font('Helvetica').fontSize(12);
  doc.text(`Date: ${new Date(revenue.dateRevenue || Date.now()).toLocaleDateString('fr-FR')}`, 40, 90);
  doc.text(`Projet: ${projectName || 'Projet non defini'}`, 40, 112);
  doc.text(`Description: ${String(revenue.description || '').trim() || '-'}`, 40, 134);

  const amount = Number(revenue.amount || 0);
  doc.rect(40, 180, 515, 80).stroke();
  doc.font('Helvetica-Bold').fontSize(14).text(`Montant facture: ${amount.toFixed(2)} EUR`, 55, 212);

  doc.font('Helvetica').fontSize(11).fillColor('#64748b').text(
    'Document genere automatiquement et archive dans la base de donnees - section Comptabilite.',
    40,
    780,
    { align: 'center', width: 515 }
  );
}

async function generateRevenueInvoicePdfBuffer(revenue, projectName) {
  return await new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({ size: 'A4', margin: 40 });
    doc.on('data', chunk => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
    renderRevenueInvoicePdf(doc, revenue, projectName);
    doc.end();
  });
}

async function archiveRevenueInvoicePdf(revenueId) {
  const revenue = await get('SELECT * FROM revenues WHERE id = ?', [revenueId]);
  if (!revenue) {
    return null;
  }

  const project = await get('SELECT nomProjet FROM projects WHERE id = ?', [revenue.projetId]);
  const projectName = project ? project.nomProjet : 'Projet';
  const documentTitle = buildProjectDocumentTitle(projectName, revenueId);
  const sectionCode = 'comptabilite';
  const safeProject = sanitizeFileName(projectName).toLowerCase();
  const fileName = `facture-revenu-${revenueId}-${safeProject}.pdf`;
  const relativePath = getArchiveRelativePath(sectionCode, fileName);
  const absoluteDir = path.join(ARCHIVE_ROOT, sectionCode);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(absoluteDir, { recursive: true });

  const buffer = await generateRevenueInvoicePdfBuffer(revenue, projectName);
  await fs.promises.writeFile(absolutePath, buffer);

  await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ? AND sectionCode = ?', ['revenue', revenueId, sectionCode]);
  await run(
    'INSERT INTO generated_documents (sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      sectionCode,
      getArchiveSectionLabel(sectionCode),
      'revenue',
      revenueId,
      documentTitle,
      fileName,
      relativePath,
      new Date().toISOString(),
      new Date().toISOString(),
    ]
  );

  return {
    title: documentTitle,
    fileName,
    relativePath,
    fileUrl: `/archives/${relativePath.replace(/\\/g, '/')}`,
  };
}

async function archiveUploadedDocument({ sectionCode, title, fileName, fileBuffer, projectId = null }) {
  const safeSection = ['achats', 'construction', 'comptabilite'].includes(sectionCode) ? sectionCode : 'construction';
  const safeName = sanitizeFileName(fileName || 'document');
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const finalFileName = `${stamp}-${safeName}`;
  const relativePath = getArchiveRelativePath(safeSection, finalFileName);
  const absoluteDir = path.join(ARCHIVE_ROOT, safeSection);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(absoluteDir, { recursive: true });
  await fs.promises.writeFile(absolutePath, fileBuffer);

  const now = new Date().toISOString();
  const numericProjectId = Number(projectId || 0);
  const linkedEntityId = Number.isFinite(numericProjectId) && numericProjectId > 0 ? numericProjectId : 0;
  const result = await run(
    'INSERT INTO generated_documents (sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [safeSection, getArchiveSectionLabel(safeSection), 'manual_upload', linkedEntityId, title || safeName, finalFileName, relativePath, now, now]
  );

  return {
    id: result.lastID,
    sectionCode: safeSection,
    title: title || safeName,
    fileName: finalFileName,
    fileUrl: `/archives/${relativePath.replace(/\\/g, '/')}`,
  };
}

async function archiveGuideDocument({ title, fileName, fileBuffer, mimeType = 'application/octet-stream', uploadedBy = 'admin', audienceScope = 'all', recipientEmployeeIds = [] }) {
  const safeName = sanitizeFileName(fileName || 'guide-document');
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const finalFileName = `${stamp}-${safeName}`;
  const relativePath = getArchiveRelativePath('guides', finalFileName);
  const absoluteDir = path.join(ARCHIVE_ROOT, 'guides');
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(absoluteDir, { recursive: true });
  await fs.promises.writeFile(absolutePath, fileBuffer);

  const now = new Date().toISOString();
  const safeTitle = String(title || '').trim() || safeName;
  const safeMimeType = String(mimeType || 'application/octet-stream').trim() || 'application/octet-stream';
  const sizeBytes = Number(fileBuffer?.length || 0);
  const safeUploadedBy = String(uploadedBy || 'admin').trim() || 'admin';
  const normalizedAudienceScope = String(audienceScope || 'all').trim().toLowerCase() === 'selected' ? 'selected' : 'all';
  const normalizedRecipientIds = normalizeNumericIdList(recipientEmployeeIds);
  const serializedRecipientIds = normalizedAudienceScope === 'selected' ? normalizedRecipientIds.join(',') : '';
  const guideColumns = await ensureGuideDocumentAudienceColumns();
  const hasAudienceScopeColumn = hasTableColumn(guideColumns, 'audienceScope');
  const hasRecipientIdsColumn = hasTableColumn(guideColumns, 'recipientEmployeeIds');
  const hasContentBlobColumn = hasTableColumn(guideColumns, 'contentBlob');

  if (normalizedAudienceScope === 'selected' && (!hasAudienceScopeColumn || !hasRecipientIdsColumn)) {
    throw new Error('Configuration guide incomplète: colonnes de ciblage absentes');
  }

  let nextGuideId = null;
  if (DB_DRIVER === 'postgres') {
    const nextGuideIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM guide_documents');
    nextGuideId = Number(nextGuideIdRow?.nextId || nextGuideIdRow?.nextid || 1);
  }

  const columns = [];
  const placeholders = [];
  const values = [];
  const pushColumn = (name, value) => {
    columns.push(name);
    placeholders.push('?');
    values.push(value);
  };

  if (nextGuideId !== null) {
    pushColumn('id', nextGuideId);
  }
  pushColumn('title', safeTitle);
  pushColumn('fileName', finalFileName);
  pushColumn('relativePath', relativePath);
  pushColumn('mimeType', safeMimeType);
  pushColumn('sizeBytes', sizeBytes);
  pushColumn('uploadedBy', safeUploadedBy);
  if (hasContentBlobColumn) {
    pushColumn('contentBlob', fileBuffer);
  }
  if (hasAudienceScopeColumn) {
    pushColumn('audienceScope', normalizedAudienceScope);
  }
  if (hasRecipientIdsColumn) {
    pushColumn('recipientEmployeeIds', serializedRecipientIds);
  }
  pushColumn('createdAt', now);
  pushColumn('updatedAt', now);

  const result = await run(
    `INSERT INTO guide_documents (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`,
    values
  );

  return {
    id: result.lastID || nextGuideId,
    title: safeTitle,
    fileName: finalFileName,
    relativePath,
    mimeType: safeMimeType,
    sizeBytes,
    uploadedBy: safeUploadedBy,
    audienceScope: normalizedAudienceScope,
    recipientEmployeeIds: normalizedRecipientIds,
    createdAt: now,
    updatedAt: now,
    fileUrl: `/archives/${relativePath.replace(/\\/g, '/')}`,
  };
}

async function reconcileDocumentArchives() {
  try {
    const orders = await all('SELECT id FROM purchase_orders ORDER BY id ASC');
    for (const order of orders) {
      try {
        await archivePurchaseOrderPdf(Number(order.id));
      } catch (e) {
        // Silently continue
      }
    }

    const revenues = await all('SELECT id FROM revenues ORDER BY id ASC');
    for (const revenue of revenues) {
      try {
        await archiveRevenueInvoicePdf(Number(revenue.id));
      } catch (e) {
        // Silently continue
      }
    }
  } catch (e) {
    // Silently fail
  }
}

async function preparePurchaseOrderItems(payload) {
  const {
    items,
    materialRequestId,
    quantiteCommandee,
    prixUnitaire,
    montantTotal,
  } = payload;

  let normalizedItems = [];
  if (Array.isArray(items) && items.length > 0) {
    normalizedItems = items;
  } else if (materialRequestId) {
    normalizedItems = [{
      materialRequestId,
      quantite: quantiteCommandee,
      prixUnitaire,
      article: '',
      details: '',
      totalLigne: montantTotal,
    }];
  }

  if (!normalizedItems.length) {
    return [];
  }

  const preparedItems = [];
  for (const rawItem of normalizedItems) {
    const requestId = Number(rawItem.materialRequestId || rawItem.requestId || materialRequestId) || null;
    let request = null;
    if (requestId) {
      request = await get('SELECT id, projetId, itemName, description FROM material_requests WHERE id = ?', [requestId]);
      if (!request) {
        // materialRequestId fourni mais introuvable — on continue sans lier
        request = null;
      }
    }

    const quantity = Number(rawItem.quantite ?? rawItem.quantity ?? quantiteCommandee);
    const unitPrice = Number(rawItem.prixUnitaire ?? rawItem.unitPrice ?? prixUnitaire);
    if (Number.isNaN(quantity) || quantity <= 0 || Number.isNaN(unitPrice) || unitPrice < 0) {
      return 'INVALID_PO_ITEM';
    }

    const article = String(rawItem.article || (request && request.itemName) || '').trim() || 'Article';
    const details = String(rawItem.details || (request && request.description) || '').trim();
    const lineTotal = Number(rawItem.totalLigne ?? quantity * unitPrice);

    preparedItems.push({
      materialRequestId: request ? request.id : null,
      projetId: request ? request.projetId : null,
      article,
      details,
      quantite: quantity,
      prixUnitaire: unitPrice,
      totalLigne: Number.isNaN(lineTotal) ? quantity * unitPrice : lineTotal,
    });
  }

  return preparedItems;
}

async function syncExpensesForPurchaseOrder(purchaseOrderId, override = {}) {
  try {
    await run('DELETE FROM expenses WHERE purchaseOrderId = ?', [purchaseOrderId]);
  } catch (e) {}

  const order = await getPurchaseOrderById(purchaseOrderId);
  if (!order || !order.items || !order.items.length) {
    return;
  }

  const expCols = await getTableColumns('expenses');
  let nextExpenseId = null;
  if (expCols.has('id')) {
    const nextExpenseIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM expenses');
    nextExpenseId = Number(nextExpenseIdRow?.nextId || nextExpenseIdRow?.nextid || 1);
  }
  const dateStr = override.dateCommande || order.dateCommande || new Date().toISOString();
  const supplier = String(override.fournisseur || order.fournisseur || '').trim();
  const createdBy = override.createdBy || 'admin';

  for (const item of order.items) {
    let itemProjetId = override.projetId || item.projetId || order.projetId || null;
    if (!itemProjetId && item.materialRequestId) {
      const request = await get('SELECT projetId FROM material_requests WHERE id = ?', [item.materialRequestId]);
      if (request) itemProjetId = request.projetId;
    }

    const columns = [];
    const values = [];
    const pushColumn = (columnName, value) => {
      if (expCols.has(columnName)) {
        columns.push(columnName);
        values.push(value);
      }
    };

    if (nextExpenseId !== null) {
      pushColumn('id', nextExpenseId);
      nextExpenseId += 1;
    }
    pushColumn('materialId', item.materialRequestId || null);
    pushColumn('projetId', itemProjetId);
    pushColumn('projectId', itemProjetId);
    pushColumn('description', item.article);
    pushColumn('item', item.article);
    pushColumn('quantite', Number(item.quantite || 0));
    pushColumn('quantity', Number(item.quantite || 0));
    pushColumn('prixUnitaire', Number(item.prixUnitaire || 0));
    pushColumn('unitPrice', Number(item.prixUnitaire || 0));
    pushColumn('montantTotal', Number(item.totalLigne || 0));
    pushColumn('totalPrice', Number(item.totalLigne || 0));
    pushColumn('dateExpense', dateStr);
    pushColumn('date', dateStr);
    pushColumn('fournisseur', supplier);
    pushColumn('supplier', supplier);
    pushColumn('categorie', 'materiaux');
    pushColumn('category', 'materiaux');
    pushColumn('statut', 'EN_ATTENTE');
    pushColumn('status', 'EN_ATTENTE');
    pushColumn('purchaseOrderId', purchaseOrderId);
    pushColumn('createdBy', createdBy);

    if (!columns.length) {
      continue;
    }

    const placeholders = columns.map(() => '?').join(', ');
    await run(`INSERT INTO expenses (${columns.join(', ')}) VALUES (${placeholders})`, values);
  }
}

async function runPurchaseOrderSideEffects(purchaseOrderId, override = {}) {
  try {
    await syncExpensesForPurchaseOrder(purchaseOrderId, override);
  } catch (error) {
    console.error('Erreur sync expenses bon de commande:', error?.message || error);
  }

  try {
    await archivePurchaseOrderPdf(purchaseOrderId);
  } catch (error) {
    console.error('Erreur archivage PDF bon de commande:', error?.message || error);
  }
}

async function reconcilePurchaseOrderExpenses() {
  try {
    const orders = await all('SELECT id FROM purchase_orders ORDER BY id ASC');
    for (const order of orders) {
      try {
        const countRow = await get('SELECT COUNT(*) as count FROM expenses WHERE purchaseOrderId = ?', [order.id]);
        if (!countRow || !countRow.count) {
          await syncExpensesForPurchaseOrder(order.id);
        }
      } catch (e) {
        // Silently continue
      }
    }
  } catch (e) {
    // Silently fail
  }
}

async function ensureMaterialRequestsForOrder(orderId, options = {}) {
  const order = await get('SELECT * FROM purchase_orders WHERE id = ?', [Number(orderId)]);
  if (!order) return [];

  const orderItems = await all('SELECT id, article, quantite, materialRequestId FROM purchase_order_items WHERE purchaseOrderId = ? ORDER BY id ASC', [Number(orderId)]);
  const existingIds = Array.from(new Set([
    Number(order.materialRequestId || 0),
    ...orderItems.map(item => Number(item.materialRequestId || 0)),
  ].filter(Boolean)));

  if (existingIds.length) {
    return existingIds;
  }

  let projectId = Number(order.projetId || order.siteId || 0);
  if (!projectId) {
    const manualProjectName = String(order.nomProjetManuel || '').trim();
    if (manualProjectName) {
      const matchedProject = await get('SELECT id FROM projects WHERE TRIM(nomProjet) = ? ORDER BY id ASC LIMIT 1', [manualProjectName]);
      projectId = Number(matchedProject?.id || 0);
    }
  }
  if (!projectId) {
    const anyProject = await get('SELECT id FROM projects ORDER BY id ASC LIMIT 1');
    projectId = Number(anyProject?.id || 0);
  }
  if (!projectId) {
    return [];
  }

  const fallbackStatus = String(options.forceStatus || '').trim() || ((String(order.statut || '').toUpperCase() === 'LIVREE' || String(order.statutValidation || '').toUpperCase() === 'LIVREE') ? 'EN_STOCK' : 'EN_COURS');
  const requestDate = String(order.dateCommande || options.dateOverride || new Date().toISOString());
  const requester = String(order.creePar || 'admin').trim() || 'admin';
  const warehouseId = String(order.warehouseId || '').trim() || null;

  const sourceItems = orderItems.length
    ? orderItems.map(item => ({
        lineId: Number(item.id || 0),
        article: String(item.article || '').trim() || `Article BC #${order.id}`,
        quantite: Math.max(1, Number(item.quantite || 0) || 1),
      }))
    : [{
        lineId: 0,
        article: `Article BC #${order.id}`,
        quantite: Math.max(1, Number(order.quantiteCommandee || 0) || 1),
      }];

  const createdRequestIds = [];
  let nextMrId = await getNextTableId('material_requests');
  for (const item of sourceItems) {
    const inserted = await run(
      'INSERT INTO material_requests (id, projetId, demandeur, etapeApprovisionnement, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut, warehouseId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        nextMrId++,
        projectId,
        requester,
        'BON_COMMANDE',
        item.article,
        `BC #${order.id} - ${String(order.fournisseur || '').trim() || 'Fournisseur'}`,
        item.quantite,
        item.quantite,
        requestDate,
        fallbackStatus,
        warehouseId,
      ]
    );

    const requestId = Number(inserted.lastID || 0);
    if (!requestId) continue;
    createdRequestIds.push(requestId);

    if (item.lineId) {
      await run('UPDATE purchase_order_items SET materialRequestId = ? WHERE id = ?', [requestId, item.lineId]);
    }
  }

  if (createdRequestIds.length) {
    await run('UPDATE purchase_orders SET materialRequestId = ? WHERE id = ? AND (materialRequestId IS NULL OR materialRequestId = 0)', [createdRequestIds[0], order.id]);
  }

  return createdRequestIds;
}

async function initDb() {
  console.log('[initDb] Démarrage de l\'initialisation...');
  await run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL,
    createdAt TEXT NOT NULL
  )`);
  console.log('[initDb] Table users créée');

  await run(`CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY,
    nomProjet TEXT NOT NULL,
    prefecture TEXT NOT NULL DEFAULT 'Non renseigne',
    nomSite TEXT NOT NULL,
    typeMaison TEXT NOT NULL DEFAULT '',
    numeroMaison TEXT NOT NULL DEFAULT '',
    description TEXT DEFAULT '',
    etapeConstruction TEXT,
    statutConstruction TEXT,
    createdAt TEXT NOT NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS project_catalog (
    id INTEGER PRIMARY KEY,
    nomProjet TEXT NOT NULL,
    typeProjet TEXT NOT NULL DEFAULT '',
    description TEXT DEFAULT '',
    createdAt TEXT NOT NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS project_folders (
    id INTEGER PRIMARY KEY,
    projectId INTEGER,
    nomProjet TEXT NOT NULL,
    prefecture TEXT NOT NULL DEFAULT 'Non renseigne',
    nomSite TEXT NOT NULL,
    description TEXT DEFAULT '',
    createdAt TEXT NOT NULL
  )`);

  try {
    await run("ALTER TABLE project_folders ADD COLUMN description TEXT DEFAULT ''");
  } catch (e) {}
  try {
    await run('ALTER TABLE project_folders ADD COLUMN projectId INTEGER');
  } catch (e) {}
  try {
    await run("ALTER TABLE project_folders ADD COLUMN prefecture TEXT NOT NULL DEFAULT 'Non renseigne'");
  } catch (e) {}
  try {
    await run("ALTER TABLE projects ADD COLUMN prefecture TEXT NOT NULL DEFAULT 'Non renseigne'");
  } catch (e) {}
  try {
    await run("ALTER TABLE project_catalog ADD COLUMN typeProjet TEXT NOT NULL DEFAULT ''");
  } catch (e) {}
  try {
    await run("ALTER TABLE project_catalog ADD COLUMN description TEXT DEFAULT ''");
  } catch (e) {}
  try {
    await run('ALTER TABLE projects ADD COLUMN isHidden INTEGER NOT NULL DEFAULT 0');
  } catch (e) {}
  try {
    await run('ALTER TABLE project_catalog ADD COLUMN isHidden INTEGER NOT NULL DEFAULT 0');
  } catch (e) {}
  try {
    await run('ALTER TABLE project_folders ADD COLUMN isHidden INTEGER NOT NULL DEFAULT 0');
  } catch (e) {}
  try {
    await run('DROP INDEX IF EXISTS idx_project_folders_name_site');
  } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS custom_stock_warehouses (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    linkedProjectId INTEGER,
    linkedProjectName TEXT,
    linkedZoneId TEXT,
    linkedZoneName TEXT,
    prefecture TEXT,
    isHidden INTEGER NOT NULL DEFAULT 0,
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  )`);

  await run(`INSERT INTO custom_stock_warehouses (
    id,
    name,
    linkedProjectId,
    linkedProjectName,
    linkedZoneId,
    linkedZoneName,
    prefecture,
    isHidden,
    createdAt,
    updatedAt
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT (id) DO NOTHING`, [
    'entrepot-songon-1',
    'Songon',
    null,
    '',
    '',
    'Songon',
    'Songon',
    0,
    new Date().toISOString(),
    new Date().toISOString(),
  ]);

  await run(`CREATE TABLE IF NOT EXISTS material_requests (
    id INTEGER PRIMARY KEY,
    projetId INTEGER NOT NULL,
    demandeur TEXT NOT NULL,
    etapeApprovisionnement TEXT NOT NULL DEFAULT '',
    itemName TEXT NOT NULL,
    description TEXT,
    quantiteDemandee REAL NOT NULL DEFAULT 0,
    quantiteRestante REAL NOT NULL DEFAULT 0,
    dateDemande TEXT NOT NULL,
    statut TEXT NOT NULL,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE CASCADE
  )`);

  try {
    await run("ALTER TABLE material_requests ADD COLUMN itemName TEXT NOT NULL DEFAULT ''");
  } catch (e) {}
  try {
    await run('ALTER TABLE material_requests ADD COLUMN quantiteDemandee REAL NOT NULL DEFAULT 0');
  } catch (e) {}
  try {
    await run('ALTER TABLE material_requests ADD COLUMN quantiteRestante REAL NOT NULL DEFAULT 0');
  } catch (e) {}
  try {
    await run("ALTER TABLE material_requests ADD COLUMN etapeApprovisionnement TEXT NOT NULL DEFAULT ''");
  } catch (e) {}
  try {
    await run('ALTER TABLE material_requests ADD COLUMN groupId TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE material_requests ADD COLUMN warehouseId TEXT');
  } catch (e) {}
  try {
    await run("ALTER TABLE projects ADD COLUMN description TEXT DEFAULT ''");
  } catch (e) {}
  try {
    await run("ALTER TABLE projects ADD COLUMN typeMaison TEXT NOT NULL DEFAULT ''");
  } catch (e) {}
  try {
    await run("ALTER TABLE projects ADD COLUMN numeroMaison TEXT NOT NULL DEFAULT ''");
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN creePar TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN montantTotal REAL');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN statutValidation TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN quantiteCommandee REAL NOT NULL DEFAULT 1');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN prixUnitaire REAL NOT NULL DEFAULT 0');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN dateLivraisonPrevue TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN dateReception TEXT');
  } catch (e) {}
  try {
    await run("ALTER TABLE purchase_orders ADD COLUMN statut TEXT NOT NULL DEFAULT 'EN_COURS'");
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN etapeApprovisionnement TEXT');
  } catch (e) {}
  try {
    await run("UPDATE purchase_orders SET creePar = COALESCE(creePar, 'admin')");
  } catch (e) {}
  try {
    await run('UPDATE purchase_orders SET montantTotal = COALESCE(montantTotal, quantiteCommandee * prixUnitaire, 0)');
  } catch (e) {}
  try {
    await run("UPDATE purchase_orders SET statutValidation = COALESCE(statutValidation, statut, 'EN_COURS')");
  } catch (e) {}
  try {
    await run("UPDATE purchase_orders SET statut = COALESCE(statut, statutValidation, 'EN_COURS')");
  } catch (e) {}
  try {
    await run('UPDATE purchase_orders SET quantiteCommandee = COALESCE(quantiteCommandee, 1)');
  } catch (e) {}
  try {
    await run('UPDATE purchase_orders SET prixUnitaire = COALESCE(prixUnitaire, montantTotal, 0)');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN materialId INTEGER');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN projetId INTEGER');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN quantite REAL');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN prixUnitaire REAL');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN montantTotal REAL');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN fournisseur TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN categorie TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN statut TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN createdBy TEXT');
  } catch (e) {}
  try {
    await run('UPDATE expenses SET projetId = COALESCE(projetId, projectId)');
  } catch (e) {}
  try {
    await run('UPDATE expenses SET quantite = COALESCE(quantite, quantity)');
  } catch (e) {}
  try {
    await run('UPDATE expenses SET prixUnitaire = COALESCE(prixUnitaire, unitPrice)');
  } catch (e) {}
  try {
    await run('UPDATE expenses SET montantTotal = COALESCE(montantTotal, totalPrice, COALESCE(quantite, quantity, 0) * COALESCE(prixUnitaire, unitPrice, 0))');
  } catch (e) {}
  try {
    await run("UPDATE expenses SET categorie = COALESCE(categorie, category, 'autres')");
  } catch (e) {}
  try {
    await run("UPDATE expenses SET statut = COALESCE(statut, 'EN_ATTENTE')");
  } catch (e) {}
  try {
    await run("UPDATE expenses SET createdBy = COALESCE(createdBy, 'admin')");
  } catch (e) {}
  try {
    await run('ALTER TABLE project_assignments ADD COLUMN assigneeName TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE expenses ADD COLUMN purchaseOrderId INTEGER');
  } catch (e) {}
  try {
    await run('ALTER TABLE project_assignments ADD COLUMN phoneNumber TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE project_assignments ADD COLUMN employeeId INTEGER');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN signatureName TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN signatureRole TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN creePar TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN montantTotal REAL');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN statutValidation TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN projetId INTEGER');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN nomProjetManuel TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN siteId INTEGER');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN nomSiteManuel TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN warehouseId TEXT');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_orders ADD COLUMN etapeApprovisionnement TEXT');
  } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS purchase_orders (
    id INTEGER PRIMARY KEY,
    materialRequestId INTEGER,
    fournisseur TEXT NOT NULL,
    quantiteCommandee REAL NOT NULL,
    prixUnitaire REAL NOT NULL,
    dateCommande TEXT NOT NULL,
    dateLivraisonPrevue TEXT,
    dateReception TEXT,
    statut TEXT NOT NULL,
    FOREIGN KEY(materialRequestId) REFERENCES material_requests(id) ON DELETE CASCADE
  )`);

  await run(`CREATE TABLE IF NOT EXISTS purchase_order_items (
    id INTEGER PRIMARY KEY,
    purchaseOrderId INTEGER NOT NULL,
    materialRequestId INTEGER,
    article TEXT NOT NULL,
    details TEXT,
    quantite REAL NOT NULL,
    prixUnitaire REAL NOT NULL,
    totalLigne REAL NOT NULL,
    FOREIGN KEY(purchaseOrderId) REFERENCES purchase_orders(id) ON DELETE CASCADE,
    FOREIGN KEY(materialRequestId) REFERENCES material_requests(id) ON DELETE CASCADE
  )`);

  try { await run(`
    INSERT INTO purchase_order_items (purchaseOrderId, materialRequestId, article, details, quantite, prixUnitaire, totalLigne)
    SELECT
      po.id,
      po.materialRequestId,
      COALESCE(mr.itemName, 'Article'),
      COALESCE(mr.description, ''),
      COALESCE(po.quantiteCommandee, 1),
      COALESCE(po.prixUnitaire, po.montantTotal, 0),
      COALESCE(po.montantTotal, COALESCE(po.quantiteCommandee, 1) * COALESCE(po.prixUnitaire, 0), 0)
    FROM purchase_orders po
    LEFT JOIN material_requests mr ON mr.id = po.materialRequestId
    WHERE NOT EXISTS (
      SELECT 1
      FROM purchase_order_items poi
      WHERE poi.purchaseOrderId = po.id
    )
  `); } catch(e) {}

  await run(`CREATE TABLE IF NOT EXISTS materials (
    id INTEGER PRIMARY KEY,
    nom TEXT NOT NULL,
    categorie TEXT NOT NULL,
    unite TEXT NOT NULL,
    stock REAL NOT NULL DEFAULT 0,
    seuil REAL NOT NULL DEFAULT 0,
    prixMoyen REAL NOT NULL DEFAULT 0
  )`);

  await run(`CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY,
    nomFournisseur TEXT NOT NULL,
    materiels TEXT NOT NULL DEFAULT '',
    prixMateriaux REAL NOT NULL DEFAULT 0,
    telephone TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    adresse TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS building_material_catalog (
    id INTEGER PRIMARY KEY,
    projectFolder TEXT NOT NULL DEFAULT '',
    materialName TEXT NOT NULL,
    unite TEXT NOT NULL DEFAULT '',
    quantiteParBatiment REAL NOT NULL DEFAULT 0,
    prixUnitaire REAL NOT NULL DEFAULT 0,
    notes TEXT NOT NULL DEFAULT '',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  )`);

  // Repair legacy mojibake sequences introduced during cross-environment syncs.
  // Use CHAR(65533) (replacement char) and UTF-8-as-CP1252 variants to stay robust across editors/OS.
  try {
    await run(`
      UPDATE project_catalog
      SET typeProjet = REPLACE(REPLACE(typeProjet, 'santÃ©', 'santé'), 'sant' || char(65533), 'santé')
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE project_catalog
      SET description = REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(description, 'santÃ©', 'santé'),
                  'sant' || char(65533),
                  'santé'
                ),
                'Ã©chelle',
                'échelle'
              ),
              char(65533) || 'chelle',
              'échelle'
            ),
            'CÃ´te',
            'Côte'
          ),
          'C' || char(65533) || 'te',
          'Côte'
        ),
        'amÃ©liorer',
        'améliorer'
      )
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE project_catalog
      SET description = REPLACE(description, 'accÃ¨s', 'accès')
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE project_catalog
      SET description = REPLACE(description, 'acc' || char(65533) || 's', 'accès')
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE project_catalog
      SET description = REPLACE(description, char(65533) || ' ', 'à ')
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE projects
      SET typeMaison = REPLACE(REPLACE(typeMaison, 'santÃ©', 'santé'), 'sant' || char(65533), 'santé')
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE building_material_catalog
      SET unite = REPLACE(REPLACE(unite, 'mÃ³', 'm³'), 'm' || char(65533), 'm³'),
          notes = REPLACE(REPLACE(notes, 'Ã‰tape', 'Étape'), char(65533) || 'tape', 'Étape')
      WHERE LOWER(TRIM(projectFolder)) = 'pinut'
    `);
  } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS stock_issues (
    id INTEGER PRIMARY KEY,
    materialRequestId INTEGER NOT NULL,
    projetId INTEGER,
    quantiteSortie REAL NOT NULL,
    issueType TEXT NOT NULL DEFAULT 'SITE_TRANSFER',
    note TEXT,
    issuedBy TEXT NOT NULL,
    issuedAt TEXT NOT NULL,
    FOREIGN KEY(materialRequestId) REFERENCES material_requests(id) ON DELETE CASCADE,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE SET NULL
  )`);

  try { await run("ALTER TABLE stock_issues ADD COLUMN issueType TEXT NOT NULL DEFAULT 'SITE_TRANSFER'"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS stock_issue_authorizations (
    id INTEGER PRIMARY KEY,
    materialRequestId INTEGER NOT NULL,
    projetId INTEGER,
    warehouseId TEXT,
    itemName TEXT NOT NULL,
    etapeApprovisionnement TEXT,
    quantiteSortie REAL NOT NULL,
    note TEXT,
    status TEXT NOT NULL DEFAULT 'EN_ATTENTE',
    requestedBy TEXT NOT NULL,
    requestedAt TEXT NOT NULL,
    decidedBy TEXT,
    decidedAt TEXT,
    decisionNote TEXT,
    signatureName TEXT,
    signatureRole TEXT,
    FOREIGN KEY(materialRequestId) REFERENCES material_requests(id) ON DELETE CASCADE,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE SET NULL
  )`);

  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN warehouseId TEXT"); } catch (e) {}
  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN etapeApprovisionnement TEXT"); } catch (e) {}
  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN status TEXT NOT NULL DEFAULT 'EN_ATTENTE'"); } catch (e) {}
  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN decidedBy TEXT"); } catch (e) {}
  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN decidedAt TEXT"); } catch (e) {}
  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN decisionNote TEXT"); } catch (e) {}
  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN signatureName TEXT"); } catch (e) {}
  try { await run("ALTER TABLE stock_issue_authorizations ADD COLUMN signatureRole TEXT"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS stock_issue_authorization_items (
    id INTEGER PRIMARY KEY,
    authorizationId INTEGER NOT NULL,
    materialRequestId INTEGER NOT NULL,
    projetId INTEGER,
    itemName TEXT NOT NULL,
    quantiteSortie REAL NOT NULL,
    etapeApprovisionnement TEXT,
    warehouseId TEXT,
    createdAt TEXT NOT NULL,
    FOREIGN KEY(authorizationId) REFERENCES stock_issue_authorizations(id) ON DELETE CASCADE,
    FOREIGN KEY(materialRequestId) REFERENCES material_requests(id) ON DELETE CASCADE,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE SET NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY,
    materialId INTEGER,
    projetId INTEGER,
    purchaseOrderId INTEGER,
    description TEXT NOT NULL,
    quantite REAL NOT NULL,
    prixUnitaire REAL NOT NULL,
    montantTotal REAL NOT NULL,
    dateExpense TEXT NOT NULL,
    fournisseur TEXT,
    categorie TEXT NOT NULL,
    statut TEXT NOT NULL,
    createdBy TEXT NOT NULL,
    FOREIGN KEY(materialId) REFERENCES materials(id),
    FOREIGN KEY(purchaseOrderId) REFERENCES purchase_orders(id) ON DELETE SET NULL,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE CASCADE
  )`);

  await run(`CREATE TABLE IF NOT EXISTS auto_vehicles (
    id INTEGER PRIMARY KEY,
    nomVehicule TEXT NOT NULL,
    marqueVehicule TEXT NOT NULL,
    immatriculation TEXT NOT NULL DEFAULT '',
    chauffeurNom TEXT NOT NULL DEFAULT '',
    gpsActif INTEGER NOT NULL DEFAULT 0,
    valeurVehicule REAL NOT NULL DEFAULT 0,
    etatVehicule TEXT NOT NULL,
    createdAt TEXT NOT NULL
  )`);

  try { await run("ALTER TABLE auto_vehicles ADD COLUMN immatriculation TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicles ADD COLUMN chauffeurNom TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicles ADD COLUMN gpsActif INTEGER NOT NULL DEFAULT 0'); } catch (error) {}

  await run(`CREATE TABLE IF NOT EXISTS auto_vehicle_locations (
    id INTEGER PRIMARY KEY,
    vehicle_id INTEGER NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    speed_kph REAL NOT NULL DEFAULT 0,
    heading REAL NOT NULL DEFAULT 0,
    accuracy_meters REAL NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'offline',
    note TEXT NOT NULL DEFAULT '',
    recorded_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    FOREIGN KEY(vehicle_id) REFERENCES auto_vehicles(id) ON DELETE CASCADE
  )`);

  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN vehicle_id INTEGER'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN vehicleId INTEGER'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN latitude REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN longitude REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN heading REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN speed_kph REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN speedKph REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN accuracy_meters REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run('ALTER TABLE auto_vehicle_locations ADD COLUMN accuracyMeters REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicle_locations ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'"); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicle_locations ADD COLUMN status TEXT NOT NULL DEFAULT 'offline'"); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicle_locations ADD COLUMN note TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicle_locations ADD COLUMN recorded_at TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicle_locations ADD COLUMN recordedAt TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicle_locations ADD COLUMN created_by TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run("ALTER TABLE auto_vehicle_locations ADD COLUMN createdBy TEXT NOT NULL DEFAULT ''"); } catch (error) {}

  await run(`CREATE TABLE IF NOT EXISTS auto_tracking_devices (
    id INTEGER PRIMARY KEY,
    vehicleId INTEGER NOT NULL UNIQUE,
    deviceName TEXT NOT NULL DEFAULT 'smartphone',
    tokenHash TEXT NOT NULL,
    isActive INTEGER NOT NULL DEFAULT 1,
    lastSeenAt TEXT,
    lastLatitude REAL,
    lastLongitude REAL,
    lastSpeedKph REAL NOT NULL DEFAULT 0,
    createdBy TEXT NOT NULL,
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL,
    FOREIGN KEY(vehicleId) REFERENCES auto_vehicles(id) ON DELETE CASCADE
  )`);

  try { await run('ALTER TABLE auto_tracking_devices ADD COLUMN vehicleId INTEGER NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run("ALTER TABLE auto_tracking_devices ADD COLUMN deviceName TEXT NOT NULL DEFAULT 'smartphone'"); } catch (error) {}
  try { await run("ALTER TABLE auto_tracking_devices ADD COLUMN tokenHash TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run('ALTER TABLE auto_tracking_devices ADD COLUMN isActive INTEGER NOT NULL DEFAULT 1'); } catch (error) {}
  try { await run('ALTER TABLE auto_tracking_devices ADD COLUMN lastSeenAt TEXT'); } catch (error) {}
  try { await run('ALTER TABLE auto_tracking_devices ADD COLUMN lastLatitude REAL'); } catch (error) {}
  try { await run('ALTER TABLE auto_tracking_devices ADD COLUMN lastLongitude REAL'); } catch (error) {}
  try { await run('ALTER TABLE auto_tracking_devices ADD COLUMN lastSpeedKph REAL NOT NULL DEFAULT 0'); } catch (error) {}
  try { await run('ALTER TABLE auto_tracking_devices ADD COLUMN createdBy TEXT NOT NULL DEFAULT "system"'); } catch (error) {}
  try { await run("ALTER TABLE auto_tracking_devices ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (error) {}
  try { await run("ALTER TABLE auto_tracking_devices ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (error) {}

  await run(`CREATE TABLE IF NOT EXISTS auto_transport_costs (
    id INTEGER PRIMARY KEY,
    vehicleId INTEGER NOT NULL,
    expenseId INTEGER,
    niveauEssenceEntree REAL NOT NULL,
    niveauEssenceSortie REAL NOT NULL,
    prixLocalEssence REAL NOT NULL,
    quantiteConsommee REAL NOT NULL,
    montantTotal REAL NOT NULL,
    dateTransport TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    createdBy TEXT NOT NULL,
    createdAt TEXT NOT NULL,
    FOREIGN KEY(vehicleId) REFERENCES auto_vehicles(id) ON DELETE RESTRICT,
    FOREIGN KEY(expenseId) REFERENCES expenses(id) ON DELETE SET NULL
  )`);

  try {
    await run('ALTER TABLE auto_transport_costs ADD COLUMN expenseId INTEGER');
  } catch (error) {
    // Colonne deja presente ou table existante non compatible; ignore.
  }

  await run(`CREATE TABLE IF NOT EXISTS revenues (
    id INTEGER PRIMARY KEY,
    projetId INTEGER NOT NULL,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    dateRevenue TEXT NOT NULL,
    createdBy TEXT NOT NULL,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE CASCADE
  )`);

  await run(`CREATE TABLE IF NOT EXISTS generated_documents (
    id INTEGER PRIMARY KEY,
    sectionCode TEXT NOT NULL,
    sectionLabel TEXT NOT NULL,
    entityType TEXT NOT NULL,
    entityId INTEGER NOT NULL,
    title TEXT NOT NULL,
    fileName TEXT NOT NULL,
    relativePath TEXT NOT NULL,
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS guide_documents (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    fileName TEXT NOT NULL,
    relativePath TEXT NOT NULL,
    mimeType TEXT NOT NULL,
    sizeBytes INTEGER NOT NULL DEFAULT 0,
    contentBlob ${DB_DRIVER === 'postgres' ? 'BYTEA' : 'BLOB'},
    uploadedBy TEXT NOT NULL,
    audienceScope TEXT NOT NULL DEFAULT 'all',
    recipientEmployeeIds TEXT NOT NULL DEFAULT '',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS user_access_profiles (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    roleSnapshot TEXT NOT NULL DEFAULT '',
    accreditationLevel TEXT NOT NULL DEFAULT 'standard',
    allowedModules TEXT NOT NULL DEFAULT '',
    deniedModules TEXT NOT NULL DEFAULT '',
    forcedModule TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL,
    updatedBy TEXT NOT NULL DEFAULT 'system'
  )`);

  await run(`CREATE TABLE IF NOT EXISTS user_access_profile_audit (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    action TEXT NOT NULL,
    payloadJson TEXT NOT NULL,
    changedBy TEXT NOT NULL,
    createdAt TEXT NOT NULL
  )`);

  if (DB_DRIVER === 'postgres') {
    try {
      await run('ALTER TABLE generated_documents ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY');
    } catch (e) {
      // Deja configure en identity, ou adaptation impossible sur ce schema.
    }
    try {
      await run('ALTER TABLE guide_documents ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY');
    } catch (e) {
      // Deja configure en identity, ou adaptation impossible sur ce schema.
    }
    // Rendre materialRequestId nullable (les bons de commande autonomes n'ont pas de demande liee)
    try {
      await run('ALTER TABLE purchase_orders ALTER COLUMN materialRequestId DROP NOT NULL');
    } catch (e) {}
    // Rendre materialRequestId nullable dans purchase_order_items aussi
    try {
      await run('ALTER TABLE purchase_order_items ALTER COLUMN materialRequestId DROP NOT NULL');
    } catch (e) {}
  }

  try { await run("ALTER TABLE generated_documents ADD COLUMN sectionCode TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE generated_documents ADD COLUMN sectionLabel TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE generated_documents ADD COLUMN entityType TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run('ALTER TABLE generated_documents ADD COLUMN entityId INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run("ALTER TABLE generated_documents ADD COLUMN title TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE generated_documents ADD COLUMN fileName TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE generated_documents ADD COLUMN relativePath TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE generated_documents ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE generated_documents ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}

  try { await run("ALTER TABLE guide_documents ADD COLUMN title TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN fileName TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN relativePath TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN mimeType TEXT NOT NULL DEFAULT 'application/octet-stream'"); } catch (e) {}
  try { await run('ALTER TABLE guide_documents ADD COLUMN sizeBytes INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  if (DB_DRIVER === 'postgres') {
    try { await run('ALTER TABLE guide_documents ADD COLUMN contentBlob BYTEA'); } catch (_e) {}
  } else {
    try { await run('ALTER TABLE guide_documents ADD COLUMN contentBlob BLOB'); } catch (_e) {}
  }
  try { await run("ALTER TABLE guide_documents ADD COLUMN uploadedBy TEXT NOT NULL DEFAULT 'admin'"); } catch (e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN audienceScope TEXT NOT NULL DEFAULT 'all'"); } catch (e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN recipientEmployeeIds TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE guide_documents ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}

  try { await run("ALTER TABLE user_access_profiles ADD COLUMN roleSnapshot TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN accreditationLevel TEXT NOT NULL DEFAULT 'standard'"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN allowedModules TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN deniedModules TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN forcedModule TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN notes TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE user_access_profiles ADD COLUMN updatedBy TEXT NOT NULL DEFAULT 'system'"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS project_assignments (
    id INTEGER PRIMARY KEY,
    projectId INTEGER NOT NULL,
    userId INTEGER NOT NULL,
    employeeId INTEGER,
    assigneeName TEXT,
    phoneNumber TEXT,
    role TEXT NOT NULL,
    assignedAt TEXT NOT NULL,
    FOREIGN KEY(projectId) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY(userId) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(employeeId) REFERENCES hr_employees(id) ON DELETE SET NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS hr_employees (
    id INTEGER PRIMARY KEY,
    fullName TEXT NOT NULL,
    jobTitle TEXT NOT NULL DEFAULT '',
    sexe TEXT NOT NULL DEFAULT '',
    typeContrat TEXT NOT NULL DEFAULT '',
    dateEmbauche TEXT NOT NULL DEFAULT '',
    phoneNumber TEXT NOT NULL DEFAULT '',
    address TEXT NOT NULL DEFAULT '',
    maritalStatus TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    username TEXT NOT NULL DEFAULT '',
    createdBy TEXT NOT NULL DEFAULT 'admin',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
  )`);

  try { await run("ALTER TABLE hr_employees ADD COLUMN jobTitle TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN sexe TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN typeContrat TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN dateEmbauche TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN phoneNumber TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN address TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN maritalStatus TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN email TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN username TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN createdBy TEXT NOT NULL DEFAULT 'admin'"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employees ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS hr_employee_documents (
    id INTEGER PRIMARY KEY,
    employeeId INTEGER NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    fileName TEXT NOT NULL,
    relativePath TEXT NOT NULL,
    fileSize INTEGER NOT NULL DEFAULT 0,
    mimeType TEXT NOT NULL DEFAULT '',
    sourceModule TEXT NOT NULL DEFAULT 'employee_dossier',
    uploadedBy TEXT NOT NULL DEFAULT 'admin',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL,
    FOREIGN KEY(employeeId) REFERENCES hr_employees(id) ON DELETE CASCADE
  )`);

  try { await run('ALTER TABLE hr_employee_documents ADD COLUMN employeeId INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN title TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN fileName TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN relativePath TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run('ALTER TABLE hr_employee_documents ADD COLUMN fileSize INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN mimeType TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN sourceModule TEXT NOT NULL DEFAULT 'employee_dossier'"); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN uploadedBy TEXT NOT NULL DEFAULT 'admin'"); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_employee_documents ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS hr_attendance (
    id INTEGER PRIMARY KEY,
    employeeId INTEGER NOT NULL,
    attendanceDate TEXT NOT NULL,
    dayDate TEXT NOT NULL,
    checkInTime TEXT,
    checkOutTime TEXT,
    statusCode TEXT NOT NULL DEFAULT 'P',
    status TEXT NOT NULL DEFAULT 'P',
    note TEXT NOT NULL DEFAULT '',
    createdBy TEXT NOT NULL DEFAULT 'admin',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL,
    FOREIGN KEY(employeeId) REFERENCES hr_employees(id) ON DELETE CASCADE
  )`);

  try { await run('ALTER TABLE hr_attendance ADD COLUMN employeeId INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN attendanceDate TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN dayDate TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run('ALTER TABLE hr_attendance ADD COLUMN checkInTime TEXT'); } catch (e) {}
  try { await run('ALTER TABLE hr_attendance ADD COLUMN checkOutTime TEXT'); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN statusCode TEXT NOT NULL DEFAULT 'P'"); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN status TEXT NOT NULL DEFAULT 'P'"); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN location TEXT NOT NULL DEFAULT 'bureau'"); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN note TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN createdBy TEXT NOT NULL DEFAULT 'admin'"); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_attendance ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS hr_contracts (
    id INTEGER PRIMARY KEY,
    employeeId INTEGER NOT NULL,
    contractStartDate TEXT NOT NULL,
    contractEndDate TEXT NOT NULL,
    reminderDate TEXT NOT NULL DEFAULT '',
    reminderNote TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ACTIF',
    createdBy TEXT NOT NULL DEFAULT 'admin',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL,
    FOREIGN KEY(employeeId) REFERENCES hr_employees(id) ON DELETE CASCADE
  )`);
  try { await run('ALTER TABLE hr_contracts ADD COLUMN employeeId INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN contractStartDate TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN contractEndDate TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN reminderDate TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN reminderNote TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIF'"); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN createdBy TEXT NOT NULL DEFAULT 'admin'"); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_contracts ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS hr_leave_requests (
    id INTEGER PRIMARY KEY,
    employeeId INTEGER NOT NULL,
    leaveType TEXT NOT NULL,
    startDate TEXT NOT NULL,
    endDate TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'EN_ATTENTE',
    decisionNote TEXT NOT NULL DEFAULT '',
    createdBy TEXT NOT NULL DEFAULT 'admin',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL,
    decidedBy TEXT,
    decidedAt TEXT,
    FOREIGN KEY(employeeId) REFERENCES hr_employees(id) ON DELETE CASCADE
  )`);

  try { await run('ALTER TABLE hr_leave_requests ADD COLUMN employeeId INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN leaveType TEXT NOT NULL DEFAULT 'annuel'"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN startDate TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN endDate TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN reason TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN status TEXT NOT NULL DEFAULT 'EN_ATTENTE'"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN decisionNote TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN createdBy TEXT NOT NULL DEFAULT 'admin'"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_leave_requests ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run('ALTER TABLE hr_leave_requests ADD COLUMN decidedBy TEXT'); } catch (e) {}
  try { await run('ALTER TABLE hr_leave_requests ADD COLUMN decidedAt TEXT'); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS hr_document_signatures (
    id INTEGER PRIMARY KEY,
    documentId INTEGER NOT NULL,
    employeeId INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    signatureData TEXT,
    signedAt TEXT,
    signedBy TEXT,
    signedPdfFileName TEXT,
    signedPdfRelativePath TEXT,
    rejectionReason TEXT,
    createdBy TEXT NOT NULL DEFAULT 'admin',
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL,
    FOREIGN KEY(documentId) REFERENCES hr_employee_documents(id) ON DELETE CASCADE,
    FOREIGN KEY(employeeId) REFERENCES hr_employees(id) ON DELETE CASCADE
  )`);

  try { await run('ALTER TABLE hr_document_signatures ADD COLUMN documentId INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run('ALTER TABLE hr_document_signatures ADD COLUMN employeeId INTEGER NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run("ALTER TABLE hr_document_signatures ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"); } catch (e) {}
  try { await run('ALTER TABLE hr_document_signatures ADD COLUMN signatureData TEXT'); } catch (e) {}
  try { await run('ALTER TABLE hr_document_signatures ADD COLUMN signedAt TEXT'); } catch (e) {}
  try { await run('ALTER TABLE hr_document_signatures ADD COLUMN signedBy TEXT'); } catch (e) {}
  try { await run("ALTER TABLE hr_document_signatures ADD COLUMN signedPdfFileName TEXT"); } catch (e) {}
  try { await run("ALTER TABLE hr_document_signatures ADD COLUMN signedPdfRelativePath TEXT"); } catch (e) {}
  try { await run('ALTER TABLE hr_document_signatures ADD COLUMN rejectionReason TEXT'); } catch (e) {}
  try { await run("ALTER TABLE hr_document_signatures ADD COLUMN createdBy TEXT NOT NULL DEFAULT 'admin'"); } catch (e) {}
  try { await run("ALTER TABLE hr_document_signatures ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run("ALTER TABLE hr_document_signatures ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''"); } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS project_progress_updates (
    id INTEGER PRIMARY KEY,
    projectId INTEGER NOT NULL,
    stage TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL,
    materialUsedQty REAL NOT NULL DEFAULT 0,
    materialUsageDetails TEXT NOT NULL DEFAULT '[]',
    progressPercent REAL,
    createdBy TEXT NOT NULL,
    createdAt TEXT NOT NULL,
    FOREIGN KEY(projectId) REFERENCES projects(id) ON DELETE CASCADE
  )`);

  try { await run("ALTER TABLE project_progress_updates ADD COLUMN title TEXT NOT NULL DEFAULT ''"); } catch (e) {}
  try { await run('ALTER TABLE project_progress_updates ADD COLUMN materialUsedQty REAL NOT NULL DEFAULT 0'); } catch (e) {}
  try { await run('ALTER TABLE project_progress_updates ADD COLUMN materialUsageDetails TEXT NOT NULL DEFAULT "[]"'); } catch (e) {}
  try { await run('ALTER TABLE project_progress_updates ADD COLUMN progressPercent REAL'); } catch (e) {}
  // Rendre materialRequestId nullable dans purchase_order_items pour les BC sans demande
  try {
    const poiCols = await all("PRAGMA table_info(purchase_order_items)");
    const mrCol = poiCols.find(c => c.name === 'materialRequestId');
    if (mrCol && mrCol.notnull === 1) {
      await run('ALTER TABLE purchase_order_items RENAME TO purchase_order_items_old');
      await run(`CREATE TABLE purchase_order_items (
        id INTEGER PRIMARY KEY,
        purchaseOrderId INTEGER NOT NULL,
        materialRequestId INTEGER,
        article TEXT NOT NULL,
        details TEXT,
        quantite REAL NOT NULL,
        prixUnitaire REAL NOT NULL,
        totalLigne REAL NOT NULL,
        FOREIGN KEY(purchaseOrderId) REFERENCES purchase_orders(id) ON DELETE CASCADE
      )`);
      await run('INSERT INTO purchase_order_items SELECT * FROM purchase_order_items_old');
      await run('DROP TABLE purchase_order_items_old');
    }
  } catch (e) {}
  // Rendre materialRequestId nullable dans purchase_orders pour les BC sans demande
  try {
    const poCols = await all("PRAGMA table_info(purchase_orders)");
    const mrCol = poCols.find(c => c.name === 'materialRequestId');
    if (mrCol && mrCol.notnull === 1) {
      await run('ALTER TABLE purchase_orders RENAME TO purchase_orders_old');
      await run(`CREATE TABLE purchase_orders (
        id INTEGER PRIMARY KEY,
        materialRequestId INTEGER,
        fournisseur TEXT NOT NULL,
        quantiteCommandee REAL NOT NULL DEFAULT 1,
        prixUnitaire REAL NOT NULL DEFAULT 0,
        dateCommande TEXT NOT NULL,
        dateLivraisonPrevue TEXT,
        dateReception TEXT,
        statut TEXT NOT NULL DEFAULT 'EN_COURS',
        creePar TEXT,
        montantTotal REAL,
        statutValidation TEXT,
        projetId INTEGER,
        nomProjetManuel TEXT,
        siteId INTEGER,
        nomSiteManuel TEXT,
        warehouseId TEXT
      )`);
      // Copier en listant les colonnes connues
      const oldCols = poCols.map(c => c.name);
      const newColNames = ['id','materialRequestId','fournisseur','quantiteCommandee','prixUnitaire','dateCommande','dateLivraisonPrevue','dateReception','statut','creePar','montantTotal','statutValidation','projetId','nomProjetManuel','siteId','nomSiteManuel','warehouseId'];
      const copyColNames = newColNames.filter(c => oldCols.includes(c));
      const colList = copyColNames.join(', ');
      await run(`INSERT INTO purchase_orders (${colList}) SELECT ${colList} FROM purchase_orders_old`);
      await run('DROP TABLE purchase_orders_old');
    }
  } catch (e) {}
  try { await run('ALTER TABLE purchase_orders ADD COLUMN warehouseId TEXT'); } catch (e) {}

  try {
  await run('CREATE INDEX IF NOT EXISTS idx_projects_site_house ON projects(nomSite, numeroMaison)');
  await run('CREATE UNIQUE INDEX IF NOT EXISTS idx_project_folders_name_prefecture ON project_folders(nomProjet, prefecture)');
  await run('CREATE UNIQUE INDEX IF NOT EXISTS idx_project_catalog_name ON project_catalog(nomProjet)');
  await run('CREATE INDEX IF NOT EXISTS idx_project_folders_project_prefecture ON project_folders(projectId, prefecture)');
  await run('CREATE INDEX IF NOT EXISTS idx_material_requests_project_status ON material_requests(projetId, statut)');
  await run('CREATE INDEX IF NOT EXISTS idx_purchase_orders_status_date ON purchase_orders(statut, dateCommande)');
  await run('CREATE INDEX IF NOT EXISTS idx_purchase_order_items_order ON purchase_order_items(purchaseOrderId)');
  await run('CREATE INDEX IF NOT EXISTS idx_expenses_project_status_date ON expenses(projetId, statut, dateExpense)');
  await run('CREATE INDEX IF NOT EXISTS idx_revenues_project_date ON revenues(projetId, dateRevenue)');
  await run('CREATE INDEX IF NOT EXISTS idx_project_assignments_project_user ON project_assignments(projectId, userId)');
  await run('CREATE INDEX IF NOT EXISTS idx_hr_employees_name ON hr_employees(fullName)');
  await run('CREATE INDEX IF NOT EXISTS idx_hr_employee_documents_employee ON hr_employee_documents(employeeId, updatedAt DESC)');
  await run('CREATE INDEX IF NOT EXISTS idx_hr_attendance_employee_date ON hr_attendance(employeeId, attendanceDate)');
  await run('CREATE INDEX IF NOT EXISTS idx_hr_attendance_daydate ON hr_attendance(dayDate)');
  await run('CREATE INDEX IF NOT EXISTS idx_hr_leave_employee_dates ON hr_leave_requests(employeeId, startDate, endDate)');
  await run('CREATE INDEX IF NOT EXISTS idx_hr_leave_status ON hr_leave_requests(status)');
  await run('CREATE INDEX IF NOT EXISTS idx_project_progress_project_date ON project_progress_updates(projectId, createdAt DESC)');
  await run('CREATE INDEX IF NOT EXISTS idx_stock_issues_project_date ON stock_issues(projetId, issuedAt)');
  await run('CREATE INDEX IF NOT EXISTS idx_generated_documents_section_updated ON generated_documents(sectionCode, updatedAt)');
  await run('CREATE INDEX IF NOT EXISTS idx_auto_vehicle_locations_vehicle_recorded ON auto_vehicle_locations(vehicle_id, recorded_at DESC)');
  await run('CREATE UNIQUE INDEX IF NOT EXISTS idx_auto_tracking_devices_vehicle ON auto_tracking_devices(vehicleId)');
  await run('CREATE INDEX IF NOT EXISTS idx_auto_tracking_devices_token_active ON auto_tracking_devices(tokenHash, isActive)');
  } catch (e) { console.error('CREATE INDEX error:', e.message); }

  const getNextUserId = async () => {
    const row = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM users');
    return Number(row?.nextId || 1);
  };

  const ensureHrEmployeeProfile = async ({ username, fullName, jobTitle, phoneNumber = '', address = '', maritalStatus = '', email = '', createdBy = username }) => {
    const usernameValue = String(username || '').trim();
    if (!usernameValue) return;

    const existing = await get('SELECT * FROM hr_employees WHERE username = ? OR createdBy = ? OR fullName = ? ORDER BY id ASC LIMIT 1', [usernameValue, usernameValue, fullName || usernameValue]);
    const now = new Date().toISOString();
    if (existing) {
      await run(
        "UPDATE hr_employees SET fullName = ?, jobTitle = ?, phoneNumber = ?, address = ?, maritalStatus = ?, email = COALESCE(NULLIF(?, ''), email), createdBy = COALESCE(NULLIF(?, ''), createdBy), updatedAt = ? WHERE id = ?",
        [
          String(fullName || usernameValue).trim(),
          String(jobTitle || '').trim(),
          String(phoneNumber || '').trim(),
          String(address || '').trim(),
          String(maritalStatus || '').trim(),
          String(email || '').trim(),
          String(createdBy || usernameValue).trim(),
          now,
          Number(existing.id),
        ]
      );
      return Number(existing.id);
    }

    const nextId = await getNextTableId('hr_employees');
    await run(
      'INSERT INTO hr_employees (id, fullName, jobTitle, phoneNumber, address, maritalStatus, email, username, createdBy, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        nextId,
        String(fullName || usernameValue).trim(),
        String(jobTitle || '').trim(),
        String(phoneNumber || '').trim(),
        String(address || '').trim(),
        String(maritalStatus || '').trim(),
        String(email || '').trim(),
        usernameValue,
        String(createdBy || usernameValue).trim() || usernameValue,
        now,
        now,
      ]
    );
    return nextId;
  };

  // Créer utilisateur admin par défaut
  const admin = await get('SELECT * FROM users WHERE username = ?', ['admin']);
  if (!admin) {
    const hashedPassword = await bcrypt.hash('admin123', 10);
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, 'admin', hashedPassword, 'admin', new Date().toISOString()]
    );
    console.log('Utilisateur admin créé avec mot de passe admin123');
  }

  const executive = await get('SELECT id FROM users WHERE username = ?', [EXECUTIVE_USERNAME]);
  const executiveHashedPassword = await bcrypt.hash(EXECUTIVE_PASSWORD, 10);
  if (!executive) {
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, EXECUTIVE_USERNAME, executiveHashedPassword, 'dirigeant', new Date().toISOString()]
    );
    console.log(`Utilisateur ${EXECUTIVE_USERNAME} cree avec role dirigeant`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [executiveHashedPassword, 'dirigeant', EXECUTIVE_USERNAME]
    );
    console.log(`Utilisateur ${EXECUTIVE_USERNAME} mis a jour avec role dirigeant`);
  }

  const hrDirector = await get('SELECT id FROM users WHERE username = ?', [HR_DIRECTOR_USERNAME]);
  const hrDirectorHashedPassword = await bcrypt.hash(HR_DIRECTOR_PASSWORD, 10);
  if (!hrDirector) {
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, HR_DIRECTOR_USERNAME, hrDirectorHashedPassword, 'directeur_rh', new Date().toISOString()]
    );
    console.log(`Utilisateur ${HR_DIRECTOR_USERNAME} cree avec role directeur_rh`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [hrDirectorHashedPassword, 'directeur_rh', HR_DIRECTOR_USERNAME]
    );
    console.log(`Utilisateur ${HR_DIRECTOR_USERNAME} mis a jour avec role directeur_rh`);
  }

  // Garantir un compte commis_stock toujours opérationnel (local + Railway)
  const commis = await get('SELECT id FROM users WHERE username = ?', [COMMIS_STOCK_USERNAME]);

  // Garantir le compte achat toujours opérationnel (local + Railway)
  const achatUser = await get('SELECT id FROM users WHERE username = ?', [ACHAT_USERNAME]);
  const achatHashedPassword = await bcrypt.hash(ACHAT_PASSWORD, 10);
  if (!achatUser) {
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, ACHAT_USERNAME, achatHashedPassword, 'achat', new Date().toISOString()]
    );
    console.log(`Utilisateur ${ACHAT_USERNAME} cree avec role achat`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [achatHashedPassword, 'achat', ACHAT_USERNAME]
    );
    console.log(`Utilisateur ${ACHAT_USERNAME} mis a jour avec role achat`);
  }
  const commisHashedPassword = await bcrypt.hash(COMMIS_STOCK_PASSWORD, 10);
  if (!commis) {
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, COMMIS_STOCK_USERNAME, commisHashedPassword, 'commis', new Date().toISOString()]
    );
    console.log(`Utilisateur ${COMMIS_STOCK_USERNAME} créé avec role commis`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [commisHashedPassword, 'commis', COMMIS_STOCK_USERNAME]
    );
    console.log(`Utilisateur ${COMMIS_STOCK_USERNAME} mis a jour avec role commis`);
  }

  // Garantir le compte chef de chantier site 15 (local + Railway)
  const siteChiefUsername = 'chef_chantier_site15';
  const siteChiefRole = 'chef_chantier_site';
  const siteChiefPassword = process.env.CHEF_SITE15_PASSWORD || 'chefsite15@123';
  const siteChief = await get('SELECT id FROM users WHERE username = ?', [siteChiefUsername]);
  const siteChiefHashedPassword = await bcrypt.hash(siteChiefPassword, 10);
  if (!siteChief) {
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, siteChiefUsername, siteChiefHashedPassword, siteChiefRole, new Date().toISOString()]
    );
    console.log(`Utilisateur ${siteChiefUsername} cree avec role ${siteChiefRole}`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [siteChiefHashedPassword, siteChiefRole, siteChiefUsername]
    );
    console.log(`Utilisateur ${siteChiefUsername} mis a jour avec role ${siteChiefRole}`);
  }

  // Garantir le compte contrôle achat (local + Railway)
  const procurementReviewer = await get('SELECT id FROM users WHERE username = ?', [PROCUREMENT_REVIEWER_USERNAME]);
  const procurementReviewerHashedPassword = await bcrypt.hash(PROCUREMENT_REVIEWER_PASSWORD, 10);
  if (!procurementReviewer) {
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, PROCUREMENT_REVIEWER_USERNAME, procurementReviewerHashedPassword, 'controle_achat', new Date().toISOString()]
    );
    console.log(`Utilisateur ${PROCUREMENT_REVIEWER_USERNAME} cree avec role controle_achat`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [procurementReviewerHashedPassword, 'controle_achat', PROCUREMENT_REVIEWER_USERNAME]
    );
    console.log(`Utilisateur ${PROCUREMENT_REVIEWER_USERNAME} mis a jour avec role controle_achat`);
  }

  await ensureHrEmployeeProfile({
    username: HR_DIRECTOR_USERNAME,
    fullName: 'Directeur RH',
    jobTitle: 'Directeur RH',
    createdBy: 'admin',
  });
  await ensureHrEmployeeProfile({
    username: PROCUREMENT_REVIEWER_USERNAME,
    fullName: 'Contrôle Achat',
    jobTitle: 'Contrôle achat',
    createdBy: PROCUREMENT_REVIEWER_USERNAME,
  });
  await ensureHrEmployeeProfile({
    username: siteChiefUsername,
    fullName: 'Chef chantier site 15',
    jobTitle: 'Chef chantier site',
    createdBy: siteChiefUsername,
  });

  // Garantir le profil KOKAN (gestionnaire stock Songon)
  const kokan = await get('SELECT id FROM users WHERE username = ?', [KOKAN_USERNAME]);
  const kokanHashedPassword = await bcrypt.hash(KOKAN_PASSWORD, 10);
  if (!kokan) {
    const nextUserId = await getNextUserId();
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextUserId, KOKAN_USERNAME, kokanHashedPassword, 'gestionnaire_stock_songon', new Date().toISOString()]
    );
    console.log(`Utilisateur ${KOKAN_USERNAME} cree avec role gestionnaire_stock_songon`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [kokanHashedPassword, 'gestionnaire_stock_songon', KOKAN_USERNAME]
    );
    console.log(`Utilisateur ${KOKAN_USERNAME} mis a jour avec role gestionnaire_stock_songon`);
  }

  await ensureHrEmployeeProfile({
    username: KOKAN_USERNAME,
    fullName: 'KOKAN',
    jobTitle: 'Gestionnaire de Stock Songon',
    createdBy: 'admin',
  });

  const identitySync = await reconcileIdentityDirectory();
  console.log(`Annuaire profils synchronise: users +${identitySync.createdUsers}/~${identitySync.updatedUsers}/-${identitySync.deletedUsers}, RH +${identitySync.createdHr}/~${identitySync.updatedHr}/-${identitySync.deletedHr || 0}`);
}

// Keep HTTP reachable immediately; DB init runs in background once.
const INIT_DB_TIMEOUT_MS = Number(process.env.INIT_DB_TIMEOUT_MS || 120_000);
let hasRunBackgroundReconciliations = false;

async function backfillGuideDocumentBlobs() {
  try {
    const rows = await all('SELECT id, relativePath, sizeBytes FROM guide_documents WHERE contentBlob IS NULL OR sizeBytes = 0');
    if (!Array.isArray(rows) || !rows.length) return;
    let backfilled = 0;
    for (const row of rows) {
      const relPath = String(row?.relativePath || '').trim();
      if (!relPath) continue;
      try {
        const absPath = path.join(ARCHIVE_ROOT, relPath);
        if (fs.existsSync(absPath)) {
          const buf = await fs.promises.readFile(absPath);
          if (buf && buf.length > 0) {
            await run('UPDATE guide_documents SET contentBlob = ?, sizeBytes = ? WHERE id = ?', [buf, buf.length, Number(row.id || 0)]);
            backfilled++;
          }
        }
      } catch (_e) {}
    }
    if (backfilled > 0) {
      console.log(`Guide blobs backfilles depuis le disque: ${backfilled}`);
    }
  } catch (_e) {}
}

function initDbWithTimeout() {
  return Promise.race([
    initDb(),
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error(`initDb timeout after ${INIT_DB_TIMEOUT_MS}ms`)), INIT_DB_TIMEOUT_MS);
    }),
  ]);
}

function runBackgroundReconciliationsOnce() {
  if (hasRunBackgroundReconciliations) {
    return;
  }

  hasRunBackgroundReconciliations = true;

  setImmediate(() => {
    reconcilePurchaseOrderExpenses().catch(err => {
      console.error('Erreur reconciliation bons/depenses:', err);
    });
  });

  setImmediate(() => {
    reconcileDocumentArchives().catch(err => {
      console.error('Erreur reconciliation archives:', err);
    });
  });

  setImmediate(() => {
    backfillGuideDocumentBlobs().catch(err => {
      console.error('Erreur backfill guide blobs:', err);
    });
  });
}

async function initializeDatabaseOnce() {
  try {
    await initDbWithTimeout();
    isReady = true;
    console.log('Initialisation base de donnees terminee. API ready.');
    runBackgroundReconciliationsOnce();
  } catch (error) {
    isReady = false;
    console.error('Erreur d\'initialisation de la base de données', error.stack || error);
  }
}

server = app.listen(PORT, () => {
  console.log(`API Construction & Logistique démarrée sur http://localhost:${PORT}`);

  if (LOCALHOST_FALLBACK_PORT > 0 && LOCALHOST_FALLBACK_PORT !== PORT) {
    localhostFallbackServer = app.listen(LOCALHOST_FALLBACK_PORT, () => {
      console.log(`Compatibilite locale active sur http://localhost:${LOCALHOST_FALLBACK_PORT}`);
    });
    localhostFallbackServer.on('error', error => {
      console.error(`Impossible d'activer la compatibilite localhost:${LOCALHOST_FALLBACK_PORT}:`, error.message || error);
    });
  }

  initializeDatabaseOnce().catch(error => {
    console.error('Erreur inattendue initDb:', error.stack || error);
  });
});

async function shutdown(signal) {
  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;
  isReady = false;
  console.log(`Signal ${signal} recu. Arret en cours...`);

  try {
    if (localhostFallbackServer) {
      await new Promise(resolve => localhostFallbackServer.close(resolve));
      localhostFallbackServer = null;
    }

    if (server) {
      await new Promise(resolve => server.close(resolve));
      server = null;
    }
  } catch (error) {
    console.error('Erreur fermeture serveur HTTP:', error);
  }

  try {
    await dbClient.close();
  } catch (error) {
    console.error('Erreur fermeture base de donnees:', error);
    process.exit(1);
    return;
  }

  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT').catch(() => process.exit(1));
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM').catch(() => process.exit(1));
});

process.on('uncaughtException', error => {
  console.error('Erreur non capturee:', error);
});

process.on('unhandledRejection', reason => {
  console.error('Promesse rejetee non geree:', reason);
});

const PRIVILEGED_ROLES = new Set(['admin']);
const ADMIN_ONLY_MODULES = new Set(['access-profiles', 'admin-mail']);
const RH_GUIDE_BASE_MODULES = new Set(['hr-employees', 'hr-employee-search', 'hr-attendance', 'hr-contracts', 'hr-calendar', 'hr-leave', 'guide-erp']);
const MODULE_ACCESS_ROUTE_RULES = {
  dashboard: [{ method: 'GET', pattern: /^\/auth\/me$/ }],
  projects: [
    { method: 'GET', pattern: /^\/projects$/ },
    { method: 'GET', pattern: /^\/project-folders$/ },
    { method: 'GET', pattern: /^\/project-catalog$/ },
    { method: 'GET', pattern: /^\/project-assignments$/ },
  ],
  'project-progress': [{ method: 'GET', pattern: /^\/project-progress$/ }],
  'journal-chantier': [{ method: 'GET', pattern: /^\/project-progress$/ }],
  materials: [
    { method: 'GET', pattern: /^\/material-requests$/ },
    { method: 'POST', pattern: /^\/material-requests$/ },
  ],
  inventory: [{ method: 'GET', pattern: /^\/stock-management\/(available|issues|orders)$/ }],
  'purchase-orders': [{ method: 'GET', pattern: /^\/purchase-orders(?:\/\d+\/pdf)?$/ }],
  'hr-contracts': [
    { method: 'GET', pattern: /^\/hr\/contracts$/ },
    { method: 'POST', pattern: /^\/hr\/contracts$/ },
    { method: 'PATCH', pattern: /^\/hr\/contracts\/\d+$/ },
    { method: 'DELETE', pattern: /^\/hr\/contracts\/\d+$/ },
  ],
  'hr-signatures': [
    { method: 'GET', pattern: /^\/hr\/signature-requests$/ },
    { method: 'POST', pattern: /^\/hr\/signature-requests$/ },
    { method: 'DELETE', pattern: /^\/hr\/signature-requests\/\d+$/ },
    { method: 'POST', pattern: /^\/hr\/signature-requests\/\d+\/sign$/ },
    { method: 'GET', pattern: /^\/hr\/signature-requests\/\d+\/download$/ },
    { method: 'GET', pattern: /^\/hr\/employee-profile\/pending-signatures$/ },
    { method: 'GET', pattern: /^\/hr\/document-signatures\/\d+$/ },
  ],
  'access-profiles': [],
  'stock-management': [
    { method: 'GET', pattern: /^\/stock-management\/(available|issues|orders)$/ },
    { method: 'PATCH', pattern: /^\/stock-management\/orders\/\d+\/arrive$/ },
  ],
  'sortie-autorisations': [
    { method: 'GET', pattern: /^\/stock-issue-authorizations(?:\/\d+\/pdf)?$/ },
    { method: 'POST', pattern: /^\/stock-issue-authorizations$/ },
    { method: 'PATCH', pattern: /^\/stock-issue-authorizations\/\d+\/decision$/ },
  ],
  'material-catalog': [{ method: 'GET', pattern: /^\/material-catalog$/ }],
  'parc-auto': [{ method: 'GET', pattern: /^\/(vehicles|auto-vehicle-locations|auto-transport-costs)$/ }],
  maps: [{ method: 'GET', pattern: /^\/(vehicles|auto-vehicle-locations|auto-transport-costs)$/ }],
  expenses: [{ method: 'GET', pattern: /^\/expenses$/ }],
  revenues: [{ method: 'GET', pattern: /^\/revenues$/ }],
  reports: [
    { method: 'GET', pattern: /^\/expenses$/ },
    { method: 'GET', pattern: /^\/revenues$/ },
    { method: 'GET', pattern: /^\/purchase-orders$/ },
  ],
  database: [{ method: 'GET', pattern: /^\/database-documents(?:\/\d+\/download)?$/ }],
  users: [{ method: 'GET', pattern: /^\/users$/ }],
  'admin-mail': [
    { method: 'GET', pattern: /^\/admin\/mail\/recipients$/ },
    { method: 'POST', pattern: /^\/admin\/mail\/send$/ },
  ],
  assignments: [{ method: 'GET', pattern: /^\/project-assignments$/ }],
  trash: [
    { method: 'DELETE', pattern: /^\/material-requests\/\d+$/ },
    { method: 'DELETE', pattern: /^\/database-documents\/\d+$/ },
  ],
};

function roleCanBypassRestrictedProfile(role) {
  return PRIVILEGED_ROLES.has(String(role || '').trim());
}

function parseCsvSet(value) {
  return new Set(
    String(value || '')
      .split(',')
      .map(item => String(item || '').trim())
      .filter(Boolean)
  );
}

function serializeCsvSet(values) {
  return Array.from(new Set(Array.from(values || []).map(item => String(item || '').trim()).filter(Boolean))).join(',');
}

function normalizeModuleList(value) {
  return new Set(
    Array.from(parseCsvSet(value)).map(item => item.toLowerCase()).filter(item => Object.prototype.hasOwnProperty.call(MODULE_ACCESS_ROUTE_RULES, item) || RH_GUIDE_BASE_MODULES.has(item))
  );
}

function sanitizeAccessProfileModulesForTargetRole(modulesSet, targetRole) {
  const normalizedRole = String(targetRole || '').trim().toLowerCase();
  const sanitized = new Set(Array.from(modulesSet || []).map(item => String(item || '').trim().toLowerCase()).filter(Boolean));
  if (normalizedRole !== 'admin') {
    for (const moduleKey of ADMIN_ONLY_MODULES) {
      sanitized.delete(moduleKey);
    }
  }
  return sanitized;
}

function computeEffectiveModulesForAccessProfile(profileRow) {
  const allowed = normalizeModuleList(profileRow?.allowedModules || '');
  const denied = normalizeModuleList(profileRow?.deniedModules || '');
  const hasStrictAllowedModules = String(profileRow?.allowedModules || '').trim().length > 0;
  const effective = hasStrictAllowedModules ? new Set(allowed) : new Set(RH_GUIDE_BASE_MODULES);
  for (const moduleKey of denied) {
    effective.delete(moduleKey);
  }
  const forcedModule = String(profileRow?.forcedModule || '').trim().toLowerCase();
  if (forcedModule && !denied.has(forcedModule)) {
    effective.add(forcedModule);
  }
  return effective;
}

function getAccessProfileBaselineModules(role, username) {
  const normalizedRole = String(role || '').trim().toLowerCase();
  const normalizedUsername = normalizeUserKey(username || '');
  const presets = {
    admin: [
      'dashboard', 'projects', 'project-progress', 'journal-chantier', 'materials', 'inventory', 'purchase-orders', 'stock-management', 'sortie-autorisations',
      'material-catalog', 'parc-auto', 'expenses', 'revenues', 'reports', 'maps', 'database', 'guide-erp', 'access-profiles', 'admin-mail', 'trash', 'assignments',
      'hr-employees', 'hr-attendance', 'hr-calendar', 'hr-leave', 'hr-signatures', 'users', 'settings', 'audit-log'
    ],
    directeur_rh: ['dashboard', 'hr-employees', 'hr-employee-search', 'hr-attendance', 'hr-contracts', 'hr-calendar', 'hr-leave', 'hr-signatures', 'database', 'guide-erp'],
    dirigeant: ['dashboard', 'projects', 'project-progress', 'journal-chantier', 'inventory', 'purchase-orders', 'sortie-autorisations', 'material-catalog', 'expenses', 'revenues', 'reports', 'maps', 'hr-employee-search', 'guide-erp'],
    achat: ['projects', 'purchase-orders', 'sortie-autorisations', 'inventory', 'database', 'trash', 'hr-employee-search', 'guide-erp'],
    controle_achat: ['purchase-orders', 'inventory', 'projects', 'assignments', 'hr-employees', 'hr-attendance', 'hr-calendar', 'hr-leave', 'material-catalog', 'stock-management', 'database', 'trash', 'hr-employee-search', 'guide-erp'],
    controle_achat_global: ['purchase-orders', 'inventory', 'projects', 'assignments', 'hr-employees', 'hr-attendance', 'hr-calendar', 'hr-leave', 'material-catalog', 'stock-management', 'database', 'trash', 'hr-employee-search', 'guide-erp'],
    commis: ['stock-management', 'inventory', 'hr-employee-search', 'guide-erp'],
    gestionnaire_stock: ['stock-management', 'inventory', 'sortie-autorisations', 'hr-employee-search', 'guide-erp'],
    gestionnaire_stock_zone: ['stock-management', 'inventory', 'sortie-autorisations', 'purchase-orders', 'material-catalog', 'database', 'materials', 'trash', 'projects', 'journal-chantier', 'hr-employee-search', 'guide-erp'],
    gestionnaire_stock_songon: ['stock-management', 'inventory', 'sortie-autorisations', 'purchase-orders', 'material-catalog', 'database', 'materials', 'trash', 'projects', 'journal-chantier', 'hr-employees', 'hr-attendance', 'hr-calendar', 'hr-leave', 'hr-employee-search', 'guide-erp'],
    chef_chantier_site: ['materials', 'material-catalog', 'stock-management', 'sortie-autorisations', 'inventory', 'journal-chantier', 'assignments', 'hr-employees', 'hr-attendance', 'hr-calendar', 'hr-leave', 'database', 'trash', 'hr-employee-search', 'guide-erp'],
    employe_standard: ['dashboard', 'hr-employees', 'hr-attendance', 'hr-calendar', 'hr-leave', 'hr-employee-search', 'database', 'trash', 'guide-erp'],
  };

  const preset = presets[normalizedRole] || [];
  const normalizedPreset = Array.from(new Set(preset.map(item => String(item || '').trim().toLowerCase()).filter(Boolean)));
  if (normalizedRole === 'gestionnaire_stock_songon' && normalizedUsername === 'kokan_sk') {
    return new Set(normalizedPreset);
  }
  return new Set(normalizedPreset);
}

function serializeModuleSetForResponse(value) {
  return Array.from(normalizeModuleList(Array.from(value || []).join(',')));
}

async function getUserAccessProfileByUsername(username) {
  const safeUsername = String(username || '').trim();
  if (!safeUsername) return null;
  return get('SELECT * FROM user_access_profiles WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) LIMIT 1', [safeUsername]);
}

function getRoleDefaultJobTitle(role) {
  const normalizedRole = String(role || '').trim().toLowerCase();
  const labels = {
    admin: 'Administrateur',
    directeur_rh: 'Directeur RH',
    dirigeant: 'Dirigeant',
    achat: 'Achat',
    commis: 'Commis',
    controle_achat: 'Controle achat',
    controle_achat_global: 'Controle achat global',
    chef_chantier_site: 'Chef chantier site',
    gestionnaire_stock: 'Gestionnaire stock',
    gestionnaire_stock_zone: 'Gestionnaire stock zone',
    gestionnaire_stock_songon: 'Gestionnaire stock Songon',
    employe_standard: 'Employe standard',
  };
  return labels[normalizedRole] || 'Employe';
}

async function ensureHrProfileForUserAccount(userRow, actor = 'system') {
  const username = String(userRow?.username || '').trim();
  if (!username) return null;

  const existingProfile = await get(
    `SELECT id, fullName, username, createdBy
     FROM hr_employees
     WHERE LOWER(TRIM(COALESCE(username, ''))) = LOWER(TRIM(?))
        OR LOWER(TRIM(COALESCE(createdBy, ''))) = LOWER(TRIM(?))
     ORDER BY updatedAt DESC, id DESC
     LIMIT 1`,
    [username, username]
  );
  if (existingProfile?.id) return existingProfile;

  const now = new Date().toISOString();
  const nextId = await getNextTableId('hr_employees');
  const defaultRoleLabel = getRoleDefaultJobTitle(userRow?.role);
  await run(
    `INSERT INTO hr_employees (id, fullName, jobTitle, phoneNumber, address, maritalStatus, email, username, createdBy, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextId,
      String(userRow?.fullName || username).trim(),
      defaultRoleLabel,
      '',
      '',
      '',
      '',
      username,
      String(actor || username).trim() || username,
      now,
      now,
    ]
  );

  return get('SELECT id, fullName, username, createdBy FROM hr_employees WHERE id = ?', [nextId]);
}

function authenticateToken(req, res, next) {
  const authHeader = req.headers.authorization;
  const queryToken = String(req.query?.mobileAuth || req.query?.accessToken || req.query?.token || '').trim();
  let token = '';

  if (authHeader) {
    token = authHeader.split(' ')[1] || '';
  }

  if (!token && queryToken) {
    token = queryToken;
  }

  if (!token) {
    return res.status(401).json({ error: 'Token manquant' });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(401).json({ error: 'Token invalide' });
    }
    req.user = user;
    next();
  });
}

function isRouteAllowedByModuleSet(method, pathName, modulesSet) {
  const safeMethod = String(method || '').toUpperCase();
  const safePath = String(pathName || '');
  for (const moduleKey of modulesSet) {
    const rules = MODULE_ACCESS_ROUTE_RULES[moduleKey] || [];
    if (rules.some(rule => rule.method === safeMethod && rule.pattern.test(safePath))) {
      return true;
    }
  }
  return false;
}

function buildAccessProfilePayloadForUser(userRow, accessProfileRow) {
  const role = String(userRow?.role || '').trim();
  const username = String(userRow?.username || '').trim();
  const baselineModules = Array.from(getAccessProfileBaselineModules(role, username));
  const effectiveModules = Array.from(computeEffectiveModulesForAccessProfile(accessProfileRow || {
    allowedModules: baselineModules.join(','),
    deniedModules: '',
    forcedModule: '',
  }));

  return {
    username,
    role,
    accessProfile: {
      accreditationLevel: String(accessProfileRow?.accreditationLevel || 'standard').trim() || 'standard',
      allowedModules: Array.from(normalizeModuleList(accessProfileRow?.allowedModules || baselineModules.join(','))),
      deniedModules: Array.from(normalizeModuleList(accessProfileRow?.deniedModules || '')),
      forcedModule: String(accessProfileRow?.forcedModule || '').trim().toLowerCase(),
      notes: String(accessProfileRow?.notes || '').trim(),
      effectiveModules,
    },
  };
}

async function broadcastAccessProfileUpdate(username) {
  const safeUsername = String(username || '').trim();
  if (!safeUsername || !profileStreamClients.size) return;

  const userRow = await get('SELECT id, username, role FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) LIMIT 1', [safeUsername]);
  if (!userRow) return;
  const accessProfile = await getUserAccessProfileByUsername(safeUsername);
  const payload = {
    type: 'access-profile-updated',
    updatedAt: new Date().toISOString(),
    ...buildAccessProfilePayloadForUser(userRow, accessProfile),
  };
  const raw = `event: access-profile\ndata: ${JSON.stringify(payload)}\n\n`;

  for (const client of Array.from(profileStreamClients)) {
    if (String(client?.username || '').toLowerCase() !== safeUsername.toLowerCase()) continue;
    try {
      client.res.write(raw);
    } catch (_err) {
      try { client.res.end(); } catch (_endErr) {}
      profileStreamClients.delete(client);
    }
  }
}

async function authorizeRoleAccess(req, res, next) {
  try {
    const role = String(req.user?.role || '').trim();
    if (!role || roleCanBypassRestrictedProfile(role)) {
      return next();
    }

    const method = String(req.method || '').toUpperCase();
    const pathName = String(req.path || '');

    const alwaysAllowedRules = [
      { method: 'GET', pattern: /^\/auth\/me$/ },
      { method: 'GET', pattern: /^\/push\/public-key$/ },
      { method: 'POST', pattern: /^\/push\/subscribe$/ },
      { method: 'GET', pattern: /^\/guide-documents$/ },
      { method: 'GET', pattern: /^\/guide-documents\/\d+\/download$/ },
      { method: 'GET', pattern: /^\/hr\/dashboard-summary$/ },
      { method: 'GET', pattern: /^\/hr\/employees$/ },
      { method: 'GET', pattern: /^\/hr\/employees\/directory$/ },
      { method: 'PATCH', pattern: /^\/hr\/employees\/\d+$/ },
      { method: 'GET', pattern: /^\/hr\/employees\/\d+\/documents$/ },
      { method: 'POST', pattern: /^\/hr\/employees\/\d+\/documents$/ },
      { method: 'DELETE', pattern: /^\/hr\/employees\/documents\/\d+$/ },
      { method: 'GET', pattern: /^\/hr\/employees\/documents\/\d+\/download$/ },
      { method: 'GET', pattern: /^\/hr\/attendance$/ },
      { method: 'POST', pattern: /^\/hr\/attendance$/ },
      { method: 'PATCH', pattern: /^\/hr\/attendance\/\d+$/ },
      { method: 'GET', pattern: /^\/hr\/leave-requests$/ },
      { method: 'POST', pattern: /^\/hr\/leave-requests$/ },
      { method: 'PATCH', pattern: /^\/hr\/leave-requests\/\d+\/status$/ },
      { method: 'GET', pattern: /^\/hr\/contracts$/ },
      { method: 'POST', pattern: /^\/hr\/contracts$/ },
      { method: 'PATCH', pattern: /^\/hr\/contracts\/\d+$/ },
      { method: 'DELETE', pattern: /^\/hr\/contracts\/\d+$/ },
      { method: 'GET', pattern: /^\/hr\/leave-calendar$/ },
      { method: 'GET', pattern: /^\/hr\/signature-requests$/ },
      { method: 'POST', pattern: /^\/hr\/signature-requests$/ },
      { method: 'DELETE', pattern: /^\/hr\/signature-requests\/\d+$/ },
      { method: 'POST', pattern: /^\/hr\/signature-requests\/\d+\/sign$/ },
      { method: 'GET', pattern: /^\/hr\/signature-requests\/\d+\/download$/ },
      { method: 'GET', pattern: /^\/hr\/employee-profile\/pending-signatures$/ },
      { method: 'GET', pattern: /^\/hr\/document-signatures\/\d+$/ },
    ];

    if (alwaysAllowedRules.some(rule => rule.method === method && rule.pattern.test(pathName))) {
      return next();
    }

    const profile = await getUserAccessProfileByUsername(req.user?.username);
    const baselineModules = Array.from(getAccessProfileBaselineModules(role, req.user?.username));
    const effectiveModules = computeEffectiveModulesForAccessProfile(profile || {
      allowedModules: baselineModules.join(','),
      deniedModules: '',
      forcedModule: '',
    });
    if (isRouteAllowedByModuleSet(method, pathName, effectiveModules)) {
      return next();
    }

    return res.status(403).json({ error: 'Acces limite au bloc RH et Guide ERP pour ce profil' });
  } catch (error) {
    return res.status(500).json({ error: 'Erreur controle acces profil', details: String(error?.message || error) });
  }
}

function hashTrackingToken(rawToken) {
  return crypto.createHash('sha256').update(String(rawToken || '')).digest('hex');
}

function extractSiteNumericToken(value) {
  const match = String(value || '').match(/(\d+)/);
  if (!match) return '';
  const parsed = Number(match[1]);
  if (!Number.isFinite(parsed)) return '';
  return String(parsed);
}

function getChefSiteScopeNumber(user) {
  const role = String(user?.role || '').trim();
  if (role !== 'chef_chantier_site') return null;
  const username = String(user?.username || '').trim();
  const match = username.match(/site[_-]?0*(\d+)/i);
  if (!match) return null;
  const parsed = Number(match[1]);
  if (!Number.isInteger(parsed) || parsed <= 0) return null;
  return String(parsed);
}

function isInChefSiteScope(user, projectLikeRow) {
  const scope = getChefSiteScopeNumber(user);
  if (!scope) return true;

  const candidates = [
    extractSiteNumericToken(projectLikeRow?.numeroMaison),
    extractSiteNumericToken(projectLikeRow?.nomSite),
  ].filter(Boolean);

  return candidates.some(token => token === scope);
}

function normalizeScopeText(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();
}

function isSongonStockManagerRole(user) {
  return String(user?.role || '').trim() === 'gestionnaire_stock_songon';
}

function isInSongonZoneScope(projectLikeRow) {
  const candidates = [
    projectLikeRow?.prefecture,
    projectLikeRow?.nomProjet,
    projectLikeRow?.nomSite,
    projectLikeRow?.nomSiteManuel,
    projectLikeRow?.zoneName,
  ]
    .map(normalizeScopeText)
    .filter(Boolean);

  return candidates.some(value => value.includes('songon'));
}

function isInUserProjectScope(user, projectLikeRow) {
  if (String(user?.role || '').trim() === 'chef_chantier_site') {
    return isInChefSiteScope(user, projectLikeRow);
  }
  if (isSongonStockManagerRole(user)) {
    return isInSongonZoneScope(projectLikeRow);
  }
  return true;
}

function isProcurementReviewerRole(user) {
  const role = String(user?.role || '').trim();
  return role === 'controle_achat' || role === 'controle_achat_global';
}

function normalizeUserKey(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();
}

function isMaterialCatalogReadOnlyUser(user) {
  const role = String(user?.role || '').trim();
  if (role === 'controle_achat_global' || role === 'dirigeant') return true;

  const username = normalizeUserKey(user?.username || '');
  return username === 'kokan_sk'
    || username === 'conducteur_de_travaux'
    || username === 'chef_chantier_sk';
}

async function getHrScopedEmployeeIdsForUser(user) {
  if (!isProcurementReviewerRole(user)) return null;

  const username = String(user?.username || '').trim();
  if (!username) return [];

  const rows = await all(
    `SELECT id
     FROM hr_employees
     WHERE LOWER(TRIM(COALESCE(username, ''))) = LOWER(TRIM(?))
        OR LOWER(TRIM(COALESCE(createdBy, ''))) = LOWER(TRIM(?))
        OR LOWER(TRIM(fullName)) = LOWER(TRIM(?))
     ORDER BY id ASC`,
    [username, username, username]
  );

  return (rows || [])
    .map(row => Number(row?.id || 0))
    .filter(id => Number.isInteger(id) && id > 0);
}

async function getHrProfileEmployeeForUser(user) {
  const username = String(user?.username || '').trim();
  if (!username) return null;

  const byIdentity = await get(
    `SELECT id, fullName, username, createdBy
     FROM hr_employees
     WHERE LOWER(TRIM(COALESCE(username, ''))) = LOWER(TRIM(?))
        OR LOWER(TRIM(COALESCE(createdBy, ''))) = LOWER(TRIM(?))
        OR LOWER(TRIM(fullName)) = LOWER(TRIM(?))
     ORDER BY updatedAt DESC, id DESC
     LIMIT 1`,
    [username, username, username]
  );
  if (byIdentity?.id) return byIdentity;

  const byAssignment = await get(
    `SELECT he.id, he.fullName, he.username, he.createdBy
     FROM project_assignments pa
     JOIN users u ON u.id = pa.userId
     JOIN hr_employees he ON he.id = pa.employeeId
     WHERE LOWER(TRIM(u.username)) = LOWER(TRIM(?))
     ORDER BY pa.assignedAt DESC, he.id DESC
     LIMIT 1`,
    [username]
  );
  if (byAssignment?.id) return byAssignment;

  return null;
}

async function mirrorSiteChiefActionToAdmin({
  user,
  projectId,
  stage = '',
  title = '',
  note = '',
}) {
  const role = String(user?.role || '').trim();
  if (role !== 'chef_chantier_site') return;

  const numericProjectId = Number(projectId || 0);
  if (!Number.isInteger(numericProjectId) || numericProjectId <= 0) return;

  const createdAt = new Date().toISOString();
  const createdBy = String(user?.username || 'chef_chantier_site').trim() || 'chef_chantier_site';
  const stageLabel = String(stage || '').trim() || 'APPROVISIONNEMENT';
  const titleLabel = String(title || '').trim() || 'Action chef chantier';
  const noteLabel = String(note || '').trim() || 'Action synchronisee pour suivi admin';
  const nextProgressId = await getNextTableId('project_progress_updates');

  await run(
    'INSERT INTO project_progress_updates (id, projectId, stage, title, note, materialUsedQty, materialUsageDetails, progressPercent, createdBy, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextProgressId,
      numericProjectId,
      stageLabel,
      titleLabel,
      noteLabel,
      0,
      '[]',
      null,
      createdBy,
      createdAt,
    ]
  );
}

function generateTrackingToken() {
  return `trk_${crypto.randomBytes(24).toString('hex')}`;
}

async function insertAutoVehicleLocationRecord({
  vehicleId,
  latitude,
  longitude,
  speedKph = 0,
  heading = 0,
  accuracyMeters = 0,
  source = 'manual',
  status = 'online',
  note = '',
  recordedAt,
  createdBy = 'system',
}) {
  const lat = Number(latitude);
  const lng = Number(longitude);
  const speed = Number(speedKph || 0);
  const direction = Number(heading || 0);
  const accuracy = Number(accuracyMeters || 0);

  if (Number.isNaN(lat) || lat < -90 || lat > 90 || Number.isNaN(lng) || lng < -180 || lng > 180) {
    throw new Error('Latitude ou longitude invalide');
  }

  if (Number.isNaN(speed) || speed < 0 || Number.isNaN(direction) || direction < 0 || Number.isNaN(accuracy) || accuracy < 0) {
    throw new Error('Vitesse, cap et precision doivent etre positifs');
  }

  const locationStatus = String(status || 'online').trim() || 'online';
  const locationSource = String(source || 'manual').trim() || 'manual';
  const effectiveRecordedAt = recordedAt ? new Date(recordedAt).toISOString() : new Date().toISOString();

  const nextLocationIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM auto_vehicle_locations');
  const nextLocationId = Number(nextLocationIdRow?.nextId || 1);

  const result = await run(
    `INSERT INTO auto_vehicle_locations (
      id,
      vehicle_id,
      vehicleId,
      latitude,
      longitude,
      speed_kph,
      speedKph,
      heading,
      accuracy_meters,
      accuracyMeters,
      source,
      status,
      note,
      recorded_at,
      recordedAt,
      created_by,
      createdBy
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextLocationId,
      Number(vehicleId),
      Number(vehicleId),
      lat,
      lng,
      speed,
      speed,
      direction,
      accuracy,
      accuracy,
      locationSource,
      locationStatus,
      String(note || '').trim(),
      effectiveRecordedAt,
      effectiveRecordedAt,
      String(createdBy || 'system').trim() || 'system',
      String(createdBy || 'system').trim() || 'system',
    ]
  );

  const location = await get(`
    SELECT
      id,
      vehicle_id AS vehicleId,
      latitude,
      longitude,
      speed_kph AS speedKph,
      heading,
      accuracy_meters AS accuracyMeters,
      source,
      status,
      note,
      recorded_at AS recordedAt,
      created_by AS createdBy
    FROM auto_vehicle_locations
    WHERE id = ?
  `, [result.lastID]);

  return location;
}

app.post('/api/auth/login', authRateLimiter, async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: 'Nom d\'utilisateur et mot de passe sont obligatoires' });
  }

  const user = await get('SELECT * FROM users WHERE username = ?', [username]);
  if (!user) {
    return res.status(401).json({ error: 'Utilisateur ou mot de passe invalide' });
  }

  const valid = await bcrypt.compare(password, user.password);
  if (!valid) {
    return res.status(401).json({ error: 'Utilisateur ou mot de passe invalide' });
  }

  if (!roleCanBypassRestrictedProfile(user.role)) {
    try {
      await ensureHrProfileForUserAccount(user, req.body?.username || user.username || 'system');
    } catch (profileErr) {
      console.warn('Auto-create HR profile failed at login:', profileErr?.message || profileErr);
    }
  }

  const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, {
    expiresIn: '6h'
  });

  res.json({ token, username: user.username });
});

app.get('/api/auth/me', authenticateToken, async (req, res) => {
  const role = String(req.user?.role || '').trim();
  const scope = role === 'gestionnaire_stock_songon'
    ? {
        warehouseId: 'entrepot-songon-2',
        zoneName: 'Songon',
        siteNumber: '',
      }
    : null;

  const accessProfile = await getUserAccessProfileByUsername(req.user?.username);
  const profilePayload = buildAccessProfilePayloadForUser(
    { username: req.user?.username, role },
    accessProfile
  );

  res.json({
    username: req.user.username,
    role: req.user.role,
    scope,
    accessProfile: profilePayload.accessProfile,
  });
});

app.get('/api/auth/profile-stream', authenticateToken, async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  if (typeof res.flushHeaders === 'function') {
    res.flushHeaders();
  }

  const username = String(req.user?.username || '').trim();
  const client = { username, res };
  profileStreamClients.add(client);

  res.write(`event: ping\ndata: ${JSON.stringify({ ts: new Date().toISOString() })}\n\n`);

  const keepAliveTimer = setInterval(() => {
    try {
      res.write(`event: ping\ndata: ${JSON.stringify({ ts: new Date().toISOString() })}\n\n`);
    } catch (_err) {
      clearInterval(keepAliveTimer);
      profileStreamClients.delete(client);
      try { res.end(); } catch (_endErr) {}
    }
  }, 25000);

  req.on('close', () => {
    clearInterval(keepAliveTimer);
    profileStreamClients.delete(client);
  });
});

app.post('/api/gps/ingest', async (req, res) => {
  const {
    deviceToken,
    latitude,
    longitude,
    speedKph = 0,
    heading = 0,
    accuracyMeters = 0,
    status = 'online',
    source = 'mobile_app',
    note = '',
    recordedAt,
  } = req.body || {};

  const rawToken = String(req.headers['x-device-token'] || req.headers['x-tracking-token'] || deviceToken || '').trim();
  if (!rawToken) {
    return res.status(401).json({ error: 'Token appareil manquant' });
  }

  const tokenHash = hashTrackingToken(rawToken);
  const device = await get(`
    SELECT td.*, v.nomVehicule, v.marqueVehicule
    FROM auto_tracking_devices td
    JOIN auto_vehicles v ON v.id = td.vehicleId
    WHERE td.tokenHash = ? AND td.isActive = 1
    LIMIT 1
  `, [tokenHash]);

  if (!device) {
    return res.status(401).json({ error: 'Token appareil invalide ou inactif' });
  }

  try {
    const location = await insertAutoVehicleLocationRecord({
      vehicleId: Number(device.vehicleId),
      latitude,
      longitude,
      speedKph,
      heading,
      accuracyMeters,
      status,
      source,
      note,
      recordedAt,
      createdBy: `device:${String(device.deviceName || 'smartphone').trim()}`,
    });

    await run(
      'UPDATE auto_tracking_devices SET lastSeenAt = ?, lastLatitude = ?, lastLongitude = ?, lastSpeedKph = ?, updatedAt = ? WHERE id = ?',
      [
        location.recordedAt,
        Number(location.latitude),
        Number(location.longitude),
        Number(location.speedKph || 0),
        new Date().toISOString(),
        Number(device.id),
      ]
    );

    await run('UPDATE auto_vehicles SET gpsActif = 1 WHERE id = ?', [Number(device.vehicleId)]);

    res.status(201).json({
      ok: true,
      vehicleId: Number(device.vehicleId),
      vehicleLabel: `${device.nomVehicule || '-'} • ${device.marqueVehicule || '-'}`,
      location,
    });
  } catch (error) {
    if (error && error.message && error.message.includes('invalide')) {
      return res.status(400).json({ error: error.message });
    }
    throw error;
  }
});

app.use('/api', authenticateToken, authorizeRoleAccess);

app.post('/api/material-requests/auto-stage', async (req, res) => {
  const {
    projetId,
    zoneName = '',
    nomProjet = '',
    catalogProjectFolder = '',
    demandeur,
    etapeApprovisionnement = '',
    warehouseId = '',
    description = '',
    dateDemande = null,
    lines = [],
  } = req.body || {};

  let projectId = Number(projetId || 0);
  if (!projectId && zoneName && nomProjet) {
    const zoneProject = await ensureZoneStockProject(nomProjet, zoneName);
    projectId = Number(zoneProject?.id || 0);
  }
  const requester = String(demandeur || '').trim();
  const stageRaw = String(etapeApprovisionnement || '').trim();
  const warehouse = String(warehouseId || '').trim();
  const stageKey = normalizeStageLabel(stageRaw);

  if (!projectId || !requester || !stageRaw || !warehouse) {
    return res.status(400).json({ error: 'projetId (ou zone+nomProjet), demandeur, etapeApprovisionnement et warehouseId sont obligatoires' });
  }

  const project = await get('SELECT id, nomProjet FROM projects WHERE id = ?', [projectId]);
  if (!project) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  const existingRequests = await all(
    'SELECT id, etapeApprovisionnement FROM material_requests WHERE projetId = ? AND statut != ?',
    [projectId, 'REJETEE']
  );
  const hasSameStageRequest = (existingRequests || []).some(row => normalizeStageLabel(row.etapeApprovisionnement) === stageKey);
  if (hasSameStageRequest) {
    return res.status(409).json({
      error: `Impossible de creer une nouvelle demande: une demande existe deja pour l'etape "${stageRaw}" sur ce site.`,
    });
  }

  const existingOrders = await all(
    `SELECT DISTINCT
      po.id,
      po.statut,
      po.etapeApprovisionnement,
      mr.etapeApprovisionnement AS requestStage
     FROM purchase_orders po
     LEFT JOIN purchase_order_items poi ON poi.purchaseOrderId = po.id
     LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
     WHERE COALESCE(po.siteId, po.projetId) = ?
       AND COALESCE(po.statut, '') != 'ANNULEE'`,
    [projectId]
  );
  const hasSameStageOrder = (existingOrders || []).some(row => {
    const stageLabel = String(row.etapeApprovisionnement || row.requestStage || '').trim();
    return normalizeStageLabel(stageLabel) === stageKey;
  });
  if (hasSameStageOrder) {
    return res.status(409).json({
      error: `Impossible de creer un nouveau bon de commande: un bon existe deja pour l'etape "${stageRaw}" sur ce site.`,
    });
  }

  const projectFolder = String(project.nomProjet || '').trim();
  const requestedCatalogFolder = String(catalogProjectFolder || '').trim();

  const normalizeFolderKey = value => String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  const allCatalogRows = await all('SELECT * FROM building_material_catalog ORDER BY materialName ASC');
  const allCatalogFolders = [...new Set((allCatalogRows || [])
    .map(row => String(row?.projectFolder || '').trim())
    .filter(Boolean))];

  const baseProjectFolderKey = normalizeFolderKey(projectFolder);
  const requestedCatalogFolderKey = normalizeFolderKey(requestedCatalogFolder);

  const discoveredFolders = allCatalogFolders.filter(folderName => {
    const key = normalizeFolderKey(folderName);
    if (!key) return false;
    if (requestedCatalogFolderKey && key === requestedCatalogFolderKey) return true;
    if (key === baseProjectFolderKey) return true;
    return key.startsWith(`${baseProjectFolderKey} - `);
  });

  const candidateFolders = [];
  const candidateFolderKeys = new Set();
  const pushCandidateFolder = folderName => {
    const raw = String(folderName || '').trim();
    if (!raw) return;
    const key = normalizeFolderKey(raw);
    if (!key || candidateFolderKeys.has(key)) return;
    candidateFolderKeys.add(key);
    candidateFolders.push(raw);
  };

  pushCandidateFolder(requestedCatalogFolder);
  pushCandidateFolder(projectFolder);
  (discoveredFolders || []).forEach(row => pushCandidateFolder(row?.projectFolder || ''));

  let resolvedProjectFolder = projectFolder;
  let stageCatalogRows = [];

  for (const folderName of candidateFolders) {
    const folderKey = normalizeFolderKey(folderName);
    const rows = (allCatalogRows || []).filter(entry => normalizeFolderKey(entry?.projectFolder || '') === folderKey);
    if (!rows.length) continue;

    const stageRows = rows.filter(entry => isCatalogStageMatching(entry.notes, stageRaw));
    if (!stageCatalogRows.length) {
      resolvedProjectFolder = folderName;
    }
    if (stageRows.length) {
      resolvedProjectFolder = folderName;
      stageCatalogRows = stageRows;
      break;
    }
  }

  const stageCatalog = (stageCatalogRows || [])
    .filter(entry => isCatalogStageMatching(entry.notes, stageRaw))
    .map(entry => ({
      materialName: String(entry.materialName || '').trim(),
      quantiteParBatiment: Number(entry.quantiteParBatiment || 0),
      prixUnitaire: Number(entry.prixUnitaire || 0),
      unite: String(entry.unite || '').trim(),
    }))
    .filter(entry => entry.materialName && entry.quantiteParBatiment > 0);

  const selectedLinesRaw = Array.isArray(lines) ? lines : [];
  const selectedLineByMaterialKey = new Map();
  for (const line of selectedLinesRaw) {
    const materialName = String(line?.itemName || line?.materialName || '').trim();
    const qty = Number(line?.quantiteDemandee || line?.quantite || 0);
    if (!materialName || Number.isNaN(qty) || qty <= 0) continue;
    const key = materialName.toLowerCase();
    if (!selectedLineByMaterialKey.has(key)) {
      selectedLineByMaterialKey.set(key, { materialName, quantiteParBatiment: 0 });
    }
    selectedLineByMaterialKey.get(key).quantiteParBatiment += qty;
  }

  if (!stageCatalog.length) {
    return res.status(404).json({
      error: `Aucun article catalogue trouve pour l'etape "${stageRaw}" sur le projet "${resolvedProjectFolder || projectFolder}".`,
    });
  }

  let sourceLines = stageCatalog;
  if (selectedLineByMaterialKey.size) {
    const catalogByName = new Map(
      stageCatalog.map(entry => [String(entry.materialName || '').trim().toLowerCase(), entry])
    );
    const unknownMaterials = [];
    const selectedLines = [];
    for (const [key, selected] of selectedLineByMaterialKey.entries()) {
      const catalogEntry = catalogByName.get(key);
      if (!catalogEntry) {
        unknownMaterials.push(selected.materialName);
        continue;
      }
      selectedLines.push({
        materialName: catalogEntry.materialName,
        quantiteParBatiment: Number(selected.quantiteParBatiment || 0),
        prixUnitaire: Number(catalogEntry.prixUnitaire || 0),
        unite: String(catalogEntry.unite || '').trim(),
      });
    }

    if (unknownMaterials.length) {
      return res.status(400).json({
        error: `Ces matériaux ne correspondent pas au catalogue de l'etape "${stageRaw}": ${unknownMaterials.join(', ')}`,
      });
    }

    if (!selectedLines.length) {
      return res.status(400).json({ error: 'Aucune ligne de demande valide' });
    }

    sourceLines = selectedLines;
  }

  const groupId = crypto.randomUUID();
  const requestDate = dateDemande ? new Date(dateDemande) : new Date();
  if (Number.isNaN(requestDate.getTime())) {
    return res.status(400).json({ error: 'dateDemande invalide' });
  }
  const nowIso = requestDate.toISOString();
  const createdRequests = [];
  let nextMrIdAutoStage = await getNextTableId('material_requests');

  for (const entry of sourceLines) {
    const inserted = await run(
      'INSERT INTO material_requests (id, projetId, demandeur, etapeApprovisionnement, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut, groupId, warehouseId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        nextMrIdAutoStage++,
        projectId,
        requester,
        stageRaw,
        entry.materialName,
        String(description || '').trim(),
        entry.quantiteParBatiment,
        entry.quantiteParBatiment,
        nowIso,
        'EN_ATTENTE',
        groupId,
        warehouse,
      ]
    );

    createdRequests.push({
      id: Number(inserted.lastID),
      itemName: entry.materialName,
      quantiteDemandee: entry.quantiteParBatiment,
      prixUnitaire: entry.prixUnitaire,
      unite: entry.unite,
    });
  }

  const autoPurchaseOrder = null;

  const createdRows = await all(
    `SELECT mr.*, p.nomProjet as projetNom, p.numeroMaison, p.typeMaison
     FROM material_requests mr
     JOIN projects p ON p.id = mr.projetId
     WHERE mr.groupId = ?
     ORDER BY mr.id ASC`,
    [groupId]
  );

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const siteLabel = String(createdRows?.[0]?.numeroMaison || '').trim() || '-';
    const itemCount = Array.isArray(createdRows) ? createdRows.length : 0;
    await mirrorSiteChiefActionToAdmin({
      user: req.user,
      projectId,
      stage: stageRaw,
      title: `Nouvelle demande approvisionnement (lot ${siteLabel})`,
      note: `${itemCount} ligne(s) creee(s) par le chef chantier ${String(req.user?.username || '').trim() || 'inconnu'}.`,
    });
  }

  res.status(201).json({
    groupId,
    stage: stageRaw,
    projetId: projectId,
    createdRequests: createdRows,
    autoPurchaseOrder,
    message: `Demande d'approvisionnement generee automatiquement pour l'etape ${stageRaw}.`,
  });
});

app.get('/api/users', async (_req, res) => {
  const rows = await all(`
    SELECT
      COALESCE(u.id, -he.id) AS id,
      COALESCE(NULLIF(TRIM(u.username), ''), NULLIF(TRIM(he.username), ''), '') AS username,
      COALESCE(NULLIF(TRIM(u.role), ''), '-') AS role,
      COALESCE(NULLIF(TRIM(he.fullName), ''), '') AS linkedEmployeeName,
      COALESCE(NULLIF(TRIM(he.email), ''), '') AS email,
      CASE
        WHEN u.id IS NULL THEN '-'
        WHEN COALESCE(TRIM(u.password), '') LIKE '$2%' AND COALESCE(TRIM(u.role), '') = 'employe_standard' AND COALESCE(TRIM(u.username), '') <> '' THEN TRIM(u.username) || '@2026'
        WHEN COALESCE(TRIM(u.password), '') LIKE '$2%' THEN '-'
        ELSE COALESCE(NULLIF(TRIM(u.password), ''), '-')
      END AS initialPasswordHint,
      0 AS hasLoggedIn,
      '' AS firstLoginAt,
      '' AS lastLoginAt,
      CASE WHEN u.id IS NOT NULL THEN 1 ELSE 0 END AS hasUserAccount
    FROM hr_employees he
    LEFT JOIN users u ON LOWER(TRIM(u.username)) = LOWER(TRIM(he.username))

    UNION ALL

    SELECT
      u.id AS id,
      COALESCE(NULLIF(TRIM(u.username), ''), '') AS username,
      COALESCE(NULLIF(TRIM(u.role), ''), '-') AS role,
      '' AS linkedEmployeeName,
      '' AS email,
      CASE
        WHEN COALESCE(TRIM(u.password), '') LIKE '$2%' AND COALESCE(TRIM(u.role), '') = 'employe_standard' AND COALESCE(TRIM(u.username), '') <> '' THEN TRIM(u.username) || '@2026'
        WHEN COALESCE(TRIM(u.password), '') LIKE '$2%' THEN '-'
        ELSE COALESCE(NULLIF(TRIM(u.password), ''), '-')
      END AS initialPasswordHint,
      0 AS hasLoggedIn,
      '' AS firstLoginAt,
      '' AS lastLoginAt,
      1 AS hasUserAccount
    FROM users u
    WHERE NOT EXISTS (
      SELECT 1
      FROM hr_employees he
      WHERE LOWER(TRIM(he.username)) = LOWER(TRIM(u.username))
    )
    ORDER BY username
  `);
  const normalizedRows = (Array.isArray(rows) ? rows : []).map(row => ({
    ...row,
    initialPasswordHint: resolveKnownUserPasswordHint(row?.username, row?.role, row?.initialPasswordHint),
  }));

  const scoreRow = row => {
    let score = 0;
    if (Number(row?.hasUserAccount || 0) === 1) score += 10;
    if (String(row?.linkedEmployeeName || '').trim()) score += 3;
    if (String(row?.initialPasswordHint || '').trim() && String(row?.initialPasswordHint || '').trim() !== '-') score += 1;
    return score;
  };

  const byUsername = new Map();
  for (const row of normalizedRows) {
    const username = String(row?.username || '').trim();
    if (!username) continue;
    const key = username.toLowerCase();
    const existing = byUsername.get(key);
    if (!existing || scoreRow(row) > scoreRow(existing)) {
      byUsername.set(key, row);
    }
  }

  const uniqueRows = Array.from(byUsername.values()).sort((a, b) => String(a?.username || '').localeCompare(String(b?.username || ''), 'fr', { sensitivity: 'base' }));
  res.json(uniqueRows);
});

function getMailTransport() {
  if (mailTransport) return mailTransport;
  if (!SMTP_HOST || !SMTP_PORT || !SMTP_USER || !SMTP_PASS) {
    throw new Error('Configuration SMTP incomplète');
  }

  mailTransport = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: SMTP_SECURE,
    auth: {
      user: SMTP_USER,
      pass: SMTP_PASS,
    },
  });

  return mailTransport;
}

function collectUniqueMailRecipients(rows) {
  const byUsername = new Map();
  for (const row of (rows || [])) {
    const username = String(row?.username || '').trim();
    if (!username) continue;
    const email = normalizeHrEmail(row?.email || '');
    const key = username.toLowerCase();
    const existing = byUsername.get(key);
    if (!existing || (!existing.email && email)) {
      byUsername.set(key, {
        username,
        role: String(row?.role || '').trim(),
        linkedEmployeeName: String(row?.linkedEmployeeName || '').trim(),
        email,
      });
    }
  }
  return Array.from(byUsername.values()).sort((a, b) => a.username.localeCompare(b.username, 'fr', { sensitivity: 'base' }));
}

app.get('/api/admin/mail/recipients', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Acces reserve a admin' });
    }

    const rows = await all(`
      SELECT
        COALESCE(u.id, -he.id) AS id,
        COALESCE(NULLIF(TRIM(u.username), ''), NULLIF(TRIM(he.username), ''), '') AS username,
        COALESCE(NULLIF(TRIM(u.role), ''), '-') AS role,
        COALESCE(NULLIF(TRIM(he.fullName), ''), '') AS linkedEmployeeName,
        COALESCE(NULLIF(TRIM(he.email), ''), '') AS email
      FROM hr_employees he
      LEFT JOIN users u ON LOWER(TRIM(u.username)) = LOWER(TRIM(he.username))

      UNION ALL

      SELECT
        u.id AS id,
        COALESCE(NULLIF(TRIM(u.username), ''), '') AS username,
        COALESCE(NULLIF(TRIM(u.role), ''), '-') AS role,
        '' AS linkedEmployeeName,
        '' AS email
      FROM users u
      WHERE NOT EXISTS (
        SELECT 1
        FROM hr_employees he
        WHERE LOWER(TRIM(he.username)) = LOWER(TRIM(u.username))
      )
      ORDER BY username
    `);

    const recipients = collectUniqueMailRecipients(rows);
    const withEmailCount = recipients.filter(item => isValidHrEmail(item.email)).length;
    return res.json({
      recipients,
      total: recipients.length,
      withEmail: withEmailCount,
    });
  } catch (error) {
    return res.status(500).json({ error: 'Erreur chargement destinataires', details: String(error?.message || error) });
  }
});

app.post('/api/admin/mail/send', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Acces reserve a admin' });
    }

    const subject = String(req.body?.subject || '').trim();
    const message = String(req.body?.message || '').trim();
    const sendToAllWithEmail = Boolean(req.body?.sendToAllWithEmail);
    const requestedUsernames = Array.isArray(req.body?.recipientUsernames)
      ? req.body.recipientUsernames.map(item => String(item || '').trim()).filter(Boolean)
      : [];

    if (!subject || !message) {
      return res.status(400).json({ error: 'Sujet et message obligatoires' });
    }

    if (!MAIL_FROM) {
      return res.status(400).json({ error: 'MAIL_FROM manquant dans la configuration SMTP' });
    }

    const rows = await all(`
      SELECT
        COALESCE(NULLIF(TRIM(u.username), ''), NULLIF(TRIM(he.username), ''), '') AS username,
        COALESCE(NULLIF(TRIM(u.role), ''), '-') AS role,
        COALESCE(NULLIF(TRIM(he.fullName), ''), '') AS linkedEmployeeName,
        COALESCE(NULLIF(TRIM(he.email), ''), '') AS email
      FROM hr_employees he
      LEFT JOIN users u ON LOWER(TRIM(u.username)) = LOWER(TRIM(he.username))

      UNION ALL

      SELECT
        COALESCE(NULLIF(TRIM(u.username), ''), '') AS username,
        COALESCE(NULLIF(TRIM(u.role), ''), '-') AS role,
        '' AS linkedEmployeeName,
        '' AS email
      FROM users u
      WHERE NOT EXISTS (
        SELECT 1
        FROM hr_employees he
        WHERE LOWER(TRIM(he.username)) = LOWER(TRIM(u.username))
      )
      ORDER BY username
    `);

    const recipients = collectUniqueMailRecipients(rows);
    const recipientByUsername = new Map(recipients.map(item => [item.username.toLowerCase(), item]));
    let targetRecipients = [];

    if (sendToAllWithEmail) {
      targetRecipients = recipients.filter(item => isValidHrEmail(item.email));
    } else {
      const seen = new Set();
      for (const requested of requestedUsernames) {
        const key = requested.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        const found = recipientByUsername.get(key);
        if (found) targetRecipients.push(found);
      }
      targetRecipients = targetRecipients.filter(item => isValidHrEmail(item.email));
    }

    if (!targetRecipients.length) {
      return res.status(400).json({ error: 'Aucun destinataire valide avec email' });
    }

    const transport = getMailTransport();
    const results = [];
    for (const recipient of targetRecipients) {
      try {
        await transport.sendMail({
          from: MAIL_FROM,
          to: recipient.email,
          subject,
          text: message,
        });
        results.push({ username: recipient.username, email: recipient.email, success: true });
      } catch (error) {
        results.push({
          username: recipient.username,
          email: recipient.email,
          success: false,
          error: String(error?.message || error),
        });
      }
    }

    const successCount = results.filter(item => item.success).length;
    const failedCount = results.length - successCount;

    return res.json({
      subject,
      totalAttempted: results.length,
      successCount,
      failedCount,
      results,
    });
  } catch (error) {
    return res.status(500).json({ error: 'Erreur envoi courriels', details: String(error?.message || error) });
  }
});

app.get('/api/admin/access-profiles', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Acces reserve a admin' });
    }

    const users = await all('SELECT id, username, role, createdAt FROM users ORDER BY username ASC');
    const profiles = await all('SELECT * FROM user_access_profiles ORDER BY username ASC');
    const profileByUsername = new Map((profiles || []).map(row => [String(row?.username || '').trim().toLowerCase(), row]));

    const rows = [];
    for (const user of (users || [])) {
      const username = String(user?.username || '').trim();
      if (!username) continue;

      const linkedEmployee = await get(
        `SELECT id, fullName, jobTitle, username, createdBy
         FROM hr_employees
         WHERE LOWER(TRIM(COALESCE(username, ''))) = LOWER(TRIM(?))
            OR LOWER(TRIM(COALESCE(createdBy, ''))) = LOWER(TRIM(?))
         ORDER BY updatedAt DESC, id DESC
         LIMIT 1`,
        [username, username]
      );

      const profile = profileByUsername.get(username.toLowerCase()) || null;
      const baselineModules = Array.from(getAccessProfileBaselineModules(user?.role || user?.roleSnapshot || profile?.roleSnapshot || '', username));
      const effectiveModules = roleCanBypassRestrictedProfile(user?.role)
        ? baselineModules
        : Array.from(computeEffectiveModulesForAccessProfile(profile || {}));

      rows.push({
        username,
        role: String(user?.role || '').trim(),
        createdAt: String(user?.createdAt || '').trim(),
        linkedEmployee,
        baselineModules,
        accreditationLevel: String(profile?.accreditationLevel || 'standard').trim() || 'standard',
        allowedModules: Array.from(normalizeModuleList(profile?.allowedModules || '')),
        deniedModules: Array.from(normalizeModuleList(profile?.deniedModules || '')),
        forcedModule: String(profile?.forcedModule || '').trim().toLowerCase(),
        notes: String(profile?.notes || '').trim(),
        effectiveModules,
      });
    }

    return res.json(rows);
  } catch (err) {
    return res.status(500).json({ error: 'Erreur chargement dossiers profils', details: String(err?.message || err) });
  }
});

app.patch('/api/admin/access-profiles/:username', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Acces reserve a admin' });
    }

    const username = String(req.params.username || '').trim();
    if (!username) {
      return res.status(400).json({ error: 'username requis' });
    }

    const target = await get('SELECT id, username, role FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) LIMIT 1', [username]);
    if (!target) {
      return res.status(404).json({ error: 'Utilisateur introuvable' });
    }

    const accreditationLevel = String(req.body?.accreditationLevel || 'standard').trim().toLowerCase() || 'standard';
    const requestedAllowedModules = normalizeModuleList(Array.isArray(req.body?.allowedModules) ? req.body.allowedModules.join(',') : req.body?.allowedModules || '');
    const requestedDeniedModules = normalizeModuleList(Array.isArray(req.body?.deniedModules) ? req.body.deniedModules.join(',') : req.body?.deniedModules || '');
    const requestedForcedModule = String(req.body?.forcedModule || '').trim().toLowerCase();
    const allowedModules = sanitizeAccessProfileModulesForTargetRole(requestedAllowedModules, target.role);
    const deniedModules = sanitizeAccessProfileModulesForTargetRole(requestedDeniedModules, target.role);
    const forcedModuleAllowed = sanitizeAccessProfileModulesForTargetRole(new Set([requestedForcedModule]), target.role);
    const forcedModule = requestedForcedModule && forcedModuleAllowed.has(requestedForcedModule) ? requestedForcedModule : '';
    const notes = String(req.body?.notes || '').trim();
    const now = new Date().toISOString();
    const actor = String(req.user?.username || 'admin').trim() || 'admin';

    const existing = await get('SELECT id FROM user_access_profiles WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) LIMIT 1', [username]);
    if (existing?.id) {
      await run(
        `UPDATE user_access_profiles
         SET roleSnapshot = ?, accreditationLevel = ?, allowedModules = ?, deniedModules = ?, forcedModule = ?, notes = ?, updatedAt = ?, updatedBy = ?
         WHERE id = ?`,
        [
          String(target.role || '').trim(),
          accreditationLevel,
          serializeCsvSet(allowedModules),
          serializeCsvSet(deniedModules),
          forcedModule,
          notes,
          now,
          actor,
          Number(existing.id),
        ]
      );
    } else {
      const nextId = await getNextTableId('user_access_profiles');
      await run(
        `INSERT INTO user_access_profiles
         (id, username, roleSnapshot, accreditationLevel, allowedModules, deniedModules, forcedModule, notes, createdAt, updatedAt, updatedBy)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          nextId,
          username,
          String(target.role || '').trim(),
          accreditationLevel,
          serializeCsvSet(allowedModules),
          serializeCsvSet(deniedModules),
          forcedModule,
          notes,
          now,
          now,
          actor,
        ]
      );
    }

    const saved = await getUserAccessProfileByUsername(username);
    const baselineModules = Array.from(getAccessProfileBaselineModules(target.role, username));
    const effectiveModules = Array.from(computeEffectiveModulesForAccessProfile(saved || {}));

    await run(
      'INSERT INTO user_access_profile_audit (id, username, action, payloadJson, changedBy, createdAt) VALUES (?, ?, ?, ?, ?, ?)',
      [
        await getNextTableId('user_access_profile_audit'),
        username,
        'update_access_profile',
        JSON.stringify({ accreditationLevel, allowedModules: Array.from(allowedModules), deniedModules: Array.from(deniedModules), forcedModule, notes }),
        actor,
        now,
      ]
    );

    await broadcastAccessProfileUpdate(username).catch(() => {});

    return res.json({
      username,
      role: String(target.role || '').trim(),
      baselineModules,
      accreditationLevel: String(saved?.accreditationLevel || 'standard').trim(),
      allowedModules: Array.from(normalizeModuleList(saved?.allowedModules || '')),
      deniedModules: Array.from(normalizeModuleList(saved?.deniedModules || '')),
      forcedModule: String(saved?.forcedModule || '').trim().toLowerCase(),
      notes: String(saved?.notes || '').trim(),
      effectiveModules,
    });
  } catch (err) {
    return res.status(500).json({ error: 'Erreur sauvegarde dossier profil', details: String(err?.message || err) });
  }
});

app.post('/api/admin/access-profiles/:username/ensure-hr-profile', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Acces reserve a admin' });
    }

    const username = String(req.params.username || '').trim();
    if (!username) {
      return res.status(400).json({ error: 'username requis' });
    }

    const userRow = await get('SELECT id, username, role FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) LIMIT 1', [username]);
    if (!userRow) {
      return res.status(404).json({ error: 'Utilisateur introuvable' });
    }

    const profile = await ensureHrProfileForUserAccount(userRow, String(req.user?.username || 'admin').trim() || 'admin');
    return res.json({ message: 'Profil RH garanti', profile });
  } catch (err) {
    return res.status(500).json({ error: 'Erreur creation profil RH', details: String(err?.message || err) });
  }
});

app.post('/api/project-catalog', async (req, res) => {
  const { nomProjet, typeProjet = '', description = '' } = req.body;
  const projectName = String(nomProjet || '').trim();
  if (!projectName) {
    return res.status(400).json({ error: 'Le nom du projet est obligatoire' });
  }

  const duplicate = await get(
    'SELECT id FROM project_catalog WHERE LOWER(nomProjet) = LOWER(?) LIMIT 1',
    [projectName]
  );
  if (duplicate) {
    return res.status(409).json({ error: 'Ce projet existe deja' });
  }

  const nextCatalogId = await getNextTableId('project_catalog');

  const result = await run(
    'INSERT INTO project_catalog (id, nomProjet, typeProjet, description, createdAt) VALUES (?, ?, ?, ?, ?)',
    [nextCatalogId, projectName, String(typeProjet || '').trim(), String(description || '').trim(), new Date().toISOString()]
  );

  const created = await get('SELECT * FROM project_catalog WHERE id = ?', [nextCatalogId || result.lastID]);
  res.status(201).json(created);
});

app.get('/api/project-catalog', async (_req, res) => {
  const rows = await all('SELECT * FROM project_catalog WHERE isHidden = 0 ORDER BY id DESC');
  const role = String(_req.user?.role || '').trim();
  if (role === 'gestionnaire_stock_songon') {
    return res.json(rows.filter(row => isInSongonZoneScope(row)));
  }
  res.json(rows);
});

app.post('/api/project-folders', async (req, res) => {
  const { projectId, nomProjet, prefecture = 'Non renseigne', description = '' } = req.body;

  let projectName = String(nomProjet || '').trim();
  const projectIdValue = Number(projectId);
  if (Number.isInteger(projectIdValue) && projectIdValue > 0) {
    const parent = await get('SELECT id, nomProjet FROM project_catalog WHERE id = ?', [projectIdValue]);
    if (!parent) {
      return res.status(400).json({ error: 'Projet introuvable pour cette zone' });
    }
    projectName = String(parent.nomProjet || '').trim();
  }

  const prefectureName = String(prefecture || '').trim() || 'Non renseigne';
  if (!projectName) {
    return res.status(400).json({ error: 'Le nom du projet est obligatoire' });
  }

  // Legacy rows can have null/missing projectId, so enforce duplicate check on logical key.
  const duplicate = await get(
    'SELECT id FROM project_folders WHERE LOWER(nomProjet) = LOWER(?) AND LOWER(prefecture) = LOWER(?) LIMIT 1',
    [projectName, prefectureName]
  );
  if (duplicate) {
    return res.status(409).json({ error: 'Ce projet existe deja pour cette prefecture' });
  }

  let result;
  try {
    const nextFolderId = await getNextTableId('project_folders');
    result = await run(
      'INSERT INTO project_folders (id, projectId, nomProjet, prefecture, nomSite, description, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [nextFolderId, Number.isInteger(projectIdValue) && projectIdValue > 0 ? projectIdValue : null, projectName, prefectureName, '', String(description || '').trim(), new Date().toISOString()]
    );
  } catch (error) {
    const message = String(error?.message || '').toLowerCase();
    if (message.includes('unique') && message.includes('project_folders')) {
      return res.status(409).json({ error: 'Ce projet existe deja pour cette prefecture' });
    }
    throw error;
  }

  const folder = await get('SELECT pf.*, pc.typeProjet FROM project_folders pf LEFT JOIN project_catalog pc ON pc.id = pf.projectId WHERE pf.id = ?', [result.lastID]);
  res.status(201).json(folder);
});

app.get('/api/project-folders', async (_req, res) => {
  const rows = await all('SELECT pf.*, pc.typeProjet FROM project_folders pf LEFT JOIN project_catalog pc ON pc.id = pf.projectId WHERE pf.isHidden = 0 ORDER BY pf.id DESC');
  const role = String(_req.user?.role || '').trim();
  if (role === 'gestionnaire_stock_songon') {
    return res.json(rows.filter(row => isInSongonZoneScope(row)));
  }
  res.json(rows);
});

async function handleDeleteProjectFolder(req, res) {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: 'Identifiant de zone invalide' });
  }

  const folder = await get('SELECT * FROM project_folders WHERE id = ?', [id]);
  if (!folder) {
    return res.status(404).json({ error: 'Zone introuvable' });
  }

  const projectName = String(folder.nomProjet || '').trim();
  const prefectureName = String(folder.prefecture || '').trim() || 'Non renseigne';
  const sites = await all(
    'SELECT id FROM projects WHERE LOWER(nomProjet) = LOWER(?) AND LOWER(prefecture) = LOWER(?)',
    [projectName, prefectureName]
  );
  const zoneWarehouses = await all(
    `SELECT id
     FROM projects
     WHERE LOWER(prefecture) = LOWER(?)
       AND UPPER(COALESCE(typeMaison, '')) = 'ZONE_STOCK'`,
    [prefectureName]
  );

  const linkedProjectIds = Array.from(
    new Set(
      [...(sites || []), ...(zoneWarehouses || [])]
        .map(row => Number(row.id))
        .filter(value => Number.isInteger(value) && value > 0)
    )
  );

  if (linkedProjectIds.length) {
    const placeholders = linkedProjectIds.map(() => '?').join(',');
    await run(`DELETE FROM project_assignments WHERE projectId IN (${placeholders})`, linkedProjectIds);
    await run(`DELETE FROM material_requests WHERE projetId IN (${placeholders})`, linkedProjectIds);
    await run(`DELETE FROM project_progress_updates WHERE projectId IN (${placeholders})`, linkedProjectIds);
    await run(`DELETE FROM revenues WHERE projetId IN (${placeholders})`, linkedProjectIds);
    await run(`DELETE FROM expenses WHERE projetId IN (${placeholders})`, linkedProjectIds);
    await run(`DELETE FROM stock_issues WHERE projetId IN (${placeholders})`, linkedProjectIds);
  }

  const deletedSitesResult = await run(
    'DELETE FROM projects WHERE LOWER(nomProjet) = LOWER(?) AND LOWER(prefecture) = LOWER(?)',
    [projectName, prefectureName]
  );
  const deletedWarehousesResult = await run(
    `DELETE FROM projects
     WHERE LOWER(prefecture) = LOWER(?)
       AND UPPER(COALESCE(typeMaison, '')) = 'ZONE_STOCK'`,
    [prefectureName]
  );

  const folderDeleteResult = await run('DELETE FROM project_folders WHERE id = ?', [id]);
  if (folderDeleteResult.changes === 0) {
    return res.status(404).json({ error: 'Zone introuvable' });
  }

  res.json({
    message: 'Zone supprimée avec succès',
    deletedZoneId: id,
    deletedSites: Number(deletedSitesResult?.changes || 0),
    deletedWarehouses: Number(deletedWarehousesResult?.changes || 0),
  });
}

app.delete('/api/project-folders/:id', handleDeleteProjectFolder);
app.delete('/api/zones/:id', handleDeleteProjectFolder);

app.post('/api/projects', async (req, res) => {
  const {
    projectId,
    nomProjet,
    prefecture = 'Non renseigne',
    nomSite = '',
    typeMaison = '',
    numeroMaison = '',
    description = '',
    etapeConstruction = '',
    statutConstruction = ''
  } = req.body;
  const projectIdValue = Number(projectId);
  let projectName = String(nomProjet || '').trim();
  let siteType = normalizeVillaTypeShortLabel(typeMaison);
  const prefectureName = String(prefecture || '').trim() || 'Non renseigne';
  const siteName = String(nomSite || '').trim();
  if (Number.isInteger(projectIdValue) && projectIdValue > 0) {
    const parent = await get('SELECT id, nomProjet, typeProjet FROM project_catalog WHERE id = ?', [projectIdValue]);
    if (!parent) {
      return res.status(400).json({ error: 'Projet introuvable pour ce site' });
    }
    projectName = String(parent.nomProjet || '').trim();
    siteType = String(parent.typeProjet || '').trim();
  }
  if (!projectName) {
    return res.status(400).json({ error: 'Le nom du projet est obligatoire' });
  }

  if (!siteName) {
    return res.status(400).json({ error: 'Le nom du site est obligatoire' });
  }

  if (Number.isInteger(projectIdValue) && projectIdValue > 0) {
    const zone = await get(
      'SELECT id FROM project_folders WHERE projectId = ? AND LOWER(prefecture) = LOWER(?) LIMIT 1',
      [projectIdValue, prefectureName]
    );
    if (!zone) {
      return res.status(400).json({ error: 'Créez d\'abord la zone (préfecture) pour ce projet' });
    }
  }

  const duplicate = await get(
    'SELECT id FROM projects WHERE LOWER(nomProjet) = LOWER(?) AND LOWER(prefecture) = LOWER(?) AND LOWER(nomSite) = LOWER(?) LIMIT 1',
    [projectName, prefectureName, siteName]
  );
  if (duplicate) {
    return res.status(409).json({ error: 'Ce site existe deja dans cette prefecture' });
  }

  const nextProjectId = await getNextTableId('projects');

  const result = await run(
    'INSERT INTO projects (id, nomProjet, prefecture, nomSite, typeMaison, numeroMaison, description, etapeConstruction, statutConstruction, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextProjectId,
      projectName,
      prefectureName,
      siteName,
      siteType,
      String(numeroMaison).trim(),
      String(description).trim(),
      etapeConstruction,
      statutConstruction,
      new Date().toISOString(),
    ]
  );

  const project = await get('SELECT * FROM projects WHERE id = ?', [nextProjectId || result.lastID]);
  res.status(201).json(project);
});

app.post('/api/projects/bulk', async (req, res) => {
  const {
    nomProjet,
    prefecture = 'Non renseigne',
    nomSite = '',
    typeMaison = '',
    numeroMaison = '',
    description = '',
    count = 1,
    etapeConstruction = '',
    statutConstruction = ''
  } = req.body;

  const projectName = String(nomProjet || '').trim();
  const prefectureName = String(prefecture || '').trim() || 'Non renseigne';
  const firstSiteName = String(nomSite || '').trim();
  const buildingType = normalizeVillaTypeShortLabel(typeMaison);
  const siteReference = String(numeroMaison || '').trim();
  const requestedCount = Number(count);

  if (!projectName) {
    return res.status(400).json({ error: 'Le nom du projet est obligatoire' });
  }

  if (!firstSiteName) {
    return res.status(400).json({ error: 'Le nom du site est obligatoire' });
  }

  if (!Number.isInteger(requestedCount) || requestedCount < 1 || requestedCount > 500) {
    return res.status(400).json({ error: 'count doit etre un entier entre 1 et 500' });
  }

  const suffixMatch = firstSiteName.match(/(.*?)(\d+)$/);
  const generatedNames = [];

  if (suffixMatch) {
    const prefix = suffixMatch[1] || '';
    const start = Number(suffixMatch[2]);
    const padSize = suffixMatch[2].length;

    for (let i = 0; i < requestedCount; i += 1) {
      generatedNames.push(`${prefix}${String(start + i).padStart(padSize, '0')}`);
    }
  } else {
    generatedNames.push(firstSiteName);
    for (let i = 1; i < requestedCount; i += 1) {
      generatedNames.push(`${firstSiteName}-${i + 1}`);
    }
  }

  const normalizedGenerated = new Set();
  for (const siteName of generatedNames) {
    const key = String(siteName).trim().toLowerCase();
    if (!key) {
      return res.status(400).json({ error: 'Un nom de site genere est invalide' });
    }
    if (normalizedGenerated.has(key)) {
      return res.status(409).json({ error: `Le site ${siteName} est duplique dans le lot` });
    }
    normalizedGenerated.add(key);
  }

  const existingRows = await all(
    'SELECT nomSite FROM projects WHERE LOWER(nomProjet) = LOWER(?) AND LOWER(prefecture) = LOWER(?)',
    [projectName, prefectureName]
  );
  const existingNames = new Set(existingRows.map(row => String(row.nomSite || '').trim().toLowerCase()));

  const conflicting = generatedNames.find(siteName => existingNames.has(String(siteName).trim().toLowerCase()));
  if (conflicting) {
    return res.status(409).json({ error: `Le site ${conflicting} existe deja dans cette prefecture` });
  }

  const createdAt = new Date().toISOString();
  const createdIds = [];
  let nextProjectId = await getNextTableId('projects');

  for (const siteName of generatedNames) {
    const result = await run(
      'INSERT INTO projects (id, nomProjet, prefecture, nomSite, typeMaison, numeroMaison, description, etapeConstruction, statutConstruction, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        nextProjectId,
        projectName,
        prefectureName,
        siteName,
        buildingType,
        siteReference,
        String(description || '').trim(),
        etapeConstruction,
        statutConstruction,
        createdAt,
      ]
    );
    createdIds.push(nextProjectId || result.lastID);
    nextProjectId += 1;
  }

  const placeholders = createdIds.map(() => '?').join(',');
  const created = await all(
    `SELECT * FROM projects WHERE id IN (${placeholders}) ORDER BY id ASC`,
    createdIds
  );

  res.status(201).json({
    count: created.length,
    firstSite: generatedNames[0],
    lastSite: generatedNames[generatedNames.length - 1],
    projects: created,
  });
});

app.get('/api/projects', async (req, res) => {
  const rows = await all('SELECT * FROM projects WHERE isHidden = 0 ORDER BY id DESC');
  const role = String(req.user?.role || '').trim();
  if (role === 'chef_chantier_site' || role === 'gestionnaire_stock_songon') {
    return res.json(rows.filter(row => isInUserProjectScope(req.user, row)));
  }
  res.json(rows);
});

app.patch('/api/projects/:id', async (req, res) => {
  const id = Number(req.params.id);
  const {
    nomProjet,
    prefecture,
    nomSite,
    typeMaison,
    numeroMaison,
    description,
    etapeConstruction,
    statutConstruction,
  } = req.body || {};

  const existingProject = await get('SELECT * FROM projects WHERE id = ?', [id]);
  if (!existingProject) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  const projectName = String(nomProjet ?? existingProject.nomProjet ?? '').trim();
  const prefectureName = String(prefecture ?? existingProject.prefecture ?? 'Non renseigne').trim() || 'Non renseigne';
  const siteName = String(nomSite ?? existingProject.nomSite ?? '').trim();
  const siteType = normalizeVillaTypeShortLabel(typeMaison ?? existingProject.typeMaison ?? '');
  const siteNumber = String(numeroMaison ?? existingProject.numeroMaison ?? '').trim();
  const siteDescription = String(description ?? existingProject.description ?? '').trim();
  const constructionStage = String(etapeConstruction ?? existingProject.etapeConstruction ?? '').trim();
  const constructionStatus = normalizeProjectConstructionStatus(statutConstruction ?? existingProject.statutConstruction ?? '');

  if (!id || !projectName) {
    return res.status(400).json({ error: 'Le nom du projet est obligatoire' });
  }

  if (!siteName) {
    return res.status(400).json({ error: 'Le nom du site est obligatoire' });
  }

  const duplicate = await get(
    'SELECT id FROM projects WHERE LOWER(nomProjet) = LOWER(?) AND LOWER(prefecture) = LOWER(?) AND LOWER(nomSite) = LOWER(?) AND id != ? LIMIT 1',
    [projectName, prefectureName, siteName, id]
  );
  if (duplicate) {
    return res.status(409).json({ error: 'Ce site existe deja dans cette prefecture' });
  }

  const result = await run(
    'UPDATE projects SET nomProjet = ?, prefecture = ?, nomSite = ?, typeMaison = ?, numeroMaison = ?, description = ?, etapeConstruction = ?, statutConstruction = ? WHERE id = ?',
    [
      projectName,
      prefectureName,
      siteName,
      siteType,
      siteNumber,
      siteDescription,
      constructionStage,
      constructionStatus,
      id,
    ]
  );

  if (result.changes === 0) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  const project = await get('SELECT * FROM projects WHERE id = ?', [id]);
  res.json(project);
});

app.delete('/api/projects/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM projects WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }
  res.json({ message: 'Projet supprime avec succes' });
});

app.post('/api/material-requests', async (req, res) => {
  const {
    projetId,
    demandeur,
    etapeApprovisionnement = '',
    itemName = '',
    description = '',
    quantiteDemandee,
    dateDemande,
    groupId = null,
    warehouseId = null,
  } = req.body;

  // Check if warehouse is hidden
  if (warehouseId && isWarehouseHidden(warehouseId)) {
    return res.status(403).json({ error: 'Cet entrepot est indisponible' });
  }

  if (!projetId || !demandeur || quantiteDemandee == null) {
    return res.status(400).json({ error: 'projetId, demandeur et quantiteDemandee sont obligatoires' });
  }

  const quantite = Number(quantiteDemandee);
  if (Number.isNaN(quantite) || quantite <= 0) {
    return res.status(400).json({ error: 'quantiteDemandee doit etre un nombre positif' });
  }

  const requestDate = dateDemande ? new Date(dateDemande) : new Date();
  if (Number.isNaN(requestDate.getTime())) {
    return res.status(400).json({ error: 'dateDemande invalide' });
  }

  const projet = await get('SELECT id, nomProjet, nomSite, numeroMaison FROM projects WHERE id = ?', [projetId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }
  if (!isInChefSiteScope(req.user, projet)) {
    return res.status(403).json({ error: 'Acces refuse: ce chef chantier est limite au site 15' });
  }

  const requestedStageRaw = String(etapeApprovisionnement || '').trim();
  const requestedStageKey = normalizeStageLabel(requestedStageRaw);
  if (requestedStageKey) {
    const existingRows = await all(
      'SELECT id, etapeApprovisionnement, groupId, statut FROM material_requests WHERE projetId = ? AND statut != ?',
      [projetId, 'REJETEE']
    );
    const hasBlockingStage = (existingRows || []).some(row => {
      const sameStage = normalizeStageLabel(row.etapeApprovisionnement) === requestedStageKey;
      if (!sameStage) return false;

      const sameGroup = groupId && String(row.groupId || '').trim() && String(row.groupId || '').trim() === String(groupId).trim();
      return !sameGroup;
    });

    if (hasBlockingStage) {
      return res.status(409).json({
        error: `Une demande existe deja pour l'etape "${requestedStageRaw}" sur ce site. Ajoute tous les materiaux de l'etape dans la meme demande.`,
      });
    }
  }

  const requestedItemName = String(itemName || '').trim();
  if (requestedItemName && requestedStageKey) {
    const projectFolder = String(projet.nomProjet || '').trim();
    const catalogRows = projectFolder
      ? await all(
          `SELECT materialName, quantiteParBatiment, notes
           FROM building_material_catalog
           WHERE projectFolder = ?
             AND LOWER(TRIM(materialName)) = LOWER(TRIM(?))`,
          [projectFolder, requestedItemName]
        )
      : [];

    const matchingCatalog = (catalogRows || []).find(row => isCatalogStageMatching(row.notes, requestedStageRaw));
    const plannedQty = Number(matchingCatalog?.quantiteParBatiment || 0);

    if (plannedQty > 0) {
      const existingMaterialRows = await all(
        `SELECT id, etapeApprovisionnement, quantiteDemandee
         FROM material_requests
         WHERE projetId = ?
           AND LOWER(TRIM(itemName)) = LOWER(TRIM(?))
           AND statut != ?`,
        [projetId, requestedItemName, 'REJETEE']
      );

      const sameStageMaterialRows = (existingMaterialRows || []).filter(
        row => normalizeStageLabel(row.etapeApprovisionnement) === requestedStageKey
      );

      const alreadyRequestedQty = sameStageMaterialRows.reduce(
        (sum, row) => sum + Number(row.quantiteDemandee || 0),
        0
      );

      const transferRows = await all(
        `SELECT si.quantiteSortie, si.issueType, si.note, mr.etapeApprovisionnement
         FROM stock_issues si
         JOIN material_requests mr ON mr.id = si.materialRequestId
         WHERE mr.projetId = ?
           AND LOWER(TRIM(mr.itemName)) = LOWER(TRIM(?))
           AND mr.statut != ?`,
        [projetId, requestedItemName, 'REJETEE']
      );

      const alreadyReceivedQty = (transferRows || []).reduce((sum, row) => {
        if (normalizeStageLabel(row.etapeApprovisionnement) !== requestedStageKey) {
          return sum;
        }

        const rawIssueType = String(row.issueType || '').trim().toUpperCase();
        const issueType = rawIssueType || (String(row.note || '').startsWith('Consommation chantier') ? 'CONSUMPTION' : 'SITE_TRANSFER');
        if (issueType !== 'SITE_TRANSFER') {
          return sum;
        }

        return sum + Number(row.quantiteSortie || 0);
      }, 0);

      if (alreadyReceivedQty >= plannedQty) {
        return res.status(409).json({
          error: `Demande rejetee automatiquement: ${requestedItemName} (${requestedStageRaw}) deja recu sur ce site selon le debourse sec (${plannedQty.toFixed(2)} prevu).`,
        });
      }

      if (alreadyRequestedQty > 0 && alreadyRequestedQty + quantite >= plannedQty) {
        return res.status(409).json({
          error: `Demande rejetee: la quantite demandee atteint la limite du debourse sec pour ${requestedItemName} (${requestedStageRaw}). Prevu: ${plannedQty.toFixed(2)}, deja demande: ${alreadyRequestedQty.toFixed(2)}.`,
        });
      }
    }
  }

  const nextMrIdPost = await getNextTableId('material_requests');
  const result = await run(
    'INSERT INTO material_requests (id, projetId, demandeur, etapeApprovisionnement, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut, groupId, warehouseId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextMrIdPost,
      projetId,
      demandeur,
      String(etapeApprovisionnement || '').trim(),
      String(itemName).trim() || 'Materiel divers',
      description || '',
      quantite,
      quantite,
      requestDate.toISOString(),
      'EN_ATTENTE',
      groupId ? String(groupId).trim() : null,
      warehouseId ? String(warehouseId).trim() : null,
    ]
  );

  const request = await get('SELECT * FROM material_requests WHERE id = ?', [result.lastID || nextMrIdPost]);

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const siteLabel = String(projet?.numeroMaison || '').trim() || '-';
    const itemLabel = String(request?.itemName || '').trim() || 'Materiel divers';
    await mirrorSiteChiefActionToAdmin({
      user: req.user,
      projectId: Number(projetId),
      stage: String(etapeApprovisionnement || '').trim(),
      title: `Demande materiau creee (lot ${siteLabel})`,
      note: `Article: ${itemLabel} | Quantite: ${quantite.toFixed(2)} | Demandeur: ${String(demandeur || '').trim()}`,
    });
  }

  res.status(201).json(request);
});

app.get('/api/material-requests', async (req, res) => {
  const rows = await all(`
    SELECT mr.*, COALESCE(p.nomProjet, 'Projet supprime') as projetNom, p.numeroMaison, p.typeMaison, pomax.dateProduitRecu,
      COALESCE(NULLIF(TRIM(mr.warehouseId), ''), po.warehouseId) as warehouseId
    FROM material_requests mr
    LEFT JOIN projects p ON p.id = mr.projetId
    LEFT JOIN (
      SELECT materialRequestId,
        MAX(id) as max_po_id,
        MAX(CASE WHEN statut = 'LIVREE' THEN COALESCE(dateReception, dateCommande) END) as dateProduitRecu
      FROM purchase_orders
      GROUP BY materialRequestId
    ) pomax ON pomax.materialRequestId = mr.id
    LEFT JOIN purchase_orders po ON po.id = pomax.max_po_id
    ORDER BY mr.dateDemande DESC
  `);
  const role = String(req.user?.role || '').trim();
  if (role === 'chef_chantier_site' || role === 'gestionnaire_stock_songon') {
    return res.json(rows.filter(row => isInUserProjectScope(req.user, row)));
  }
  res.json(rows);
});

app.patch('/api/material-requests/:id/remaining', async (req, res) => {
  const id = Number(req.params.id);
  const { quantiteRestante } = req.body;

  const result = await run('UPDATE material_requests SET quantiteRestante = ? WHERE id = ?', [quantiteRestante, id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Demande non trouvée' });
  }

  const request = await get('SELECT * FROM material_requests WHERE id = ?', [id]);
  res.json(request);
});

app.patch('/api/material-requests/:id/statut', async (req, res) => {
  const id = Number(req.params.id);
  const { statut } = req.body;

  if (!['EN_ATTENTE', 'APPROUVEE', 'REJETEE', 'LIVREE', 'EN_COURS', 'EN_STOCK', 'EPUISE'].includes(statut)) {
    return res.status(400).json({ error: 'Statut invalide' });
  }

  const result = await run('UPDATE material_requests SET statut = ? WHERE id = ?', [statut, id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Demande non trouvée' });
  }

  const request = await get('SELECT * FROM material_requests WHERE id = ?', [id]);
  res.json(request);
});

app.post('/api/material-requests/group-authorization', async (req, res) => {
  const {
    requestIds = [],
    signatureName = '',
    signatureRole = '',
    signedAt = null,
  } = req.body || {};

  const normalizedIds = Array.from(new Set(
    (Array.isArray(requestIds) ? requestIds : [])
      .map(value => Number(value))
      .filter(id => Number.isInteger(id) && id > 0)
  ));

  if (!normalizedIds.length) {
    return res.status(400).json({ error: 'Aucune demande valide fournie' });
  }
  if (!String(signatureName || '').trim() || !signedAt) {
    return res.status(400).json({ error: 'Signature requise pour autoriser le transfert' });
  }

  const placeholders = normalizedIds.map(() => '?').join(',');
  const requests = await all(
    `SELECT mr.*, p.nomProjet, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id IN (${placeholders})`,
    normalizedIds
  );

  if (!requests.length) {
    return res.status(404).json({ error: 'Demandes introuvables' });
  }

  const docs = [];
  const approvedRequestIds = [];
  const rejectedRequestIds = [];
  const insufficiencies = [];
  const nowIso = signedAt ? new Date(signedAt).toISOString() : new Date().toISOString();

  const stockRows = await all(
    `SELECT
      COALESCE(NULLIF(TRIM(warehouseId), ''), '') AS warehouseId,
      LOWER(TRIM(itemName)) AS materialKey,
      COALESCE(SUM(COALESCE(quantiteRestante, 0)), 0) AS availableQty
     FROM material_requests
     WHERE statut IN ('EN_STOCK', 'EPUISE')
     GROUP BY COALESCE(NULLIF(TRIM(warehouseId), ''), ''), LOWER(TRIM(itemName))`
  );
  const availableByWarehouseMaterial = new Map();
  for (const row of stockRows) {
    const key = `${String(row?.warehouseId || '').trim()}::${String(row?.materialKey || '').trim()}`;
    availableByWarehouseMaterial.set(key, Number(row?.availableQty || 0));
  }

  for (const request of requests) {
    const requestedQty = Number(request.quantiteDemandee || 0);
    const warehouseId = String(request.warehouseId || '').trim();
    const materialName = String(request.itemName || '').trim();
    const materialKey = materialName.toLowerCase();
    const stockKey = `${warehouseId}::${materialKey}`;
    const availableQty = Number(availableByWarehouseMaterial.get(stockKey) || 0);
    const hasEnoughStock = Boolean(warehouseId) && Boolean(materialKey) && requestedQty > 0 && availableQty >= requestedQty;
    const decisionStatus = hasEnoughStock ? 'VALIDEE' : 'REJETEE';

    if (hasEnoughStock) {
      availableByWarehouseMaterial.set(stockKey, availableQty - requestedQty);
      approvedRequestIds.push(request.id);
    } else {
      rejectedRequestIds.push(request.id);
      insufficiencies.push({
        requestId: request.id,
        itemName: materialName || 'Article',
        warehouseId,
        warehouseLabel: resolveWarehouseLabel(warehouseId),
        requestedQty,
        availableQty,
      });
    }

    await run('UPDATE material_requests SET statut = ? WHERE id = ?', [decisionStatus, request.id]);

    const pseudoOrder = {
      id: `REQ-${request.id}`,
      fournisseur: 'Sans bon de commande',
      montantTotal: 0,
      quantiteCommandee: Number(request.quantiteDemandee || 0),
      prixUnitaire: 0,
    };
    const items = [{
      article: String(request.itemName || 'Article'),
      quantite: Number(request.quantiteDemandee || 0),
      prixUnitaire: 0,
      totalLigne: 0,
    }];

    const doc = await archiveMaterialRequestAuthorizationPdf({
      order: pseudoOrder,
      request,
      items,
      signatureName: String(signatureName || '').trim(),
      signatureRole: String(signatureRole || '').trim(),
      signedAt: nowIso,
      decisionStatus,
    });
    docs.push({ requestId: request.id, decisionStatus, doc });
  }

  res.json({
    updatedRequestIds: requests.map(row => row.id),
    approvedRequestIds,
    rejectedRequestIds,
    insufficiencies,
    docs,
    message: rejectedRequestIds.length
      ? 'Certaines demandes ont ete refusees: stock insuffisant dans l\'entrepot de la zone.'
      : 'Demandes autorisees et autorisations generees.',
  });
});

app.patch('/api/material-requests/:id', async (req, res) => {
  const id = Number(req.params.id);
  const {
    projetId,
    demandeur,
    etapeApprovisionnement = '',
    itemName = '',
    description = '',
    quantiteDemandee,
    dateDemande,
  } = req.body;

  if (!id || !projetId || !demandeur || quantiteDemandee == null) {
    return res.status(400).json({ error: 'projetId, demandeur et quantiteDemandee sont obligatoires' });
  }

  const quantite = Number(quantiteDemandee);
  if (Number.isNaN(quantite) || quantite <= 0) {
    return res.status(400).json({ error: 'quantiteDemandee doit etre un nombre positif' });
  }

  const requestDate = dateDemande ? new Date(dateDemande) : new Date();
  if (Number.isNaN(requestDate.getTime())) {
    return res.status(400).json({ error: 'dateDemande invalide' });
  }

  const projet = await get('SELECT id FROM projects WHERE id = ?', [projetId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  const existing = await get('SELECT quantiteRestante FROM material_requests WHERE id = ?', [id]);
  if (!existing) {
    return res.status(404).json({ error: 'Demande non trouvee' });
  }

  const newRemaining = Math.min(Number(existing.quantiteRestante || 0), quantite);

  const result = await run(
    'UPDATE material_requests SET projetId = ?, demandeur = ?, etapeApprovisionnement = ?, itemName = ?, description = ?, quantiteDemandee = ?, quantiteRestante = ?, dateDemande = ? WHERE id = ?',
    [
      projetId,
      String(demandeur).trim(),
      String(etapeApprovisionnement || '').trim(),
      String(itemName).trim() || 'Materiel divers',
      String(description || ''),
      quantite,
      newRemaining,
      requestDate.toISOString(),
      id,
    ]
  );

  if (result.changes === 0) {
    return res.status(404).json({ error: 'Demande non trouvee' });
  }

  const request = await get(`
    SELECT mr.*, p.nomProjet as projetNom, p.numeroMaison, p.typeMaison, po.dateProduitRecu
    FROM material_requests mr
    JOIN projects p ON p.id = mr.projetId
    LEFT JOIN (
      SELECT materialRequestId, MAX(COALESCE(dateReception, dateCommande)) as dateProduitRecu
      FROM purchase_orders
      WHERE statut = 'LIVREE'
      GROUP BY materialRequestId
    ) po ON po.materialRequestId = mr.id
    WHERE mr.id = ?
  `, [id]);

  res.json(request);
});

app.post('/api/purchase-orders', async (req, res) => {
  const {
    materialRequestId,
    creePar = 'admin',
    fournisseur,
    quantiteCommandee,
    prixUnitaire,
    montantTotal,
    dateLivraisonPrevue,
    dateCommande,
    items,
    projetId = null,
    nomProjetManuel = null,
    siteId = null,
    nomSiteManuel = null,
    warehouseId = null,
    etapeApprovisionnement = null,
    signatureName = null,
    signatureRole = null,
  } = req.body;
  const createdBy = String(req.user?.username || creePar || 'admin').trim() || 'admin';

  // Check if warehouse is hidden
  if (warehouseId && isWarehouseHidden(warehouseId)) {
    return res.status(403).json({ error: 'Cet entrepot est indisponible' });
  }

  if (!fournisseur || !String(fournisseur).trim()) {
    return res.status(400).json({ error: 'Le fournisseur est obligatoire' });
  }

  const preparedItems = await preparePurchaseOrderItems({
    items,
    materialRequestId,
    quantiteCommandee,
    prixUnitaire,
    montantTotal,
  });

  if (preparedItems === 'INVALID_PO_ITEM') {
    return res.status(400).json({ error: 'Chaque article doit avoir une quantite et un prix valides' });
  }

  if (!preparedItems || !preparedItems.length) {
    if (preparedItems === null) {
      return res.status(404).json({ error: 'Demande de materiel introuvable pour un article' });
    }
    return res.status(400).json({ error: 'Ajoute au moins un article pour le bon de commande' });
  }

  const firstRequestId = preparedItems[0].materialRequestId;
  const computedTotal = preparedItems.reduce((sum, item) => sum + Number(item.totalLigne || 0), 0);

  let resolvedProjetId = projetId ? Number(projetId) : null;
  let resolvedSiteId = siteId ? Number(siteId) : null;
  let resolvedWarehouseId = warehouseId ? String(warehouseId).trim() : null;
  let resolvedNomProjet = nomProjetManuel || null;
  let resolvedNomSite = nomSiteManuel || null;
  let resolvedEtape = String(etapeApprovisionnement || '').trim() || null;

  if (firstRequestId) {
    const linkedRequest = await get(`
      SELECT mr.projetId, mr.warehouseId, mr.etapeApprovisionnement, p.nomProjet, p.nomSite, p.numeroMaison
      FROM material_requests mr
      LEFT JOIN projects p ON p.id = mr.projetId
      WHERE mr.id = ?
    `, [firstRequestId]);

    if (linkedRequest) {
      if (!resolvedProjetId) {
        resolvedProjetId = Number(linkedRequest.projetId || 0) || null;
      }
      if (!resolvedSiteId && linkedRequest.projetId) {
        resolvedSiteId = Number(linkedRequest.projetId || 0) || null;
      }
      if (!resolvedNomProjet && linkedRequest.nomProjet) {
        resolvedNomProjet = linkedRequest.nomProjet;
      }
      if (!resolvedNomSite) {
        resolvedNomSite = [linkedRequest.nomSite, linkedRequest.numeroMaison].filter(Boolean).join(' ') || null;
      }
      if (!resolvedWarehouseId) {
        resolvedWarehouseId = String(linkedRequest.warehouseId || '').trim() || null;
      }
      if (!resolvedEtape) {
        resolvedEtape = String(linkedRequest.etapeApprovisionnement || '').trim() || null;
      }
    }
  }

  if (resolvedProjetId && !resolvedNomProjet) {
    const proj = await get('SELECT nomProjet, nomSite, numeroMaison FROM projects WHERE id = ?', [Number(resolvedProjetId)]);
    if (proj) {
      resolvedNomProjet = proj.nomProjet || null;
      if (!resolvedNomSite) resolvedNomSite = [proj.nomSite, proj.numeroMaison].filter(Boolean).join(' ') || null;
    }
  }

  const resolvedStageKey = normalizeStageLabel(resolvedEtape);
  const resolvedSiteKey = Number(resolvedSiteId || resolvedProjetId || 0);
  if (resolvedStageKey && resolvedSiteKey) {
    const stageOrders = await all(
      `SELECT DISTINCT
        po.id,
        po.etapeApprovisionnement,
        po.statut,
        mr.etapeApprovisionnement AS requestStage
       FROM purchase_orders po
       LEFT JOIN purchase_order_items poi ON poi.purchaseOrderId = po.id
       LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
       WHERE COALESCE(po.siteId, po.projetId) = ?
         AND COALESCE(po.statut, '') != 'ANNULEE'`,
      [resolvedSiteKey]
    );

    const hasSameStageOrder = (stageOrders || []).some(row => {
      const stageLabel = String(row.etapeApprovisionnement || row.requestStage || '').trim();
      return normalizeStageLabel(stageLabel) === resolvedStageKey;
    });

    if (hasSameStageOrder) {
      return res.status(409).json({
        error: `Un bon de commande existe deja pour l'etape "${resolvedEtape}" sur ce site.`,
      });
    }
  }

  const orderDate = dateCommande ? new Date(dateCommande) : new Date();
  if (Number.isNaN(orderDate.getTime())) {
    return res.status(400).json({ error: 'dateCommande invalide' });
  }

  const nextPurchaseOrderIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_orders');
  const purchaseOrderId = Number(nextPurchaseOrderIdRow?.nextId || nextPurchaseOrderIdRow?.nextid || 1);

  const nextPurchaseOrderItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_order_items');
  let nextPurchaseOrderItemId = Number(nextPurchaseOrderItemIdRow?.nextId || nextPurchaseOrderItemIdRow?.nextid || 1);

  const result = await run(
    'INSERT INTO purchase_orders (id, materialRequestId, creePar, fournisseur, montantTotal, quantiteCommandee, prixUnitaire, dateCommande, dateLivraisonPrevue, statut, statutValidation, projetId, nomProjetManuel, siteId, nomSiteManuel, warehouseId, etapeApprovisionnement, signatureName, signatureRole) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      purchaseOrderId,
      firstRequestId,
      createdBy,
      String(fournisseur).trim(),
      computedTotal,
      preparedItems[0].quantite,
      preparedItems[0].prixUnitaire,
      orderDate.toISOString(),
      dateLivraisonPrevue || null,
      'EN_COURS',
      'EN_COURS',
      resolvedProjetId,
      resolvedNomProjet,
      resolvedSiteId,
      resolvedNomSite,
      resolvedWarehouseId,
      resolvedEtape,
      signatureName ? String(signatureName).trim() : null,
      signatureRole ? String(signatureRole).trim() : null,
    ]
  );

  for (const item of preparedItems) {
    await run(
      'INSERT INTO purchase_order_items (id, purchaseOrderId, materialRequestId, article, details, quantite, prixUnitaire, totalLigne) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [nextPurchaseOrderItemId++, purchaseOrderId, item.materialRequestId || null, item.article, item.details, item.quantite, item.prixUnitaire, item.totalLigne]
    );
    if (item.materialRequestId) {
      await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['EN_COURS', item.materialRequestId]);
    }
  }

  await runPurchaseOrderSideEffects(purchaseOrderId, {
    fournisseur: String(fournisseur).trim(),
    dateCommande: orderDate.toISOString(),
    createdBy: req.user ? req.user.username : 'admin',
  });

  const order = await getPurchaseOrderById(purchaseOrderId);
  res.status(201).json(order);
});

app.get('/api/purchase-orders', async (_req, res) => {
  const rows = await all('SELECT * FROM purchase_orders ORDER BY dateCommande DESC');
  const enriched = await enrichPurchaseOrders(rows);
  res.json(enriched);
});

app.get('/api/purchase-orders/:id/pdf', async (req, res) => {
  const id = Number(req.params.id);
  const order = await getPurchaseOrderById(id);

  if (!order) {
    return res.status(404).json({ error: 'Bon de commande non trouve' });
  }

  const filename = `bon-commande-${id}.pdf`;
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  const buffer = await generatePurchaseOrderPdfBuffer(order);
  res.end(buffer);
});

app.patch('/api/purchase-orders/:id/validation', async (req, res) => {
  const id = Number(req.params.id);
  const { statut, signatureName = '', signatureRole = '', signedAt = null } = req.body || {};
  const role = String(req.user?.role || '').trim();

  if (!['EN_COURS', 'VALIDEE', 'LIVREE', 'ANNULEE'].includes(statut)) {
    return res.status(400).json({ error: 'Statut invalide' });
  }

  if (role !== 'admin' && role !== 'dirigeant') {
    return res.status(403).json({ error: 'Seul le dirigeant peut valider/rejeter les bons de commande' });
  }

  const existingOrder = await get('SELECT id, creePar, statutValidation FROM purchase_orders WHERE id = ?', [id]);
  if (!existingOrder) {
    return res.status(404).json({ error: 'Commande non trouvée' });
  }

  if (role === 'dirigeant') {
    if (statut === 'LIVREE' || statut === 'EN_COURS') {
      return res.status(400).json({ error: 'Le dirigeant peut uniquement valider ou rejeter un bon' });
    }
  }

  if ((statut === 'VALIDEE' || statut === 'ANNULEE') && (!String(signatureName || '').trim() || !signedAt)) {
    return res.status(400).json({ error: 'Signature requise pour valider ou rejeter' });
  }

  const receptionDate = statut === 'LIVREE' ? new Date().toISOString() : null;
  const normalizedSignatureName = String(signatureName || '').trim();
  const normalizedSignatureRole = String(signatureRole || '').trim();
  const result = await run(
    `UPDATE purchase_orders
     SET statut = ?,
         statutValidation = ?,
         dateReception = CASE WHEN ? = 'LIVREE' THEN COALESCE(dateReception, ?) ELSE dateReception END,
         signatureName = CASE WHEN ? IN ('VALIDEE', 'ANNULEE') THEN ? ELSE signatureName END,
         signatureRole = CASE WHEN ? IN ('VALIDEE', 'ANNULEE') THEN ? ELSE signatureRole END
     WHERE id = ?`,
    [
      statut,
      statut,
      statut,
      receptionDate,
      statut,
      normalizedSignatureName || null,
      statut,
      normalizedSignatureRole || null,
      id,
    ]
  );
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Commande non trouvée' });
  }

  try {
    await archivePurchaseOrderPdf(id);
  } catch (error) {
    console.error('Erreur regeneration PDF bon de commande:', error?.message || error);
  }

  const linkedItems = await all('SELECT materialRequestId FROM purchase_order_items WHERE purchaseOrderId = ?', [id]);
  const linkedRequestIds = Array.from(new Set(linkedItems.map(item => Number(item.materialRequestId)).filter(Boolean)));
  if (!linkedRequestIds.length) {
    const legacyLink = await get('SELECT materialRequestId FROM purchase_orders WHERE id = ?', [id]);
    const fallbackRequestId = Number(legacyLink?.materialRequestId || 0);
    if (Number.isInteger(fallbackRequestId) && fallbackRequestId > 0) {
      linkedRequestIds.push(fallbackRequestId);
    }
  }

  for (const requestId of linkedRequestIds) {
    if (statut === 'LIVREE') {
      await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['EN_STOCK', requestId]);
    } else if (statut === 'VALIDEE') {
      await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['VALIDEE', requestId]);
    } else if (statut === 'EN_COURS') {
      await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['EN_COURS', requestId]);
    } else if (statut === 'ANNULEE') {
      await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['REJETEE', requestId]);
    }
  }

  const order = await getPurchaseOrderById(id);
  res.json({ ...order, authorizationDocs: [] });
});

app.get('/api/transfer-authorizations', async (req, res) => {
  const warehouseId = String(req.query.warehouseId || '').trim();
  const params = [];
  let whereClause = `WHERE mr.statut IN ('VALIDEE', 'REJETEE')`;

  if (warehouseId) {
    whereClause += " AND COALESCE(mr.warehouseId, '') = ?";
    params.push(warehouseId);
  }

  const rows = await all(
    `SELECT
      mr.id AS requestId,
      mr.itemName,
      mr.demandeur,
      mr.warehouseId,
      mr.statut AS requestStatus,
      mr.dateDemande AS requestDateDemande,
      mr.etapeApprovisionnement AS requestEtape,
      p.nomProjet,
      p.numeroMaison,
      po.id AS purchaseOrderId,
      po.fournisseur,
      po.dateCommande,
      po.etapeApprovisionnement AS poEtape,
      gd.id AS documentId,
      gd.title,
      gd.fileName,
      gd.relativePath,
      gd.createdAt AS documentCreatedAt,
      gd.updatedAt AS documentUpdatedAt
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     LEFT JOIN purchase_order_items poi ON poi.materialRequestId = mr.id
     LEFT JOIN purchase_orders po ON po.id = poi.purchaseOrderId
     LEFT JOIN generated_documents gd ON gd.entityType = 'material_request_authorization' AND gd.entityId = mr.id
     ${whereClause}
     ORDER BY COALESCE(gd.updatedAt, mr.dateDemande, po.dateCommande) DESC, mr.id DESC`,
    params
  );

  const dedup = new Map();
  for (const row of rows) {
    const requestId = Number(row.requestId || 0);
    if (!requestId) continue;
    if (dedup.has(requestId)) continue;

    dedup.set(requestId, {
      requestId,
      itemName: row.itemName,
      demandeur: row.demandeur,
      warehouseId: row.warehouseId,
      status: row.requestStatus || 'EN_ATTENTE',
      nomProjet: row.nomProjet,
      numeroMaison: row.numeroMaison,
      requestEtape: row.requestEtape || null,
      poEtape: row.poEtape || null,
      purchaseOrderId: row.purchaseOrderId ? Number(row.purchaseOrderId) : null,
      fournisseur: row.fournisseur,
      dateCommande: row.dateCommande,
      decidedAt: row.documentUpdatedAt || row.requestDateDemande || row.dateCommande,
      fileName: row.fileName || null,
      fileUrl: row.relativePath ? `/archives/${String(row.relativePath).replace(/\\/g, '/')}` : null,
    });
  }

  res.json(Array.from(dedup.values()));
});

app.get('/api/material-requests/:id/authorization-documents', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'ID demande invalide' });
  }

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const scopedRequest = await get(
      `SELECT mr.id, p.nomSite, p.numeroMaison
       FROM material_requests mr
       LEFT JOIN projects p ON p.id = mr.projetId
       WHERE mr.id = ?`,
      [id]
    );
    if (!scopedRequest || !isInChefSiteScope(req.user, scopedRequest)) {
      return res.status(403).json({ error: 'Acces refuse: ce document ne concerne pas votre site' });
    }
  }

  const rows = await all(
    'SELECT * FROM generated_documents WHERE entityType = ? AND entityId = ? ORDER BY updatedAt DESC, id DESC',
    ['material_request_authorization', id]
  );

  const docs = (rows || []).map(row => ({
    ...row,
    fileUrl: `/archives/${String(row.relativePath || '').replace(/\\/g, '/')}`,
  }));
  res.json(docs);
});

app.get('/api/material-requests/group/pdf', async (req, res) => {
  const ids = String(req.query.ids || '')
    .split(',')
    .map(value => Number(value))
    .filter(value => Number.isInteger(value) && value > 0);

  const uniqueIds = Array.from(new Set(ids));
  if (!uniqueIds.length) {
    return res.status(400).json({ error: 'Aucune demande valide fournie' });
  }

  const placeholders = uniqueIds.map(() => '?').join(',');
  const requests = await all(
    `SELECT mr.*, p.nomProjet, p.nomSite, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id IN (${placeholders})
     ORDER BY mr.id ASC`,
    uniqueIds
  );

  if (!requests.length) {
    return res.status(404).json({ error: 'Demande introuvable' });
  }

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const hasOutOfScopeRequest = requests.some(request => !isInChefSiteScope(req.user, request));
    if (hasOutOfScopeRequest) {
      return res.status(403).json({ error: 'Acces refuse: une ou plusieurs demandes ne concernent pas votre site' });
    }
  }

  const primaryRequest = requests[0];
  const pseudoOrder = {
    id: `REQ-${primaryRequest.id}`,
    fournisseur: 'Demande approvisionnement',
    montantTotal: 0,
    quantiteCommandee: Number(primaryRequest.quantiteDemandee || 0),
    prixUnitaire: 0,
    warehouseId: primaryRequest.warehouseId,
  };
  const items = requests.map(request => ({
    article: String(request.itemName || 'Article'),
    quantite: Number(request.quantiteDemandee || 0),
    prixUnitaire: 0,
    totalLigne: 0,
  }));

  const buffer = await generateMaterialAuthorizationPdfBuffer({
    order: pseudoOrder,
    request: primaryRequest,
    items,
    signatureName: String(primaryRequest.demandeur || '').trim(),
    signatureRole: 'Demandeur',
    signedAt: primaryRequest.dateDemande || new Date().toISOString(),
    decisionStatus: 'DEMANDEE',
    documentKind: 'request',
  });

  const fileName = sanitizeFileName(`demande-approvisionnement-${uniqueIds.join('-')}.pdf`);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
  res.end(buffer);
});

app.get('/api/purchase-orders/:id/authorization-documents', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'ID bon invalide' });
  }

  const order = await getPurchaseOrderById(id);
  if (!order) {
    return res.status(404).json({ error: 'Bon de commande non trouve' });
  }

  const requestIds = Array.from(new Set((order.items || []).map(item => Number(item.materialRequestId)).filter(requestId => Number.isInteger(requestId) && requestId > 0)));
  if (!requestIds.length) {
    return res.json([]);
  }

  const placeholders = requestIds.map(() => '?').join(',');
  const requestRows = await all(
    `SELECT mr.id, mr.itemName, mr.demandeur, mr.warehouseId, mr.statut, p.nomProjet, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id IN (${placeholders})`,
    requestIds
  );
  const requestById = new Map(requestRows.map(row => [Number(row.id), row]));

  const docs = [];
  for (const requestId of requestIds) {
    const rows = await all(
      'SELECT * FROM generated_documents WHERE entityType = ? AND entityId = ? ORDER BY updatedAt DESC, id DESC',
      ['material_request_authorization', requestId]
    );
    const latest = rows && rows.length ? rows[0] : null;
    const request = requestById.get(Number(requestId)) || null;
    docs.push({
      requestId,
      request,
      doc: latest
        ? {
            ...latest,
            fileUrl: `/archives/${String(latest.relativePath || '').replace(/\\/g, '/')}`,
          }
        : null,
    });
  }

  res.json(docs);
});

app.get('/api/stock-management/orders', async (req, res) => {
  const purchaseOrderColumns = await getTableColumns('purchase_orders');
  const selectMontantTotal = purchaseOrderColumns.has('montantTotal') ? 'po.montantTotal' : '0 AS montantTotal';
  const selectWarehouseId = purchaseOrderColumns.has('warehouseId') ? 'po.warehouseId' : 'NULL AS warehouseId';
  const selectSiteId = purchaseOrderColumns.has('siteId') ? 'po.siteId' : 'NULL AS siteId';
  const selectNomProjetManuel = purchaseOrderColumns.has('nomProjetManuel') ? 'po.nomProjetManuel' : 'NULL AS nomProjetManuel';
  const selectNomSiteManuel = purchaseOrderColumns.has('nomSiteManuel') ? 'po.nomSiteManuel' : 'NULL AS nomSiteManuel';
  const selectPoEtape = purchaseOrderColumns.has('etapeApprovisionnement')
    ? 'po.etapeApprovisionnement AS poEtape'
    : 'NULL AS poEtape';

  const rows = await all(`
    SELECT
      po.id,
      po.fournisseur,
      po.statut,
      po.dateCommande,
      po.dateReception,
      ${selectMontantTotal},
      ${selectWarehouseId},
      ${selectSiteId},
      ${selectNomProjetManuel},
      ${selectNomSiteManuel},
      ${selectPoEtape},
      poi.article,
      poi.quantite,
      poi.materialRequestId,
      mr.etapeApprovisionnement AS itemEtape,
      p.nomProjet,
      p.numeroMaison,
      p.prefecture,
      p.typeMaison
    FROM purchase_orders po
    LEFT JOIN purchase_order_items poi ON poi.purchaseOrderId = po.id
    LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
    LEFT JOIN projects p ON p.id = mr.projetId
    WHERE UPPER(COALESCE(po.statutValidation, po.statut, '')) IN ('VALIDEE', 'LIVREE')
    ORDER BY po.dateCommande DESC, po.id DESC, poi.id ASC
  `);

  const byOrder = new Map();
  for (const row of rows) {
    const orderId = Number(row.id);
    if (!byOrder.has(orderId)) {
      byOrder.set(orderId, {
        id: orderId,
        fournisseur: row.fournisseur,
        statut: row.statut,
        dateCommande: row.dateCommande,
        dateReception: row.dateReception,
        montantTotal: Number(row.montantTotal || 0),
        warehouseId: row.warehouseId || null,
        siteId: row.siteId || null,
        nomProjetManuel: row.nomProjetManuel || null,
        nomSiteManuel: row.nomSiteManuel || null,
        numeroMaison: row.numeroMaison || null,
        zoneName: null,
        isZoneOrder: /^zone\s+/i.test(String(row.nomSiteManuel || '').trim()),
        etapeApprovisionnement: row.poEtape || null,
        projects: new Set(),
        items: [],
      });
    }

    const order = byOrder.get(orderId);
    if (row.nomProjet) {
      order.projects.add(row.nomProjet);
    }
    if (String(row.typeMaison || '').trim().toUpperCase() === 'ZONE_STOCK') {
      order.isZoneOrder = true;
      if (!order.zoneName) {
        order.zoneName = String(row.prefecture || '').trim() || null;
      }
    }
    if (row.article) {
      order.items.push({
        article: row.article,
        quantite: Number(row.quantite || 0),
        materialRequestId: Number(row.materialRequestId || 0),
        etapeApprovisionnement: row.itemEtape || null,
      });
    }
  }

  let result = Array.from(byOrder.values()).map(order => ({
    ...order,
    nomProjet: String(order.nomProjetManuel || '').trim() || Array.from(order.projects)[0] || null,
    zoneName: order.zoneName || String(order.nomSiteManuel || '').trim().replace(/^zone\s*/i, '').trim() || null,
    projects: Array.from(order.projects),
  }));

  const role = String(req.user?.role || '').trim();
  if (role === 'chef_chantier_site' || role === 'gestionnaire_stock_songon') {
    result = result.filter(order => isInUserProjectScope(req.user, {
      numeroMaison: order.numeroMaison,
      nomSite: order.nomSiteManuel,
      nomProjet: order.nomProjet,
      zoneName: order.zoneName,
    }));
  }

  res.json(result);
});

app.patch('/api/stock-management/orders/:id/arrive', async (req, res) => {
  const orderId = Number(req.params.id);
  if (!orderId) {
    return res.status(400).json({ error: 'ID commande invalide' });
  }

  const orderBeforeUpdate = await get('SELECT id, statut, statutValidation FROM purchase_orders WHERE id = ?', [orderId]);
  if (!orderBeforeUpdate) {
    return res.status(404).json({ error: 'Commande introuvable' });
  }

  const validationStatus = String(orderBeforeUpdate.statutValidation || orderBeforeUpdate.statut || '').trim().toUpperCase();
  if (validationStatus !== 'VALIDEE' && validationStatus !== 'LIVREE') {
    return res.status(409).json({ error: 'Le bon doit etre validé avant de marquer arrivé' });
  }

  const arrivalDate = new Date().toISOString();

  const updateOrder = await run(
    'UPDATE purchase_orders SET statut = ?, statutValidation = ?, dateReception = ? WHERE id = ?',
    ['LIVREE', 'LIVREE', arrivalDate, orderId]
  );
  if (updateOrder.changes === 0) {
    return res.status(404).json({ error: 'Commande introuvable' });
  }

  const orderRow = await get('SELECT * FROM purchase_orders WHERE id = ?', [orderId]);
  if (!orderRow) {
    return res.status(404).json({ error: 'Commande introuvable' });
  }

  const directRequestId = Number(orderRow.materialRequestId || 0);
  const linkedRequestIds = await ensureMaterialRequestsForOrder(orderId, {
    forceStatus: 'EN_STOCK',
    dateOverride: arrivalDate,
  });

  try {
    const legacyDeliveredOrders = await all(`
      SELECT po.id
      FROM purchase_orders po
      LEFT JOIN purchase_order_items poi ON poi.purchaseOrderId = po.id
      GROUP BY po.id
      HAVING (UPPER(COALESCE(po.statut, '')) = 'LIVREE' OR UPPER(COALESCE(po.statutValidation, '')) = 'LIVREE')
        AND COALESCE(po.materialRequestId, 0) = 0
        AND COALESCE(MAX(COALESCE(poi.materialRequestId, 0)), 0) = 0
    `);

    for (const row of legacyDeliveredOrders) {
      try {
        await ensureMaterialRequestsForOrder(Number(row.id), { forceStatus: 'EN_STOCK' });
      } catch (error) {
        // Continue migration even if one order fails.
      }
    }
  } catch (e) {}

  for (const requestId of linkedRequestIds) {
    if (orderRow.warehouseId && String(orderRow.warehouseId).trim()) {
      await run(
        "UPDATE material_requests SET statut = ?, warehouseId = COALESCE(NULLIF(TRIM(warehouseId), ''), ?) WHERE id = ?",
        ['EN_STOCK', String(orderRow.warehouseId).trim(), requestId]
      );
    } else {
      await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['EN_STOCK', requestId]);
    }
  }

  const order = await getPurchaseOrderById(orderId);
  res.json(order);
});

app.get('/api/stock-management/available', async (req, res) => {
  const rows = await all(`
    SELECT mr.id, mr.projetId, p.nomProjet, p.prefecture, p.nomSite, p.numeroMaison, p.typeMaison, mr.itemName, mr.quantiteDemandee, mr.quantiteRestante, mr.statut, mr.etapeApprovisionnement, mr.warehouseId
    FROM material_requests mr
    JOIN projects p ON p.id = mr.projetId
    WHERE mr.statut IN ('EN_STOCK', 'EPUISE')
    ORDER BY p.nomProjet ASC, mr.itemName ASC, mr.id DESC
  `);
  const role = String(req.user?.role || '').trim();
  const scopedRows = (role === 'chef_chantier_site' || role === 'gestionnaire_stock_songon')
    ? rows.filter(row => isInUserProjectScope(req.user, row))
    : rows;

  res.json(scopedRows.map(row => {
    const isZone = String(row.typeMaison || '').toUpperCase() === 'ZONE_STOCK';
    return {
      ...row,
      quantiteDemandee: Number(row.quantiteDemandee || 0),
      quantiteRestante: Number(row.quantiteRestante || 0),
      zoneName: isZone ? (row.prefecture || '') : '',
      sourceType: isZone ? 'ZONE_STOCK' : 'SITE',
    };
  }));
});

app.get('/api/stock-management/issues', async (req, res) => {
  const rows = await all(`
    SELECT si.*,
           COALESCE(NULLIF(TRIM(si.issueType), ''), CASE WHEN si.note LIKE 'Consommation chantier%' THEN 'CONSUMPTION' ELSE 'SITE_TRANSFER' END) AS issueType,
           mr.itemName, p.nomProjet, p.nomSite, p.numeroMaison
    FROM stock_issues si
    LEFT JOIN material_requests mr ON mr.id = si.materialRequestId
    LEFT JOIN projects p ON p.id = si.projetId
    ORDER BY si.issuedAt DESC, si.id DESC
    LIMIT 100
  `);
  const role = String(req.user?.role || '').trim();
  const scopedRows = (role === 'chef_chantier_site' || role === 'gestionnaire_stock_songon')
    ? rows.filter(row => isInUserProjectScope(req.user, row))
    : rows;
  res.json(scopedRows);
});

app.get('/api/stock-issue-authorizations', async (req, res) => {
  const warehouseId = String(req.query.warehouseId || '').trim();
  const statusRaw = String(req.query.status || '').trim().toUpperCase();
  const status = (statusRaw === '' || statusRaw === 'ALL' || statusRaw === 'TOUS') ? '' : statusRaw;
  const params = [];
  const whereParts = [];

  if (warehouseId) {
    whereParts.push("COALESCE(NULLIF(TRIM(sia.warehouseId), ''), '') = ?");
    params.push(warehouseId);
  }
  if (status) {
    whereParts.push("UPPER(COALESCE(sia.status, 'EN_ATTENTE')) = ?");
    params.push(status);
  }

  const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
  const rows = await all(
    `SELECT
      sia.*,
      mr.quantiteRestante,
      mr.quantiteDemandee,
      mr.etapeApprovisionnement AS requestEtape,
      p.nomProjet,
      p.numeroMaison,
      gd.title,
      gd.fileName,
      gd.relativePath
     FROM stock_issue_authorizations sia
     LEFT JOIN material_requests mr ON mr.id = sia.materialRequestId
     LEFT JOIN projects p ON p.id = sia.projetId
     LEFT JOIN generated_documents gd ON gd.entityType = 'stock_issue_authorization' AND gd.entityId = sia.id
     ${whereClause}
     ORDER BY COALESCE(sia.decidedAt, sia.requestedAt) DESC, sia.id DESC`,
    params
  );

  const role = String(req.user?.role || '').trim();
  const scopedRows = (role === 'chef_chantier_site' || role === 'gestionnaire_stock_songon')
    ? (rows || []).filter(row => isInUserProjectScope(req.user, row))
    : (rows || []);

  const authorizationIds = scopedRows.map(row => Number(row.id)).filter(Boolean);
  const itemRows = authorizationIds.length
    ? await all(
        `SELECT authorizationId, materialRequestId, projetId, itemName, quantiteSortie, etapeApprovisionnement, warehouseId
         FROM stock_issue_authorization_items
         WHERE authorizationId IN (${authorizationIds.map(() => '?').join(', ')})
         ORDER BY id ASC`,
        authorizationIds
      )
    : [];

  const itemsByAuthorizationId = itemRows.reduce((acc, row) => {
    const key = Number(row.authorizationId || 0);
    if (!key) return acc;
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});

  res.json(scopedRows.map(row => {
    const items = itemsByAuthorizationId[Number(row.id)] || [{
      materialRequestId: row.materialRequestId,
      projetId: row.projetId,
      itemName: row.itemName,
      quantiteSortie: row.quantiteSortie,
      etapeApprovisionnement: row.etapeApprovisionnement || row.requestEtape || null,
      warehouseId: row.warehouseId || null,
    }];

    return {
      ...row,
      items,
      itemCount: items.length,
      totalQuantite: Number(row.quantiteSortie || 0),
      documentTitle: row.title || null,
      fileUrl: row.relativePath ? `/archives/${String(row.relativePath).replace(/\\/g, '/')}` : null,
    };
  }));
});

app.get('/api/stock-issue-authorizations/:id/pdf', async (req, res) => {
  const id = Number(req.params.id || 0);
  if (!id) {
    return res.status(400).json({ error: 'ID autorisation invalide' });
  }

  const authRow = await get(
    `SELECT sia.*, p.nomProjet, p.nomSite, p.numeroMaison
     FROM stock_issue_authorizations sia
     LEFT JOIN projects p ON p.id = sia.projetId
     WHERE sia.id = ?`,
    [id]
  );
  if (!authRow) {
    return res.status(404).json({ error: 'Autorisation introuvable' });
  }

  const authRole = String(req.user?.role || '').trim();
  if ((authRole === 'chef_chantier_site' || authRole === 'gestionnaire_stock_songon') && !isInUserProjectScope(req.user, authRow)) {
    return res.status(403).json({ error: 'Acces refuse: ce document ne concerne pas votre perimetre' });
  }

  let row = await get(
    `SELECT gd.fileName, gd.relativePath
     FROM generated_documents gd
     WHERE gd.entityType = 'stock_issue_authorization' AND gd.entityId = ?
     ORDER BY gd.updatedAt DESC, gd.id DESC
     LIMIT 1`,
    [id]
  );

  const generateAuthorizationPdf = async () => {
    const authItems = await all(
      `SELECT si.*
       FROM stock_issue_authorization_items si
       WHERE si.authorizationId = ?
       ORDER BY si.id ASC`,
      [id]
    );
    const effectiveItems = authItems.length ? authItems : [{
      materialRequestId: authRow.materialRequestId,
      projetId: authRow.projetId,
      itemName: authRow.itemName,
      quantiteSortie: authRow.quantiteSortie,
      etapeApprovisionnement: authRow.etapeApprovisionnement,
      warehouseId: authRow.warehouseId,
    }];

    const requestRow = await get(
      `SELECT mr.*, p.nomProjet, p.numeroMaison
       FROM material_requests mr
       LEFT JOIN projects p ON p.id = mr.projetId
       WHERE mr.id = ?`,
      [Number(authRow.materialRequestId)]
    );

    const generated = await archiveStockIssueAuthorizationPdf({
      authorization: {
        ...authRow,
        id,
        quantiteSortie: Number(authRow.quantiteSortie || 0),
        items: effectiveItems,
      },
      request: requestRow || authRow,
      signatureName: String(authRow.signatureName || authRow.requestedBy || '').trim(),
      signatureRole: String(authRow.signatureRole || 'Demandeur').trim(),
      signedAt: String(authRow.decidedAt || authRow.requestedAt || new Date().toISOString()),
      decisionStatus: String(authRow.status || 'EN_ATTENTE').toUpperCase(),
    });

    return {
      fileName: generated.fileName,
      relativePath: generated.relativePath,
    };
  };

  if (!row || !row.relativePath) {
    row = await generateAuthorizationPdf();
  }

  let filePath = path.join(ARCHIVE_ROOT, String(row.relativePath || ''));
  if (!fs.existsSync(filePath)) {
    row = await generateAuthorizationPdf();
    filePath = path.join(ARCHIVE_ROOT, String(row.relativePath || ''));
  }

  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: 'Fichier PDF introuvable' });
  }

  const fileName = sanitizeFileName(String(row.fileName || `autorisation-sortie-${id}.pdf`));
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
  res.sendFile(filePath);
});

app.post('/api/stock-issue-authorizations', async (req, res) => {
  const {
    materialRequestId,
    quantiteSortie,
    note = '',
    entries = [],
    etapeApprovisionnement = '',
    targetProjetId = null,
  } = req.body || {};

  const normalizedEntries = Array.isArray(entries) && entries.length
    ? entries
    : [{ materialRequestId, quantiteSortie }];

  const prepared = [];
  let resolvedProjectId = null;
  let resolvedWarehouseId = '';
  for (const entry of normalizedEntries) {
    const requestId = Number(entry?.materialRequestId || 0);
    const outQty = Number(entry?.quantiteSortie || 0);
    if (!requestId || Number.isNaN(outQty) || outQty <= 0) {
      return res.status(400).json({ error: 'Chaque ligne doit contenir materialRequestId et quantiteSortie valides' });
    }

    const requestRow = await get(
      `SELECT mr.id, mr.projetId, mr.itemName, mr.etapeApprovisionnement, mr.quantiteRestante, mr.warehouseId, p.nomProjet, p.numeroMaison
       FROM material_requests mr
       LEFT JOIN projects p ON p.id = mr.projetId
       WHERE mr.id = ?`,
      [requestId]
    );

    if (!requestRow) {
      return res.status(404).json({ error: `Matériel introuvable (id ${requestId})` });
    }

    const createRole = String(req.user?.role || '').trim();
    if ((createRole === 'chef_chantier_site' || createRole === 'gestionnaire_stock_songon') && !isInUserProjectScope(req.user, requestRow)) {
      return res.status(403).json({ error: 'Acces refuse: demande autorisee uniquement dans votre perimetre' });
    }

    const remaining = Number(requestRow.quantiteRestante || 0);
    if (outQty > remaining) {
      return res.status(400).json({ error: `Quantité demandée (${outQty.toFixed(2)}) supérieure au stock restant (${remaining.toFixed(2)}) pour ${requestRow.itemName}` });
    }

    if (resolvedProjectId == null) {
      resolvedProjectId = Number(requestRow.projetId || 0) || null;
      resolvedWarehouseId = String(requestRow.warehouseId || '').trim();
    }
    if (resolvedProjectId != null && Number(requestRow.projetId || 0) !== Number(resolvedProjectId || 0)) {
      return res.status(400).json({ error: 'Tous les matériaux doivent appartenir au même site' });
    }
    if (resolvedWarehouseId && String(requestRow.warehouseId || '').trim() !== resolvedWarehouseId) {
      return res.status(400).json({ error: 'Tous les matériaux doivent appartenir au même entrepôt' });
    }

    prepared.push({
      requestId,
      outQty,
      requestRow,
    });
  }

  // Check if warehouse is hidden
  if (resolvedWarehouseId && isWarehouseHidden(resolvedWarehouseId)) {
    return res.status(403).json({ error: 'Cet entrepot est indisponible' });
  }

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const targetId = Number(targetProjetId || 0);
    if (!Number.isInteger(targetId) || targetId <= 0) {
      return res.status(400).json({ error: 'Site destinataire obligatoire pour ce profil' });
    }

    const targetProject = await get('SELECT id, nomSite, numeroMaison FROM projects WHERE id = ?', [targetId]);
    if (!targetProject || !isInChefSiteScope(req.user, targetProject)) {
      return res.status(403).json({ error: 'Acces refuse: sortie autorisee uniquement vers le site 15' });
    }
  }

  const targetId = Number(targetProjetId || 0);
  if (targetId > 0 && Number(resolvedProjectId || 0) > 0 && targetId !== Number(resolvedProjectId || 0)) {
    return res.status(400).json({ error: 'La quantité doit provenir du stock du site destinataire sélectionné' });
  }

  const requestedBy = req.user ? req.user.username : 'admin';
  const requestedAt = new Date().toISOString();
  const totalQty = prepared.reduce((sum, row) => sum + Number(row.outQty || 0), 0);
  const resolvedStage = String(etapeApprovisionnement || prepared[0]?.requestRow?.etapeApprovisionnement || '').trim() || null;
  const firstRow = prepared[0]?.requestRow || {};

  // ── Guards: EN_ATTENTE duplicate and catalog-quantity exhaustion ──────────
  if (resolvedProjectId && resolvedStage) {
    // 1. Block if a pending authorization already exists for this project + stage
    const existingPending = await get(
      `SELECT id FROM stock_issue_authorizations
       WHERE projetId = ? AND etapeApprovisionnement = ? AND status = 'EN_ATTENTE' LIMIT 1`,
      [resolvedProjectId, resolvedStage]
    );
    if (existingPending) {
      return res.status(409).json({
        error: `Une demande de sortie est déjà en attente pour l'étape "${resolvedStage}" sur ce site (réf. #${existingPending.id}). Attends qu'elle soit validée ou rejetée avant d'en soumettre une nouvelle.`,
      });
    }

    // 2. Block if catalog quantity already exhausted for any requested material
    const projectFolder = String(firstRow.nomProjet || '').trim();
    if (projectFolder) {
      for (const row of prepared) {
        const itemName = String(row.requestRow.itemName || '').trim();
        if (!itemName) continue;

        // Get catalog entry for this material in this project folder
        const catalogRows = await all(
          `SELECT quantiteParBatiment, notes FROM building_material_catalog
           WHERE projectFolder = ? AND materialName = ?`,
          [projectFolder, itemName]
        );
        // Pick the entry whose notes match the requested stage
        const catalogEntry = catalogRows.find(c => isCatalogStageMatching(c.notes, resolvedStage));
        if (!catalogEntry || !Number(catalogEntry.quantiteParBatiment)) continue;

        const plannedQty = Number(catalogEntry.quantiteParBatiment);

        // Sum total already issued (across all validated authorizations) for this project + material
        const issuedRow = await get(
          `SELECT COALESCE(SUM(si.quantiteSortie), 0) AS total
           FROM stock_issues si
           JOIN material_requests mr ON mr.id = si.materialRequestId
           WHERE mr.projetId = ? AND mr.itemName = ?`,
          [resolvedProjectId, itemName]
        );
        const alreadyIssued = Number(issuedRow?.total || 0);

        if (alreadyIssued + row.outQty > plannedQty) {
          return res.status(409).json({
            error: `Quantité prévue au déboursé sec épuisée pour "${itemName}" (prévu: ${plannedQty}, déjà sorti: ${alreadyIssued.toFixed(2)}, demandé: ${row.outQty.toFixed(2)}).`,
          });
        }
      }
    }
  }

  const nextAuthorizationIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issue_authorizations');
  const authorizationId = Number(nextAuthorizationIdRow?.nextId || nextAuthorizationIdRow?.nextid || 1);

  const nextAuthorizationItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issue_authorization_items');
  let nextAuthorizationItemId = Number(nextAuthorizationItemIdRow?.nextId || nextAuthorizationItemIdRow?.nextid || 1);

  await run(
    `INSERT INTO stock_issue_authorizations
      (id, materialRequestId, projetId, warehouseId, itemName, etapeApprovisionnement, quantiteSortie, note, status, requestedBy, requestedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      authorizationId,
      prepared[0].requestId,
      resolvedProjectId,
      resolvedWarehouseId || null,
      prepared.length > 1 ? `${prepared.length} articles` : (String(firstRow.itemName || '').trim() || 'Article'),
      resolvedStage,
      totalQty,
      String(note || '').trim(),
      'EN_ATTENTE',
      requestedBy,
      requestedAt,
    ]
  );

  for (const row of prepared) {
    await run(
      `INSERT INTO stock_issue_authorization_items
        (id, authorizationId, materialRequestId, projetId, itemName, quantiteSortie, etapeApprovisionnement, warehouseId, createdAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        nextAuthorizationItemId++,
        authorizationId,
        row.requestId,
        row.requestRow.projetId || null,
        String(row.requestRow.itemName || '').trim() || 'Article',
        row.outQty,
        String(resolvedStage || row.requestRow.etapeApprovisionnement || '').trim() || null,
        String(row.requestRow.warehouseId || '').trim() || null,
        requestedAt,
      ]
    );
  }

  await archiveStockIssueAuthorizationPdf({
    authorization: {
      id: authorizationId,
      materialRequestId: prepared[0].requestId,
      projetId: resolvedProjectId,
      warehouseId: resolvedWarehouseId || null,
      itemName: prepared.length > 1 ? `${prepared.length} articles` : firstRow.itemName,
      etapeApprovisionnement: resolvedStage,
      quantiteSortie: totalQty,
      requestedBy,
      requestedAt,
      items: prepared.map(row => ({
        materialRequestId: row.requestId,
        projetId: row.requestRow.projetId || null,
        itemName: String(row.requestRow.itemName || '').trim() || 'Article',
        quantiteSortie: row.outQty,
        etapeApprovisionnement: String(resolvedStage || row.requestRow.etapeApprovisionnement || '').trim() || null,
        warehouseId: String(row.requestRow.warehouseId || '').trim() || null,
      })),
    },
    request: firstRow,
    signatureName: String(requestedBy || '').trim(),
    signatureRole: 'Demandeur',
    signedAt: requestedAt,
    decisionStatus: 'EN_ATTENTE',
  });

  res.status(201).json({ created: [{
    id: authorizationId,
    materialRequestId: prepared[0].requestId,
    projetId: resolvedProjectId,
    warehouseId: resolvedWarehouseId || null,
    itemName: prepared.length > 1 ? `${prepared.length} articles` : firstRow.itemName,
    etapeApprovisionnement: resolvedStage,
    quantiteSortie: totalQty,
    status: 'EN_ATTENTE',
    requestedBy,
    requestedAt,
    nomProjet: firstRow.nomProjet,
    numeroMaison: firstRow.numeroMaison,
    itemCount: prepared.length,
    items: prepared.map(row => ({ itemName: row.requestRow.itemName, quantiteSortie: row.outQty, materialRequestId: row.requestId })),
  }] });
});

app.patch('/api/stock-issue-authorizations/:id/decision', async (req, res) => {
  const id = Number(req.params.id || 0);
  const {
    status,
    decisionNote = '',
    signatureName = '',
    signatureRole = '',
    signedAt = null,
  } = req.body || {};

  const decision = String(status || '').trim().toUpperCase();
  if (!id || !['VALIDEE', 'REJETEE'].includes(decision)) {
    return res.status(400).json({ error: 'Decision invalide' });
  }
  if (req.user && (req.user.role === 'commis' || req.user.role === 'chef_chantier_site' || req.user.role === 'gestionnaire_stock_zone' || req.user.role === 'gestionnaire_stock_songon')) {
    return res.status(403).json({ error: 'Ce profil ne peut pas valider/rejeter une sortie' });
  }
  if (!String(signatureName || '').trim() || !String(signatureRole || '').trim()) {
    return res.status(400).json({ error: 'Signature et fonction obligatoires pour valider ou rejeter' });
  }

  const authRow = await get(
    `SELECT sia.*, mr.quantiteRestante, mr.statut as requestStatus,
            p.nomProjet, p.numeroMaison
     FROM stock_issue_authorizations sia
     LEFT JOIN material_requests mr ON mr.id = sia.materialRequestId
     LEFT JOIN projects p ON p.id = sia.projetId
     WHERE sia.id = ?`,
    [id]
  );

  if (!authRow) {
    return res.status(404).json({ error: 'Demande de sortie introuvable' });
  }
  if (String(authRow.status || 'EN_ATTENTE').toUpperCase() !== 'EN_ATTENTE') {
    return res.status(409).json({ error: 'Cette demande a deja ete traitee' });
  }

  const decidedAt = signedAt ? new Date(signedAt).toISOString() : new Date().toISOString();
  const decidedBy = req.user ? req.user.username : 'admin';

  const authItems = await all(
    `SELECT si.*, mr.quantiteRestante, mr.statut AS requestStatus
     FROM stock_issue_authorization_items si
     LEFT JOIN material_requests mr ON mr.id = si.materialRequestId
     WHERE si.authorizationId = ?
     ORDER BY si.id ASC`,
    [id]
  );
  const effectiveItems = authItems.length ? authItems : [{
    materialRequestId: authRow.materialRequestId,
    projetId: authRow.projetId,
    itemName: authRow.itemName,
    quantiteSortie: authRow.quantiteSortie,
    etapeApprovisionnement: authRow.etapeApprovisionnement,
    warehouseId: authRow.warehouseId,
    quantiteRestante: authRow.quantiteRestante,
    requestStatus: authRow.requestStatus,
  }];

  if (decision === 'VALIDEE') {
    let nextStockIssueId = await getNextTableId('stock_issues');
    for (const item of effectiveItems) {
      const remaining = Number(item.quantiteRestante || 0);
      const outQty = Number(item.quantiteSortie || 0);
      if (outQty > remaining) {
        return res.status(400).json({ error: `Stock insuffisant au moment de la validation pour ${item.itemName}` });
      }

      const newRemaining = Math.max(0, remaining - outQty);
      const newStatus = newRemaining > 0 ? 'EN_STOCK' : 'EPUISE';
      await run('UPDATE material_requests SET quantiteRestante = ?, statut = ? WHERE id = ?', [newRemaining, newStatus, Number(item.materialRequestId)]);

      await run(
        'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [
          nextStockIssueId++,
          Number(item.materialRequestId),
          item.projetId || authRow.projetId || null,
          outQty,
          'SITE_TRANSFER',
          String(authRow.note || '').trim() || `Sortie stock validée (autorisation #${id})`,
          decidedBy,
          decidedAt,
        ]
      );
    }
  }

  await run(
    `UPDATE stock_issue_authorizations
     SET status = ?, decidedBy = ?, decidedAt = ?, decisionNote = ?, signatureName = ?, signatureRole = ?
     WHERE id = ?`,
    [decision, decidedBy, decidedAt, String(decisionNote || '').trim(), String(signatureName || '').trim(), String(signatureRole || '').trim(), id]
  );

  const requestRow = await get(
    `SELECT mr.*, p.nomProjet, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id = ?`,
    [Number(authRow.materialRequestId)]
  );

  const doc = await archiveStockIssueAuthorizationPdf({
    authorization: {
      ...authRow,
      id,
      quantiteSortie: Number(authRow.quantiteSortie || 0),
      items: effectiveItems,
    },
    request: requestRow || authRow,
    signatureName: String(signatureName || '').trim(),
    signatureRole: String(signatureRole || '').trim(),
    signedAt: decidedAt,
    decisionStatus: decision,
  });

  const updated = await get('SELECT * FROM stock_issue_authorizations WHERE id = ?', [id]);
  res.json({ ...updated, doc });
});

app.post('/api/stock-management/issues', async (req, res) => {
  const { materialRequestId, quantiteSortie, note = '' } = req.body || {};
  const requestId = Number(materialRequestId);
  const outQty = Number(quantiteSortie);

  if (!requestId || Number.isNaN(outQty) || outQty <= 0) {
    return res.status(400).json({ error: 'materialRequestId et quantiteSortie valides sont obligatoires' });
  }

  const requestRow = await get(
    `SELECT mr.id, mr.projetId, mr.quantiteRestante, mr.statut, mr.warehouseId, p.nomSite, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id = ?`,
    [requestId]
  );
  if (!requestRow) {
    return res.status(404).json({ error: 'Matériel introuvable' });
  }

  // Check if warehouse is hidden
  const warehouseId = String(requestRow.warehouseId || '').trim();
  if (warehouseId && isWarehouseHidden(warehouseId)) {
    return res.status(403).json({ error: 'Cet entrepot est indisponible' });
  }

  const issueRole = String(req.user?.role || '').trim();
  if ((issueRole === 'chef_chantier_site' || issueRole === 'gestionnaire_stock_songon') && !isInUserProjectScope(req.user, requestRow)) {
    return res.status(403).json({ error: 'Acces refuse: sortie autorisee uniquement dans votre perimetre' });
  }

  const remaining = Number(requestRow.quantiteRestante || 0);
  if (outQty > remaining) {
    return res.status(400).json({ error: 'Quantité sortie supérieure au stock restant' });
  }

  const newRemaining = Math.max(0, remaining - outQty);
  const newStatus = newRemaining > 0 ? 'EN_STOCK' : 'EPUISE';
  await run('UPDATE material_requests SET quantiteRestante = ?, statut = ? WHERE id = ?', [newRemaining, newStatus, requestId]);

  const now = new Date().toISOString();
  const nextSiIdDirect = await getNextTableId('stock_issues');
  await run(
    'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    [nextSiIdDirect, requestId, requestRow.projetId || null, outQty, 'SITE_TRANSFER', String(note || '').trim(), req.user ? req.user.username : 'admin', now]
  );

  const updated = await get(`
    SELECT mr.id, mr.projetId, p.nomProjet, mr.itemName, mr.quantiteDemandee, mr.quantiteRestante, mr.statut
    FROM material_requests mr
    JOIN projects p ON p.id = mr.projetId
    WHERE mr.id = ?
  `, [requestId]);

  res.status(201).json(updated);
});

app.patch('/api/purchase-orders/:id', async (req, res) => {
  const id = Number(req.params.id);
  const { fournisseur, montantTotal, dateLivraisonPrevue = null, dateCommande, items, materialRequestId, quantiteCommandee, prixUnitaire } = req.body;
  const preparedItems = await preparePurchaseOrderItems({
    items,
    materialRequestId,
    quantiteCommandee,
    prixUnitaire,
    montantTotal,
  });

  if (preparedItems === 'INVALID_PO_ITEM') {
    return res.status(400).json({ error: 'Chaque article doit avoir une quantite et un prix valides' });
  }

  const totalAmount = preparedItems && preparedItems.length
    ? preparedItems.reduce((sum, item) => sum + Number(item.totalLigne || 0), 0)
    : Number(montantTotal);

  if (!id || !fournisseur || Number.isNaN(totalAmount) || totalAmount < 0) {
    return res.status(400).json({ error: 'fournisseur et montantTotal sont obligatoires' });
  }

  if (preparedItems === null) {
    return res.status(404).json({ error: 'Demande de materiel introuvable pour un article' });
  }

  const parsedOrderDate = dateCommande ? new Date(dateCommande) : null;
  if (dateCommande && parsedOrderDate && Number.isNaN(parsedOrderDate.getTime())) {
    return res.status(400).json({ error: 'dateCommande invalide' });
  }

  const result = await run(
    'UPDATE purchase_orders SET fournisseur = ?, montantTotal = ?, quantiteCommandee = ?, prixUnitaire = ?, dateLivraisonPrevue = ?, dateCommande = COALESCE(?, dateCommande) WHERE id = ?',
    [
      String(fournisseur).trim(),
      totalAmount,
      preparedItems && preparedItems.length ? preparedItems[0].quantite : 1,
      preparedItems && preparedItems.length ? preparedItems[0].prixUnitaire : totalAmount,
      dateLivraisonPrevue,
      parsedOrderDate ? parsedOrderDate.toISOString() : null,
      id,
    ]
  );

  if (result.changes === 0) {
    return res.status(404).json({ error: 'Bon de commande non trouve' });
  }

  if (preparedItems && preparedItems.length) {
    const nextPurchaseOrderItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_order_items');
    let nextPurchaseOrderItemId = Number(nextPurchaseOrderItemIdRow?.nextId || nextPurchaseOrderItemIdRow?.nextid || 1);
    await run('DELETE FROM purchase_order_items WHERE purchaseOrderId = ?', [id]);
    for (const item of preparedItems) {
      await run(
        'INSERT INTO purchase_order_items (id, purchaseOrderId, materialRequestId, article, details, quantite, prixUnitaire, totalLigne) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [nextPurchaseOrderItemId++, id, item.materialRequestId, item.article, item.details, item.quantite, item.prixUnitaire, item.totalLigne]
      );
    }
  }

  await runPurchaseOrderSideEffects(id, {
    fournisseur: String(fournisseur).trim(),
    dateCommande: parsedOrderDate ? parsedOrderDate.toISOString() : undefined,
    createdBy: req.user ? req.user.username : 'admin',
  });

  const order = await getPurchaseOrderById(id);
  res.json(order);
});

app.delete('/api/purchase-orders/:id', async (req, res) => {
  const id = Number(req.params.id);
  const docs = await all('SELECT relativePath FROM generated_documents WHERE entityType = ? AND entityId = ?', ['purchase_order', id]);
  const result = await run('DELETE FROM purchase_orders WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Bon de commande non trouvé' });
  }
  // Supprimer les dépenses générées automatiquement par ce bon de commande
  try { await run('DELETE FROM expenses WHERE purchaseOrderId = ?', [id]); } catch(e) {}
  try { await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ?', ['purchase_order', id]); } catch (e) {}
  for (const doc of docs) {
    const filePath = path.join(ARCHIVE_ROOT, doc.relativePath);
    if (fs.existsSync(filePath)) {
      try { await fs.promises.unlink(filePath); } catch (e) {}
    }
  }
  res.json({ message: 'Bon de commande supprimé' });
});

app.get('/api/database-documents', async (req, res) => {
  try {
    const sectionCode = String(req.query.section || '').trim();
    let rows = sectionCode
      ? await all('SELECT * FROM generated_documents WHERE sectionCode = ? ORDER BY updatedAt DESC, id DESC', [sectionCode])
      : await all('SELECT * FROM generated_documents ORDER BY updatedAt DESC, id DESC');

    const scopedRole = String(req.user?.role || '').trim();
    const isScopedRole = scopedRole === 'chef_chantier_site' || scopedRole === 'gestionnaire_stock_songon';
    const isSiteChiefDocumentAllowed = async row => {
      if (!isScopedRole) return true;

      const entityType = String(row?.entityType || '').trim().toLowerCase();
      const entityId = Number(row?.entityId || 0);
      if (!Number.isInteger(entityId) || entityId <= 0) return false;

      if (entityType === 'material_request_authorization') {
        const scopedRow = await get(
          `SELECT p.nomSite, p.numeroMaison
           FROM material_requests mr
           LEFT JOIN projects p ON p.id = mr.projetId
           WHERE mr.id = ?`,
          [entityId]
        );
        return !!scopedRow && isInUserProjectScope(req.user, scopedRow);
      }

      if (entityType === 'purchase_order') {
        const linkedRequests = await all(
          `SELECT p.nomSite, p.numeroMaison
           FROM purchase_order_items poi
           LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
           LEFT JOIN projects p ON p.id = mr.projetId
           WHERE poi.purchaseOrderId = ?`,
          [entityId]
        );
        if (Array.isArray(linkedRequests) && linkedRequests.length) {
          return linkedRequests.some(rowItem => isInUserProjectScope(req.user, rowItem));
        }

        const orderFallback = await get(
          `SELECT p.nomSite, p.numeroMaison
           FROM purchase_orders po
           LEFT JOIN projects p ON p.id = COALESCE(po.siteId, po.projetId)
           WHERE po.id = ?`,
          [entityId]
        );
        return !!orderFallback && isInUserProjectScope(req.user, orderFallback);
      }

      if (entityType === 'stock_issue_authorization') {
        const scopedRow = await get(
          `SELECT p.nomSite, p.numeroMaison
           FROM stock_issue_authorizations sia
           LEFT JOIN material_requests mr ON mr.id = sia.materialRequestId
           LEFT JOIN projects p ON p.id = COALESCE(sia.projetId, mr.projetId)
           WHERE sia.id = ?`,
          [entityId]
        );
        return !!scopedRow && isInUserProjectScope(req.user, scopedRow);
      }

      return false;
    };

    if (isScopedRole) {
      const scopedRows = [];
      for (const row of rows) {
        if (await isSiteChiefDocumentAllowed(row)) {
          scopedRows.push(row);
        }
      }
      rows = scopedRows;
    }

    if (!rows || !Array.isArray(rows)) {
      return res.json([]);
    }

    const normalizedRows = [];
    for (const row of rows) {
      try {
        let resolvedTitle = String(row.title || '').trim();
        const entityType = String(row.entityType || '').trim().toLowerCase();
        const entityId = Number(row.entityId || 0);
        let projectId = 0;

        if ((entityType === 'manual_upload' || entityType === 'upload') && Number.isFinite(entityId) && entityId > 0) {
          projectId = entityId;
        } else if (entityType === 'purchase_order' && Number.isFinite(entityId) && entityId > 0) {
          try {
            const order = await get(`
              SELECT po.projetId, po.materialRequestId, po.nomSiteManuel, po.etapeApprovisionnement,
                     p.numeroMaison,
                     p2.numeroMaison as requestNumeroMaison,
                     mr.etapeApprovisionnement as requestStage
              FROM purchase_orders po
              LEFT JOIN projects p ON p.id = COALESCE(po.siteId, po.projetId)
              LEFT JOIN material_requests mr ON mr.id = po.materialRequestId
              LEFT JOIN projects p2 ON p2.id = mr.projetId
              WHERE po.id = ?
            `, [entityId]);
            const stageLabel = resolvePurchaseOrderStageDisplay(order?.etapeApprovisionnement)
              || resolvePurchaseOrderStageDisplay(order?.requestStage)
              || 'Étape';
            const siteLabel = extractSiteNumberLabel(order?.numeroMaison || order?.requestNumeroMaison || order?.nomSiteManuel || '-');
            resolvedTitle = `${stageLabel}-${siteLabel}`;
          } catch (e) {
            // Silently continue if purchase_order lookup fails
          }
        } else if (entityType === 'revenue' && Number.isFinite(entityId) && entityId > 0) {
          try {
            const revenue = await get('SELECT projetId FROM revenues WHERE id = ?', [entityId]);
            projectId = Number(revenue?.projetId || 0);
          } catch (e) {
            // Silently continue if revenue lookup fails
          }
        }

        if (Number.isFinite(projectId) && projectId > 0) {
          try {
            const project = await get('SELECT nomProjet, numeroMaison FROM projects WHERE id = ?', [projectId]);
            if (project) {
              const projectName = String(project.nomProjet || 'Projet').trim() || 'Projet';
              const houseNumber = String(project.numeroMaison || '').trim() || '-';
              resolvedTitle = `${projectName}-${houseNumber}`;
            }
          } catch (e) {
            // Silently continue if project lookup fails
          }
        }

        if (!resolvedTitle) {
          resolvedTitle = String(row.fileName || 'Document').trim() || 'Document';
        }

        resolvedTitle = resolvedTitle.replace(/\s*-\s*/g, '-').replace(/\s+/g, ' ').trim();

        if (resolvedTitle && resolvedTitle !== String(row.title || '').trim()) {
          try {
            await run('UPDATE generated_documents SET title = ?, updatedAt = ? WHERE id = ?', [resolvedTitle, new Date().toISOString(), row.id]);
          } catch (e) {
            // Silently continue if update fails
          }
        }

        normalizedRows.push({
          ...row,
          title: resolvedTitle,
          fileUrl: `/archives/${String(row.relativePath || '').replace(/\\/g, '/')}`,
        });
      } catch (rowErr) {
        // Skip problematic rows
        console.error('Error processing document row:', rowErr);
      }
    }

    res.json(normalizedRows);
  } catch (err) {
    console.error('Error fetching documents:', err);
    res.status(500).json({ error: 'Erreur lors du chargement des documents', details: String(err.message) });
  }
});

app.post('/api/database-documents/upload', async (req, res) => {
  const { sectionCode, title, fileName, contentBase64, projetId } = req.body || {};
  if (!fileName || !contentBase64) {
    return res.status(400).json({ error: 'fileName et contentBase64 sont obligatoires' });
  }

  let buffer;
  try {
    buffer = Buffer.from(String(contentBase64), 'base64');
  } catch (e) {
    return res.status(400).json({ error: 'Fichier invalide (base64)' });
  }

  if (!buffer || !buffer.length) {
    return res.status(400).json({ error: 'Le fichier est vide' });
  }

  let resolvedTitle = String(title || '').trim();
  const parsedProjectId = Number(projetId || 0);
  if (Number.isFinite(parsedProjectId) && parsedProjectId > 0) {
    const project = await get('SELECT nomProjet, numeroMaison FROM projects WHERE id = ?', [parsedProjectId]);
    if (project) {
      const projectName = String(project.nomProjet || 'Projet').trim() || 'Projet';
      const houseNumber = String(project.numeroMaison || '').trim() || '-';
      resolvedTitle = `${projectName}-${houseNumber}`;
    }
  }

  const archived = await archiveUploadedDocument({
    sectionCode: String(sectionCode || 'construction').trim().toLowerCase(),
    title: resolvedTitle,
    fileName: String(fileName || '').trim(),
    fileBuffer: buffer,
    projectId: parsedProjectId,
  });

  res.status(201).json(archived);
});

app.delete('/api/database-documents/:id', async (req, res) => {
  const id = Number(req.params.id);
  const row = await get('SELECT * FROM generated_documents WHERE id = ?', [id]);
  if (!row) {
    return res.status(404).json({ error: 'Document introuvable' });
  }

  await run('DELETE FROM generated_documents WHERE id = ?', [id]);
  const filePath = path.join(ARCHIVE_ROOT, String(row.relativePath || ''));
  if (fs.existsSync(filePath)) {
    try { await fs.promises.unlink(filePath); } catch (e) {}
  }

  res.json({ message: 'Document supprimé' });
});

app.get('/api/database-documents/:id/download', async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: 'ID document invalide' });
  }

  const row = await get('SELECT * FROM generated_documents WHERE id = ?', [id]);
  if (!row) {
    return res.status(404).json({ error: 'Document introuvable' });
  }

  const docRole = String(req.user?.role || '').trim();
  if (docRole === 'chef_chantier_site' || docRole === 'gestionnaire_stock_songon') {
    const entityType = String(row.entityType || '').trim().toLowerCase();
    const entityId = Number(row.entityId || 0);
    let inScope = false;

    if (entityType === 'material_request_authorization' && entityId > 0) {
      const scopedRow = await get(
        `SELECT p.nomSite, p.numeroMaison
         FROM material_requests mr
         LEFT JOIN projects p ON p.id = mr.projetId
         WHERE mr.id = ?`,
        [entityId]
      );
      inScope = !!scopedRow && isInUserProjectScope(req.user, scopedRow);
    } else if (entityType === 'purchase_order' && entityId > 0) {
      const linkedRows = await all(
        `SELECT p.nomSite, p.numeroMaison
         FROM purchase_order_items poi
         LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
         LEFT JOIN projects p ON p.id = mr.projetId
         WHERE poi.purchaseOrderId = ?`,
        [entityId]
      );
      inScope = Array.isArray(linkedRows) && linkedRows.some(scopeRow => isInUserProjectScope(req.user, scopeRow));
      if (!inScope) {
        const fallbackRow = await get(
          `SELECT p.nomSite, p.numeroMaison
           FROM purchase_orders po
           LEFT JOIN projects p ON p.id = COALESCE(po.siteId, po.projetId)
           WHERE po.id = ?`,
          [entityId]
        );
        inScope = !!fallbackRow && isInUserProjectScope(req.user, fallbackRow);
      }
    } else if (entityType === 'stock_issue_authorization' && entityId > 0) {
      const scopedRow = await get(
        `SELECT p.nomSite, p.numeroMaison
         FROM stock_issue_authorizations sia
         LEFT JOIN material_requests mr ON mr.id = sia.materialRequestId
         LEFT JOIN projects p ON p.id = COALESCE(sia.projetId, mr.projetId)
         WHERE sia.id = ?`,
        [entityId]
      );
      inScope = !!scopedRow && isInUserProjectScope(req.user, scopedRow);
    }

    if (!inScope) {
      return res.status(403).json({ error: 'Acces refuse: ce document ne concerne pas votre site' });
    }
  }

  const relativePath = String(row.relativePath || '').trim();
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  if (!relativePath || !fs.existsSync(absolutePath)) {
    return res.status(404).json({ error: 'Fichier introuvable' });
  }

  const fileName = sanitizeFileName(row.fileName || path.basename(relativePath) || 'document.pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
  res.setHeader('Content-Type', 'application/pdf');
  return res.sendFile(absolutePath);
});

app.get('/api/guide-documents', async (_req, res) => {
  try {
    let rows = await all('SELECT * FROM guide_documents ORDER BY updatedAt DESC, id DESC');
    const role = String(_req.user?.role || '').trim();
    const canManage = role === 'admin';
    if (!canManage) {
      const profile = await getHrProfileEmployeeForUser(_req.user);
      const currentEmployeeId = Number(profile?.id || 0);
      rows = (Array.isArray(rows) ? rows : []).filter(row => {
        const scope = String(row?.audienceScope || 'all').trim().toLowerCase() === 'selected' ? 'selected' : 'all';
        if (scope === 'all') return true;
        if (!Number.isInteger(currentEmployeeId) || currentEmployeeId <= 0) return false;
        const recipients = normalizeNumericIdList(row?.recipientEmployeeIds);
        return recipients.includes(currentEmployeeId);
      });
    }

    if (String(_req.query?.download || '').trim().toLowerCase() === 'all') {
      const docsToArchive = (Array.isArray(rows) ? rows : []).map(row => {
        const resolved = resolveExistingGuideAbsolutePath(row);
        if (!resolved) return null;
        const fileName = sanitizeFileName(String(row?.fileName || path.basename(resolved.absolutePath) || 'guide-document'));
        return {
          id: Number(row?.id || 0),
          title: String(row?.title || '').trim(),
          fileName,
          absolutePath: resolved.absolutePath,
          mimeType: String(row?.mimeType || 'application/octet-stream').trim() || 'application/octet-stream',
        };
      }).filter(Boolean);

      if (!docsToArchive.length) {
        return res.status(404).json({ error: 'Aucun document guide disponible au telechargement' });
      }

      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      const archiveName = `guide-erp-${stamp}.zip`;
      res.setHeader('Content-Type', 'application/zip');
      res.setHeader('Content-Disposition', `attachment; filename="${archiveName}"`);

      const archive = archiver('zip', { zlib: { level: 9 } });
      archive.on('error', err => {
        if (!res.headersSent) {
          res.status(500).json({ error: 'Erreur generation archive guide', details: String(err?.message || err) });
        } else {
          res.destroy(err);
        }
      });

      archive.pipe(res);
      docsToArchive.forEach((doc, index) => {
        const prefix = String(doc.title || `document-${index + 1}`).trim();
        const safePrefix = sanitizeFileName(prefix).slice(0, 60) || `document-${index + 1}`;
        const entryName = `${String(index + 1).padStart(2, '0')}-${safePrefix}-${doc.fileName}`;
        archive.file(doc.absolutePath, { name: entryName });
      });

      await archive.finalize();
      return;
    }

    const normalizedRows = (Array.isArray(rows) ? rows : []).map(row => {
      const resolved = resolveExistingGuideAbsolutePath(row);
      const resolvedRelativePath = String(resolved?.relativePath || row?.relativePath || '').replace(/\\/g, '/');
      return {
        ...row,
        audienceScope: String(row?.audienceScope || 'all').trim().toLowerCase() === 'selected' ? 'selected' : 'all',
        recipientEmployeeIds: normalizeNumericIdList(row?.recipientEmployeeIds),
        fileUrl: resolvedRelativePath ? `/archives/${resolvedRelativePath}` : '',
      };
    });
    return res.json(normalizedRows);
  } catch (err) {
    return res.status(500).json({ error: 'Erreur lors du chargement du guide ERP', details: String(err?.message || err) });
  }
});

app.post('/api/guide-documents/upload', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Seul l\'admin peut importer des documents guide' });
    }

    const { title, fileName, contentBase64, mimeType, audienceScope, recipientEmployeeIds } = req.body || {};
    if (!fileName || !contentBase64) {
      return res.status(400).json({ error: 'fileName et contentBase64 sont obligatoires' });
    }

    let buffer;
    try {
      buffer = Buffer.from(String(contentBase64), 'base64');
    } catch (_err) {
      return res.status(400).json({ error: 'Fichier invalide (base64)' });
    }

    if (!buffer || !buffer.length) {
      return res.status(400).json({ error: 'Le fichier est vide' });
    }

    const normalizedAudienceScope = String(audienceScope || 'all').trim().toLowerCase() === 'selected' ? 'selected' : 'all';
    const normalizedRecipientIds = normalizeNumericIdList(recipientEmployeeIds);
    if (normalizedAudienceScope === 'selected' && !normalizedRecipientIds.length) {
      return res.status(400).json({ error: 'Choisis au moins un employe destinataire' });
    }

    if (normalizedRecipientIds.length) {
      const placeholders = normalizedRecipientIds.map(() => '?').join(', ');
      const found = await all(`SELECT id FROM hr_employees WHERE id IN (${placeholders})`, normalizedRecipientIds);
      const foundIds = new Set((found || []).map(row => Number(row?.id || 0)).filter(id => Number.isInteger(id) && id > 0));
      const missing = normalizedRecipientIds.filter(id => !foundIds.has(id));
      if (missing.length) {
        return res.status(400).json({ error: 'Certains employes selectionnes sont introuvables' });
      }
    }

    const doc = await archiveGuideDocument({
      title: String(title || '').trim(),
      fileName: String(fileName || '').trim(),
      fileBuffer: buffer,
      mimeType: String(mimeType || 'application/octet-stream').trim() || 'application/octet-stream',
      uploadedBy: String(req.user?.username || 'admin').trim() || 'admin',
      audienceScope: normalizedAudienceScope,
      recipientEmployeeIds: normalizedAudienceScope === 'selected' ? normalizedRecipientIds : [],
    });

    return res.status(201).json(doc);
  } catch (err) {
    return res.status(500).json({ error: 'Erreur publication document guide', details: String(err?.message || err) });
  }
});

app.get('/api/guide-documents/recipients/employees', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Seul l\'admin peut choisir des destinataires' });
    }

    const rows = await all(
      `SELECT id, fullName, jobTitle,
              COALESCE(username, createdBy, '') AS username
       FROM hr_employees
       ORDER BY fullName ASC, id ASC`
    );

    return res.json((rows || []).map(row => ({
      id: Number(row?.id || 0),
      fullName: String(row?.fullName || '').trim(),
      jobTitle: String(row?.jobTitle || '').trim(),
      username: String(row?.username || '').trim(),
    })).filter(row => Number.isInteger(row.id) && row.id > 0));
  } catch (err) {
    return res.status(500).json({ error: 'Erreur chargement destinataires', details: String(err?.message || err) });
  }
});

app.patch('/api/guide-documents/:id/rename', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Seul l\'admin peut renommer les documents guide' });
    }

    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ error: 'ID document invalide' });
    }

    const nextTitle = String(req.body?.title || '').trim();
    if (!nextTitle) {
      return res.status(400).json({ error: 'Le nouveau titre est obligatoire' });
    }

    const row = await get('SELECT * FROM guide_documents WHERE id = ?', [id]);
    if (!row) {
      return res.status(404).json({ error: 'Document introuvable' });
    }

    const updatedAt = new Date().toISOString();
    await run('UPDATE guide_documents SET title = ?, updatedAt = ? WHERE id = ?', [nextTitle, updatedAt, id]);
    const refreshed = await get('SELECT * FROM guide_documents WHERE id = ?', [id]);
    return res.json({
      ...refreshed,
      fileUrl: `/archives/${String(refreshed?.relativePath || '').replace(/\\/g, '/')}`,
    });
  } catch (err) {
    return res.status(500).json({ error: 'Erreur renommage document guide', details: String(err?.message || err) });
  }
});

app.delete('/api/guide-documents/:id', async (req, res) => {
  try {
    const role = String(req.user?.role || '').trim();
    if (role !== 'admin') {
      return res.status(403).json({ error: 'Seul l\'admin peut supprimer les documents guide' });
    }

    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ error: 'ID document invalide' });
    }

    const row = await get('SELECT * FROM guide_documents WHERE id = ?', [id]);
    if (!row) {
      return res.status(404).json({ error: 'Document introuvable' });
    }

    const scope = String(row?.audienceScope || 'all').trim().toLowerCase() === 'selected' ? 'selected' : 'all';
    if (scope !== 'all') {
      return res.status(400).json({ error: 'Suppression autorisee uniquement pour les documents publies a tout le monde' });
    }

    await run('DELETE FROM guide_documents WHERE id = ?', [id]);

    const relativePath = String(row.relativePath || '').trim();
    const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
    if (relativePath && fs.existsSync(absolutePath)) {
      try { await fs.promises.unlink(absolutePath); } catch (_e) {}
    }

    return res.json({ message: 'Document guide supprime' });
  } catch (err) {
    return res.status(500).json({ error: 'Erreur suppression document guide', details: String(err?.message || err) });
  }
});

app.get('/api/guide-documents/:id/download', async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ error: 'ID document invalide' });
    }

    const row = await get('SELECT * FROM guide_documents WHERE id = ?', [id]);
    if (!row) {
      return res.status(404).json({ error: 'Document introuvable' });
    }

    const role = String(req.user?.role || '').trim();
    const canManage = role === 'admin';
    if (!canManage) {
      const scope = String(row?.audienceScope || 'all').trim().toLowerCase() === 'selected' ? 'selected' : 'all';
      if (scope === 'selected') {
        const profile = await getHrProfileEmployeeForUser(req.user);
        const currentEmployeeId = Number(profile?.id || 0);
        const recipients = normalizeNumericIdList(row?.recipientEmployeeIds);
        if (!Number.isInteger(currentEmployeeId) || currentEmployeeId <= 0 || !recipients.includes(currentEmployeeId)) {
          return res.status(403).json({ error: 'Acces refuse: document non adresse a cet employe' });
        }
      }
    }

    const resolvedPath = resolveExistingGuideAbsolutePath(row);
    if (!resolvedPath) {
      return res.status(404).json({ error: 'Fichier introuvable' });
    }

    const fileName = sanitizeFileName(String(row.fileName || path.basename(resolvedPath.absolutePath) || 'guide-document'));
    const mimeType = String(row.mimeType || 'application/octet-stream').trim() || 'application/octet-stream';
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    res.setHeader('Content-Type', mimeType);
    return res.sendFile(resolvedPath.absolutePath);
  } catch (err) {
    return res.status(500).json({ error: 'Erreur telechargement document guide', details: String(err?.message || err) });
  }
});

app.delete('/api/material-requests/:id', async (req, res) => {
  const id = Number(req.params.id);
  const requestRow = await get(
    `SELECT mr.id, mr.projetId, p.nomSite, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id = ?`,
    [id]
  );
  if (!requestRow) {
    return res.status(404).json({ error: 'Demande de matériel non trouvée' });
  }

  if (String(req.user?.role || '').trim() === 'chef_chantier_site' && !isInChefSiteScope(req.user, requestRow)) {
    return res.status(403).json({ error: 'Acces refuse: demande hors perimetre du site 15' });
  }

  const docs = await all('SELECT id, relativePath FROM generated_documents WHERE entityType = ? AND entityId = ?', ['material_request_authorization', id]);
  const result = await run('DELETE FROM material_requests WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Demande de matériel non trouvée' });
  }
  try {
    await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ?', ['material_request_authorization', id]);
  } catch (e) {}
  for (const doc of docs) {
    const filePath = path.join(ARCHIVE_ROOT, String(doc.relativePath || ''));
    if (fs.existsSync(filePath)) {
      try { await fs.promises.unlink(filePath); } catch (e) {}
    }
  }
  res.json({ message: 'Demande de matériel supprimée' });
});

app.delete('/api/revenues/:id', async (req, res) => {
  const id = Number(req.params.id);
  const docs = await all('SELECT relativePath FROM generated_documents WHERE entityType = ? AND entityId = ?', ['revenue', id]);
  const result = await run('DELETE FROM revenues WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Revenu non trouvé' });
  }
  try { await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ?', ['revenue', id]); } catch (e) {}
  for (const doc of docs) {
    const filePath = path.join(ARCHIVE_ROOT, doc.relativePath);
    if (fs.existsSync(filePath)) {
      try { await fs.promises.unlink(filePath); } catch (e) {}
    }
  }
  res.json({ message: 'Revenu supprimé' });
});

app.delete('/api/expenses', async (_req, res) => {
  await run('DELETE FROM expenses');
  res.json({ message: 'Toutes les dépenses ont été supprimées' });
});

app.delete('/api/expenses/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM expenses WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Dépense non trouvée' });
  }
  res.json({ message: 'Dépense supprimée' });
});

app.post('/api/expenses', async (req, res) => {
  const {
    materialId,
    projetId,
    description = '',
    quantite,
    prixUnitaire,
    fournisseur = '',
    categorie,
    item,
    category,
    quantity,
    unitPrice,
    projectId,
  } = req.body;

  const expenseDescription = description || item || '';
  const expenseCategory = categorie || category;
  const expenseQuantity = Number(quantite ?? quantity);
  const expenseUnitPrice = Number(prixUnitaire ?? unitPrice);
  const expenseProjectId = projetId || projectId || null;

  if (!expenseDescription || !expenseQuantity || !expenseUnitPrice || !expenseCategory) {
    return res.status(400).json({ error: 'Champs obligatoires manquants' });
  }

  const expense = await insertExpenseRecord({
    materialId: materialId || null,
    projetId: expenseProjectId,
    description: expenseDescription,
    quantite: expenseQuantity,
    prixUnitaire: expenseUnitPrice,
    fournisseur,
    categorie: expenseCategory,
    createdBy: req.user.username,
    dateExpense: new Date().toISOString(),
  });

  res.status(201).json(expense);
});

app.get('/api/expenses', async (_req, res) => {
  const rows = await all(`
    SELECT e.*, m.nom as materialNom, p.nomProjet as projetNom
    FROM expenses e
    LEFT JOIN materials m ON m.id = e.materialId
    LEFT JOIN projects p ON p.id = e.projetId
    ORDER BY e.dateExpense DESC
  `);
  res.json(rows);
});

app.patch('/api/expenses/:id/status', async (req, res) => {
  const id = Number(req.params.id);
  const { statut } = req.body;

  if (!['EN_ATTENTE', 'VALIDEE', 'REJETEE'].includes(statut)) {
    return res.status(400).json({ error: 'Statut invalide' });
  }

  const result = await run('UPDATE expenses SET statut = ? WHERE id = ?', [statut, id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Dépense non trouvée' });
  }

  const expense = await get('SELECT * FROM expenses WHERE id = ?', [id]);
  res.json(expense);
});

app.post('/api/revenues', async (req, res) => {
  const { projetId, description, amount, dateRevenue } = req.body;
  if (!projetId || !description || !amount) {
    return res.status(400).json({ error: 'Champs obligatoires manquants' });
  }

  const numericProjectId = Number(projetId);
  const numericAmount = Number(amount);
  if (!numericProjectId || Number.isNaN(numericAmount) || numericAmount <= 0) {
    return res.status(400).json({ error: 'Projet et montant valides sont obligatoires' });
  }

  const nextRevenueId = await getNextTableId('revenues');
  const result = await run(
    'INSERT INTO revenues (id, projetId, description, amount, dateRevenue, createdBy) VALUES (?, ?, ?, ?, ?, ?)',
    [nextRevenueId, numericProjectId, String(description).trim(), numericAmount, dateRevenue ? new Date(dateRevenue).toISOString() : new Date().toISOString(), req.user.username]
  );

  await archiveRevenueInvoicePdf(result.lastID || nextRevenueId);

  const revenue = await get('SELECT * FROM revenues WHERE id = ?', [result.lastID || nextRevenueId]);
  res.status(201).json(revenue);
});

app.get('/api/revenues', async (_req, res) => {
  const rows = await all(`
    SELECT r.*, p.nomProjet as projetNom
    FROM revenues r
    LEFT JOIN projects p ON p.id = r.projetId
    ORDER BY r.dateRevenue DESC
  `);
  res.json(rows);
});

app.get('/api/auto-vehicles', async (_req, res) => {
  const rows = await all('SELECT * FROM auto_vehicles ORDER BY createdAt DESC, id DESC');
  res.json(rows);
});

app.post('/api/auto-vehicles', async (req, res) => {
  const {
    nomVehicule = '',
    marqueVehicule = '',
    immatriculation = '',
    chauffeurNom = '',
    gpsActif = false,
    valeurVehicule,
    etatVehicule = '',
  } = req.body || {};

  const nom = String(nomVehicule).trim();
  const marque = String(marqueVehicule).trim();
  const plaque = String(immatriculation).trim();
  const chauffeur = String(chauffeurNom).trim();
  const etat = String(etatVehicule).trim();
  const valeur = Number(valeurVehicule);
  const gpsEnabled = gpsActif ? 1 : 0;

  if (!nom || !marque || !etat || Number.isNaN(valeur) || valeur < 0) {
    return res.status(400).json({ error: 'Nom, marque, valeur et etat du vehicule sont obligatoires' });
  }

  const nextId = await getNextTableId('auto_vehicles');

  const result = await run(
    'INSERT INTO auto_vehicles (id, nomVehicule, marqueVehicule, immatriculation, chauffeurNom, gpsActif, valeurVehicule, etatVehicule, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [nextId, nom, marque, plaque, chauffeur, gpsEnabled, valeur, etat, new Date().toISOString()]
  );

  const vehicleId = Number(result.lastID || nextId);
  const vehicle = await get('SELECT * FROM auto_vehicles WHERE id = ?', [vehicleId]);
  res.status(201).json(vehicle);
});

app.get('/api/auto-vehicles/:id/tracking-device', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'ID vehicule invalide' });
  }

  const vehicle = await get('SELECT * FROM auto_vehicles WHERE id = ?', [id]);
  if (!vehicle) {
    return res.status(404).json({ error: 'Vehicule non trouve' });
  }

  const device = await get(
    'SELECT id, vehicleId, deviceName, isActive, lastSeenAt, lastLatitude, lastLongitude, lastSpeedKph, createdBy, createdAt, updatedAt FROM auto_tracking_devices WHERE vehicleId = ? LIMIT 1',
    [id]
  );

  res.json({ vehicle, device: device || null });
});

app.post('/api/auto-vehicles/:id/tracking-device', async (req, res) => {
  const id = Number(req.params.id);
  const deviceName = String(req.body?.deviceName || 'smartphone').trim() || 'smartphone';
  if (!id) {
    return res.status(400).json({ error: 'ID vehicule invalide' });
  }

  const vehicle = await get('SELECT * FROM auto_vehicles WHERE id = ?', [id]);
  if (!vehicle) {
    return res.status(404).json({ error: 'Vehicule non trouve' });
  }

  const rawToken = generateTrackingToken();
  const tokenHash = hashTrackingToken(rawToken);
  const now = new Date().toISOString();
  const existing = await get('SELECT id FROM auto_tracking_devices WHERE vehicleId = ? LIMIT 1', [id]);

  if (existing) {
    await run(
      'UPDATE auto_tracking_devices SET deviceName = ?, tokenHash = ?, isActive = 1, updatedAt = ? WHERE vehicleId = ?',
      [deviceName, tokenHash, now, id]
    );
  } else {
    const nextDeviceIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM auto_tracking_devices');
    const nextDeviceId = Number(nextDeviceIdRow?.nextId || 1);

    await run(
      'INSERT INTO auto_tracking_devices (id, vehicleId, deviceName, tokenHash, isActive, createdBy, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [nextDeviceId, id, deviceName, tokenHash, 1, req.user.username, now, now]
    );
  }

  await run('UPDATE auto_vehicles SET gpsActif = 1 WHERE id = ?', [id]);

  const trackerPath = '/tracker.html';
  const baseUrl = `${req.protocol}://${req.get('host')}`;
  const trackerUrl = `${baseUrl}${trackerPath}?vehicleId=${id}&token=${encodeURIComponent(rawToken)}`;

  res.status(201).json({
    vehicleId: id,
    deviceName,
    token: rawToken,
    ingestUrl: `${baseUrl}/api/gps/ingest`,
    trackerUrl,
    note: 'Conserve ce token de maniere securisee. Il ne sera plus affiché en clair.',
  });
});

app.delete('/api/auto-vehicles/:id/tracking-device', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'ID vehicule invalide' });
  }

  const existing = await get('SELECT id FROM auto_tracking_devices WHERE vehicleId = ? LIMIT 1', [id]);
  if (!existing) {
    return res.status(404).json({ error: 'Aucun appareil de tracking configure pour ce vehicule' });
  }

  await run('UPDATE auto_tracking_devices SET isActive = 0, updatedAt = ? WHERE vehicleId = ?', [new Date().toISOString(), id]);
  res.json({ message: 'Tracking smartphone desactive pour ce vehicule' });
});

app.get('/api/auto-vehicle-locations', async (_req, res) => {
  const [vehicles, rows] = await Promise.all([
    all('SELECT * FROM auto_vehicles ORDER BY createdAt DESC, id DESC'),
    all(`
      SELECT
        id,
        vehicle_id AS vehicleId,
        latitude,
        longitude,
        speed_kph AS speedKph,
        heading,
        accuracy_meters AS accuracyMeters,
        source,
        status,
        note,
        recorded_at AS recordedAt,
        created_by AS createdBy
      FROM auto_vehicle_locations
      ORDER BY recorded_at DESC, id DESC
    `),
  ]);

  const latestByVehicleId = new Map();
  rows.forEach(row => {
    const vehicleId = Number(row.vehicleId);
    if (!latestByVehicleId.has(vehicleId)) {
      latestByVehicleId.set(vehicleId, row);
    }
  });

  res.json(vehicles.map(vehicle => ({
    ...vehicle,
    lastLocation: latestByVehicleId.get(Number(vehicle.id)) || null,
  })));
});

app.get('/api/auto-vehicles/:id/locations', async (req, res) => {
  const id = Number(req.params.id);
  const limit = Math.min(Math.max(Number(req.query.limit || 25), 1), 200);

  if (!id) {
    return res.status(400).json({ error: 'ID vehicule invalide' });
  }

  const vehicle = await get('SELECT * FROM auto_vehicles WHERE id = ?', [id]);
  if (!vehicle) {
    return res.status(404).json({ error: 'Vehicule non trouve' });
  }

  const rows = await all(
    `SELECT
      id,
      vehicle_id AS vehicleId,
      latitude,
      longitude,
      speed_kph AS speedKph,
      heading,
      accuracy_meters AS accuracyMeters,
      source,
      status,
      note,
      recorded_at AS recordedAt,
      created_by AS createdBy
    FROM auto_vehicle_locations
    WHERE vehicle_id = ?
    ORDER BY recorded_at DESC, id DESC
    LIMIT ?`,
    [id, limit]
  );

  res.json({
    vehicle,
    locations: rows,
  });
});

app.post('/api/auto-vehicles/:id/locations', async (req, res) => {
  const id = Number(req.params.id);
  const {
    latitude,
    longitude,
    speedKph = 0,
    heading = 0,
    accuracyMeters = 0,
    source = 'manual',
    status = 'online',
    note = '',
    recordedAt,
  } = req.body || {};

  if (!id) {
    return res.status(400).json({ error: 'ID vehicule invalide' });
  }

  const vehicle = await get('SELECT * FROM auto_vehicles WHERE id = ?', [id]);
  if (!vehicle) {
    return res.status(404).json({ error: 'Vehicule non trouve' });
  }

  try {
    const location = await insertAutoVehicleLocationRecord({
      vehicleId: id,
      latitude,
      longitude,
      speedKph,
      heading,
      accuracyMeters,
      source,
      status,
      note,
      recordedAt,
      createdBy: req.user.username,
    });

    res.status(201).json({
      ...location,
      nomVehicule: vehicle.nomVehicule,
      marqueVehicule: vehicle.marqueVehicule,
    });
  } catch (error) {
    if (error && error.message && error.message.includes('invalide')) {
      return res.status(400).json({ error: error.message });
    }
    throw error;
  }
});

app.delete('/api/auto-vehicles/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'ID vehicule invalide' });
  }

  const usage = await get('SELECT id FROM auto_transport_costs WHERE vehicleId = ? LIMIT 1', [id]);
  if (usage) {
    return res.status(400).json({ error: 'Impossible de supprimer ce vehicule: des couts de transport sont deja lies' });
  }

  const result = await run('DELETE FROM auto_vehicles WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Vehicule non trouve' });
  }

  res.json({ message: 'Vehicule supprime' });
});

app.get('/api/auto-transport-costs', async (_req, res) => {
  const rows = await all(`
    SELECT atc.*, av.nomVehicule, av.marqueVehicule
    FROM auto_transport_costs atc
    JOIN auto_vehicles av ON av.id = atc.vehicleId
    ORDER BY atc.dateTransport DESC, atc.createdAt DESC, atc.id DESC
  `);
  res.json(rows);
});

app.post('/api/auto-transport-costs', async (req, res) => {
  const {
    vehicleId,
    niveauEssenceEntree,
    niveauEssenceSortie,
    prixLocalEssence,
    dateTransport,
    note = '',
  } = req.body || {};

  const numericVehicleId = Number(vehicleId);
  const fuelIn = Number(niveauEssenceEntree);
  const fuelOut = Number(niveauEssenceSortie);
  const localFuelPrice = Number(prixLocalEssence);

  if (!numericVehicleId || Number.isNaN(fuelIn) || Number.isNaN(fuelOut) || Number.isNaN(localFuelPrice)) {
    return res.status(400).json({ error: 'Vehicule, niveaux essence et prix local sont obligatoires' });
  }

  if (fuelIn < 0 || fuelOut < 0 || localFuelPrice <= 0) {
    return res.status(400).json({ error: 'Les valeurs numeriques doivent etre positives' });
  }

  if (fuelOut > fuelIn) {
    return res.status(400).json({ error: 'Le niveau de sortie ne peut pas etre superieur au niveau d\'entree' });
  }

  const quantiteConsommee = fuelIn - fuelOut;
  if (quantiteConsommee <= 0) {
    return res.status(400).json({ error: 'La consommation doit etre superieure a zero' });
  }

  const vehicle = await get('SELECT * FROM auto_vehicles WHERE id = ?', [numericVehicleId]);
  if (!vehicle) {
    return res.status(404).json({ error: 'Vehicule introuvable' });
  }

  const effectiveDate = dateTransport ? new Date(dateTransport).toISOString() : new Date().toISOString();
  const expenseDescription = `Carburant - ${vehicle.nomVehicule} (${vehicle.marqueVehicule})`;
  const expense = await insertExpenseRecord({
    projetId: null,
    description: expenseDescription,
    quantite: quantiteConsommee,
    prixUnitaire: localFuelPrice,
    fournisseur: 'Station locale',
    categorie: 'transport',
    createdBy: req.user.username,
    dateExpense: effectiveDate,
  });

  const montantTotal = quantiteConsommee * localFuelPrice;
  const result = await run(
    `INSERT INTO auto_transport_costs (
      vehicleId,
      expenseId,
      niveauEssenceEntree,
      niveauEssenceSortie,
      prixLocalEssence,
      quantiteConsommee,
      montantTotal,
      dateTransport,
      note,
      createdBy,
      createdAt
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      numericVehicleId,
      expense.id,
      fuelIn,
      fuelOut,
      localFuelPrice,
      quantiteConsommee,
      montantTotal,
      effectiveDate,
      String(note || '').trim(),
      req.user.username,
      new Date().toISOString(),
    ]
  );

  const cost = await get(`
    SELECT atc.*, av.nomVehicule, av.marqueVehicule
    FROM auto_transport_costs atc
    JOIN auto_vehicles av ON av.id = atc.vehicleId
    WHERE atc.id = ?
  `, [result.lastID]);

  res.status(201).json(cost);
});

app.delete('/api/auto-transport-costs/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'ID cout transport invalide' });
  }

  const row = await get('SELECT expenseId FROM auto_transport_costs WHERE id = ?', [id]);
  if (!row) {
    return res.status(404).json({ error: 'Ligne de cout transport introuvable' });
  }

  await run('DELETE FROM auto_transport_costs WHERE id = ?', [id]);
  if (row.expenseId) {
    await run('DELETE FROM expenses WHERE id = ?', [row.expenseId]);
  }

  res.json({ message: 'Cout transport supprime' });
});

function isValidIsoDate(dateValue) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(dateValue || '').trim());
}

function isValidTimeValue(timeValue) {
  if (!String(timeValue || '').trim()) return true;
  return /^\d{2}:\d{2}$/.test(String(timeValue || '').trim());
}

function getAbidjanNowParts(baseDate = new Date()) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Africa/Abidjan',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  const parts = formatter.formatToParts(baseDate);
  const byType = {};
  parts.forEach(part => {
    byType[part.type] = part.value;
  });
  return {
    date: `${byType.year}-${byType.month}-${byType.day}`,
    time: `${byType.hour}:${byType.minute}`,
  };
}

function normalizeAttendancePunchType(rawValue) {
  const value = String(rawValue || '').trim().toLowerCase();
  if (value === 'checkin' || value === 'arrival') return 'checkin';
  if (value === 'checkout' || value === 'departure') return 'checkout';
  return 'auto';
}

function isHrAttendanceDateLocked() {
  return String(process.env.HR_ATTENDANCE_LOCK_TODAY || '1').trim() !== '0';
}

function normalizeHrCode(rawCode) {
  const value = String(rawCode || '').trim().toUpperCase();
  const allowed = new Set(['P', 'R', 'MS', 'A', 'CA', 'CM', 'CP']);
  return allowed.has(value) ? value : 'P';
}

function inferAttendanceCode(checkInTime, manualCode = '') {
  if (String(manualCode || '').trim()) {
    return normalizeHrCode(manualCode);
  }
  const timeValue = String(checkInTime || '').trim();
  if (!timeValue) return 'P';
  return timeValue > '08:30' ? 'R' : 'P';
}

function normalizeLeaveTypeCode(leaveType) {
  const value = normalizeTextValue(leaveType);
  if (value.includes('maternite')) return 'CM';
  if (value.includes('paternite')) return 'CP';
  if (value.includes('maladie')) return 'MS';
  if (value.includes('absence')) return 'A';
  return 'CA';
}

function normalizeTextValue(rawValue) {
  return String(rawValue || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();
}

function normalizeVillaTypeShortLabel(rawValue) {
  const value = String(rawValue || '').trim();
  if (!value) return '';
  const normalized = value.toUpperCase().replace(/^VILLA\s+/i, '').trim();
  const match = normalized.match(/^T\s*([0-9]+)$/i) || normalized.match(/\bT\s*([0-9]+)\b/i);
  if (match) return `T${match[1]}`;
  return normalized;
}

function normalizeProjectConstructionStatus(rawValue) {
  const value = normalizeTextValue(rawValue);
  if (!value) return '';
  if (value.includes('construit') || value.includes('termine') || value.includes('acheve') || value.includes('livre')) {
    return 'CONSTRUIT';
  }
  if (value.includes('construction') || value.includes('cours') || value.includes('demarre')) {
    return 'EN_CONSTRUCTION';
  }
  if (value.includes('pas') || value.includes('debut')) {
    return 'PAS_DEMARRE';
  }
  return String(rawValue || '').trim().toUpperCase();
}

function formatMonthRange(monthValue) {
  const [yearRaw, monthRaw] = String(monthValue || '').split('-');
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
    return null;
  }
  const startDate = `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-01`;
  const endDateObj = new Date(Date.UTC(year, month, 0));
  const endDate = `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(endDateObj.getUTCDate()).padStart(2, '0')}`;
  const daysInMonth = endDateObj.getUTCDate();
  return { year, month, startDate, endDate, daysInMonth };
}

function listDatesInRange(startDate, endDate) {
  const start = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start > end) return [];

  const dates = [];
  const cursor = new Date(start);
  while (cursor <= end) {
    dates.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
}

function getHrAttendanceStatusLabel(code, isWeekend, leaveTypeLabel = '') {
  const normalizedCode = normalizeHrCode(code);
  if (['MS', 'CA', 'CM', 'CP'].includes(normalizedCode)) {
    return leaveTypeLabel ? `Conge (${leaveTypeLabel})` : 'Conge';
  }
  if (normalizedCode === 'R') return 'Present (retard)';
  if (normalizedCode === 'P') return 'Present';
  if (normalizedCode === 'A') return 'Absent';
  return isWeekend ? 'Weekend' : 'Absent';
}

function getHrLeaveTypeLabel(leaveType) {
  const leaveCode = normalizeLeaveTypeCode(leaveType);
  return {
    MS: 'Conge maladie',
    CA: 'Conge annuel',
    CM: 'Conge maternite',
    CP: 'Conge paternite',
    A: 'Absence autorisee',
  }[leaveCode] || 'Conge';
}

async function generateHrLeaveApprovalPdfBuffer({ employee, leaveRequest }) {
  return await new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({ size: 'A4', margin: 44 });
    doc.on('data', chunk => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    const employeeName = String(employee?.fullName || `Employe #${Number(employee?.id || 0)}`).trim();
    const jobTitle = String(employee?.jobTitle || '-').trim() || '-';
    const leaveTypeLabel = getHrLeaveTypeLabel(leaveRequest?.leaveType);
    const startDate = String(leaveRequest?.startDate || '').slice(0, 10) || '-';
    const endDate = String(leaveRequest?.endDate || '').slice(0, 10) || '-';
    const decidedBy = String(leaveRequest?.decidedBy || 'directeur_rh').trim() || 'directeur_rh';
    const decidedAt = String(leaveRequest?.decidedAt || new Date().toISOString()).trim();
    const reason = String(leaveRequest?.reason || '').trim() || 'Aucun motif renseigne';
    const decisionNote = String(leaveRequest?.decisionNote || '').trim();

    const signatureFontPath = resolveSignatureFontPath();
    const signatureFontName = signatureFontPath ? 'SignatureScript' : 'Helvetica-Oblique';
    if (signatureFontPath) {
      try {
        doc.registerFont(signatureFontName, signatureFontPath);
      } catch (_err) {
        // Fallback to Helvetica-Oblique when custom script font is unavailable.
      }
    }

    const safeDecisionDate = Number.isNaN(new Date(decidedAt).getTime())
      ? new Date().toLocaleString('fr-FR')
      : new Date(decidedAt).toLocaleString('fr-FR');
    const pageHeight = doc.page.height;
    const contentWidth = doc.page.width - 88;
    const footerBaseY = pageHeight - 118;

    doc.rect(44, 44, contentWidth, 92).fill('#e2e8f0');
    doc.fillColor('#0f172a').font('Helvetica-Bold').fontSize(20)
      .text('DEMANDE DE CONGE', 44, 70, { width: contentWidth, align: 'center' });
    doc.font('Helvetica').fontSize(10).fillColor('#334155')
      .text('Document officiel de validation RH', 44, 100, { width: contentWidth, align: 'center' });

    doc.roundedRect(44, 154, contentWidth, 156, 8).lineWidth(1).strokeColor('#cbd5e1').stroke();
    doc.font('Helvetica-Bold').fontSize(11).fillColor('#0f172a').text('Informations employe', 58, 170);
    doc.font('Helvetica').fontSize(11).fillColor('#1f2937');
    doc.text(`Nom complet: ${employeeName}`, 58, 194, { width: contentWidth - 28 });
    doc.text(`Poste: ${jobTitle}`, 58, 214, { width: contentWidth - 28 });
    doc.text(`Type de conge: ${leaveTypeLabel}`, 58, 234, { width: contentWidth - 28 });
    doc.text(`Periode: du ${startDate} au ${endDate}`, 58, 254, { width: contentWidth - 28 });

    doc.roundedRect(44, 326, contentWidth, 130, 8).lineWidth(1).strokeColor('#cbd5e1').stroke();
    doc.font('Helvetica-Bold').fontSize(11).fillColor('#0f172a').text('Motif de la demande', 58, 342);
    doc.font('Helvetica').fontSize(10.5).fillColor('#111827').text(reason, 58, 364, {
      width: contentWidth - 28,
      height: 80,
      ellipsis: true,
    });

    if (decisionNote) {
      doc.roundedRect(44, 468, contentWidth, 86, 8).lineWidth(1).strokeColor('#cbd5e1').stroke();
      doc.font('Helvetica-Bold').fontSize(11).fillColor('#0f172a').text('Note de decision RH', 58, 484);
      doc.font('Helvetica').fontSize(10.5).fillColor('#111827').text(decisionNote, 58, 506, {
        width: contentWidth - 28,
        height: 38,
        ellipsis: true,
      });
    }

    doc.font('Helvetica').fontSize(10).fillColor('#1f2937')
      .text('Decision: VALIDE', 58, footerBaseY - 26, { width: 250, align: 'left' });
    doc.text(`Validee par: ${decidedBy}`, 58, footerBaseY - 10, { width: 250, align: 'left' });
    doc.text(`Date de validation: ${safeDecisionDate}`, 58, footerBaseY + 6, { width: 300, align: 'left' });

    doc.lineWidth(2).strokeColor('#166534').fillColor('#166534');
    doc.roundedRect(48, footerBaseY + 26, 148, 52, 8).stroke();
    doc.font('Helvetica-Bold').fontSize(18).fillColor('#166534').text('VALIDE', 48, footerBaseY + 40, {
      width: 148,
      align: 'center',
    });

    doc.font('Helvetica').fontSize(9).fillColor('#0f172a').text('Signature directeur RH', 336, footerBaseY + 10, {
      width: 216,
      align: 'right',
    });
    doc.font(signatureFontName).fontSize(27).fillColor('#111827').text('directeur_rh', 336, footerBaseY + 24, {
      width: 216,
      align: 'right',
    });

    doc.font('Helvetica').fontSize(9).fillColor('#475569').text('Document genere automatiquement par Ryan ERP.', 44, pageHeight - 36, {
      width: contentWidth,
      align: 'center',
    });
    doc.end();
  });
}

async function archiveOrUpdateHrLeaveDecisionDocument(leaveRequestRow, employeeRow) {
  const leaveRequestId = Number(leaveRequestRow?.id || 0);
  const employeeId = Number(employeeRow?.id || leaveRequestRow?.employeeId || 0);
  if (!leaveRequestId || !employeeId) return null;

  const leaveTypeLabel = getHrLeaveTypeLabel(leaveRequestRow?.leaveType);
  const employeeNameSafe = sanitizeFileName(String(employeeRow?.fullName || `employe-${employeeId}`));
  const periodToken = `${String(leaveRequestRow?.startDate || '').slice(0, 10)}-${String(leaveRequestRow?.endDate || '').slice(0, 10)}`.replace(/[^0-9-]/g, '');
  const fileName = sanitizeFileName(`Demande de conge - ${employeeNameSafe} - ${periodToken || leaveRequestId}.pdf`);
  const relativePath = path.join('construction', 'hr-presence', `employee-${employeeId}`, fileName);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(path.dirname(absolutePath), { recursive: true });

  const pdfBuffer = await generateHrLeaveApprovalPdfBuffer({ employee: employeeRow, leaveRequest: leaveRequestRow });
  await fs.promises.writeFile(absolutePath, pdfBuffer);

  const title = `${leaveTypeLabel} - ${String(employeeRow?.fullName || `Employe ${employeeId}`)} - ${String(leaveRequestRow?.startDate || '').slice(0, 10)}`;
  const nowIso = new Date().toISOString();
  const existing = await get(
    'SELECT id, relativePath FROM generated_documents WHERE sectionCode = ? AND entityType = ? AND entityId = ? ORDER BY id DESC LIMIT 1',
    ['hr_presence', 'hr_leave_request', leaveRequestId]
  );

  if (existing?.relativePath) {
    const oldAbsolutePath = path.join(ARCHIVE_ROOT, String(existing.relativePath));
    if (oldAbsolutePath !== absolutePath && fs.existsSync(oldAbsolutePath)) {
      try { await fs.promises.unlink(oldAbsolutePath); } catch (_error) {}
    }
  }

  if (existing?.id) {
    await run(
      `UPDATE generated_documents
       SET sectionLabel = ?, title = ?, fileName = ?, relativePath = ?, updatedAt = ?
       WHERE id = ?`,
      ['Presence RH', title, fileName, relativePath, nowIso, Number(existing.id)]
    );
  } else {
    const nextDocumentId = await getNextTableId('generated_documents');
    await run(
      `INSERT INTO generated_documents
        (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [nextDocumentId, 'hr_presence', 'Presence RH', 'hr_leave_request', leaveRequestId, title, fileName, relativePath, nowIso, nowIso]
    );
  }

  return { title, fileName, relativePath, sectionCode: 'hr_presence' };
}

async function deleteHrLeaveDecisionDocument(leaveRequestId) {
  const numericLeaveRequestId = Number(leaveRequestId || 0);
  if (!numericLeaveRequestId) return;

  const rows = await all(
    'SELECT id, relativePath FROM generated_documents WHERE sectionCode = ? AND entityType = ? AND entityId = ?',
    ['hr_presence', 'hr_leave_request', numericLeaveRequestId]
  );

  for (const row of rows || []) {
    const relativePath = String(row?.relativePath || '').trim();
    if (relativePath) {
      const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
      if (fs.existsSync(absolutePath)) {
        try { await fs.promises.unlink(absolutePath); } catch (_error) {}
      }
    }
    await run('DELETE FROM generated_documents WHERE id = ?', [Number(row.id)]);
  }
}

async function generateHrAttendanceSheetPdfBuffer({ employee, range, rows, monthLabel }) {
  return await new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({ size: 'A4', margin: 40 });
    doc.on('data', chunk => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    const employeeName = String(employee?.fullName || `Employe #${Number(employee?.id || 0)}`).trim();
    const jobTitle = String(employee?.jobTitle || '-').trim() || '-';
    const monthText = String(monthLabel || `${String(range.year).padStart(4, '0')}-${String(range.month).padStart(2, '0')}`).trim();

    const renderHeader = () => {
      doc.font('Helvetica-Bold').fontSize(16).fillColor('#0f172a').text('Fiche mensuelle de presence', { align: 'center' });
      doc.moveDown(0.6);
      doc.font('Helvetica').fontSize(10).fillColor('#334155');
      doc.text(`Employe: ${employeeName}`);
      doc.text(`Poste: ${jobTitle}`);
      doc.text(`Mois: ${monthText}`);
      doc.text(`Genere le: ${new Date().toLocaleString('fr-FR')}`);
      doc.moveDown(0.8);
    };

    const headers = ['Date', 'Jour', 'Code', 'Statut'];
    const colWidths = [95, 95, 60, 260];
    const rowHeight = 19;
    const startX = doc.page.margins.left;
    const bottomLimit = doc.page.height - doc.page.margins.bottom - 20;

    const drawTableHeader = y => {
      let cursorX = startX;
      doc.save();
      doc.rect(startX, y, colWidths.reduce((sum, width) => sum + width, 0), rowHeight).fill('#e2e8f0');
      doc.restore();
      doc.font('Helvetica-Bold').fontSize(10).fillColor('#0f172a');
      headers.forEach((header, index) => {
        doc.text(header, cursorX + 5, y + 5, { width: colWidths[index] - 10, align: 'left' });
        cursorX += colWidths[index];
      });
    };

    const drawTableRow = (y, row, isOdd) => {
      const tableWidth = colWidths.reduce((sum, width) => sum + width, 0);
      if (isOdd) {
        doc.save();
        doc.rect(startX, y, tableWidth, rowHeight).fill('#f8fafc');
        doc.restore();
      }
      doc.font('Helvetica').fontSize(9).fillColor('#1e293b');
      let cursorX = startX;
      const values = [row.dateLabel, row.dayLabel, row.codeLabel, row.statusLabel];
      values.forEach((value, index) => {
        doc.text(String(value || ''), cursorX + 5, y + 5, { width: colWidths[index] - 10, align: 'left' });
        cursorX += colWidths[index];
      });

      doc.save();
      doc.moveTo(startX, y + rowHeight).lineTo(startX + tableWidth, y + rowHeight).lineWidth(0.5).strokeColor('#cbd5e1').stroke();
      doc.restore();
    };

    renderHeader();
    let y = doc.y;
    drawTableHeader(y);
    y += rowHeight;

    rows.forEach((row, index) => {
      if (y + rowHeight > bottomLimit) {
        doc.addPage();
        renderHeader();
        y = doc.y;
        drawTableHeader(y);
        y += rowHeight;
      }
      drawTableRow(y, row, index % 2 === 1);
      y += rowHeight;
    });

    doc.end();
  });
}

async function generateOrUpdateHrAttendanceSheet(employeeId, monthValue) {
  const numericEmployeeId = Number(employeeId || 0);
  if (!Number.isInteger(numericEmployeeId) || numericEmployeeId <= 0) return null;

  const range = formatMonthRange(monthValue);
  if (!range) return null;

  const employee = await get('SELECT id, fullName, jobTitle FROM hr_employees WHERE id = ?', [numericEmployeeId]);
  if (!employee) return null;

  const attendanceRows = await all(
    `SELECT COALESCE(NULLIF(attendanceDate, ''), dayDate) AS effectiveDate,
            COALESCE(NULLIF(statusCode, ''), NULLIF(status, ''), 'P') AS code
     FROM hr_attendance
     WHERE employeeId = ?
       AND COALESCE(NULLIF(attendanceDate, ''), dayDate) >= ?
       AND COALESCE(NULLIF(attendanceDate, ''), dayDate) <= ?`,
    [numericEmployeeId, range.startDate, range.endDate]
  );

  const leaveRows = await all(
    `SELECT leaveType, startDate, endDate
     FROM hr_leave_requests
     WHERE employeeId = ?
       AND status = 'APPROUVEE'
       AND startDate <= ?
       AND endDate >= ?`,
    [numericEmployeeId, range.endDate, range.startDate]
  );

  const attendanceByDate = new Map();
  (Array.isArray(attendanceRows) ? attendanceRows : []).forEach(row => {
    const dateKey = String(row?.effectiveDate || '').slice(0, 10);
    if (!isValidIsoDate(dateKey)) return;
    attendanceByDate.set(dateKey, normalizeHrCode(row?.code));
  });

  const leaveByDate = new Map();
  (Array.isArray(leaveRows) ? leaveRows : []).forEach(leave => {
    const effectiveStart = String(leave?.startDate || '').slice(0, 10);
    const effectiveEnd = String(leave?.endDate || '').slice(0, 10);
    if (!isValidIsoDate(effectiveStart) || !isValidIsoDate(effectiveEnd)) return;

    const boundedStart = effectiveStart < range.startDate ? range.startDate : effectiveStart;
    const boundedEnd = effectiveEnd > range.endDate ? range.endDate : effectiveEnd;
    const leaveCode = normalizeLeaveTypeCode(leave?.leaveType);
    const leaveLabel = {
      MS: 'maladie',
      CA: 'annuel',
      CM: 'maternite',
      CP: 'paternite',
      A: 'absence',
    }[leaveCode] || 'conge';

    listDatesInRange(boundedStart, boundedEnd).forEach(dateValue => {
      leaveByDate.set(dateValue, { code: leaveCode, label: leaveLabel });
    });
  });

  const dayNames = ['Dimanche', 'Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi'];
  const rows = [];
  for (let day = 1; day <= range.daysInMonth; day += 1) {
    const dateValue = `${String(range.year).padStart(4, '0')}-${String(range.month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    const dateObj = new Date(`${dateValue}T00:00:00.000Z`);
    const weekday = dateObj.getUTCDay();
    const isWeekend = weekday === 0 || weekday === 6;

    const leaveEntry = leaveByDate.get(dateValue) || null;
    const hasAttendanceEntry = attendanceByDate.has(dateValue);
    const code = leaveEntry?.code || attendanceByDate.get(dateValue) || '';
    const normalizedCode = code ? normalizeHrCode(code) : '';
    const effectiveCode = normalizedCode || '-';
    const statusLabel = leaveEntry
      ? getHrAttendanceStatusLabel(effectiveCode, isWeekend, leaveEntry?.label || '')
      : hasAttendanceEntry
        ? getHrAttendanceStatusLabel(effectiveCode, isWeekend, '')
        : (isWeekend ? 'Weekend' : 'Neant');

    rows.push({
      dateLabel: dateValue,
      dayLabel: dayNames[weekday] || '-',
      codeLabel: effectiveCode,
      statusLabel,
    });
  }

  const monthToken = `${String(range.year).padStart(4, '0')}-${String(range.month).padStart(2, '0')}`;
  const monthLabel = new Date(Date.UTC(range.year, range.month - 1, 1)).toLocaleDateString('fr-FR', { month: 'long', year: 'numeric' });
  const employeeNameSafe = sanitizeFileName(String(employee.fullName || `employe-${numericEmployeeId}`));
  const fileName = sanitizeFileName(`presence-${employeeNameSafe}-${monthToken}.pdf`);
  const relativePath = path.join('construction', 'hr-presence', `employee-${numericEmployeeId}`, fileName);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(path.dirname(absolutePath), { recursive: true });

  const pdfBuffer = await generateHrAttendanceSheetPdfBuffer({ employee, range, rows, monthLabel });
  await fs.promises.writeFile(absolutePath, pdfBuffer);

  const title = `Feuille de presence ${String(employee.fullName || `Employe ${numericEmployeeId}`)} - ${monthToken}`;
  const nowIso = new Date().toISOString();

  const existing = await get(
    'SELECT id, relativePath FROM generated_documents WHERE sectionCode = ? AND entityType = ? AND entityId = ? AND title = ? ORDER BY id DESC LIMIT 1',
    ['hr_presence', 'hr_attendance_sheet', numericEmployeeId, title]
  );

  if (existing?.relativePath) {
    const oldAbsolutePath = path.join(ARCHIVE_ROOT, String(existing.relativePath));
    if (oldAbsolutePath !== absolutePath && fs.existsSync(oldAbsolutePath)) {
      try {
        await fs.promises.unlink(oldAbsolutePath);
      } catch (unlinkError) {
        console.warn('Unable to remove previous HR presence sheet:', unlinkError.message);
      }
    }
  }

  if (existing?.id) {
    await run(
      `UPDATE generated_documents
       SET sectionLabel = ?, fileName = ?, relativePath = ?, updatedAt = ?
       WHERE id = ?`,
      ['Presence RH', fileName, relativePath, nowIso, Number(existing.id)]
    );
  } else {
    const nextDocumentId = await getNextTableId('generated_documents');
    await run(
      `INSERT INTO generated_documents
        (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        nextDocumentId,
        'hr_presence',
        'Presence RH',
        'hr_attendance_sheet',
        numericEmployeeId,
        title,
        fileName,
        relativePath,
        nowIso,
        nowIso,
      ]
    );
  }

  return {
    title,
    fileName,
    relativePath,
    sectionCode: 'hr_presence',
  };
}

async function archiveHrEmployeeDocument({ employeeId, title = '', fileName = '', fileBuffer, mimeType = '', sourceModule = 'employee_dossier', uploadedBy = 'admin' }) {
  const numericEmployeeId = Number(employeeId || 0);
  if (!Number.isInteger(numericEmployeeId) || numericEmployeeId <= 0) {
    throw new Error('Employe invalide');
  }

  if (!fileBuffer || !Buffer.isBuffer(fileBuffer) || fileBuffer.length === 0) {
    throw new Error('Fichier vide');
  }

  const safeName = sanitizeFileName(fileName || 'document');
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const finalFileName = `${stamp}-${safeName}`;
  const relativePath = path.join('construction', 'hr-employees', `employee-${numericEmployeeId}`, finalFileName);
  const absolutePath = path.join(ARCHIVE_ROOT, relativePath);
  fs.mkdirSync(path.dirname(absolutePath), { recursive: true });
  await fs.promises.writeFile(absolutePath, fileBuffer);

  const now = new Date().toISOString();
  const nextDocId = await getNextTableId('hr_employee_documents');
  const normalizedSourceModule = String(sourceModule || '').trim() === 'signature_request' ? 'signature_request' : 'employee_dossier';
  await run(
    `INSERT INTO hr_employee_documents
      (id, employeeId, title, fileName, relativePath, fileSize, mimeType, sourceModule, uploadedBy, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextDocId,
      numericEmployeeId,
      String(title || '').trim() || safeName,
      finalFileName,
      relativePath,
      Number(fileBuffer.length || 0),
      String(mimeType || '').trim(),
      normalizedSourceModule,
      String(uploadedBy || 'admin').trim() || 'admin',
      now,
      now,
    ]
  );

  const created = await get('SELECT * FROM hr_employee_documents WHERE id = ?', [nextDocId]);
  return {
    ...created,
    fileUrl: `/archives/${String(relativePath || '').replace(/\\/g, '/')}`,
  };
}

app.get('/api/hr/employees', async (_req, res) => {
  const role = String(_req.user?.role || '').trim();
  const isHrDirectorRole = role === 'directeur_rh';
  if (!roleCanBypassRestrictedProfile(role) && !isHrDirectorRole) {
    let profileEmployee = await getHrProfileEmployeeForUser(_req.user);
    if (!profileEmployee?.id) {
      const userRow = await get('SELECT id, username, role FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) LIMIT 1', [String(_req.user?.username || '').trim()]);
      if (userRow?.username) {
        profileEmployee = await ensureHrProfileForUserAccount(userRow, 'auto_profile_sync');
      }
    }

    if (!profileEmployee?.id) {
      return res.json([]);
    }

    const row = await get(
      `SELECT id, fullName, jobTitle,
              COALESCE(NULLIF(sexe, ''), 'Neant') AS sexe,
              COALESCE(NULLIF(typeContrat, ''), 'Neant') AS typeContrat,
              COALESCE(NULLIF(dateEmbauche, ''), SUBSTR(createdAt, 1, 10)) AS dateEmbauche,
              phoneNumber, address, maritalStatus, COALESCE(NULLIF(email, ''), '') AS email,
              COALESCE(username, createdBy, '') AS username, createdBy, createdAt, updatedAt
       FROM hr_employees
       WHERE id = ?
       LIMIT 1`,
      [Number(profileEmployee.id)]
    );

    return res.json(row ? [row] : []);
  }

  const scopedEmployeeIds = await getHrScopedEmployeeIdsForUser(_req.user);
  if (scopedEmployeeIds && !scopedEmployeeIds.length) {
    return res.json([]);
  }

  const whereClause = scopedEmployeeIds
    ? `WHERE id IN (${scopedEmployeeIds.map(() => '?').join(', ')})`
    : '';

  const rows = await all(
    `SELECT id, fullName, jobTitle,
            COALESCE(NULLIF(sexe, ''), 'Neant') AS sexe,
            COALESCE(NULLIF(typeContrat, ''), 'Neant') AS typeContrat,
            COALESCE(NULLIF(dateEmbauche, ''), SUBSTR(createdAt, 1, 10)) AS dateEmbauche,
            phoneNumber, address, maritalStatus, COALESCE(NULLIF(email, ''), '') AS email,
            COALESCE(username, createdBy, '') AS username, createdBy, createdAt, updatedAt
     FROM hr_employees
     ${whereClause}
     ORDER BY fullName ASC, id ASC`,
    scopedEmployeeIds || []
  );
  res.json(rows);
});

app.get('/api/hr/employees/directory', async (req, res) => {
  const role = String(req.user?.role || '').trim();
  if (role === 'directeur_rh') {
    const rows = await all(
      `SELECT id, fullName, jobTitle,
              COALESCE(NULLIF(sexe, ''), 'Neant') AS sexe,
              COALESCE(NULLIF(typeContrat, ''), 'Neant') AS typeContrat,
              COALESCE(NULLIF(dateEmbauche, ''), SUBSTR(createdAt, 1, 10)) AS dateEmbauche,
              phoneNumber, address, maritalStatus, COALESCE(NULLIF(email, ''), '') AS email,
              COALESCE(username, createdBy, '') AS username, createdBy, createdAt, updatedAt
       FROM hr_employees
       ORDER BY fullName ASC, id ASC`
    );

    return res.json(rows);
  }

  const scopedEmployeeIds = await getHrScopedEmployeeIdsForUser(req.user);
  if (scopedEmployeeIds && !scopedEmployeeIds.length) {
    return res.json([]);
  }

  const whereClause = scopedEmployeeIds
    ? `WHERE id IN (${scopedEmployeeIds.map(() => '?').join(', ')})`
    : '';

  const rows = await all(
    `SELECT id, fullName, jobTitle,
            COALESCE(NULLIF(sexe, ''), 'Neant') AS sexe,
            COALESCE(NULLIF(typeContrat, ''), 'Neant') AS typeContrat,
            COALESCE(NULLIF(dateEmbauche, ''), SUBSTR(createdAt, 1, 10)) AS dateEmbauche,
            phoneNumber, address, maritalStatus, COALESCE(NULLIF(email, ''), '') AS email,
            COALESCE(username, createdBy, '') AS username, createdBy, createdAt, updatedAt
     FROM hr_employees
     ${whereClause}
     ORDER BY fullName ASC, id ASC`,
    scopedEmployeeIds || []
  );

  return res.json(rows);
});

app.get('/api/hr/dashboard-summary', async (req, res) => {
  const scopedEmployeeIds = await getHrScopedEmployeeIdsForUser(req.user);
  if (scopedEmployeeIds && !scopedEmployeeIds.length) {
    return res.json({ totalEmployees: 0, present: 0, absent: 0, onLeave: 0 });
  }

  const todayIso = new Date().toISOString().slice(0, 10);
  const whereEmployees = scopedEmployeeIds
    ? `WHERE id IN (${scopedEmployeeIds.map(() => '?').join(', ')})`
    : '';
  const whereAttendance = scopedEmployeeIds
    ? `AND a.employeeId IN (${scopedEmployeeIds.map(() => '?').join(', ')})`
    : '';
  const whereLeave = scopedEmployeeIds
    ? `AND lr.employeeId IN (${scopedEmployeeIds.map(() => '?').join(', ')})`
    : '';

  const totalEmployeesRow = await get(
    `SELECT COUNT(*) AS count
     FROM hr_employees
     ${whereEmployees}`,
    scopedEmployeeIds || []
  );

  const presentRow = await get(
    `SELECT COUNT(DISTINCT a.employeeId) AS count
     FROM hr_attendance a
     WHERE COALESCE(NULLIF(a.attendanceDate, ''), a.dayDate) = ?
       AND UPPER(COALESCE(a.statusCode, a.status, 'P')) IN ('P', 'PR', 'PRESENT')
       ${whereAttendance}`,
    [todayIso].concat(scopedEmployeeIds || [])
  );

  const absentRow = await get(
    `SELECT COUNT(DISTINCT a.employeeId) AS count
     FROM hr_attendance a
     WHERE COALESCE(NULLIF(a.attendanceDate, ''), a.dayDate) = ?
       AND UPPER(COALESCE(a.statusCode, a.status, 'A')) IN ('A', 'ABS', 'ABSENT')
       ${whereAttendance}`,
    [todayIso].concat(scopedEmployeeIds || [])
  );

  const onLeaveRow = await get(
    `SELECT COUNT(DISTINCT lr.employeeId) AS count
     FROM hr_leave_requests lr
     WHERE UPPER(COALESCE(lr.status, '')) = 'APPROUVEE'
       AND lr.startDate <= ?
       AND lr.endDate >= ?
       ${whereLeave}`,
    [todayIso, todayIso].concat(scopedEmployeeIds || [])
  );

  res.json({
    totalEmployees: Number(totalEmployeesRow?.count || 0),
    present: Number(presentRow?.count || 0),
    absent: Number(absentRow?.count || 0),
    onLeave: Number(onLeaveRow?.count || 0),
  });
});

const HR_MARITAL_STATUS_ALLOWED = new Set([
  '',
  'Célibataire',
  'Marié(e)',
  'Divorcé(e)',
  'Veuf(ve)',
  'Union libre',
]);

function normalizeHrMaritalStatus(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  const normalized = normalizeTextValue(raw);
  const aliasMap = {
    celibataire: 'Célibataire',
    'marie(e)': 'Marié(e)',
    mariee: 'Marié(e)',
    marie: 'Marié(e)',
    'divorce(e)': 'Divorcé(e)',
    divorcee: 'Divorcé(e)',
    divorce: 'Divorcé(e)',
    'veuf(ve)': 'Veuf(ve)',
    veuve: 'Veuf(ve)',
    veuf: 'Veuf(ve)',
    'union libre': 'Union libre',
  };

  return aliasMap[normalized] || raw;
}

function normalizeHrEmail(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw.toLowerCase();
}

function isValidHrEmail(emailValue) {
  if (!emailValue) return true;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(emailValue));
}

function normalizeHrSexe(value) {
  const raw = String(value || '').trim().toUpperCase();
  if (raw === 'F') return 'F';
  if (raw === 'M') return 'M';
  if (!raw || raw === 'NEANT' || raw === 'NÉANT') return 'Neant';
  return 'Neant';
}

function normalizeHrContractType(value) {
  const raw = String(value || '').trim();
  if (!raw) return 'Neant';

  const normalized = raw.toUpperCase();
  const allowedMap = {
    'NEANT': 'Neant',
    'NÉANT': 'Neant',
    'CDI': 'CDI',
    'CDD': 'CDD',
    'STAGE': 'Stage',
    'FREELANCE': 'Freelance',
    'INTERIM': 'Intérim',
    'INTÉRIM': 'Intérim',
    'CONSULTANT': 'Consultant',
    'AUTRE': 'Autre',
  };

  return allowedMap[normalized] || 'Neant';
}

function normalizeHrHireDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  const fr = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (fr) {
    return `${fr[3]}-${fr[2]}-${fr[1]}`;
  }

  return '';
}

app.post('/api/hr/employees', async (req, res) => {
  if (!roleCanBypassRestrictedProfile(req.user?.role)) {
    return res.status(403).json({ error: 'Creation employe reservee a admin/directeur_rh' });
  }

  const { fullName, jobTitle = '', sexe = '', typeContrat = '', dateEmbauche = '', phoneNumber = '', address = '', maritalStatus = '', email = '', username = '' } = req.body || {};
  const nameValue = String(fullName || '').trim();
  const rawHireDateValue = String(dateEmbauche || '').trim();
  const hireDateValue = normalizeHrHireDate(dateEmbauche);
  const normalizedEmail = normalizeHrEmail(email);
  if (!nameValue) {
    return res.status(400).json({ error: 'Nom employe obligatoire' });
  }
  if (rawHireDateValue && !hireDateValue) {
    return res.status(400).json({ error: "Date d'embauche invalide (format AAAA-MM-JJ)" });
  }
  if (!isValidHrEmail(normalizedEmail)) {
    return res.status(400).json({ error: 'Adresse email invalide' });
  }

  const now = new Date().toISOString();
  const nextId = await getNextTableId('hr_employees');
  await run(
    `INSERT INTO hr_employees (id, fullName, jobTitle, sexe, typeContrat, dateEmbauche, phoneNumber, address, maritalStatus, email, username, createdBy, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextId,
      nameValue,
      String(jobTitle || '').trim(),
      normalizeHrSexe(sexe),
      normalizeHrContractType(typeContrat),
      hireDateValue,
      String(phoneNumber || '').trim(),
      String(address || '').trim(),
      normalizeHrMaritalStatus(maritalStatus),
      normalizedEmail,
      String(username || '').trim(),
      String(req.user?.username || 'admin').trim() || 'admin',
      now,
      now,
    ]
  );

  const employee = await get('SELECT * FROM hr_employees WHERE id = ?', [nextId]);
  res.status(201).json(employee);
});

app.get('/api/hr/employees/:id/documents', async (req, res) => {
  const employeeId = Number(req.params.id || 0);
  if (!Number.isInteger(employeeId) || employeeId <= 0) {
    return res.status(400).json({ error: 'Employe invalide' });
  }

  const employee = await get('SELECT id FROM hr_employees WHERE id = ?', [employeeId]);
  if (!employee) {
    return res.status(404).json({ error: 'Employe introuvable' });
  }

  if (!roleCanBypassRestrictedProfile(req.user?.role)) {
    const profileEmployee = await getHrProfileEmployeeForUser(req.user);
    if (!profileEmployee?.id || Number(profileEmployee.id) !== employeeId) {
      return res.status(403).json({ error: 'Acces refuse a ce dossier employe' });
    }
  }

  const includeSignature = String(req.query.includeSignature || '').trim() === '1';
  const whereClauses = ['employeeId = ?'];
  const params = [employeeId];
  if (!includeSignature) {
    whereClauses.push("COALESCE(sourceModule, 'employee_dossier') <> 'signature_request'");
  }

  const rows = await all(
    `SELECT id, employeeId, title, fileName, relativePath, fileSize, mimeType, sourceModule, uploadedBy, createdAt, updatedAt
     FROM hr_employee_documents
     WHERE ${whereClauses.join(' AND ')}
     ORDER BY updatedAt DESC, id DESC`,
    params
  );

  res.json((rows || []).map(row => ({
    ...row,
    fileUrl: `/archives/${String(row.relativePath || '').replace(/\\/g, '/')}`,
  })));
});

app.post('/api/hr/employees/:id/documents', async (req, res) => {
  const employeeId = Number(req.params.id || 0);
  if (!Number.isInteger(employeeId) || employeeId <= 0) {
    return res.status(400).json({ error: 'Employe invalide' });
  }

  const employee = await get('SELECT id FROM hr_employees WHERE id = ?', [employeeId]);
  if (!employee) {
    return res.status(404).json({ error: 'Employe introuvable' });
  }

  if (!roleCanBypassRestrictedProfile(req.user?.role)) {
    const profileEmployee = await getHrProfileEmployeeForUser(req.user);
    if (!profileEmployee?.id || Number(profileEmployee.id) !== employeeId) {
      return res.status(403).json({ error: 'Acces refuse a ce dossier employe' });
    }
  }

  const { title = '', fileName = '', contentBase64 = '', mimeType = '', sourceModule = 'employee_dossier' } = req.body || {};
  if (!String(fileName || '').trim() || !String(contentBase64 || '').trim()) {
    return res.status(400).json({ error: 'fileName et contentBase64 sont obligatoires' });
  }

  let buffer;
  try {
    buffer = Buffer.from(String(contentBase64 || ''), 'base64');
  } catch (error) {
    return res.status(400).json({ error: 'Fichier invalide (base64)' });
  }

  if (!buffer || !buffer.length) {
    return res.status(400).json({ error: 'Le fichier est vide' });
  }

  const doc = await archiveHrEmployeeDocument({
    employeeId,
    title,
    fileName,
    fileBuffer: buffer,
    mimeType,
    sourceModule: String(sourceModule || '').trim() === 'signature_request' ? 'signature_request' : 'employee_dossier',
    uploadedBy: String(req.user?.username || 'admin').trim() || 'admin',
  });

  res.status(201).json(doc);
});

app.delete('/api/hr/employees/documents/:docId', async (req, res) => {
  const docId = Number(req.params.docId || 0);
  if (!Number.isInteger(docId) || docId <= 0) {
    return res.status(400).json({ error: 'Document invalide' });
  }

  const row = await get('SELECT * FROM hr_employee_documents WHERE id = ?', [docId]);
  if (!row) {
    return res.status(404).json({ error: 'Document introuvable' });
  }

  if (!roleCanBypassRestrictedProfile(req.user?.role)) {
    const profileEmployee = await getHrProfileEmployeeForUser(req.user);
    if (!profileEmployee?.id || Number(profileEmployee.id) !== Number(row.employeeId)) {
      return res.status(403).json({ error: 'Acces refuse a ce document' });
    }
  }

  await run('DELETE FROM hr_employee_documents WHERE id = ?', [docId]);
  const filePath = path.join(ARCHIVE_ROOT, String(row.relativePath || ''));
  if (String(row.relativePath || '').trim() && fs.existsSync(filePath)) {
    try { await fs.promises.unlink(filePath); } catch (e) {}
  }

  res.json({ message: 'Document supprime' });
});

app.get('/api/hr/employees/documents/:docId/download', async (req, res) => {
  const docId = Number(req.params.docId || 0);
  if (!Number.isInteger(docId) || docId <= 0) {
    return res.status(400).json({ error: 'Document invalide' });
  }

  const row = await get('SELECT * FROM hr_employee_documents WHERE id = ?', [docId]);
  if (!row) {
    return res.status(404).json({ error: 'Document introuvable' });
  }

  if (!roleCanBypassRestrictedProfile(req.user?.role)) {
    const profileEmployee = await getHrProfileEmployeeForUser(req.user);
    if (!profileEmployee?.id || Number(profileEmployee.id) !== Number(row.employeeId)) {
      return res.status(403).json({ error: 'Acces refuse a ce document' });
    }
  }

  const filePath = path.join(ARCHIVE_ROOT, String(row.relativePath || ''));
  if (!String(row.relativePath || '').trim() || !fs.existsSync(filePath)) {
    return res.status(404).json({ error: 'Fichier introuvable' });
  }

  const safeFileName = sanitizeFileName(String(row.fileName || 'document'));
  res.setHeader('Content-Disposition', `attachment; filename="${safeFileName}"`);
  res.setHeader('Content-Type', String(row.mimeType || 'application/octet-stream') || 'application/octet-stream');
  return res.sendFile(filePath);
});

app.patch('/api/hr/employees/:id', async (req, res) => {
  const id = Number(req.params.id);
  const { fullName, jobTitle = '', sexe = '', typeContrat = '', dateEmbauche = '', phoneNumber = '', address = '', maritalStatus = '', email = '', username = '' } = req.body || {};

  if (String(req.user?.role || '').trim() === 'employe_standard') {
    const profileEmployee = await getHrProfileEmployeeForUser(req.user);
    if (!profileEmployee?.id || Number(profileEmployee.id) !== Number(id)) {
      return res.status(403).json({ error: 'Vous pouvez modifier uniquement votre profil' });
    }

    const normalizedMarital = normalizeHrMaritalStatus(maritalStatus);
    if (!HR_MARITAL_STATUS_ALLOWED.has(normalizedMarital)) {
      return res.status(400).json({ error: 'Situation maritale invalide' });
    }

    const normalizedEmail = normalizeHrEmail(email);
    if (!isValidHrEmail(normalizedEmail)) {
      return res.status(400).json({ error: 'Adresse email invalide' });
    }

    const nameValue = String(fullName || profileEmployee.fullName || '').trim();
    if (!nameValue) {
      return res.status(400).json({ error: 'Nom employe obligatoire' });
    }

    const rawHireDateValue = String(dateEmbauche || '').trim();
    const hireDateValue = normalizeHrHireDate(dateEmbauche);
    if (rawHireDateValue && !hireDateValue) {
      return res.status(400).json({ error: "Date d'embauche invalide (format AAAA-MM-JJ)" });
    }

    const result = await run(
      'UPDATE hr_employees SET fullName = ?, jobTitle = ?, sexe = ?, typeContrat = ?, dateEmbauche = ?, phoneNumber = ?, address = ?, maritalStatus = ?, email = ?, username = ?, updatedAt = ? WHERE id = ?',
      [
        nameValue,
        String(jobTitle || '').trim(),
        normalizeHrSexe(sexe),
        normalizeHrContractType(typeContrat),
        hireDateValue,
        String(phoneNumber || '').trim(),
        String(address || '').trim(),
        normalizedMarital,
        normalizedEmail,
        String(username || profileEmployee.username || '').trim(),
        new Date().toISOString(),
        id,
      ]
    );

    if (!result.changes) {
      return res.status(404).json({ error: 'Employe introuvable' });
    }

    const employee = await get('SELECT * FROM hr_employees WHERE id = ?', [id]);
    return res.json(employee);
  }

  const nameValue = String(fullName || '').trim();
  const rawHireDateValue = String(dateEmbauche || '').trim();
  const hireDateValue = normalizeHrHireDate(dateEmbauche);
  const normalizedEmail = normalizeHrEmail(email);
  if (!id || !nameValue) {
    return res.status(400).json({ error: 'Employe invalide' });
  }
  if (rawHireDateValue && !hireDateValue) {
    return res.status(400).json({ error: "Date d'embauche invalide (format AAAA-MM-JJ)" });
  }
  if (!isValidHrEmail(normalizedEmail)) {
    return res.status(400).json({ error: 'Adresse email invalide' });
  }

  const result = await run(
    'UPDATE hr_employees SET fullName = ?, jobTitle = ?, sexe = ?, typeContrat = ?, dateEmbauche = ?, phoneNumber = ?, address = ?, maritalStatus = ?, email = ?, username = ?, updatedAt = ? WHERE id = ?',
    [
      nameValue,
      String(jobTitle || '').trim(),
      normalizeHrSexe(sexe),
      normalizeHrContractType(typeContrat),
      hireDateValue,
      String(phoneNumber || '').trim(),
      String(address || '').trim(),
      normalizeHrMaritalStatus(maritalStatus),
      normalizedEmail,
      String(username || '').trim(),
      new Date().toISOString(),
      id,
    ]
  );
  if (!result.changes) {
    return res.status(404).json({ error: 'Employe introuvable' });
  }

  const employee = await get('SELECT * FROM hr_employees WHERE id = ?', [id]);
  res.json(employee);
});

app.delete('/api/hr/employees/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'Employe invalide' });
  }

  const docs = await all('SELECT id, relativePath FROM hr_employee_documents WHERE employeeId = ?', [id]);

  const result = await run('DELETE FROM hr_employees WHERE id = ?', [id]);
  if (!result.changes) {
    return res.status(404).json({ error: 'Employe introuvable' });
  }

  for (const doc of (docs || [])) {
    const filePath = path.join(ARCHIVE_ROOT, String(doc.relativePath || ''));
    if (String(doc.relativePath || '').trim() && fs.existsSync(filePath)) {
      try { await fs.promises.unlink(filePath); } catch (e) {}
    }
  }

  res.json({ message: 'Employe supprime' });
});

app.get('/api/hr/attendance', async (req, res) => {
  const employeeId = Number(req.query.employeeId || 0);
  const date = String(req.query.date || '').trim();
  const month = String(req.query.month || '').trim();
  const scopedEmployeeIds = await getHrScopedEmployeeIdsForUser(req.user);

  if (scopedEmployeeIds && !scopedEmployeeIds.length) {
    return res.json([]);
  }

  const conditions = [];
  const params = [];
  if (scopedEmployeeIds) {
    conditions.push(`a.employeeId IN (${scopedEmployeeIds.map(() => '?').join(', ')})`);
    params.push(...scopedEmployeeIds);
  }
  if (employeeId > 0) {
    conditions.push('a.employeeId = ?');
    params.push(employeeId);
  }
  if (isValidIsoDate(date)) {
    conditions.push("COALESCE(NULLIF(a.attendanceDate, ''), a.dayDate) = ?");
    params.push(date);
  } else if (/^\d{4}-\d{2}$/.test(month)) {
    const range = formatMonthRange(month);
    if (range) {
      conditions.push("COALESCE(NULLIF(a.attendanceDate, ''), a.dayDate) >= ?");
      conditions.push("COALESCE(NULLIF(a.attendanceDate, ''), a.dayDate) <= ?");
      params.push(range.startDate, range.endDate);
    }
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const rows = await all(
    `SELECT a.*, e.fullName, e.jobTitle, e.phoneNumber,
      COALESCE(NULLIF(a.attendanceDate, ''), a.dayDate) AS effectiveDate
     FROM hr_attendance a
     JOIN hr_employees e ON e.id = a.employeeId
     ${whereClause}
     ORDER BY effectiveDate DESC, e.fullName ASC`,
    params
  );
  res.json(rows);
});

app.post('/api/hr/attendance', async (req, res) => {
  const {
    employeeId,
    attendanceDate,
    dayDate,
    statusCode = '',
    location = 'bureau',
    note = '',
    punchType = 'auto',
  } = req.body || {};

  const numericEmployeeId = Number(employeeId || 0);
  const nowParts = getAbidjanNowParts(new Date());
  const providedDate = String(attendanceDate || dayDate || '').trim();
  const effectiveDate = isValidIsoDate(providedDate) ? providedDate : nowParts.date;

  if (!numericEmployeeId || !isValidIsoDate(effectiveDate)) {
    return res.status(400).json({ error: 'Employe et date obligatoires' });
  }

  const employee = await get('SELECT id FROM hr_employees WHERE id = ?', [numericEmployeeId]);
  if (!employee) {
    return res.status(404).json({ error: 'Employe introuvable' });
  }

  const normalizedPunchType = normalizeAttendancePunchType(punchType);
  if (isHrAttendanceDateLocked() && effectiveDate !== nowParts.date) {
    return res.status(400).json({ error: `Le pointage doit être fait à la date du jour (heure CI: ${nowParts.date})` });
  }
  const now = new Date().toISOString();
  const existing = await get(
    "SELECT id, checkInTime, checkOutTime, statusCode, status, note FROM hr_attendance WHERE employeeId = ? AND COALESCE(NULLIF(attendanceDate, ''), dayDate) = ?",
    [numericEmployeeId, effectiveDate]
  );

  if (existing) {
    const nextCheckInTime = String(existing.checkInTime || '').trim();
    const nextCheckOutTime = String(existing.checkOutTime || '').trim();
    const computedCheckIn =
      normalizedPunchType === 'checkin'
        ? nowParts.time
        : (normalizedPunchType === 'auto' && !nextCheckInTime)
          ? nowParts.time
          : nextCheckInTime;
    const computedCheckOut =
      normalizedPunchType === 'checkout'
        ? nowParts.time
        : (normalizedPunchType === 'auto' && nextCheckInTime)
          ? nowParts.time
          : nextCheckOutTime;
    const codeValue = inferAttendanceCode(computedCheckIn, statusCode || existing.statusCode || existing.status);

    await run(
      `UPDATE hr_attendance
       SET attendanceDate = ?, dayDate = ?, checkInTime = ?, checkOutTime = ?,
           statusCode = ?, status = ?, location = ?, note = ?, updatedAt = ?
       WHERE id = ?`,
      [
        effectiveDate,
        effectiveDate,
        computedCheckIn,
        computedCheckOut,
        codeValue,
        codeValue,
        String(location || existing.location || 'bureau').trim() || 'bureau',
        String(note || existing.note || '').trim(),
        now,
        Number(existing.id),
      ]
    );
    const row = await get('SELECT * FROM hr_attendance WHERE id = ?', [Number(existing.id)]);
    try {
      await generateOrUpdateHrAttendanceSheet(numericEmployeeId, String(effectiveDate).slice(0, 7));
    } catch (sheetError) {
      console.error('Error generating HR attendance sheet:', sheetError);
    }
    return res.json(row);
  }

  const nextId = await getNextTableId('hr_attendance');
  const computedCheckIn = normalizedPunchType === 'checkout' ? '' : nowParts.time;
  const computedCheckOut = normalizedPunchType === 'checkout' ? nowParts.time : '';
  const codeValue = inferAttendanceCode(computedCheckIn, statusCode);
  await run(
    `INSERT INTO hr_attendance
      (id, employeeId, attendanceDate, dayDate, checkInTime, checkOutTime, statusCode, status, note, createdBy, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextId,
      numericEmployeeId,
      effectiveDate,
      effectiveDate,
      computedCheckIn,
      computedCheckOut,
      codeValue,
      codeValue,
      String(note || '').trim(),
      String(req.user?.username || 'admin').trim() || 'admin',
      now,
      now,
    ]
  );

  const row = await get('SELECT * FROM hr_attendance WHERE id = ?', [nextId]);
  try {
    await generateOrUpdateHrAttendanceSheet(numericEmployeeId, String(effectiveDate).slice(0, 7));
  } catch (sheetError) {
    console.error('Error generating HR attendance sheet:', sheetError);
  }
  res.status(201).json(row);
});

app.patch('/api/hr/attendance/:id', async (req, res) => {
  const id = Number(req.params.id);
  const {
    checkInTime = '',
    checkOutTime = '',
    statusCode = '',
    note = '',
    punchType = '',
  } = req.body || {};

  if (!id || !isValidTimeValue(checkInTime) || !isValidTimeValue(checkOutTime)) {
    return res.status(400).json({ error: 'Pointage invalide' });
  }

  const current = await get('SELECT * FROM hr_attendance WHERE id = ?', [id]);
  if (!current) {
    return res.status(404).json({ error: 'Pointage introuvable' });
  }

  const nowParts = getAbidjanNowParts(new Date());
  const normalizedPunchType = normalizeAttendancePunchType(punchType);
  const currentEffectiveDate = String(current.attendanceDate || current.dayDate || '').slice(0, 10);
  if (String(punchType || '').trim() && isHrAttendanceDateLocked() && currentEffectiveDate && currentEffectiveDate !== nowParts.date) {
    return res.status(400).json({ error: `Le pointage doit être fait à la date du jour (heure CI: ${nowParts.date})` });
  }
  let nextCheckIn = String(checkInTime || '').trim() || String(current.checkInTime || '').trim();
  let nextCheckOut = String(checkOutTime || '').trim() || String(current.checkOutTime || '').trim();
  if (normalizedPunchType === 'checkin') {
    nextCheckIn = nowParts.time;
  } else if (normalizedPunchType === 'checkout') {
    nextCheckOut = nowParts.time;
  } else if (String(punchType || '').trim() && normalizedPunchType === 'auto') {
    if (!nextCheckIn) {
      nextCheckIn = nowParts.time;
    } else {
      nextCheckOut = nowParts.time;
    }
  }

  const codeValue = inferAttendanceCode(nextCheckIn, statusCode || current.statusCode || current.status);
  await run(
    `UPDATE hr_attendance
     SET checkInTime = ?, checkOutTime = ?, statusCode = ?, status = ?, note = ?, updatedAt = ?
     WHERE id = ?`,
    [
      nextCheckIn,
      nextCheckOut,
      codeValue,
      codeValue,
      String(note || current.note || '').trim(),
      new Date().toISOString(),
      id,
    ]
  );

  const row = await get('SELECT * FROM hr_attendance WHERE id = ?', [id]);
  try {
    const rowDate = String(row?.attendanceDate || row?.dayDate || '').slice(0, 7);
    if (rowDate) {
      await generateOrUpdateHrAttendanceSheet(Number(row.employeeId || 0), rowDate);
    }
  } catch (sheetError) {
    console.error('Error generating HR attendance sheet:', sheetError);
  }
  res.json(row);
});

app.get('/api/hr/leave-requests', async (req, res) => {
  const employeeId = Number(req.query.employeeId || 0);
  const scopedEmployeeIds = await getHrScopedEmployeeIdsForUser(req.user);

  if (scopedEmployeeIds && !scopedEmployeeIds.length) {
    return res.json([]);
  }

  const conditions = [];
  const params = [];
  if (scopedEmployeeIds) {
    conditions.push(`lr.employeeId IN (${scopedEmployeeIds.map(() => '?').join(', ')})`);
    params.push(...scopedEmployeeIds);
  }
  if (employeeId > 0) {
    conditions.push('lr.employeeId = ?');
    params.push(employeeId);
  }
  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const rows = await all(
    `SELECT lr.*, e.fullName, e.jobTitle, e.phoneNumber
     FROM hr_leave_requests lr
     JOIN hr_employees e ON e.id = lr.employeeId
     ${whereClause}
     ORDER BY lr.createdAt DESC, lr.id DESC`,
    params
  );
  res.json(rows);
});

app.post('/api/hr/leave-requests', async (req, res) => {
  const { employeeId, leaveType, startDate, endDate, reason = '' } = req.body || {};
  const numericEmployeeId = Number(employeeId || 0);
  const leaveTypeValue = String(leaveType || '').trim();
  const startDateValue = String(startDate || '').trim();
  const endDateValue = String(endDate || '').trim();

  if (!numericEmployeeId || !leaveTypeValue || !isValidIsoDate(startDateValue) || !isValidIsoDate(endDateValue)) {
    return res.status(400).json({ error: 'Champs demande de conge invalides' });
  }
  if (endDateValue < startDateValue) {
    return res.status(400).json({ error: 'Date fin inferieure a date debut' });
  }

  const employee = await get('SELECT id FROM hr_employees WHERE id = ?', [numericEmployeeId]);
  if (!employee) {
    return res.status(404).json({ error: 'Employe introuvable' });
  }

  const nextId = await getNextTableId('hr_leave_requests');
  const now = new Date().toISOString();
  await run(
    `INSERT INTO hr_leave_requests
      (id, employeeId, leaveType, startDate, endDate, reason, status, decisionNote, createdBy, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextId,
      numericEmployeeId,
      leaveTypeValue,
      startDateValue,
      endDateValue,
      String(reason || '').trim(),
      'EN_ATTENTE',
      '',
      String(req.user?.username || 'admin').trim() || 'admin',
      now,
      now,
    ]
  );

  const row = await get('SELECT * FROM hr_leave_requests WHERE id = ?', [nextId]);
  res.status(201).json(row);
});

app.patch('/api/hr/leave-requests/:id/status', async (req, res) => {
  const id = Number(req.params.id);
  const status = String(req.body?.status || '').trim().toUpperCase();
  const decisionNote = String(req.body?.decisionNote || '').trim();
  if (!id || !['EN_ATTENTE', 'APPROUVEE', 'REJETEE'].includes(status)) {
    return res.status(400).json({ error: 'Statut conge invalide' });
  }

  const result = await run(
    'UPDATE hr_leave_requests SET status = ?, decisionNote = ?, decidedBy = ?, decidedAt = ?, updatedAt = ? WHERE id = ?',
    [
      status,
      decisionNote,
      String(req.user?.username || 'admin').trim() || 'admin',
      new Date().toISOString(),
      new Date().toISOString(),
      id,
    ]
  );
  if (!result.changes) {
    return res.status(404).json({ error: 'Demande de conge introuvable' });
  }

  const row = await get('SELECT * FROM hr_leave_requests WHERE id = ?', [id]);
  if (status === 'APPROUVEE') {
    const employeeRow = await get('SELECT id, fullName, jobTitle FROM hr_employees WHERE id = ?', [Number(row?.employeeId || 0)]);
    if (employeeRow) {
      await archiveOrUpdateHrLeaveDecisionDocument(row, employeeRow);
    }
  } else {
    await deleteHrLeaveDecisionDocument(id);
  }
  res.json(row);
});

app.get('/api/hr/contracts', async (req, res) => {
  const employeeId = Number(req.query.employeeId || 0);
  const scopedEmployeeIds = await getHrScopedEmployeeIdsForUser(req.user);
  if (scopedEmployeeIds && !scopedEmployeeIds.length) {
    return res.json([]);
  }

  const conditions = [];
  const params = [];
  if (scopedEmployeeIds) {
    conditions.push(`hc.employeeId IN (${scopedEmployeeIds.map(() => '?').join(', ')})`);
    params.push(...scopedEmployeeIds);
  }
  if (employeeId > 0) {
    conditions.push('hc.employeeId = ?');
    params.push(employeeId);
  }
  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const rows = await all(
    `SELECT hc.*, e.fullName, e.jobTitle, e.phoneNumber, e.username
     FROM hr_contracts hc
     JOIN hr_employees e ON e.id = hc.employeeId
     ${whereClause}
     ORDER BY hc.contractEndDate ASC, hc.contractStartDate ASC, hc.id ASC`,
    params
  );
  res.json(rows);
});

app.post('/api/hr/contracts', async (req, res) => {
  const {
    employeeId,
    contractStartDate,
    contractEndDate,
    reminderDate = '',
    reminderNote = '',
    status = 'ACTIF',
  } = req.body || {};
  const numericEmployeeId = Number(employeeId || 0);
  const startDateValue = String(contractStartDate || '').trim();
  const endDateValue = String(contractEndDate || '').trim();
  const reminderDateValue = String(reminderDate || '').trim();
  const statusValue = String(status || 'ACTIF').trim().toUpperCase();

  if (!numericEmployeeId || !isValidIsoDate(startDateValue) || !isValidIsoDate(endDateValue)) {
    return res.status(400).json({ error: 'Champs contrat invalides' });
  }
  if (endDateValue < startDateValue) {
    return res.status(400).json({ error: 'Date de fin de contrat inferieure a la date de debut' });
  }
  if (reminderDateValue && !isValidIsoDate(reminderDateValue)) {
    return res.status(400).json({ error: 'Date de rappel invalide' });
  }

  const employee = await get('SELECT id FROM hr_employees WHERE id = ?', [numericEmployeeId]);
  if (!employee) {
    return res.status(404).json({ error: 'Employe introuvable' });
  }

  const nextId = await getNextTableId('hr_contracts');
  const now = new Date().toISOString();
  await run(
    `INSERT INTO hr_contracts
      (id, employeeId, contractStartDate, contractEndDate, reminderDate, reminderNote, status, createdBy, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)` ,
    [
      nextId,
      numericEmployeeId,
      startDateValue,
      endDateValue,
      reminderDateValue,
      String(reminderNote || '').trim(),
      statusValue || 'ACTIF',
      String(req.user?.username || 'admin').trim() || 'admin',
      now,
      now,
    ]
  );

  const row = await get('SELECT * FROM hr_contracts WHERE id = ?', [nextId]);
  res.status(201).json(row);
});

app.patch('/api/hr/contracts/:id', async (req, res) => {
  const id = Number(req.params.id);
  const current = await get('SELECT * FROM hr_contracts WHERE id = ?', [id]);
  if (!current) {
    return res.status(404).json({ error: 'Contrat introuvable' });
  }

  const contractStartDate = String(req.body?.contractStartDate || current.contractStartDate || '').trim();
  const contractEndDate = String(req.body?.contractEndDate || current.contractEndDate || '').trim();
  const reminderDate = String(req.body?.reminderDate || current.reminderDate || '').trim();
  const reminderNote = String(req.body?.reminderNote || current.reminderNote || '').trim();
  const status = String(req.body?.status || current.status || 'ACTIF').trim().toUpperCase();

  if (!isValidIsoDate(contractStartDate) || !isValidIsoDate(contractEndDate)) {
    return res.status(400).json({ error: 'Dates de contrat invalides' });
  }
  if (contractEndDate < contractStartDate) {
    return res.status(400).json({ error: 'Date de fin de contrat inferieure a la date de debut' });
  }
  if (reminderDate && !isValidIsoDate(reminderDate)) {
    return res.status(400).json({ error: 'Date de rappel invalide' });
  }

  await run(
    `UPDATE hr_contracts
     SET contractStartDate = ?, contractEndDate = ?, reminderDate = ?, reminderNote = ?, status = ?, updatedAt = ?
     WHERE id = ?`,
    [contractStartDate, contractEndDate, reminderDate, reminderNote, status, new Date().toISOString(), id]
  );
  const row = await get('SELECT * FROM hr_contracts WHERE id = ?', [id]);
  res.json(row);
});

app.delete('/api/hr/contracts/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM hr_contracts WHERE id = ?', [id]);
  if (!result.changes) {
    return res.status(404).json({ error: 'Contrat introuvable' });
  }
  res.json({ ok: true });
});

app.get('/api/hr/leave-calendar', async (req, res) => {
  const monthRaw = String(req.query.month || '').trim() || new Date().toISOString().slice(0, 7);
  const range = formatMonthRange(monthRaw);
  if (!range) {
    return res.status(400).json({ error: 'Format mois invalide. Utilise YYYY-MM' });
  }

  const scopedEmployeeIds = await getHrScopedEmployeeIdsForUser(req.user);
  if (scopedEmployeeIds && !scopedEmployeeIds.length) {
    return res.json({
      month: monthRaw,
      daysInMonth: range.daysInMonth,
      legend: {
        P: 'Presence',
        R: 'Retard',
        MS: 'Conge maladie',
        A: 'Absence',
        CA: 'Conge annuel',
        CM: 'Conge maternite',
        CP: 'Conge paternite',
      },
      employees: [],
    });
  }

  const employeeIdFilter = Number(req.query.employeeId || 0);
  const employeeConditions = [];
  const employeeParams = [];
  if (scopedEmployeeIds) {
    employeeConditions.push(`id IN (${scopedEmployeeIds.map(() => '?').join(', ')})`);
    employeeParams.push(...scopedEmployeeIds);
  }
  if (employeeIdFilter > 0) {
    employeeConditions.push('id = ?');
    employeeParams.push(employeeIdFilter);
  }
  const employeeWhere = employeeConditions.length ? `WHERE ${employeeConditions.join(' AND ')}` : '';
  const employees = await all(
    `SELECT id, fullName, jobTitle, phoneNumber
     FROM hr_employees
     ${employeeWhere}
     ORDER BY fullName ASC, id ASC`,
    employeeParams
  );

  if (!employees.length) {
    return res.json({
      month: monthRaw,
      daysInMonth: range.daysInMonth,
      legend: {
        P: 'Presence',
        R: 'Retard',
        MS: 'Conge maladie',
        A: 'Absence',
        CA: 'Conge annuel',
        CM: 'Conge maternite',
        CP: 'Conge paternite',
      },
      employees: [],
    });
  }

  const employeeIds = employees.map(row => Number(row.id)).filter(id => Number.isInteger(id) && id > 0);
  const placeholders = employeeIds.map(() => '?').join(', ');

  const attendanceRows = await all(
    `SELECT employeeId, COALESCE(NULLIF(attendanceDate, ''), dayDate) AS effectiveDate,
            COALESCE(NULLIF(statusCode, ''), NULLIF(status, ''), 'P') AS code
     FROM hr_attendance
     WHERE employeeId IN (${placeholders})
       AND COALESCE(NULLIF(attendanceDate, ''), dayDate) >= ?
       AND COALESCE(NULLIF(attendanceDate, ''), dayDate) <= ?`,
    [...employeeIds, range.startDate, range.endDate]
  );

  const leaveRows = await all(
    `SELECT employeeId, leaveType, startDate, endDate
     FROM hr_leave_requests
     WHERE employeeId IN (${placeholders})
       AND status = 'APPROUVEE'
       AND startDate <= ?
       AND endDate >= ?`,
    [...employeeIds, range.endDate, range.startDate]
  );

  const codeMapByEmployee = new Map();
  for (const employee of employees) {
    codeMapByEmployee.set(Number(employee.id), {});
  }

  for (const row of attendanceRows) {
    const employeeId = Number(row.employeeId || 0);
    const day = Number(String(row.effectiveDate || '').slice(8, 10));
    if (!codeMapByEmployee.has(employeeId) || !Number.isInteger(day) || day < 1 || day > range.daysInMonth) continue;
    codeMapByEmployee.get(employeeId)[String(day)] = normalizeHrCode(row.code);
  }

  for (const leave of leaveRows) {
    const employeeId = Number(leave.employeeId || 0);
    if (!codeMapByEmployee.has(employeeId)) continue;
    const effectiveStart = leave.startDate < range.startDate ? range.startDate : String(leave.startDate || range.startDate).slice(0, 10);
    const effectiveEnd = leave.endDate > range.endDate ? range.endDate : String(leave.endDate || range.endDate).slice(0, 10);
    const leaveCode = normalizeLeaveTypeCode(leave.leaveType);
    const dayEntries = listDatesInRange(effectiveStart, effectiveEnd);
    dayEntries.forEach(dateValue => {
      const day = Number(String(dateValue || '').slice(8, 10));
      if (Number.isInteger(day) && day >= 1 && day <= range.daysInMonth) {
        codeMapByEmployee.get(employeeId)[String(day)] = leaveCode;
      }
    });
  }

  const payloadEmployees = employees.map(employee => ({
    id: Number(employee.id),
    fullName: employee.fullName,
    jobTitle: employee.jobTitle,
    phoneNumber: employee.phoneNumber,
    codes: codeMapByEmployee.get(Number(employee.id)) || {},
  }));

  res.json({
    month: monthRaw,
    daysInMonth: range.daysInMonth,
    legend: {
      P: 'Presence',
      R: 'Retard',
      MS: 'Conge maladie',
      A: 'Absence',
      CA: 'Conge annuel',
      CM: 'Conge maternite',
      CP: 'Conge paternite',
    },
    employees: payloadEmployees,
  });
});

// ===== Document Signature Routes =====

app.post('/api/hr/signature-requests', async (req, res) => {
  const { documentId, employeeId } = req.body || {};
  const numericDocumentId = Number(documentId || 0);
  const numericEmployeeId = Number(employeeId || 0);

  if (!numericDocumentId || !numericEmployeeId) {
    return res.status(400).json({ error: 'documentId et employeeId requis' });
  }

  const doc = await get('SELECT * FROM hr_employee_documents WHERE id = ?', [numericDocumentId]);
  if (!doc) {
    return res.status(404).json({ error: 'Document non trouve' });
  }

  const isPdf = /pdf$/i.test(String(doc.mimeType || '')) || /\.pdf$/i.test(String(doc.fileName || ''));
  if (!isPdf) {
    return res.status(400).json({ error: 'Seuls les documents PDF peuvent etre soumis a la signature' });
  }

  const employee = await get('SELECT id FROM hr_employees WHERE id = ?', [numericEmployeeId]);
  if (!employee) {
    return res.status(404).json({ error: 'Employe non trouve' });
  }

  const nextId = await getNextTableId('hr_document_signatures');
  const now = new Date().toISOString();
  await run(
    `INSERT INTO hr_document_signatures 
     (id, documentId, employeeId, status, createdBy, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [nextId, numericDocumentId, numericEmployeeId, 'pending', String(req.user?.username || 'admin').trim() || 'admin', now, now]
  );

  const row = await get('SELECT * FROM hr_document_signatures WHERE id = ?', [nextId]);
  res.status(201).json(row);
});

app.get('/api/hr/signature-requests', async (req, res) => {
  const role = String(req.user?.role || '').trim();
  const isPrivileged = role === 'admin' || role === 'directeur_rh';

  const profileEmployee = isPrivileged ? null : await getHrProfileEmployeeForUser(req.user);
  if (!isPrivileged && !profileEmployee?.id) {
    return res.json([]);
  }

  const whereClause = isPrivileged ? '' : 'WHERE ds.employeeId = ?';
  const params = isPrivileged ? [] : [Number(profileEmployee.id)];

  const rows = await all(
    `SELECT ds.*, hd.fileName, hd.mimeType, hd.relativePath AS documentRelativePath, hd.title AS documentTitle, he.fullName as employeeFullName, he.username AS employeeUsername
     FROM hr_document_signatures ds
     JOIN hr_employee_documents hd ON ds.documentId = hd.id
     JOIN hr_employees he ON ds.employeeId = he.id
     ${whereClause}
     ORDER BY ds.createdAt DESC`
    ,
    params
  );
  res.json(rows || []);
});

app.get('/api/hr/employee-profile/pending-signatures', async (req, res) => {
  if (!String(req.user?.username || '').trim()) {
    return res.status(403).json({ error: 'Acces refuse' });
  }

  const employee = await getHrProfileEmployeeForUser(req.user);
  if (!employee) {
    return res.json([]);
  }

  const rows = await all(
    `SELECT ds.*, ds.id AS requestId, ds.createdBy AS submitterName, hd.fileName, hd.mimeType, hd.relativePath AS documentRelativePath, hd.title AS documentTitle
     FROM hr_document_signatures ds
     JOIN hr_employee_documents hd ON ds.documentId = hd.id
     WHERE ds.employeeId = ? AND ds.status = 'pending'
     ORDER BY ds.createdAt DESC`,
    [Number(employee.id)]
  );
  return res.json(rows || []);
});

app.get('/api/hr/employees/:employeeId/documents', async (req, res) => {
  const employeeId = Number(req.params.employeeId);
  try {
    const rows = await all(
      `SELECT id, title, fileName, fileSize, relativePath, description, createdAt
       FROM hr_employee_documents
       WHERE employeeId = ?
       ORDER BY createdAt DESC`,
      [employeeId]
    );
    res.json(rows || []);
  } catch (err) {
    console.error('Error fetching employee documents:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/hr/signature-requests/:requestId/sign', async (req, res) => {
  const requestId = Number(req.params.requestId);
  const { signatureName, signatureRole = '' } = req.body || {};

  if (!requestId) {
    return res.status(400).json({ error: 'requestId requis' });
  }

  const signatureRequest = await get(
    `SELECT ds.*, he.username AS employeeUsername
     FROM hr_document_signatures ds
     JOIN hr_employees he ON ds.employeeId = he.id
     WHERE ds.id = ?`,
    [requestId]
  );

  if (!signatureRequest) {
    return res.status(404).json({ error: 'Demande de signature non trouvee' });
  }

  const signerUsername = String(req.user?.username || '').trim();
  const requestEmployeeUsername = String(signatureRequest.employeeUsername || '').trim();
  if (!signerUsername || signerUsername !== requestEmployeeUsername) {
    return res.status(403).json({ error: 'Seul l employe peut signer' });
  }

  const now = new Date().toISOString();
  const signatureData = String(signatureName || '').trim();
  const documentRow = await get('SELECT * FROM hr_employee_documents WHERE id = ?', [Number(signatureRequest.documentId || 0)]);
  if (!documentRow) {
    return res.status(404).json({ error: 'Document source introuvable' });
  }

  let signedPdf = null;
  try {
    signedPdf = await archiveSignedHrDocument({
      documentRow,
      signatureRequest,
      signatureName: signatureData,
      signatureRole: String(signatureRole || '').trim(),
      signedAt: now,
    });
  } catch (err) {
    return res.status(400).json({ error: err.message });
  }
  
  await run(
    `UPDATE hr_document_signatures 
     SET status = ?, signatureData = ?, signedBy = ?, signedAt = ?, signedPdfFileName = ?, signedPdfRelativePath = ?, updatedAt = ?
     WHERE id = ?`,
    ['signed', signatureData, req.user.username, now, signedPdf.fileName, signedPdf.relativePath, now, requestId]
  );

  const row = await get(
    `SELECT ds.*, hd.fileName AS documentFileName, hd.mimeType AS documentMimeType, hd.relativePath AS documentRelativePath, hd.title AS documentTitle, he.fullName AS employeeFullName
     FROM hr_document_signatures ds
     JOIN hr_employee_documents hd ON hd.id = ds.documentId
     JOIN hr_employees he ON he.id = ds.employeeId
     WHERE ds.id = ?`,
    [requestId]
  );
  res.json({ ...row, signedDocumentUrl: signedPdf.fileUrl });
});

app.delete('/api/hr/signature-requests/:id', async (req, res) => {
  const id = Number(req.params.id || 0);
  if (!id) {
    return res.status(400).json({ error: 'id requis' });
  }

  const row = await get('SELECT id, signedPdfRelativePath FROM hr_document_signatures WHERE id = ?', [id]);
  if (!row) {
    return res.status(404).json({ error: 'Demande de signature introuvable' });
  }

  const relativePath = String(row.signedPdfRelativePath || '').trim();
  if (relativePath) {
    const signedAbsolutePath = path.join(ARCHIVE_ROOT, relativePath);
    if (fs.existsSync(signedAbsolutePath)) {
      try {
        await fs.promises.unlink(signedAbsolutePath);
      } catch (unlinkError) {
        console.warn('Unable to delete signed HR PDF:', unlinkError.message);
      }
    }
  }

  await run('DELETE FROM hr_document_signatures WHERE id = ?', [id]);
  return res.json({ message: 'Demande de signature supprimee' });
});

app.get('/api/hr/signature-requests/:requestId/download', async (req, res) => {
  const requestId = Number(req.params.requestId || 0);
  if (!requestId) {
    return res.status(400).json({ error: 'requestId requis' });
  }

  const row = await get(
    `SELECT ds.*, hd.fileName AS documentFileName, hd.mimeType AS documentMimeType, hd.relativePath AS documentRelativePath, hd.title AS documentTitle, he.fullName AS employeeFullName, he.username AS employeeUsername
     FROM hr_document_signatures ds
     JOIN hr_employee_documents hd ON hd.id = ds.documentId
     JOIN hr_employees he ON he.id = ds.employeeId
     WHERE ds.id = ?`,
    [requestId]
  );

  if (!row) {
    return res.status(404).json({ error: 'Demande de signature non trouvee' });
  }
  if (String(row.status || '').trim() !== 'signed') {
    return res.status(409).json({ error: 'Le document n est pas encore signe' });
  }

  let signedPath = String(row.signedPdfRelativePath || '').trim() ? path.join(ARCHIVE_ROOT, String(row.signedPdfRelativePath)) : '';
  if (!signedPath || !fs.existsSync(signedPath)) {
    const documentRow = await get('SELECT * FROM hr_employee_documents WHERE id = ?', [Number(row.documentId || 0)]);
    if (!documentRow) {
      return res.status(404).json({ error: 'Document source introuvable' });
    }
    const rebuilt = await archiveSignedHrDocument({
      documentRow,
      signatureRequest: row,
      signatureName: String(row.signatureData || row.signedBy || row.employeeFullName || '').trim(),
      signatureRole: 'Employe',
      signedAt: String(row.signedAt || ''),
    });
    signedPath = path.join(ARCHIVE_ROOT, String(rebuilt.relativePath || ''));
    await run('UPDATE hr_document_signatures SET signedPdfFileName = ?, signedPdfRelativePath = ?, updatedAt = ? WHERE id = ?', [rebuilt.fileName, rebuilt.relativePath, new Date().toISOString(), requestId]);
  }

  const downloadName = sanitizeFileName(String(row.signedPdfFileName || `document-signe-${requestId}.pdf`));
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${downloadName}"`);
  return res.sendFile(signedPath);
});

app.get('/api/hr/document-signatures/:docId', async (req, res) => {
  const docId = Number(req.params.docId);
  if (!docId) {
    return res.status(400).json({ error: 'docId requis' });
  }

  const rows = await all(
    `SELECT ds.*, he.fullName as employeeFullName
     FROM hr_document_signatures ds
     JOIN hr_employees he ON ds.employeeId = he.id
     WHERE ds.documentId = ?
     ORDER BY ds.createdAt DESC`,
    [docId]
  );
  res.json(rows || []);
});

// Get documents for an employee
app.get('/api/hr/employees/:employeeId/documents', async (req, res) => {
  try {
    const employeeId = Number(req.params.employeeId || 0);
    if (!employeeId) return res.status(400).json({ error: 'Employee ID required' });

    // Role-based access check
    if (!isAdmin(req) && !isHrDirector(req)) {
      return res.status(403).json({ error: 'Access denied' });
    }

    const docs = await all(
      'SELECT * FROM hr_employee_documents WHERE employeeId = ? ORDER BY createdAt DESC',
      [employeeId]
    );
    res.json(docs || []);
  } catch (err) {
    console.error('Error fetching employee documents:', err);
    res.status(500).json({ error: 'Failed to fetch documents' });
  }
});

app.post('/api/project-assignments', async (req, res) => {
  async function ensureAssignmentEmployee({ assigneeName, role, phoneNumber = '', createdBy = 'admin', projectName = '', siteNumber = '' }) {
    const fullName = String(assigneeName || '').trim();
    const jobTitle = String(role || '').trim();
    if (!fullName || !jobTitle) return null;

    const roleNormalized = normalizeTextValue(jobTitle);
    const isChefChantier = roleNormalized.includes('chef') && roleNormalized.includes('chantier');
    const addressLabel = isChefChantier
      ? `Projet: ${String(projectName || '').trim() || '-'} | Site: ${String(siteNumber || '').trim() || '-'}`
      : '';

    const normalizedPhone = String(phoneNumber || '').trim();
    const existingEmployee = await get(
      `SELECT *
       FROM hr_employees
       WHERE LOWER(TRIM(fullName)) = LOWER(TRIM(?))
       ORDER BY updatedAt DESC, id DESC
       LIMIT 1`,
      [fullName]
    );

    if (existingEmployee && Number(existingEmployee.id) > 0) {
      const preservedAddress = isChefChantier
        ? addressLabel
        : String(existingEmployee.address || '').trim();
      await run(
        'UPDATE hr_employees SET fullName = ?, jobTitle = ?, phoneNumber = ?, address = ?, updatedAt = ? WHERE id = ?',
        [
          fullName,
          jobTitle,
          normalizedPhone,
          preservedAddress,
          new Date().toISOString(),
          Number(existingEmployee.id),
        ]
      );
      return Number(existingEmployee.id);
    }

    const now = new Date().toISOString();
    const nextEmployeeId = await getNextTableId('hr_employees');
    await run(
      `INSERT INTO hr_employees (id, fullName, jobTitle, phoneNumber, address, maritalStatus, createdBy, createdAt, updatedAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        nextEmployeeId,
        fullName,
        jobTitle,
        normalizedPhone,
        addressLabel,
        '',
        String(createdBy || 'admin').trim() || 'admin',
        now,
        now,
      ]
    );

    return nextEmployeeId;
  }

  const { projetId, projectId, userId, assigneeName, role, phoneNumber = '' } = req.body;
  const assignmentProjectId = Number(projectId || projetId);
  if (!assignmentProjectId || !role || !assigneeName || !String(assigneeName).trim()) {
    return res.status(400).json({ error: 'Champs obligatoires manquants' });
  }

  const numericUserId = Number(userId);
  const effectiveUserId = Number.isInteger(numericUserId) && numericUserId > 0 ? numericUserId : req.user.id;

  const projet = await get('SELECT id, nomProjet, numeroMaison, nomSite FROM projects WHERE id = ?', [assignmentProjectId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const scopedProject = await get(
      'SELECT id, numeroMaison, nomSite FROM projects WHERE id = ?',
      [assignmentProjectId]
    );
    if (!scopedProject || !isInChefSiteScope(req.user, scopedProject)) {
      return res.status(403).json({ error: 'Acces refuse a ce site' });
    }
  }

  const linkedEmployeeId = await ensureAssignmentEmployee({
    assigneeName,
    role,
    phoneNumber,
    createdBy: req.user.username,
    projectName: projet.nomProjet,
    siteNumber: projet.numeroMaison || projet.nomSite,
  });

  const result = await run(
    'INSERT INTO project_assignments (projectId, userId, employeeId, assigneeName, phoneNumber, role, assignedAt) VALUES (?, ?, ?, ?, ?, ?, ?)',
    [
      assignmentProjectId,
      effectiveUserId,
      linkedEmployeeId,
      String(assigneeName).trim(),
      String(phoneNumber).trim(),
      String(role).trim(),
      new Date().toISOString(),
    ]
  );

  const assignment = await get(`
    SELECT pa.*, p.nomProjet, p.nomSite, p.numeroMaison, u.username, COALESCE(pa.assigneeName, u.username) as displayName
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projectId
    JOIN users u ON u.id = pa.userId
    WHERE pa.id = ?
  `, [result.lastID]);

  res.status(201).json(assignment);
});

app.get('/api/project-assignments', async (_req, res) => {
  const rows = await all(`
    SELECT pa.*, p.nomProjet, p.nomSite, p.numeroMaison, u.username, COALESCE(pa.assigneeName, u.username) as displayName
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projectId
    JOIN users u ON u.id = pa.userId
    ORDER BY pa.assignedAt DESC
  `);
  if (String(_req.user?.role || '').trim() === 'chef_chantier_site') {
    const scopedRows = rows.filter(row => isInChefSiteScope(_req.user, row));
    return res.json(scopedRows);
  }
  res.json(rows);
});

app.patch('/api/project-assignments/:id', async (req, res) => {
  const id = Number(req.params.id);
  const { assigneeName, role, phoneNumber = '' } = req.body;

  if (!id || !assigneeName || !String(assigneeName).trim() || !role || !String(role).trim()) {
    return res.status(400).json({ error: 'Nom et role sont obligatoires' });
  }

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const scopedAssignment = await get(
      `SELECT pa.id, p.numeroMaison, p.nomSite
       FROM project_assignments pa
       JOIN projects p ON p.id = pa.projectId
       WHERE pa.id = ?`,
      [id]
    );
    if (!scopedAssignment || !isInChefSiteScope(req.user, scopedAssignment)) {
      return res.status(403).json({ error: 'Acces refuse a cette assignation' });
    }
  }

  const existingAssignment = await get(
    `SELECT pa.id, pa.employeeId, p.nomProjet, p.nomSite, p.numeroMaison
     FROM project_assignments pa
     JOIN projects p ON p.id = pa.projectId
     WHERE pa.id = ?`,
    [id]
  );
  if (!existingAssignment) {
    return res.status(404).json({ error: 'Assignation non trouvee' });
  }

  const normalizedAssignee = String(assigneeName).trim();
  const normalizedRole = String(role).trim();
  const normalizedPhone = String(phoneNumber).trim();
  const roleNormalized = normalizeTextValue(normalizedRole);
  const isChefChantier = roleNormalized.includes('chef') && roleNormalized.includes('chantier');
  const addressLabel = isChefChantier
    ? `Projet: ${String(existingAssignment.nomProjet || '').trim() || '-'} | Site: ${String(existingAssignment.numeroMaison || existingAssignment.nomSite || '').trim() || '-'}`
    : '';
  const nowIso = new Date().toISOString();
  let linkedEmployeeId = Number(existingAssignment.employeeId || 0);

  if (linkedEmployeeId > 0) {
    const employee = await get('SELECT * FROM hr_employees WHERE id = ?', [linkedEmployeeId]);
    if (employee) {
      const preservedAddress = isChefChantier
        ? addressLabel
        : String(employee.address || '').trim();
      await run(
        'UPDATE hr_employees SET fullName = ?, jobTitle = ?, phoneNumber = ?, address = ?, updatedAt = ? WHERE id = ?',
        [normalizedAssignee, normalizedRole, normalizedPhone, preservedAddress, nowIso, linkedEmployeeId]
      );
    } else {
      linkedEmployeeId = 0;
    }
  }

  if (!linkedEmployeeId) {
    const existingEmployee = normalizedPhone
      ? await get(
        `SELECT id
         FROM hr_employees
         WHERE LOWER(TRIM(fullName)) = LOWER(TRIM(?))
           AND LOWER(TRIM(jobTitle)) = LOWER(TRIM(?))
           AND TRIM(phoneNumber) = TRIM(?)
         ORDER BY id DESC
         LIMIT 1`,
        [normalizedAssignee, normalizedRole, normalizedPhone]
      )
      : await get(
        `SELECT id
         FROM hr_employees
         WHERE LOWER(TRIM(fullName)) = LOWER(TRIM(?))
           AND LOWER(TRIM(jobTitle)) = LOWER(TRIM(?))
         ORDER BY id DESC
         LIMIT 1`,
        [normalizedAssignee, normalizedRole]
      );

    if (existingEmployee && Number(existingEmployee.id) > 0) {
      linkedEmployeeId = Number(existingEmployee.id);
    } else {
      const nextEmployeeId = await getNextTableId('hr_employees');
      await run(
        `INSERT INTO hr_employees (id, fullName, jobTitle, phoneNumber, address, maritalStatus, createdBy, createdAt, updatedAt)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          nextEmployeeId,
          normalizedAssignee,
          normalizedRole,
          normalizedPhone,
          addressLabel,
          '',
          String(req.user.username || 'admin').trim() || 'admin',
          nowIso,
          nowIso,
        ]
      );
      linkedEmployeeId = nextEmployeeId;
    }
  }

  const result = await run(
    'UPDATE project_assignments SET employeeId = ?, assigneeName = ?, role = ?, phoneNumber = ? WHERE id = ?',
    [linkedEmployeeId, normalizedAssignee, normalizedRole, normalizedPhone, id]
  );

  if (result.changes === 0) {
    return res.status(404).json({ error: 'Assignation non trouvee' });
  }

  const assignment = await get(`
    SELECT pa.*, p.nomProjet, p.nomSite, p.numeroMaison, u.username, COALESCE(pa.assigneeName, u.username) as displayName
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projectId
    JOIN users u ON u.id = pa.userId
    WHERE pa.id = ?
  `, [id]);

  res.json(assignment);
});

app.delete('/api/project-assignments/:id', async (req, res) => {
  const id = Number(req.params.id);

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    const scopedAssignment = await get(
      `SELECT pa.id, p.numeroMaison, p.nomSite
       FROM project_assignments pa
       JOIN projects p ON p.id = pa.projectId
       WHERE pa.id = ?`,
      [id]
    );
    if (!scopedAssignment || !isInChefSiteScope(req.user, scopedAssignment)) {
      return res.status(403).json({ error: 'Acces refuse a cette assignation' });
    }
  }

  const result = await run('DELETE FROM project_assignments WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Assignation non trouvée' });
  }
  res.json({ message: 'Assignation supprimée' });
});

function parseProjectProgressMaterialUsage(rawValue) {
  if (!rawValue) return [];
  try {
    const parsed = JSON.parse(rawValue);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
}

function formatProjectProgressRow(row) {
  if (!row) return row;
  return {
    ...row,
    materialUsedQty: Number(row.materialUsedQty || 0),
    progressPercent: row.progressPercent === null || row.progressPercent === undefined ? null : Number(row.progressPercent),
    materialUsageDetails: parseProjectProgressMaterialUsage(row.materialUsageDetails),
  };
}

app.post('/api/project-progress', async (req, res) => {
  const {
    projetId,
    projectId,
    stage,
    etape,
    title = '',
    note,
    commentaire,
    materialUsedQty = null,
    materialUsageDetails = null,
    progressPercent = null,
    dateEtape,
  } = req.body || {};

  const progressProjectId = Number(projectId || projetId);
  const stageLabel = String(stage || etape || '').trim();
  const progressStageKey = normalizeStageLabel(stageLabel);
  const noteValue = String(note || commentaire || '').trim();

  if (!progressProjectId) {
    return res.status(400).json({ error: 'Projet obligatoire' });
  }
  if (!stageLabel) {
    return res.status(400).json({ error: 'Etape obligatoire' });
  }
  if (!noteValue) {
    return res.status(400).json({ error: 'Note obligatoire' });
  }

  const projet = await get('SELECT id, nomProjet, nomSite, numeroMaison FROM projects WHERE id = ?', [progressProjectId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }
  if (!isInChefSiteScope(req.user, projet)) {
    return res.status(403).json({ error: 'Acces refuse: ce journal ne concerne pas votre site' });
  }

  let normalizedPercent = null;
  if (progressPercent !== null && progressPercent !== undefined && String(progressPercent).trim() !== '') {
    const parsed = Number(progressPercent);
    if (Number.isNaN(parsed) || parsed < 0 || parsed > 100) {
      return res.status(400).json({ error: 'Le pourcentage doit etre compris entre 0 et 100' });
    }
    normalizedPercent = parsed;
  }

  const normalizedMaterialUsageDetails = Array.isArray(materialUsageDetails)
    ? materialUsageDetails
    : [];

  const normalizedUsageLines = [];
  let totalUsageFromLines = 0;
  for (const line of normalizedMaterialUsageDetails) {
    const itemName = String(line?.itemName || '').trim();
    const qty = Number(line?.quantite || line?.quantity || 0);

    if (!itemName) {
      return res.status(400).json({ error: 'Chaque ligne de consommation doit avoir un matériau' });
    }
    if (Number.isNaN(qty) || qty <= 0) {
      return res.status(400).json({ error: 'Chaque quantité de matériau utilisée doit être supérieure à 0' });
    }

    const existingLine = normalizedUsageLines.find(entry => entry.itemName.toLowerCase() === itemName.toLowerCase());
    if (existingLine) {
      existingLine.quantite += qty;
    } else {
      normalizedUsageLines.push({ itemName, quantite: qty });
    }
    totalUsageFromLines += qty;
  }

  let normalizedMaterialUsedQty = totalUsageFromLines;
  if (!normalizedUsageLines.length && materialUsedQty !== null && materialUsedQty !== undefined && String(materialUsedQty).trim() !== '') {
    const parsedUsed = Number(materialUsedQty);
    if (Number.isNaN(parsedUsed) || parsedUsed < 0) {
      return res.status(400).json({ error: 'La quantité de matériel utilisée doit être positive ou nulle' });
    }
    normalizedMaterialUsedQty = parsedUsed;
  }

  const issueTypeExpr = "COALESCE(NULLIF(TRIM(si.issueType), ''), CASE WHEN si.note LIKE 'Consommation chantier%' THEN 'CONSUMPTION' ELSE 'SITE_TRANSFER' END)";

  const availableRowsByMaterial = new Map();
  if (normalizedUsageLines.length > 0) {
    for (const usageLine of normalizedUsageLines) {
      const rawRows = await all(
        `SELECT mr.id,
                mr.itemName,
                mr.etapeApprovisionnement,
                mr.dateDemande,
                COALESCE(SUM(CASE WHEN ${issueTypeExpr} = 'SITE_TRANSFER' THEN COALESCE(si.quantiteSortie, 0) ELSE 0 END), 0) AS transferredQty,
                COALESCE(SUM(CASE WHEN ${issueTypeExpr} = 'CONSUMPTION' THEN COALESCE(si.quantiteSortie, 0) ELSE 0 END), 0) AS consumedQty
         FROM material_requests mr
         LEFT JOIN stock_issues si ON si.materialRequestId = mr.id
         WHERE mr.projetId = ?
           AND LOWER(TRIM(mr.itemName)) = LOWER(TRIM(?))
         GROUP BY mr.id, mr.itemName, mr.dateDemande
         ORDER BY mr.dateDemande ASC, mr.id ASC`,
        [progressProjectId, usageLine.itemName]
      );

      const availableRows = rawRows
        .map(row => {
          const transferredQty = Number(row.transferredQty || 0);
          const consumedQty = Number(row.consumedQty || 0);
          const siteRemaining = Math.max(transferredQty - consumedQty, 0);
          return {
            id: Number(row.id),
            itemName: row.itemName,
            stageKey: normalizeStageLabel(row.etapeApprovisionnement),
            siteRemaining,
          };
        })
        .filter(row => !progressStageKey || row.stageKey === progressStageKey)
        .filter(row => row.siteRemaining > 0);

      const totalAvailable = availableRows.reduce((sum, row) => sum + Number(row.siteRemaining || 0), 0);
      if (usageLine.quantite > totalAvailable) {
        return res.status(400).json({
          error: `Stock insuffisant sur site pour ${usageLine.itemName} (${stageLabel}). Disponible: ${totalAvailable.toFixed(2)}, demandé: ${usageLine.quantite.toFixed(2)}`,
        });
      }

      availableRowsByMaterial.set(usageLine.itemName.toLowerCase(), availableRows);
    }
  } else if (normalizedMaterialUsedQty > 0) {
    const rawRows = await all(
      `SELECT mr.id,
              mr.itemName,
              mr.etapeApprovisionnement,
              mr.dateDemande,
              COALESCE(SUM(CASE WHEN ${issueTypeExpr} = 'SITE_TRANSFER' THEN COALESCE(si.quantiteSortie, 0) ELSE 0 END), 0) AS transferredQty,
              COALESCE(SUM(CASE WHEN ${issueTypeExpr} = 'CONSUMPTION' THEN COALESCE(si.quantiteSortie, 0) ELSE 0 END), 0) AS consumedQty
       FROM material_requests mr
       LEFT JOIN stock_issues si ON si.materialRequestId = mr.id
       WHERE mr.projetId = ?
       GROUP BY mr.id, mr.itemName, mr.dateDemande
       ORDER BY mr.dateDemande ASC, mr.id ASC`,
      [progressProjectId]
    );

    const availableRows = rawRows
      .map(row => {
        const transferredQty = Number(row.transferredQty || 0);
        const consumedQty = Number(row.consumedQty || 0);
        const siteRemaining = Math.max(transferredQty - consumedQty, 0);
        return {
          id: Number(row.id),
          itemName: row.itemName,
          stageKey: normalizeStageLabel(row.etapeApprovisionnement),
          siteRemaining,
        };
      })
      .filter(row => !progressStageKey || row.stageKey === progressStageKey)
      .filter(row => row.siteRemaining > 0);

    const totalAvailable = availableRows.reduce((sum, row) => sum + Number(row.siteRemaining || 0), 0);
    if (normalizedMaterialUsedQty > totalAvailable) {
      return res.status(400).json({
        error: `Stock insuffisant sur site pour l'étape ${stageLabel}. Disponible: ${totalAvailable.toFixed(2)}, demandé: ${normalizedMaterialUsedQty.toFixed(2)}`,
      });
    }

    availableRowsByMaterial.set('__legacy__', availableRows);
  }

  let createdAt = new Date().toISOString();
  if (dateEtape !== null && dateEtape !== undefined && String(dateEtape).trim() !== '') {
    const rawDateEtape = String(dateEtape).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(rawDateEtape)) {
      // Force midi UTC pour éviter les décalages de jour lors de l'affichage local.
      createdAt = `${rawDateEtape}T12:00:00.000Z`;
    } else {
      const parsedDate = new Date(rawDateEtape);
      if (Number.isNaN(parsedDate.getTime())) {
        return res.status(400).json({ error: 'Date etape invalide' });
      }
      createdAt = parsedDate.toISOString();
    }
  }

  const result = await run(
    'INSERT INTO project_progress_updates (id, projectId, stage, title, note, materialUsedQty, materialUsageDetails, progressPercent, createdBy, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      await getNextTableId('project_progress_updates'),
      progressProjectId,
      stageLabel,
      String(title || '').trim(),
      noteValue,
      normalizedMaterialUsedQty,
      JSON.stringify(normalizedUsageLines),
      normalizedPercent,
      req.user ? req.user.username : 'admin',
      createdAt,
    ]
  );

  if (normalizedUsageLines.length > 0) {
    let nextSiIdProgress = await getNextTableId('stock_issues');
    for (const usageLine of normalizedUsageLines) {
      let remainingToIssue = usageLine.quantite;
      const availableRows = availableRowsByMaterial.get(usageLine.itemName.toLowerCase()) || [];

      for (const row of availableRows) {
        if (remainingToIssue <= 0) break;
        const currentRemaining = Number(row.siteRemaining || 0);
        if (currentRemaining <= 0) continue;

        const outQty = Math.min(currentRemaining, remainingToIssue);
        await run(
          'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
          [
            nextSiIdProgress++,
            Number(row.id),
            progressProjectId,
            outQty,
            'CONSUMPTION',
            `Consommation chantier - ${usageLine.itemName} - suivi étape #${result.lastID}`,
            req.user ? req.user.username : 'admin',
            createdAt,
          ]
        );

        remainingToIssue -= outQty;
      }
    }
  } else if (normalizedMaterialUsedQty > 0) {
    let remainingToIssue = normalizedMaterialUsedQty;
    let nextSiIdLegacy = await getNextTableId('stock_issues');
    for (const row of availableRowsByMaterial.get('__legacy__') || []) {
      if (remainingToIssue <= 0) break;
      const currentRemaining = Number(row.siteRemaining || 0);
      if (currentRemaining <= 0) continue;

      const outQty = Math.min(currentRemaining, remainingToIssue);
      await run(
        'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [
          nextSiIdLegacy++,
          Number(row.id),
          progressProjectId,
          outQty,
          'CONSUMPTION',
          `Consommation chantier - suivi étape #${result.lastID}`,
          req.user ? req.user.username : 'admin',
          createdAt,
        ]
      );

      remainingToIssue -= outQty;
    }
  }

  const row = await get(`
    SELECT ppu.*, p.nomProjet, p.nomSite, p.numeroMaison, p.typeMaison
    FROM project_progress_updates ppu
    JOIN projects p ON p.id = ppu.projectId
    WHERE ppu.id = ?
  `, [result.lastID]);

  res.status(201).json(formatProjectProgressRow(row));
});

app.get('/api/project-progress', async (req, res) => {
  const projectId = Number(req.query.projectId || 0);

  const baseQuery = `
    SELECT ppu.*, p.nomProjet, p.nomSite, p.numeroMaison, p.typeMaison
    FROM project_progress_updates ppu
    JOIN projects p ON p.id = ppu.projectId
  `;

  const rows = projectId
    ? await all(`${baseQuery} WHERE p.id = ? ORDER BY ppu.createdAt DESC, ppu.id DESC`, [projectId])
    : await all(`${baseQuery} ORDER BY ppu.createdAt DESC, ppu.id DESC`);

  const role = String(req.user?.role || '').trim();
  const scopedRows = (role === 'chef_chantier_site' || role === 'gestionnaire_stock_songon')
    ? rows.filter(row => isInUserProjectScope(req.user, row))
    : rows;

  res.json(scopedRows.map(formatProjectProgressRow));
});

app.delete('/api/project-progress', async (req, res) => {
  if (String(req.user?.role || '').trim() !== 'admin') {
    return res.status(403).json({ error: 'Acces refuse: admin uniquement' });
  }

  const result = await run('DELETE FROM project_progress_updates');
  res.json({ deleted: Number(result?.changes || 0) });
});

app.get('/api/admin/dashboard', async (req, res) => {
  const stats = await get(`
    SELECT
      (SELECT COUNT(DISTINCT NULLIF(TRIM(nomProjet), '')) FROM projects) as totalProjects,
      (
        SELECT COUNT(*)
        FROM (
          SELECT DISTINCT
            LOWER(TRIM(COALESCE(nomProjet, ''))) AS dossierProjet,
            LOWER(TRIM(COALESCE(numeroMaison, ''))) AS numeroMaison
          FROM projects
          WHERE TRIM(COALESCE(numeroMaison, '')) <> ''
        ) maisons
      ) as totalEtablissements,
      (SELECT COUNT(*) FROM project_assignments) as totalAssignments,
      (SELECT COUNT(*) FROM purchase_orders) as totalOrders,
      (SELECT COALESCE(SUM(montantTotal), 0) FROM expenses WHERE statut = 'VALIDEE') as totalExpenses,
      (SELECT COALESCE(SUM(amount), 0) FROM revenues) as totalRevenues
  `);

  res.json({
    totalProjects: stats.totalProjects || 0,
    totalEtablissements: stats.totalEtablissements || 0,
    totalAssignments: stats.totalAssignments || 0,
    totalOrders: stats.totalOrders || 0,
    totalExpenses: stats.totalExpenses || 0,
    totalRevenues: stats.totalRevenues || 0,
    profit: (stats.totalRevenues || 0) - (stats.totalExpenses || 0)
  });
});

app.post('/api/suppliers', async (req, res) => {
  const {
    nomFournisseur,
    materiels = '',
    prixMateriaux = 0,
    telephone = '',
    email = '',
    adresse = '',
    notes = '',
  } = req.body || {};

  const supplierName = String(nomFournisseur || '').trim();
  if (!supplierName) {
    return res.status(400).json({ error: 'Le nom du fournisseur est obligatoire' });
  }

  const numericPrice = Number(prixMateriaux || 0);
  if (Number.isNaN(numericPrice) || numericPrice < 0) {
    return res.status(400).json({ error: 'Le prix des materiaux doit etre un nombre positif' });
  }

  const now = new Date().toISOString();
  const result = await run(
    `INSERT INTO suppliers (
      nomFournisseur,
      materiels,
      prixMateriaux,
      telephone,
      email,
      adresse,
      notes,
      createdAt,
      updatedAt
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      supplierName,
      String(materiels || '').trim(),
      numericPrice,
      String(telephone || '').trim(),
      String(email || '').trim(),
      String(adresse || '').trim(),
      String(notes || '').trim(),
      now,
      now,
    ]
  );

  const supplier = await get('SELECT * FROM suppliers WHERE id = ?', [result.lastID]);
  res.status(201).json(supplier);
});

app.get('/api/suppliers', async (_req, res) => {
  const rows = await all('SELECT * FROM suppliers ORDER BY nomFournisseur ASC, id DESC');
  res.json(rows);
});

app.patch('/api/suppliers/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!id) {
    return res.status(400).json({ error: 'ID fournisseur invalide' });
  }

  const {
    nomFournisseur,
    materiels = '',
    prixMateriaux = 0,
    telephone = '',
    email = '',
    adresse = '',
    notes = '',
  } = req.body || {};

  const supplierName = String(nomFournisseur || '').trim();
  if (!supplierName) {
    return res.status(400).json({ error: 'Le nom du fournisseur est obligatoire' });
  }

  const numericPrice = Number(prixMateriaux || 0);
  if (Number.isNaN(numericPrice) || numericPrice < 0) {
    return res.status(400).json({ error: 'Le prix des materiaux doit etre un nombre positif' });
  }

  const result = await run(
    `UPDATE suppliers
     SET nomFournisseur = ?,
         materiels = ?,
         prixMateriaux = ?,
         telephone = ?,
         email = ?,
         adresse = ?,
         notes = ?,
         updatedAt = ?
     WHERE id = ?`,
    [
      supplierName,
      String(materiels || '').trim(),
      numericPrice,
      String(telephone || '').trim(),
      String(email || '').trim(),
      String(adresse || '').trim(),
      String(notes || '').trim(),
      new Date().toISOString(),
      id,
    ]
  );

  if (result.changes === 0) {
    return res.status(404).json({ error: 'Fournisseur non trouve' });
  }

  const supplier = await get('SELECT * FROM suppliers WHERE id = ?', [id]);
  res.json(supplier);
});

app.delete('/api/suppliers/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM suppliers WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Fournisseur non trouve' });
  }
  res.json({ message: 'Fournisseur supprime' });
});

// ── Building Material Catalog ────────────────────────────────────────────────
app.get('/api/material-catalog', async (req, res) => {
  const folder = req.query.folder ? String(req.query.folder).trim() : null;
  const rows = folder
    ? await all('SELECT * FROM building_material_catalog WHERE projectFolder = ? ORDER BY materialName ASC', [folder])
    : await all('SELECT * FROM building_material_catalog ORDER BY projectFolder ASC, materialName ASC');
  res.json(rows);
});

app.post('/api/material-catalog', async (req, res) => {
  if (isMaterialCatalogReadOnlyUser(req.user)) {
    return res.status(403).json({ error: 'Catalogue en lecture seule pour ce profil' });
  }

  const {
    id = null,
    projectFolder = '',
    materialName,
    unite = '',
    quantiteParBatiment = 0,
    prixUnitaire = 0,
    notes = '',
    createdAt = null,
    updatedAt = null,
  } = req.body || {};

  const name = String(materialName || '').trim();
  if (!name) {
    return res.status(400).json({ error: 'Le nom du matériel est obligatoire' });
  }

  const qty = Number(quantiteParBatiment);
  const price = Number(prixUnitaire);
  if (Number.isNaN(qty) || qty < 0) {
    return res.status(400).json({ error: 'Quantité par bâtiment invalide' });
  }
  if (Number.isNaN(price) || price < 0) {
    return res.status(400).json({ error: 'Prix unitaire invalide' });
  }

  const now = new Date().toISOString();
  const explicitId = Number(id);
  const nextCatalogMaterialId = Number.isInteger(explicitId) && explicitId > 0 ? explicitId : await getNextTableId('building_material_catalog');
  const createdAtValue = String(createdAt || '').trim() || now;
  const updatedAtValue = String(updatedAt || '').trim() || createdAtValue;
  const result = await run(
    `INSERT INTO building_material_catalog
      (id, projectFolder, materialName, unite, quantiteParBatiment, prixUnitaire, notes, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextCatalogMaterialId,
      String(projectFolder || '').trim(),
      name,
      String(unite || '').trim(),
      qty,
      price,
      String(notes || '').trim(),
      createdAtValue,
      updatedAtValue,
    ]
  );

  const row = await get('SELECT * FROM building_material_catalog WHERE id = ?', [nextCatalogMaterialId || result.lastID]);
  res.status(201).json(row);
});

app.patch('/api/material-catalog/:id', async (req, res) => {
  if (isMaterialCatalogReadOnlyUser(req.user)) {
    return res.status(403).json({ error: 'Catalogue en lecture seule pour ce profil' });
  }

  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: 'ID invalide' });

  const {
    projectFolder = '',
    materialName,
    unite = '',
    quantiteParBatiment = 0,
    prixUnitaire = 0,
    notes = '',
  } = req.body || {};

  const name = String(materialName || '').trim();
  if (!name) return res.status(400).json({ error: 'Le nom du matériel est obligatoire' });

  const qty = Number(quantiteParBatiment);
  const price = Number(prixUnitaire);
  if (Number.isNaN(qty) || qty < 0) return res.status(400).json({ error: 'Quantité invalide' });
  if (Number.isNaN(price) || price < 0) return res.status(400).json({ error: 'Prix invalide' });

  const result = await run(
    `UPDATE building_material_catalog
     SET projectFolder = ?, materialName = ?, unite = ?, quantiteParBatiment = ?, prixUnitaire = ?, notes = ?, updatedAt = ?
     WHERE id = ?`,
    [
      String(projectFolder || '').trim(),
      name,
      String(unite || '').trim(),
      qty,
      price,
      String(notes || '').trim(),
      new Date().toISOString(),
      id,
    ]
  );

  if (result.changes === 0) return res.status(404).json({ error: 'Entrée catalogue introuvable' });
  const row = await get('SELECT * FROM building_material_catalog WHERE id = ?', [id]);
  res.json(row);
});


// Suppression d’une entrée ou d’un catalogue entier
app.delete('/api/material-catalog/:id', async (req, res) => {
  if (isMaterialCatalogReadOnlyUser(req.user)) {
    return res.status(403).json({ error: 'Catalogue en lecture seule pour ce profil' });
  }

  const id = Number(req.params.id);
  const result = await run('DELETE FROM building_material_catalog WHERE id = ?', [id]);
  if (result.changes === 0) return res.status(404).json({ error: 'Entrée catalogue introuvable' });
  res.json({ message: 'Entrée supprimée' });
});

app.delete('/api/material-catalog', async (req, res) => {
  if (isMaterialCatalogReadOnlyUser(req.user)) {
    return res.status(403).json({ error: 'Catalogue en lecture seule pour ce profil' });
  }

  const folder = req.query.projectFolder ? String(req.query.projectFolder).trim() : null;
  if (!folder) return res.status(400).json({ error: 'projectFolder requis' });
  const result = await run('DELETE FROM building_material_catalog WHERE projectFolder = ?', [folder]);
  res.json({ message: `Catalogue supprimé pour ${folder}`, deleted: result.changes });
});

app.post('/api/materials', async (req, res) => {
  const { nom, categorie, unite, stock = 0, seuil = 0, prixMoyen = 0 } = req.body;
  if (!nom || !categorie || !unite) {
    return res.status(400).json({ error: 'Champs obligatoires manquants' });
  }

  const result = await run(
    'INSERT INTO materials (nom, categorie, unite, stock, seuil, prixMoyen) VALUES (?, ?, ?, ?, ?, ?)',
    [nom, categorie, unite, stock, seuil, prixMoyen]
  );

  const material = await get('SELECT * FROM materials WHERE id = ?', [result.lastID]);
  res.status(201).json(material);
});

app.get('/api/materials', async (_req, res) => {
  const rows = await all('SELECT * FROM materials ORDER BY categorie, nom');
  res.json(rows);
});


