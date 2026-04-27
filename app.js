// app.js
// API Construction & Logistique avec une base de données SQLite et authentification

const express = require('express');
const path = require('path');
const fs = require('fs');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const PDFDocument = require('pdfkit');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');
const { spawn } = require('child_process');
const { createDbClient } = require('./db/client');

const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || '1mb' }));
app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginResourcePolicy: { policy: 'cross-origin' },
}));
app.use(express.static(path.join(__dirname, 'public')));

const APP_DATA_DIR = process.env.APP_DATA_DIR || (process.env.RAILWAY_ENVIRONMENT ? '/data' : __dirname);
const DB_FILE = process.env.DB_FILE || path.join(APP_DATA_DIR, 'data.db');
const ARCHIVE_ROOT = process.env.ARCHIVE_ROOT || path.join(APP_DATA_DIR, 'archives');
const JWT_SECRET = process.env.JWT_SECRET || 'erp-secret-2026';
const PORT = process.env.PORT || 4000;
const COMMIS_STOCK_USERNAME = process.env.COMMIS_STOCK_USERNAME || 'commis_stock';
const COMMIS_STOCK_PASSWORD = process.env.COMMIS_STOCK_PASSWORD || 'stock123';
const GEST_STOCK_USERNAME = process.env.GEST_STOCK_USERNAME || 'gestionnaire_stock';
const GEST_STOCK_PASSWORD = process.env.GEST_STOCK_PASSWORD || 'geststock123';
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
let autoBackupTimer = null;
let autoBackupInProgress = false;
let autoBackupPending = false;

fs.mkdirSync(ARCHIVE_ROOT, { recursive: true });
app.use('/archives', express.static(ARCHIVE_ROOT));

if (JWT_SECRET === 'erp-secret-2026') {
  console.warn('Avertissement securite: JWT_SECRET par defaut detecte. Configure une valeur forte en production.');
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

app.get('/healthz', (_req, res) => {
  if (isShuttingDown) {
    return res.status(503).json({ status: 'shutting-down' });
  }
  res.json({ status: 'ok' });
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
    ? (siteValueRaw.toLowerCase().includes('site') ? siteValueRaw : `Site Numero ${siteValueRaw}`)
    : 'Site non renseigne';
  const orderDate = new Date(order.dateCommande || Date.now());
  const orderDateLabel = Number.isNaN(orderDate.getTime()) ? new Date().toLocaleDateString('fr-FR') : orderDate.toLocaleDateString('fr-FR');
  const statusValue = String(order.statutValidation || order.statut || '').trim().toUpperCase();
  const isRejected = statusValue === 'ANNULEE' || statusValue === 'REJETEE';
  const stampLabel = isRejected ? 'Rejet\u00e9' : 'Valid\u00e9';
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

  doc.font('Helvetica-Bold').fontSize(10).text('Site :', 40, 122);
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

  // Footer: signature and decision stamp.
  const footerY = 730;
  doc.moveTo(40, footerY).lineTo(555, footerY).lineWidth(1).strokeColor('#94a3b8').stroke();
  doc.font('Helvetica').fontSize(9).fillColor('#475569').text(`Edite le : ${new Date().toLocaleString('fr-FR')}`, 40, footerY + 10);

  doc.save();
  doc.lineWidth(2).strokeColor(isRejected ? '#b91c1c' : '#166534').fillColor(isRejected ? '#b91c1c' : '#166534');
  doc.roundedRect(40, 742, 160, 52, 8).stroke();
  doc.font('Helvetica-Bold').fontSize(8).text('TAMPON', 48, 748, { width: 144, align: 'left' });
  doc.font('Helvetica-Bold').fontSize(18).text(stampLabel, 48, 760, { width: 144, align: 'left' });
  doc.restore();

  // Signature on the right (handwritten style when available).
  const signerName = String(order.signatureName || order.createdBy || '').trim() || 'Signature';
  const signerRole = String(order.signatureRole || '').trim();
  doc.font('Helvetica').fontSize(9).fillColor('#0f172a').text('Signature autorisee', 335, 742, { width: 220, align: 'right' });
  doc.font(signatureFontName).fontSize(26).fillColor('#111827').text(signerName, 335, 754, { width: 220, align: 'right' });
  if (signerRole) {
    doc.font('Helvetica').fontSize(9).fillColor('#334155').text(signerRole, 335, 784, { width: 220, align: 'right' });
  }
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
    .map(label => String(label || '').trim())
    .filter(Boolean);
}

function isCatalogStageMatching(catalogNotes, requestedStage) {
  const requestedKey = normalizeStageLabel(requestedStage);
  if (!requestedKey) return true;

  const stages = parseCatalogStageLabels(catalogNotes);
  if (!stages.length) return true;

  return stages.some(stageLabel => {
    const catalogKey = normalizeStageLabel(stageLabel);
    if (!catalogKey) return false;
    return catalogKey === requestedKey || catalogKey.includes(requestedKey) || requestedKey.includes(catalogKey);
  });
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
  const stripped = raw.replace(/^site\s*(numero|n°|no)?\s*/i, '').trim();
  const numberMatch = stripped.match(/\d+/);
  if (numberMatch && numberMatch[0]) {
    return numberMatch[0];
  }
  return stripped || raw;
}

function buildPurchaseOrderDocumentTitle(order) {
  const stageRaw = resolvePurchaseOrderStageDisplay(order?.etapeApprovisionnement)
    || resolvePurchaseOrderStageDisplay(order?.items?.[0]?.etapeApprovisionnement)
    || '';
  const stageLabel = stageRaw || 'Étape';

  const siteRaw = String(order?.numeroMaison || order?.nomSiteManuel || '').trim();
  const siteLabel = extractSiteNumberLabel(siteRaw);

  return `${stageLabel}-${siteLabel}`;
}

function buildStageSiteTitle(stageValue, siteValue) {
  const stageLabel = String(stageValue || '').trim() || 'Etape';
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
  // Project is always PINUT
  const projectTitle = 'PINUT';
  const siteValueRaw = String(request.numeroMaison || request.nomSite || '').trim();
  const siteLabel = siteValueRaw
    ? (siteValueRaw.toLowerCase().includes('site') ? siteValueRaw : `Site Numero ${siteValueRaw}`)
    : 'Site non renseigne';
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
  doc.font('Helvetica-Bold').fontSize(10).text('Site :', 300, 66);
  doc.font('Helvetica').fontSize(10).text(siteLabel, 340, 66);

  doc.font('Helvetica-Bold').fontSize(10).text('Date :', 40, 82);
  doc.font('Helvetica').fontSize(10).text(documentDate.toLocaleDateString('fr-FR'), 105, 82);
  doc.font('Helvetica-Bold').fontSize(10).text(referenceLabel, 300, 82);
  doc.font('Helvetica').fontSize(10).text(referenceValue, 440, 82);

  // Separator
  doc.moveTo(40, 100).lineTo(555, 100).lineWidth(1).strokeColor('#cccccc').stroke();

  // === INFORMATIONS DEMANDE ===
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#000000').text('Informations de la demande', 40, 110);

  doc.font('Helvetica-Bold').fontSize(10).text('Demandeur :', 40, 128);
  doc.font('Helvetica').fontSize(10).text(String(request.demandeur || '-').trim() || '-', 130, 128);
  doc.font('Helvetica-Bold').fontSize(10).text('\u00c9tape :', 300, 128);
  doc.font('Helvetica').fontSize(10).text(requestStageLabel, 345, 128);

  doc.font('Helvetica-Bold').fontSize(10).text('Entrepot :', 40, 144);
  doc.font('Helvetica').fontSize(10).text(warehouseLabel, 130, 144);
  doc.font('Helvetica-Bold').fontSize(10).text('Statut :', 300, 144);
  doc.font('Helvetica').fontSize(10).text(requestStatusLabel, 345, 144);

  // Separator
  doc.moveTo(40, 162).lineTo(555, 162).lineWidth(1).strokeColor('#cccccc').stroke();

  // === TABLE MATERIEL AUTORISE ===
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#000000').text(materialSectionTitle, 40, 172);

  const startX = 40;
  const tableTop = 190;
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
  // Cap at 18 rows max to guarantee one page
  const displayRows = rows.slice(0, 18);
  displayRows.forEach((row, idx) => drawRow(tableTop + rowHeight * (idx + 1), row, false));

  // === FOOTER: signature + tampon (fixed at bottom of page) ===
  const footerY = 730;
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
  await run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL,
    createdAt TEXT NOT NULL
  )`);

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
    await run('DROP INDEX IF EXISTS idx_project_folders_name_site');
  } catch (e) {}

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
  try {
    await run(`
      UPDATE project_catalog
      SET typeProjet = REPLACE(typeProjet, 'sant�', 'santé')
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
                REPLACE(description, 'sant�', 'santé'),
                '�chelle',
                'échelle'
              ),
              'C�te',
              'Côte'
            ),
            'am�liorer',
            'améliorer'
          ),
          'acc�s',
          'accès'
        ),
        '� ',
        'à '
      )
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE projects
      SET typeMaison = REPLACE(typeMaison, 'sant�', 'santé')
      WHERE LOWER(TRIM(nomProjet)) = 'pinut'
    `);
  } catch (e) {}
  try {
    await run(`
      UPDATE building_material_catalog
      SET unite = REPLACE(unite, 'm�', 'm³'),
          notes = REPLACE(notes, '�tape', 'Étape')
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

  if (DB_DRIVER === 'postgres') {
    try {
      await run('ALTER TABLE generated_documents ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY');
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

  await run(`CREATE TABLE IF NOT EXISTS project_assignments (
    id INTEGER PRIMARY KEY,
    projectId INTEGER NOT NULL,
    userId INTEGER NOT NULL,
    assigneeName TEXT,
    phoneNumber TEXT,
    role TEXT NOT NULL,
    assignedAt TEXT NOT NULL,
    FOREIGN KEY(projectId) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY(userId) REFERENCES users(id) ON DELETE CASCADE
  )`);

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

  // Garantir un compte commis_stock toujours opérationnel (local + Railway)
  const commis = await get('SELECT id FROM users WHERE username = ?', [COMMIS_STOCK_USERNAME]);
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

  // Supprimer les profils retires du lien public et de Railway.
  const removedPublicProfiles = await run(
    'DELETE FROM users WHERE role = ? OR username IN (?, ?, ?, ?)',
    ['gestionnaire_stock', GEST_STOCK_USERNAME, 'chef_adzope_site15', 'controle_achat_global', 'gest_zone_adzope']
  );
  if (Number(removedPublicProfiles?.changes || 0) > 0) {
    console.log(`Profils publics supprimes: ${Number(removedPublicProfiles.changes || 0)}`);
  }
}

initDb().then(() => {
  server = app.listen(PORT, () => {
    isReady = true;
    console.log(`API Construction & Logistique démarrée sur http://localhost:${PORT}`);
    
    // Exécuter les réconciliations en arrière-plan sans bloquer
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
  });
}).catch(error => {
  isReady = false;
  console.error('Erreur d\'initialisation de la base de données', error.stack || error);
  process.exit(1);
});

async function shutdown(signal) {
  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;
  isReady = false;
  console.log(`Signal ${signal} recu. Arret en cours...`);

  try {
    if (server) {
      await new Promise(resolve => server.close(resolve));
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

function authenticateToken(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return res.status(401).json({ error: 'Token manquant' });
  }

  const token = authHeader.split(' ')[1];
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

function authorizeRoleAccess(req, res, next) {
  const role = req.user && req.user.role;
  if (role !== 'commis' && role !== 'gestionnaire_stock' && role !== 'chef_chantier_site') {
    return next();
  }

  const method = String(req.method || '').toUpperCase();
  const pathName = String(req.path || '');

  const commisRules = [
    { method: 'GET', pattern: /^\/projects$/ },
    { method: 'GET', pattern: /^\/material-requests$/ },
    { method: 'POST', pattern: /^\/material-requests$/ },
    { method: 'POST', pattern: /^\/material-requests\/auto-stage$/ },
    { method: 'GET', pattern: /^\/stock-management\/orders$/ },
    { method: 'PATCH', pattern: /^\/stock-management\/orders\/\d+\/arrive$/ },
    { method: 'GET', pattern: /^\/stock-management\/available$/ },
    { method: 'GET', pattern: /^\/stock-management\/issues$/ },
    { method: 'POST', pattern: /^\/stock-management\/issues$/ },
    { method: 'GET', pattern: /^\/transfer-authorizations$/ },
  ];

  const gestStockRules = [
    { method: 'GET',   pattern: /^\/projects$/ },
    { method: 'GET',   pattern: /^\/material-requests$/ },
    { method: 'POST',  pattern: /^\/material-requests\/auto-stage$/ },
    { method: 'GET',   pattern: /^\/purchase-orders$/ },
    { method: 'PATCH', pattern: /^\/purchase-orders\/\d+\/validation$/ },
    { method: 'GET',   pattern: /^\/stock-management\/orders$/ },
    { method: 'PATCH', pattern: /^\/stock-management\/orders\/\d+\/arrive$/ },
    { method: 'GET',   pattern: /^\/stock-management\/available$/ },
    { method: 'GET',   pattern: /^\/stock-management\/issues$/ },
    { method: 'POST',  pattern: /^\/stock-management\/issues$/ },
    { method: 'GET',   pattern: /^\/transfer-authorizations$/ },
  ];

  const siteChiefRules = [
    { method: 'GET',  pattern: /^\/projects$/ },
    { method: 'GET',  pattern: /^\/project-progress$/ },
    { method: 'POST', pattern: /^\/project-progress$/ },
    { method: 'GET',  pattern: /^\/project-folders$/ },
    { method: 'GET',  pattern: /^\/project-catalog$/ },
    { method: 'GET',  pattern: /^\/material-catalog$/ },
    { method: 'GET',  pattern: /^\/material-requests$/ },
    { method: 'POST', pattern: /^\/material-requests$/ },
    { method: 'POST', pattern: /^\/material-requests\/auto-stage$/ },
    { method: 'DELETE', pattern: /^\/material-requests\/\d+$/ },
    { method: 'GET',  pattern: /^\/material-requests\/\d+\/authorization-documents$/ },
    { method: 'GET',  pattern: /^\/material-requests\/group\/pdf$/ },
    { method: 'GET',  pattern: /^\/stock-management\/orders$/ },
    { method: 'GET',  pattern: /^\/stock-management\/available$/ },
    { method: 'GET',  pattern: /^\/stock-management\/issues$/ },
    { method: 'POST', pattern: /^\/stock-management\/issues$/ },
    { method: 'GET',  pattern: /^\/stock-issue-authorizations$/ },
    { method: 'POST', pattern: /^\/stock-issue-authorizations$/ },
    { method: 'GET',  pattern: /^\/stock-issue-authorizations\/\d+\/pdf$/ },
    { method: 'GET',  pattern: /^\/database-documents$/ },
    { method: 'GET',  pattern: /^\/database-documents\/\d+\/download$/ },
  ];

  const rules = role === 'gestionnaire_stock'
    ? gestStockRules
    : role === 'chef_chantier_site'
      ? siteChiefRules
      : commisRules;
  const isAllowed = rules.some(rule => rule.method === method && rule.pattern.test(pathName));
  if (isAllowed) {
    return next();
  }

  return res.status(403).json({ error: 'Acces refuse pour ce role' });
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

  const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, {
    expiresIn: '6h'
  });

  res.json({ token, username: user.username });
});

app.get('/api/auth/me', authenticateToken, async (req, res) => {
  res.json({ username: req.user.username, role: req.user.role, scope: null });
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
  const catalogRows = await all(
    'SELECT * FROM building_material_catalog WHERE projectFolder = ? ORDER BY materialName ASC',
    [projectFolder]
  );

  const stageCatalog = (catalogRows || [])
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
      error: `Aucun article catalogue trouve pour l'etape "${stageRaw}" sur le projet "${projectFolder}".`,
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
      title: `Nouvelle demande approvisionnement (site ${siteLabel})`,
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
  const rows = await all('SELECT id, username, role FROM users ORDER BY username');
  res.json(rows);
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
  const rows = await all('SELECT * FROM project_catalog ORDER BY id DESC');
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
  const rows = await all('SELECT pf.*, pc.typeProjet FROM project_folders pf LEFT JOIN project_catalog pc ON pc.id = pf.projectId ORDER BY pf.id DESC');
  res.json(rows);
});

app.delete('/api/project-folders/:id', async (req, res) => {
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

  const deletedSitesResult = await run(
    'DELETE FROM projects WHERE LOWER(nomProjet) = LOWER(?) AND LOWER(prefecture) = LOWER(?)',
    [projectName, prefectureName]
  );

  if (Array.isArray(sites) && sites.length) {
    const siteIds = sites.map(site => Number(site.id)).filter(value => Number.isInteger(value));
    if (siteIds.length) {
      const placeholders = siteIds.map(() => '?').join(',');
      await run(`DELETE FROM project_assignments WHERE projectId IN (${placeholders})`, siteIds);
      await run(`DELETE FROM material_requests WHERE projetId IN (${placeholders})`, siteIds);
      await run(`DELETE FROM project_progress_updates WHERE projectId IN (${placeholders})`, siteIds);
      await run(`DELETE FROM revenues WHERE projetId IN (${placeholders})`, siteIds);
      await run(`DELETE FROM expenses WHERE projetId IN (${placeholders})`, siteIds);
      await run(`DELETE FROM stock_issues WHERE projetId IN (${placeholders})`, siteIds);
    }
  }

  const folderDeleteResult = await run('DELETE FROM project_folders WHERE id = ?', [id]);
  if (folderDeleteResult.changes === 0) {
    return res.status(404).json({ error: 'Zone introuvable' });
  }

  res.json({
    message: 'Zone supprimée avec succès',
    deletedZoneId: id,
    deletedSites: Number(deletedSitesResult?.changes || 0),
  });
});

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
  let siteType = String(typeMaison || '').trim();
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
  const buildingType = String(typeMaison || '').trim();
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
  const rows = await all('SELECT * FROM projects ORDER BY id DESC');
  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    return res.json(rows.filter(row => isInChefSiteScope(req.user, row)));
  }
  res.json(rows);
});

app.patch('/api/projects/:id', async (req, res) => {
  const id = Number(req.params.id);
  const { nomProjet, prefecture = 'Non renseigne', nomSite = '', typeMaison = '', numeroMaison = '', description = '' } = req.body;
  const projectName = String(nomProjet || '').trim();
  const prefectureName = String(prefecture || '').trim() || 'Non renseigne';
  const siteName = String(nomSite || '').trim();

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
    'UPDATE projects SET nomProjet = ?, prefecture = ?, nomSite = ?, typeMaison = ?, numeroMaison = ?, description = ? WHERE id = ?',
    [
      projectName,
      prefectureName,
      siteName,
      String(typeMaison).trim(),
      String(numeroMaison).trim(),
      String(description).trim(),
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
      title: `Demande materiau creee (site ${siteLabel})`,
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
  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    return res.json(rows.filter(row => isInChefSiteScope(req.user, row)));
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
      String(creePar || 'admin').trim() || 'admin',
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

  if (!['EN_COURS', 'VALIDEE', 'LIVREE', 'ANNULEE'].includes(statut)) {
    return res.status(400).json({ error: 'Statut invalide' });
  }

  if ((statut === 'VALIDEE' || statut === 'ANNULEE') && (!String(signatureName || '').trim() || !signedAt)) {
    return res.status(400).json({ error: 'Signature requise pour valider ou rejeter' });
  }

  const receptionDate = statut === 'LIVREE' ? new Date().toISOString() : null;
  const result = await run(
    "UPDATE purchase_orders SET statut = ?, statutValidation = ?, dateReception = CASE WHEN ? = 'LIVREE' THEN COALESCE(dateReception, ?) ELSE dateReception END WHERE id = ?",
    [statut, statut, statut, receptionDate, id]
  );
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Commande non trouvée' });
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

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
    result = result.filter(order => isInChefSiteScope(req.user, {
      numeroMaison: order.numeroMaison,
      nomSite: order.nomSiteManuel,
    }));
  }

  res.json(result);
});

app.patch('/api/stock-management/orders/:id/arrive', async (req, res) => {
  const orderId = Number(req.params.id);
  if (!orderId) {
    return res.status(400).json({ error: 'ID commande invalide' });
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
  const scopedRows = String(req.user?.role || '').trim() === 'chef_chantier_site'
    ? rows.filter(row => isInChefSiteScope(req.user, row))
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
  const scopedRows = String(req.user?.role || '').trim() === 'chef_chantier_site'
    ? rows.filter(row => isInChefSiteScope(req.user, row))
    : rows;
  res.json(scopedRows);
});

app.get('/api/stock-issue-authorizations', async (req, res) => {
  const warehouseId = String(req.query.warehouseId || '').trim();
  const status = String(req.query.status || '').trim().toUpperCase();
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

  const scopedRows = String(req.user?.role || '').trim() === 'chef_chantier_site'
    ? (rows || []).filter(row => isInChefSiteScope(req.user, row))
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

  if (String(req.user?.role || '').trim() === 'chef_chantier_site' && !isInChefSiteScope(req.user, authRow)) {
    return res.status(403).json({ error: 'Acces refuse: ce document ne concerne pas votre site' });
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
  if (req.user && (req.user.role === 'commis' || req.user.role === 'chef_chantier_site' || req.user.role === 'gestionnaire_stock_zone')) {
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
    `SELECT mr.id, mr.projetId, mr.quantiteRestante, mr.statut, p.nomSite, p.numeroMaison
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     WHERE mr.id = ?`,
    [requestId]
  );
  if (!requestRow) {
    return res.status(404).json({ error: 'Matériel introuvable' });
  }

  if (String(req.user?.role || '').trim() === 'chef_chantier_site' && !isInChefSiteScope(req.user, requestRow)) {
    return res.status(403).json({ error: 'Acces refuse: sortie autorisee uniquement vers le site 15' });
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

    const isSiteChiefRole = String(req.user?.role || '').trim() === 'chef_chantier_site';
    const isSiteChiefDocumentAllowed = async row => {
      if (!isSiteChiefRole) return true;

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
        return !!scopedRow && isInChefSiteScope(req.user, scopedRow);
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
          return linkedRequests.some(rowItem => isInChefSiteScope(req.user, rowItem));
        }

        const orderFallback = await get(
          `SELECT p.nomSite, p.numeroMaison
           FROM purchase_orders po
           LEFT JOIN projects p ON p.id = COALESCE(po.siteId, po.projetId)
           WHERE po.id = ?`,
          [entityId]
        );
        return !!orderFallback && isInChefSiteScope(req.user, orderFallback);
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
        return !!scopedRow && isInChefSiteScope(req.user, scopedRow);
      }

      return false;
    };

    if (isSiteChiefRole) {
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

  if (String(req.user?.role || '').trim() === 'chef_chantier_site') {
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
      inScope = !!scopedRow && isInChefSiteScope(req.user, scopedRow);
    } else if (entityType === 'purchase_order' && entityId > 0) {
      const linkedRows = await all(
        `SELECT p.nomSite, p.numeroMaison
         FROM purchase_order_items poi
         LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
         LEFT JOIN projects p ON p.id = mr.projetId
         WHERE poi.purchaseOrderId = ?`,
        [entityId]
      );
      inScope = Array.isArray(linkedRows) && linkedRows.some(scopeRow => isInChefSiteScope(req.user, scopeRow));
      if (!inScope) {
        const fallbackRow = await get(
          `SELECT p.nomSite, p.numeroMaison
           FROM purchase_orders po
           LEFT JOIN projects p ON p.id = COALESCE(po.siteId, po.projetId)
           WHERE po.id = ?`,
          [entityId]
        );
        inScope = !!fallbackRow && isInChefSiteScope(req.user, fallbackRow);
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
      inScope = !!scopedRow && isInChefSiteScope(req.user, scopedRow);
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

  const result = await run(
    'INSERT INTO auto_vehicles (nomVehicule, marqueVehicule, immatriculation, chauffeurNom, gpsActif, valeurVehicule, etatVehicule, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    [nom, marque, plaque, chauffeur, gpsEnabled, valeur, etat, new Date().toISOString()]
  );

  const vehicle = await get('SELECT * FROM auto_vehicles WHERE id = ?', [result.lastID]);
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

app.post('/api/project-assignments', async (req, res) => {
  const { projetId, projectId, userId, assigneeName, role, phoneNumber = '' } = req.body;
  const assignmentProjectId = Number(projectId || projetId);
  if (!assignmentProjectId || !role || !assigneeName || !String(assigneeName).trim()) {
    return res.status(400).json({ error: 'Champs obligatoires manquants' });
  }

  const numericUserId = Number(userId);
  const effectiveUserId = Number.isInteger(numericUserId) && numericUserId > 0 ? numericUserId : req.user.id;

  const projet = await get('SELECT id FROM projects WHERE id = ?', [assignmentProjectId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  const result = await run(
    'INSERT INTO project_assignments (projectId, userId, assigneeName, phoneNumber, role, assignedAt) VALUES (?, ?, ?, ?, ?, ?)',
    [
      assignmentProjectId,
      effectiveUserId,
      String(assigneeName).trim(),
      String(phoneNumber).trim(),
      String(role).trim(),
      new Date().toISOString(),
    ]
  );

  const assignment = await get(`
    SELECT pa.*, p.nomProjet, u.username, COALESCE(pa.assigneeName, u.username) as displayName
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projectId
    JOIN users u ON u.id = pa.userId
    WHERE pa.id = ?
  `, [result.lastID]);

  res.status(201).json(assignment);
});

app.get('/api/project-assignments', async (_req, res) => {
  const rows = await all(`
    SELECT pa.*, p.nomProjet, u.username, COALESCE(pa.assigneeName, u.username) as displayName
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projectId
    JOIN users u ON u.id = pa.userId
    ORDER BY pa.assignedAt DESC
  `);
  res.json(rows);
});

app.patch('/api/project-assignments/:id', async (req, res) => {
  const id = Number(req.params.id);
  const { assigneeName, role, phoneNumber = '' } = req.body;

  if (!id || !assigneeName || !String(assigneeName).trim() || !role || !String(role).trim()) {
    return res.status(400).json({ error: 'Nom et role sont obligatoires' });
  }

  const result = await run(
    'UPDATE project_assignments SET assigneeName = ?, role = ?, phoneNumber = ? WHERE id = ?',
    [String(assigneeName).trim(), String(role).trim(), String(phoneNumber).trim(), id]
  );

  if (result.changes === 0) {
    return res.status(404).json({ error: 'Assignation non trouvee' });
  }

  const assignment = await get(`
    SELECT pa.*, p.nomProjet, u.username, COALESCE(pa.assigneeName, u.username) as displayName
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projectId
    JOIN users u ON u.id = pa.userId
    WHERE pa.id = ?
  `, [id]);

  res.json(assignment);
});

app.delete('/api/project-assignments/:id', async (req, res) => {
  const id = Number(req.params.id);
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

  const scopedRows = String(req.user?.role || '').trim() === 'chef_chantier_site'
    ? rows.filter(row => isInChefSiteScope(req.user, row))
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
  const {
    projectFolder = '',
    materialName,
    unite = '',
    quantiteParBatiment = 0,
    prixUnitaire = 0,
    notes = '',
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
  const nextCatalogMaterialId = await getNextTableId('building_material_catalog');
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
      now,
      now,
    ]
  );

  const row = await get('SELECT * FROM building_material_catalog WHERE id = ?', [nextCatalogMaterialId || result.lastID]);
  res.status(201).json(row);
});

app.patch('/api/material-catalog/:id', async (req, res) => {
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

app.delete('/api/material-catalog/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM building_material_catalog WHERE id = ?', [id]);
  if (result.changes === 0) return res.status(404).json({ error: 'Entrée catalogue introuvable' });
  res.json({ message: 'Entrée supprimée' });
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


