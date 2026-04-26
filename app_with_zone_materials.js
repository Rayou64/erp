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
const ZONE_STOCK_MANAGER_USERNAME = process.env.ZONE_STOCK_MANAGER_USERNAME || 'gestionnaire_stock_adzope';
const ZONE_STOCK_MANAGER_ALIAS_USERNAME = process.env.ZONE_STOCK_MANAGER_ALIAS_USERNAME || 'gest_zone_adzope';
const ZONE_STOCK_MANAGER_PASSWORD = process.env.ZONE_STOCK_MANAGER_PASSWORD || 'gestadzope@2026';
const SITE_CHIEF_USERNAME = process.env.SITE_CHIEF_USERNAME || 'chef_adzope_site15';
const SITE_CHIEF_PASSWORD = process.env.SITE_CHIEF_PASSWORD || 'chefsite15@2026';
const PROCUREMENT_REVIEWER_USERNAME = process.env.PROCUREMENT_REVIEWER_USERNAME || 'controle_achat_global';
const PROCUREMENT_REVIEWER_PASSWORD = process.env.PROCUREMENT_REVIEWER_PASSWORD || 'achatglobal@2026';
const ZONE_STOCK_MANAGER_ZONE_NAME = (process.env.ZONE_STOCK_MANAGER_ZONE_NAME || 'ADZOPE').trim();
const ZONE_STOCK_MANAGER_DEFAULT_WAREHOUSE_ID = process.env.ZONE_STOCK_MANAGER_DEFAULT_WAREHOUSE_ID || 'entrepot-plateau';
const SITE_CHIEF_PROJECT_NAME = (process.env.SITE_CHIEF_PROJECT_NAME || 'PINUT').trim();
const SITE_CHIEF_ZONE_NAME = (process.env.SITE_CHIEF_ZONE_NAME || 'ADZOPE').trim();
const SITE_CHIEF_SITE_NUMBER = Number(process.env.SITE_CHIEF_SITE_NUMBER || 15);
const SITE_CHIEF_DEFAULT_WAREHOUSE_ID = process.env.SITE_CHIEF_DEFAULT_WAREHOUSE_ID || 'entrepot-plateau';
const ZONE_STOCK_MANAGER_ALLOWED_USERNAMES = new Set(
  [ZONE_STOCK_MANAGER_USERNAME, ZONE_STOCK_MANAGER_ALIAS_USERNAME]
    .map(value => String(value || '').trim())
    .filter(Boolean)
);
const API_RATE_WINDOW_MS = Number(process.env.API_RATE_WINDOW_MS || 60_000);
const API_RATE_MAX = Number(process.env.API_RATE_MAX || 600);
const AUTH_RATE_WINDOW_MS = Number(process.env.AUTH_RATE_WINDOW_MS || 15 * 60_000);
const AUTH_RATE_MAX = Number(process.env.AUTH_RATE_MAX || 25);

let isReady = false;
let isShuttingDown = false;
let server = null;

fs.mkdirSync(ARCHIVE_ROOT, { recursive: true });
app.use('/archives', express.static(ARCHIVE_ROOT));

if (JWT_SECRET === 'erp-secret-2026') {
  console.warn('Avertissement securite: JWT_SECRET par defaut detecte. Configure une valeur forte en production.');
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

  if (availableColumns.has('id')) {
    const nextIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM expenses');
    const nextId = Number(nextIdRow?.nextId || nextIdRow?.nextid || 1);
    insertColumns.push('id');
    insertValues.push(nextId);
  }

  function pushColumnIfExists(columnName, value) {
    if (availableColumns.has(columnName)) {
      insertColumns.push(columnName);
      insertValues.push(value);
    }
  }

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
  const result = await run(sql, insertValues);

  return get('SELECT * FROM expenses WHERE id = ?', [result.lastID]);
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
    path.join(__dirname, 'node_modules', '@fontsource', 'great-vibes', 'files', 'great-vibes-latin-ext-400-normal.woff'),
    path.join(__dirname, 'node_modules', '@fontsource', 'great-vibes', 'files', 'great-vibes-latin-400-normal.woff'),
    path.join(__dirname, 'public', 'fonts', 'PinyonScript-Regular.ttf'),
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
    SELECT poi.*, mr.itemName as requestItemName, mr.etapeApprovisionnement, p.nomProjet, p.numeroMaison, p.prefecture, p.typeMaison
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
      prefecture: row.prefecture || null,
      projectTypeMaison: row.typeMaison || null,
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

  // Load zone-stock project metadata for POs whose siteId is a ZONE_STOCK project
  const siteIds = Array.from(new Set(orders.map(o => Number(o.siteId || 0)).filter(id => id > 0)));
  const siteProjectMap = new Map(); // siteId → {typeMaison, prefecture}
  if (siteIds.length) {
    const sitePlaceholders = siteIds.map(() => '?').join(',');
    const siteRows = await all(
      `SELECT id, typeMaison, prefecture FROM projects WHERE id IN (${sitePlaceholders})`,
      siteIds
    );
    siteRows.forEach(row => siteProjectMap.set(Number(row.id), { typeMaison: row.typeMaison, prefecture: row.prefecture }));
  }

  return orders.map(order => {
    const items = itemsByOrderId[Number(order.id)] || [];
    const firstItem = items[0] || null;
    const totalFromItems = items.reduce((sum, item) => sum + Number(item.totalLigne || 0), 0);
    const projectNames = Array.from(new Set(items.map(item => item.projetNom).filter(Boolean)));
    const houseNumbers = Array.from(new Set(items.map(item => String(item.numeroMaison || '').trim()).filter(Boolean)));
    const zones = Array.from(new Set(items.map(item => String(item.prefecture || '').trim()).filter(Boolean)));
    const hasZoneStockItem = items.some(item => isZoneStockProjectRow({ typeMaison: item.projectTypeMaison }));
    const stages = Array.from(new Set(items.map(item => String(item.etapeApprovisionnement || '').trim()).filter(Boolean)));

    // Also detect zone orders by the PO's own siteId being a ZONE_STOCK project
    const siteProject = siteProjectMap.get(Number(order.siteId || 0)) || null;
    const isSiteZoneStock = Boolean(siteProject && isZoneStockProjectRow(siteProject));
    const resolvedIsZoneOrder = hasZoneStockItem || isSiteZoneStock;
    const resolvedZones = zones.length ? zones : (isSiteZoneStock && siteProject.prefecture ? [siteProject.prefecture] : []);

    const resolvedNomProjet = projectNames.length
      ? projectNames.join(', ')
      : (order.nomProjetManuel || order.nomProjet || '-');
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
      numeroMaison: resolvedIsZoneOrder && resolvedZones.length ? `Zone ${resolvedZones.join(', ')}` : resolvedSite,
      zoneName: resolvedZones[0] || null,
      isZoneOrder: resolvedIsZoneOrder,
      etapeApprovisionnement: String(
        order.etapeApprovisionnement
          || stages[0]
          || fallbackStageByRequestId.get(Number(order.materialRequestId || 0))
          || ''
      ).trim(),
      projetId: order.projetId || null,
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

function renderPurchaseOrderPdf(doc, order) {
  const total = Number(order.montantTotal || 0);
  const purchaseOrderTitle = buildPurchaseOrderDocumentTitle(order);
  const siteValueRaw = String(order.numeroMaison || order.nomSiteManuel || '').trim();
  const siteLabel = siteValueRaw
    ? (siteValueRaw.toLowerCase().includes('site') ? siteValueRaw : `Site Numero ${siteValueRaw}`)
    : 'Site non renseigne';
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

  doc.font('Helvetica');
  const logoPath = resolveSafariLogoPath();
  if (logoPath) {
    doc.image(logoPath, 40, 30, { fit: [200, 76], align: 'left', valign: 'top' });
  } else {
    doc.rect(40, 30, 200, 76).stroke();
    doc.fontSize(11).text('LOGO SAFARI', 95, 62, { width: 90, align: 'center' });
  }

  doc.font('Helvetica-Bold').fontSize(24).fillColor('#000000').text('SAFARI CONSTRUCTIONS', 250, 56);
  doc.font('Helvetica-Bold').fontSize(30).fillColor('#006633').text('BON DE COMMANDE', 40, 134);
  doc.fillColor('#000000');

  const infoY = 186;
  doc.font('Helvetica').fontSize(12);
  doc.text(`Nom : ${String(order.fournisseur || '').trim() || '________________________________'}`, 40, infoY);
  doc.text('Telephone : __________________________', 40, infoY + 24);
  doc.text('Email : ______________________________', 40, infoY + 48);
  doc.text('Adresse : ____________________________', 40, infoY + 72);
  doc.font('Helvetica-Bold').text(`Titre du bon : ${purchaseOrderTitle}`, 40, infoY + 96);
  doc.font('Helvetica-Bold').text(`Site : ${siteLabel}`, 40, infoY + 116);

  const tableTop = infoY + 152;
  const startX = 40;
  const widths = [145, 145, 145, 80];
  const headers = ['Article', 'Prix', 'Quantite', 'Total'];
  const rowHeight = 24;

  function drawGridRow(y, values, isHeader = false) {
    let x = startX;
    values.forEach((value, index) => {
      doc.rect(x, y, widths[index], rowHeight).stroke();
      doc.font(isHeader ? 'Helvetica-Bold' : 'Helvetica').fontSize(11).text(String(value), x + 8, y + 6, {
        width: widths[index] - 16,
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
    `${Number(item.prixUnitaire || 0).toFixed(2)}`,
    Number(item.quantite || 0).toFixed(2),
    `${Number(item.totalLigne || 0).toFixed(2)}`,
  ]);

  const minRows = 7;
  while (itemRows.length < minRows) {
    itemRows.push(['', '', '', '']);
  }

  itemRows.forEach((row, idx) => drawGridRow(tableTop + rowHeight * (idx + 1), row, false));

  doc.font('Helvetica-Bold').fontSize(12).text(`Total : ${total.toFixed(2)} EUR`, 40, tableTop + rowHeight * (itemRows.length + 1) + 20);

  // Tampon de decision en bas a gauche.
  doc.save();
  doc.lineWidth(2).strokeColor(isRejected ? '#b91c1c' : '#166534').fillColor(isRejected ? '#b91c1c' : '#166534');
  doc.roundedRect(40, 736, 160, 52, 8).stroke();
  doc.font('Helvetica-Bold').fontSize(8).text('TAMPON', 48, 742, { width: 144, align: 'left' });
  doc.font('Helvetica-Bold').fontSize(18).text(stampLabel, 48, 754, { width: 144, align: 'left' });
  doc.restore();

  // Signature a droite (style manuscrit si disponible).
  const signerName = String(order.signatureName || order.createdBy || '').trim() || 'Signature';
  const signerRole = String(order.signatureRole || '').trim();
  doc.font('Helvetica').fontSize(9).fillColor('#0f172a').text('Signature autoris\u00e9e', 335, 738, { width: 220, align: 'right' });
  doc.font(signatureFontName).fontSize(26).fillColor('#111827').text(signerName, 335, 750, { width: 220, align: 'right' });
  if (signerRole) {
    doc.font('Helvetica').fontSize(9).fillColor('#334155').text(signerRole, 335, 780, { width: 220, align: 'right' });
  }

  doc.font('Helvetica-Oblique').fontSize(9).fillColor('#555555').text('Safari Constructions - Professionnalisme & Qualite', 40, 796, {
    align: 'center',
    width: 515,
  });
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
  const nextDocumentIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM generated_documents');
  const nextDocumentId = Number(nextDocumentIdRow?.nextId || nextDocumentIdRow?.nextid || 1);
  await run(
    'INSERT INTO generated_documents (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [nextDocumentId, sectionCode, 'Achats', 'purchase_order', purchaseOrderId, `Bon de commande #${purchaseOrderId}`, fileName, relativePath, new Date().toISOString(), new Date().toISOString()]
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

  // Zone-only orders: use strict "NomProjet-NomZone" format
  const isZoneOrder = Boolean(order?.isZoneOrder);
  if (!stageRaw || isZoneOrder) {
    const nomProjet = String(order?.nomProjet || order?.nomProjetManuel || '').trim();
    const zonePart = String(order?.zoneName || '').trim()
      || String(order?.nomSiteManuel || '').replace(/^zone\s*/i, '').trim()
      || String(order?.numeroMaison || '').replace(/^zone\s*/i, '').trim();
    if (nomProjet) {
      return zonePart ? `${nomProjet}-${zonePart}` : nomProjet;
    }
  }

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
  const nextDocumentIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM generated_documents');
  const nextDocumentId = Number(nextDocumentIdRow?.nextId || nextDocumentIdRow?.nextid || 1);
  await run(
    'INSERT INTO generated_documents (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, pdf_data, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [nextDocumentId, sectionCode, 'Achats', 'purchase_order', purchaseOrderId, documentTitle, fileName, relativePath, buffer, new Date().toISOString(), new Date().toISOString()]
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
  const { order, request, items, signatureName, signatureRole, signedAt, decisionStatus = 'VALIDEE' } = payload;
  // Project is always PINUT
  const projectTitle = 'PINUT';
  const siteValueRaw = String(request.numeroMaison || request.nomSite || '').trim();
  const siteLabel = siteValueRaw
    ? (siteValueRaw.toLowerCase().includes('site') ? siteValueRaw : `Site Numero ${siteValueRaw}`)
    : 'Site non renseigne';
  const normalizedDecision = String(decisionStatus || 'VALIDEE').toUpperCase();
  const isRejected = normalizedDecision === 'REJETEE' || normalizedDecision === 'ANNULEE';
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

  // === HEADER (compact, absolute positions) ===
  doc.font('Helvetica-Bold').fontSize(17).fillColor('#000000').text('AUTORISATION DE RETRAIT DE MATERIEL', 40, 36, { width: 515, align: 'center' });

  // Info line: project / site / date / BC# in two columns
  doc.font('Helvetica-Bold').fontSize(10).fillColor('#000000').text('Projet :', 40, 66);
  doc.font('Helvetica').fontSize(10).text(projectTitle, 105, 66);
  doc.font('Helvetica-Bold').fontSize(10).text('Site :', 300, 66);
  doc.font('Helvetica').fontSize(10).text(siteLabel, 340, 66);

  doc.font('Helvetica-Bold').fontSize(10).text('Date :', 40, 82);
  doc.font('Helvetica').fontSize(10).text(new Date(signedAt || Date.now()).toLocaleDateString('fr-FR'), 105, 82);
  doc.font('Helvetica-Bold').fontSize(10).text('Bon de commande :', 300, 82);
  doc.font('Helvetica').fontSize(10).text(`#${order.id}`, 440, 82);

  // Separator
  doc.moveTo(40, 100).lineTo(555, 100).lineWidth(1).strokeColor('#cccccc').stroke();

  // === INFORMATIONS DEMANDE ===
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#000000').text('Informations de la demande', 40, 110);

  doc.font('Helvetica-Bold').fontSize(10).text('Demandeur :', 40, 128);
  doc.font('Helvetica').fontSize(10).text(String(request.demandeur || '-').trim() || '-', 130, 128);
  doc.font('Helvetica-Bold').fontSize(10).text('\u00c9tape :', 300, 128);
  doc.font('Helvetica').fontSize(10).text(String(request.etapeApprovisionnement || '-').trim() || '-', 345, 128);

  doc.font('Helvetica-Bold').fontSize(10).text('Entrepot :', 40, 144);
  doc.font('Helvetica').fontSize(10).text(resolveWarehouseLabel(request.warehouseId), 130, 144);

  // Separator
  doc.moveTo(40, 162).lineTo(555, 162).lineWidth(1).strokeColor('#cccccc').stroke();

  // === TABLE MATERIEL AUTORISE ===
  doc.font('Helvetica-Bold').fontSize(11).fillColor('#000000').text('Mat\u00e9riel autoris\u00e9', 40, 172);

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
  doc.font('Helvetica').fontSize(9).fillColor('#555555').text(`Sign\u00e9 le : ${new Date(signedAt || Date.now()).toLocaleString('fr-FR')}`, 40, footerY + 10);

  // Signature (right)
  const sigName = String(signatureName || '').trim() || 'Signature';
  doc.font('Helvetica').fontSize(9).fillColor('#0f172a').text('Signature autoris\u00e9e', 335, footerY + 10, { width: 220, align: 'right' });
  doc.font(signatureFontName).fontSize(26).fillColor('#111827').text(sigName, 335, footerY + 22, { width: 220, align: 'right' });
  if (String(signatureRole || '').trim()) {
    doc.font('Helvetica').fontSize(9).fillColor('#334155').text(String(signatureRole || '').trim(), 335, footerY + 54, { width: 220, align: 'right' });
  }

  // Tampon (left)
  doc.save();
  doc.lineWidth(2).strokeColor(isRejected ? '#b91c1c' : '#166534').fillColor(isRejected ? '#b91c1c' : '#166534');
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
  const nextDocumentIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM generated_documents');
  const nextDocumentId = Number(nextDocumentIdRow?.nextId || nextDocumentIdRow?.nextid || 1);
  const result = await run(
    'INSERT INTO generated_documents (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, pdf_data, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextDocumentId,
      sectionCode,
      getArchiveSectionLabel(sectionCode),
      'material_request_authorization',
      request.id,
      formTitle,
      fileName,
      relativePath,
      buffer,
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
  const nextDocumentIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM generated_documents');
  const nextDocumentId = Number(nextDocumentIdRow?.nextId || nextDocumentIdRow?.nextid || 1);
  await run(
    'INSERT INTO generated_documents (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, pdf_data, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [nextDocumentId, sectionCode, sectionLabel, 'stock_issue_authorization', authorization.id, title, fileName, relativePath, buffer, new Date().toISOString(), new Date().toISOString()]
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
  const nextDocumentIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM generated_documents');
  const nextDocumentId = Number(nextDocumentIdRow?.nextId || nextDocumentIdRow?.nextid || 1);
  await run(
    'INSERT INTO generated_documents (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, pdf_data, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextDocumentId,
      sectionCode,
      getArchiveSectionLabel(sectionCode),
      'revenue',
      revenueId,
      documentTitle,
      fileName,
      relativePath,
      buffer,
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
  const nextDocumentIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM generated_documents');
  const nextDocumentId = Number(nextDocumentIdRow?.nextId || nextDocumentIdRow?.nextid || 1);
  const result = await run(
    'INSERT INTO generated_documents (id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, pdf_data, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [nextDocumentId, safeSection, getArchiveSectionLabel(safeSection), 'manual_upload', linkedEntityId, title || safeName, finalFileName, relativePath, fileBuffer, now, now]
  );

  return {
    id: result.lastID || nextDocumentId,
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
  for (const item of sourceItems) {
    const nextRequestIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM material_requests');
    const nextRequestId = Number(nextRequestIdRow?.nextId || nextRequestIdRow?.nextid || 1);
    const inserted = await run(
      'INSERT INTO material_requests (id, projetId, demandeur, etapeApprovisionnement, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut, warehouseId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        nextRequestId,
        projectId,
        requester,
        String(order.etapeApprovisionnement || '').trim() || 'BON_COMMANDE',
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
     await run(`ALTER TABLE project_folders ADD COLUMN projectId INTEGER`);
  } catch (e) {}
  try {
     await run(`ALTER TABLE project_folders ADD COLUMN prefecture TEXT NOT NULL DEFAULT 'Non renseigne'`);
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
    await run(`ALTER TABLE material_requests ADD COLUMN itemName TEXT NOT NULL DEFAULT ''`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE material_requests ADD COLUMN quantiteDemandee REAL NOT NULL DEFAULT 0`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE material_requests ADD COLUMN quantiteRestante REAL NOT NULL DEFAULT 0`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE material_requests ADD COLUMN etapeApprovisionnement TEXT NOT NULL DEFAULT ''`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE material_requests ADD COLUMN groupId TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE material_requests ADD COLUMN warehouseId TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE projects ADD COLUMN description TEXT DEFAULT ''`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE projects ADD COLUMN typeMaison TEXT NOT NULL DEFAULT ''`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE projects ADD COLUMN numeroMaison TEXT NOT NULL DEFAULT ''`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN creePar TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN montantTotal REAL`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN statutValidation TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN quantiteCommandee REAL NOT NULL DEFAULT 1`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN prixUnitaire REAL NOT NULL DEFAULT 0`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN dateLivraisonPrevue TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN dateReception TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN creePar TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN montantTotal REAL`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN statutValidation TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN projetId INTEGER`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN nomProjetManuel TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN siteId INTEGER`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN nomSiteManuel TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN warehouseId TEXT`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN statut TEXT NOT NULL DEFAULT 'EN_COURS'`);
  } catch (e) {}
  try {
    await run(`ALTER TABLE purchase_orders ADD COLUMN etapeApprovisionnement TEXT`);
  } catch (e) {}
  try {
    await run(`UPDATE purchase_orders SET creePar = COALESCE(creePar, 'admin')`);
  } catch (e) {}
  try {
    await run(`UPDATE purchase_orders SET montantTotal = COALESCE(montantTotal, quantiteCommandee * prixUnitaire, 0)`);
  } catch (e) {}
  try {
    await run(`UPDATE purchase_orders SET statutValidation = COALESCE(statutValidation, statut, 'EN_COURS')`);
  } catch (e) {}
  try {
    await run(`UPDATE purchase_orders SET statut = COALESCE(statut, statutValidation, 'EN_COURS')`);
  } catch (e) {}
  try {
    await run(`UPDATE purchase_orders SET quantiteCommandee = COALESCE(quantiteCommandee, 1)`);
  } catch (e) {}
  try {
    await run(`UPDATE purchase_orders SET prixUnitaire = COALESCE(prixUnitaire, montantTotal, 0)`);
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
    await run(`UPDATE expenses SET categorie = COALESCE(categorie, category, 'autres')`);
  } catch (e) {}
  try {
    await run(`UPDATE expenses SET statut = COALESCE(statut, 'EN_ATTENTE')`);
  } catch (e) {}
  try {
    await run(`UPDATE expenses SET createdBy = COALESCE(createdBy, 'admin')`);
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
    await run('ALTER TABLE purchase_orders ALTER COLUMN materialRequestId DROP NOT NULL');
  } catch (e) {}
  try {
    await run('ALTER TABLE purchase_order_items ALTER COLUMN materialRequestId DROP NOT NULL');
  } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS purchase_orders (
    id INTEGER PRIMARY KEY,
    materialRequestId INTEGER NOT NULL,
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
    materialRequestId INTEGER NOT NULL,
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

  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN vehicleId INTEGER NOT NULL DEFAULT 0`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN deviceName TEXT NOT NULL DEFAULT 'smartphone'`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN tokenHash TEXT NOT NULL DEFAULT ''`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN isActive INTEGER NOT NULL DEFAULT 1`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN lastSeenAt TEXT`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN lastLatitude REAL`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN lastLongitude REAL`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN lastSpeedKph REAL NOT NULL DEFAULT 0`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN createdBy TEXT NOT NULL DEFAULT 'system'`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''`); } catch (error) {}
  try { await run(`ALTER TABLE auto_tracking_devices ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''`); } catch (error) {}

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
    await run(`ALTER TABLE auto_transport_costs ADD COLUMN expenseId INTEGER`);
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

  try { await run(`ALTER TABLE generated_documents ADD COLUMN sectionCode TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN sectionLabel TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN entityType TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN entityId INTEGER NOT NULL DEFAULT 0`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN title TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN fileName TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN relativePath TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN createdAt TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN updatedAt TEXT NOT NULL DEFAULT ''`); } catch (e) {}
  try { await run(`ALTER TABLE generated_documents ADD COLUMN pdf_data BYTEA`); } catch (e) {}

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
  try { await run(`ALTER TABLE project_progress_updates ADD COLUMN materialUsageDetails TEXT NOT NULL DEFAULT '[]'`); } catch (e) {}
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
        warehouseId TEXT,
        etapeApprovisionnement TEXT,
        signatureName TEXT,
        signatureRole TEXT
      )`);
      // Copier en listant les colonnes connues
      const oldCols = poCols.map(c => c.name);
      const newColNames = ['id','materialRequestId','fournisseur','quantiteCommandee','prixUnitaire','dateCommande','dateLivraisonPrevue','dateReception','statut','creePar','montantTotal','statutValidation','projetId','nomProjetManuel','siteId','nomSiteManuel','warehouseId','etapeApprovisionnement','signatureName','signatureRole'];
      const copyColNames = newColNames.filter(c => oldCols.includes(c));
      const colList = copyColNames.join(', ');
      await run(`INSERT INTO purchase_orders (${colList}) SELECT ${colList} FROM purchase_orders_old`);
      await run('DROP TABLE purchase_orders_old');
    }
  } catch (e) {}
  try { await run('ALTER TABLE purchase_orders ADD COLUMN warehouseId TEXT'); } catch (e) {}
  try { await run('ALTER TABLE purchase_orders ADD COLUMN etapeApprovisionnement TEXT'); } catch (e) {}
  try { await run('ALTER TABLE purchase_orders ADD COLUMN signatureName TEXT'); } catch (e) {}
  try { await run('ALTER TABLE purchase_orders ADD COLUMN signatureRole TEXT'); } catch (e) {}

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

  // Créer utilisateur admin par défaut
  const admin = await get('SELECT * FROM users WHERE username = ?', ['admin']);
  if (!admin) {
    const nextAdminIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM users');
    const nextAdminId = Number(nextAdminIdRow?.nextId || nextAdminIdRow?.nextid || 1);
    const hashedPassword = await bcrypt.hash('admin123', 10);
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextAdminId, 'admin', hashedPassword, 'admin', new Date().toISOString()]
    );
    console.log('Utilisateur admin créé avec mot de passe admin123');
  }

  // Garantir un compte commis_stock toujours opérationnel (local + Railway)
  const commis = await get('SELECT id FROM users WHERE username = ?', [COMMIS_STOCK_USERNAME]);
  const commisHashedPassword = await bcrypt.hash(COMMIS_STOCK_PASSWORD, 10);
  if (!commis) {
    const nextCommisIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM users');
    const nextCommisId = Number(nextCommisIdRow?.nextId || nextCommisIdRow?.nextid || 1);
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextCommisId, COMMIS_STOCK_USERNAME, commisHashedPassword, 'commis', new Date().toISOString()]
    );
    console.log(`Utilisateur ${COMMIS_STOCK_USERNAME} créé avec role commis`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [commisHashedPassword, 'commis', COMMIS_STOCK_USERNAME]
    );
    console.log(`Utilisateur ${COMMIS_STOCK_USERNAME} mis a jour avec role commis`);
  }

  // Supprimer les profils retires du lien public (gestionnaire_stock et chef site 15)
  const removedPublicProfiles = await run(
    'DELETE FROM users WHERE role = ? OR username IN (?, ?)',
    ['gestionnaire_stock', GEST_STOCK_USERNAME, SITE_CHIEF_USERNAME]
  );
  if (Number(removedPublicProfiles?.changes || 0) > 0) {
    console.log(`Profils publics supprimes: ${Number(removedPublicProfiles.changes || 0)}`);
  }

  // Garantir les identifiants gestionnaire de stock pour la zone Adzope
  const zoneStockManagerHashedPassword = await bcrypt.hash(ZONE_STOCK_MANAGER_PASSWORD, 10);
  const zoneStockManagerUsernames = Array.from(ZONE_STOCK_MANAGER_ALLOWED_USERNAMES);
  for (const zoneUsername of zoneStockManagerUsernames) {
    const zoneStockManager = await get('SELECT id FROM users WHERE username = ?', [zoneUsername]);
    if (!zoneStockManager) {
      const nextZoneManagerIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM users');
      const nextZoneManagerId = Number(nextZoneManagerIdRow?.nextId || nextZoneManagerIdRow?.nextid || 1);
      await run(
        'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
        [nextZoneManagerId, zoneUsername, zoneStockManagerHashedPassword, 'gestionnaire_stock_zone', new Date().toISOString()]
      );
      console.log(`Utilisateur ${zoneUsername} créé avec role gestionnaire_stock_zone`);
    } else {
      await run(
        'UPDATE users SET password = ?, role = ? WHERE username = ?',
        [zoneStockManagerHashedPassword, 'gestionnaire_stock_zone', zoneUsername]
      );
      console.log(`Utilisateur ${zoneUsername} mis a jour avec role gestionnaire_stock_zone`);
    }
  }

  // Nettoyer les comptes legacy de zone tout en conservant les identifiants autorisés.
  const cleanupPlaceholders = zoneStockManagerUsernames.map(() => '?').join(', ');
  const cleanupResult = await run(
    `DELETE FROM users WHERE role = ? AND username NOT IN (${cleanupPlaceholders})`,
    ['gestionnaire_stock_zone', ...zoneStockManagerUsernames]
  );
  if (Number(cleanupResult?.changes || 0) > 0) {
    console.log(`Comptes gestionnaire_stock_zone legacy supprimes: ${Number(cleanupResult.changes || 0)}`);
  }

  // Garantir un compte de controle achat global (lecture + validation sorties)
  const procurementReviewer = await get('SELECT id FROM users WHERE username = ?', [PROCUREMENT_REVIEWER_USERNAME]);
  const procurementReviewerPassword = await bcrypt.hash(PROCUREMENT_REVIEWER_PASSWORD, 10);
  if (!procurementReviewer) {
    const nextReviewerIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM users');
    const nextReviewerId = Number(nextReviewerIdRow?.nextId || nextReviewerIdRow?.nextid || 1);
    await run(
      'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
      [nextReviewerId, PROCUREMENT_REVIEWER_USERNAME, procurementReviewerPassword, 'controle_achat', new Date().toISOString()]
    );
    console.log(`Utilisateur ${PROCUREMENT_REVIEWER_USERNAME} créé avec role controle_achat`);
  } else {
    await run(
      'UPDATE users SET password = ?, role = ? WHERE username = ?',
      [procurementReviewerPassword, 'controle_achat', PROCUREMENT_REVIEWER_USERNAME]
    );
    console.log(`Utilisateur ${PROCUREMENT_REVIEWER_USERNAME} mis a jour avec role controle_achat`);
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
    if (ZONE_STOCK_MANAGER_ALLOWED_USERNAMES.has(String(user?.username || '').trim())) {
      user.role = 'gestionnaire_stock_zone';
    }
    if (String(user?.username || '').trim() === SITE_CHIEF_USERNAME) {
      user.role = 'chef_chantier_site';
    }
    req.user = user;
    next();
  });
}

function normalizeScopeText(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();
}

function extractSiteNumber(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const match = raw.match(/\d+/);
  return match ? Number(match[0]) : null;
}

function isSiteChiefRole(role) {
  return String(role || '').trim() === 'chef_chantier_site';
}

function isZoneStockManagerRole(role) {
  return String(role || '').trim() === 'gestionnaire_stock_zone';
}

async function getProjectIdsForZone(zoneName, options = {}) {
  const rows = await all(
    `SELECT id, nomProjet, prefecture, nomSite, numeroMaison, typeMaison
     FROM projects`
  );

  const expectedZone = normalizeScopeText(zoneName);
  const expectedProject = normalizeScopeText(options.projectName);
  const expectedSiteNumber = extractSiteNumber(options.siteNumber);
  const includeZoneStock = options.includeZoneStock !== false;

  return rows
    .filter(row => normalizeScopeText(row.prefecture) === expectedZone)
    .filter(row => includeZoneStock || !isZoneStockProjectRow(row))
    .filter(row => !expectedProject || normalizeScopeText(row.nomProjet) === expectedProject)
    .filter(row => {
      if (!Number.isFinite(expectedSiteNumber)) return true;
      const numberFromNumeroMaison = extractSiteNumber(row.numeroMaison);
      const numberFromNomSite = extractSiteNumber(row.nomSite);
      const resolved = Number.isFinite(numberFromNumeroMaison) ? numberFromNumeroMaison : numberFromNomSite;
      return Number.isFinite(resolved) && Number(resolved) === expectedSiteNumber;
    })
    .map(row => Number(row.id))
    .filter(id => Number.isInteger(id) && id > 0);
}

async function getSiteChiefScopedProjectIds() {
  return getProjectIdsForZone(SITE_CHIEF_ZONE_NAME, {
    includeZoneStock: false,
    projectName: SITE_CHIEF_PROJECT_NAME,
    siteNumber: SITE_CHIEF_SITE_NUMBER,
  });
}

async function getZoneStockManagerScopedProjectIds() {
  return getProjectIdsForZone(ZONE_STOCK_MANAGER_ZONE_NAME, {
    includeZoneStock: true,
  });
}

function getUserOperationalScope(user) {
  if (isSiteChiefRole(user?.role)) {
    return {
      warehouseId: SITE_CHIEF_DEFAULT_WAREHOUSE_ID,
      projectName: SITE_CHIEF_PROJECT_NAME,
      zoneName: SITE_CHIEF_ZONE_NAME,
      siteNumber: SITE_CHIEF_SITE_NUMBER,
    };
  }

  if (isZoneStockManagerRole(user?.role)) {
    return {
      warehouseId: ZONE_STOCK_MANAGER_DEFAULT_WAREHOUSE_ID,
      zoneName: ZONE_STOCK_MANAGER_ZONE_NAME,
      siteNumber: null,
      projectName: null,
    };
  }

  return null;
}

async function getScopedProjectIdsForUser(user) {
  if (isSiteChiefRole(user?.role)) {
    return getSiteChiefScopedProjectIds();
  }
  if (isZoneStockManagerRole(user?.role)) {
    return getZoneStockManagerScopedProjectIds();
  }
  return null;
}

async function isProjectAllowedForUser(user, projectId) {
  const numericProjectId = Number(projectId || 0);
  if (!numericProjectId) return false;

  const scopedIds = await getScopedProjectIdsForUser(user);
  if (scopedIds === null) return true;
  return scopedIds.includes(numericProjectId);
}

function getAllowedWarehouseIdsForUser(user) {
  const scope = getUserOperationalScope(user);
  const warehouseId = String(scope?.warehouseId || '').trim();
  return warehouseId ? [warehouseId] : null;
}

function isWarehouseAllowedForUser(user, warehouseId) {
  const allowedIds = getAllowedWarehouseIdsForUser(user);
  if (allowedIds === null) return true;
  const normalizedWarehouseId = String(warehouseId || '').trim();
  return allowedIds.includes(normalizedWarehouseId);
}

async function getGeneratedDocumentProjectIds(row) {
  const entityType = String(row?.entityType || '').trim().toLowerCase();
  const entityId = Number(row?.entityId || 0);
  if (!entityType || !Number.isInteger(entityId) || entityId <= 0) {
    return [];
  }

  if (entityType === 'manual_upload' || entityType === 'upload') {
    return [entityId];
  }

  if (entityType === 'revenue') {
    const revenue = await get('SELECT projetId FROM revenues WHERE id = ?', [entityId]);
    const projectId = Number(revenue?.projetId || 0);
    return projectId > 0 ? [projectId] : [];
  }

  if (entityType === 'purchase_order') {
    const order = await getPurchaseOrderById(entityId);
    if (!order) {
      return [];
    }
    const ids = new Set();
    const siteId = Number(order.siteId || 0);
    const projectId = Number(order.projetId || 0);
    if (siteId > 0) ids.add(siteId);
    if (projectId > 0) ids.add(projectId);
    return Array.from(ids);
  }

  return [];
}

async function isGeneratedDocumentAllowedForUser(user, row) {
  const scopedIds = await getScopedProjectIdsForUser(user);
  if (scopedIds === null) {
    return true;
  }

  const allowed = new Set(scopedIds);
  const projectIds = await getGeneratedDocumentProjectIds(row);
  if (!projectIds.length) {
    return false;
  }

  return projectIds.some(projectId => allowed.has(Number(projectId)));
}

function authorizeRoleAccess(req, res, next) {
  const role = req.user && req.user.role;
  if (role !== 'commis' && role !== 'gestionnaire_stock' && role !== 'gestionnaire_stock_zone' && role !== 'chef_chantier_site' && role !== 'controle_achat') {
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
    { method: 'GET',   pattern: /^\/stock-issue-authorizations\/\d+\/pdf$/ },
    { method: 'GET',   pattern: /^\/transfer-authorizations$/ },
  ];

  const zoneStockManagerRules = [
    { method: 'GET',   pattern: /^\/auth\/me$/ },
    { method: 'GET',   pattern: /^\/projects$/ },
    { method: 'GET',   pattern: /^\/project-folders$/ },
    { method: 'GET',   pattern: /^\/project-catalog$/ },
    { method: 'GET',   pattern: /^\/project-progress$/ },
    { method: 'GET',   pattern: /^\/material-requests$/ },
    { method: 'POST',  pattern: /^\/material-requests$/ },
    { method: 'POST',  pattern: /^\/material-requests\/auto-stage$/ },
    { method: 'GET',   pattern: /^\/stock-management\/orders$/ },
    { method: 'PATCH', pattern: /^\/stock-management\/orders\/\d+\/arrive$/ },
    { method: 'GET',   pattern: /^\/stock-management\/available$/ },
    { method: 'GET',   pattern: /^\/stock-management\/issues$/ },
    { method: 'POST',  pattern: /^\/stock-management\/issues$/ },
    { method: 'GET',   pattern: /^\/stock-issue-authorizations$/ },
    { method: 'GET',   pattern: /^\/stock-issue-authorizations\/\d+\/pdf$/ },
    { method: 'GET',   pattern: /^\/material-catalog$/ },
    { method: 'GET',   pattern: /^\/database-documents$/ },
    { method: 'DELETE',pattern: /^\/database-documents\/\d+$/ },
    { method: 'GET',   pattern: /^\/database-documents\/\d+\/download$/ },
    { method: 'DELETE',pattern: /^\/stock-management\/orders\/\d+$/ },
  ];

  const siteChiefRules = [
    { method: 'GET',   pattern: /^\/auth\/me$/ },
    { method: 'GET',   pattern: /^\/projects$/ },
    { method: 'GET',   pattern: /^\/project-folders$/ },
    { method: 'GET',   pattern: /^\/material-requests$/ },
    { method: 'POST',  pattern: /^\/material-requests$/ },
    { method: 'POST',  pattern: /^\/material-requests\/auto-stage$/ },
    { method: 'DELETE',pattern: /^\/material-requests\/\d+$/ },
    { method: 'GET',   pattern: /^\/stock-management\/orders$/ },
    { method: 'GET',   pattern: /^\/stock-management\/available$/ },
    { method: 'GET',   pattern: /^\/stock-management\/issues$/ },
    { method: 'POST',  pattern: /^\/stock-management\/issues$/ },
    { method: 'GET',   pattern: /^\/stock-issue-authorizations$/ },
    { method: 'POST',  pattern: /^\/stock-issue-authorizations$/ },
    { method: 'GET',   pattern: /^\/stock-issue-authorizations\/\d+\/pdf$/ },
    { method: 'GET',   pattern: /^\/material-requests\/\d+\/authorization-documents$/ },
    { method: 'GET',   pattern: /^\/material-catalog$/ },
    { method: 'GET',   pattern: /^\/expenses$/ },
    { method: 'GET',   pattern: /^\/project-progress$/ },
    { method: 'POST',  pattern: /^\/project-progress$/ },
    { method: 'GET',   pattern: /^\/database-documents$/ },
    { method: 'POST',  pattern: /^\/database-documents\/upload$/ },
    { method: 'DELETE',pattern: /^\/database-documents\/\d+$/ },
    { method: 'GET',   pattern: /^\/database-documents\/\d+\/download$/ },
  ];

  const procurementReviewerRules = [
    { method: 'GET',   pattern: /^\/projects$/ },
    { method: 'GET',   pattern: /^\/project-assignments$/ },
    { method: 'GET',   pattern: /^\/project-folders$/ },
    { method: 'GET',   pattern: /^\/project-catalog$/ },
    { method: 'GET',   pattern: /^\/material-requests$/ },
    { method: 'POST',  pattern: /^\/material-requests\/create-po-from-group$/ },
    { method: 'PATCH', pattern: /^\/material-requests\/\d+\/statut$/ },
    { method: 'POST',  pattern: /^\/material-requests\/group-authorization$/ },
    { method: 'POST',  pattern: /^\/purchase-orders$/ },
    { method: 'GET',   pattern: /^\/purchase-orders$/ },
    { method: 'GET',   pattern: /^\/purchase-orders\/\d+\/pdf$/ },
    { method: 'PATCH', pattern: /^\/purchase-orders\/\d+$/ },
    { method: 'PATCH', pattern: /^\/purchase-orders\/\d+\/validation$/ },
    { method: 'DELETE',pattern: /^\/purchase-orders\/\d+$/ },
    { method: 'GET',   pattern: /^\/purchase-orders\/\d+\/authorization-documents$/ },
    { method: 'GET',   pattern: /^\/transfer-authorizations$/ },
    { method: 'GET',   pattern: /^\/stock-management\/orders$/ },
    { method: 'GET',   pattern: /^\/stock-management\/available$/ },
    { method: 'GET',   pattern: /^\/stock-management\/issues$/ },
    { method: 'GET',   pattern: /^\/stock-issue-authorizations$/ },
    { method: 'PATCH', pattern: /^\/stock-issue-authorizations\/\d+\/decision$/ },
    { method: 'GET',   pattern: /^\/stock-issue-authorizations\/\d+\/pdf$/ },
    { method: 'GET',   pattern: /^\/material-catalog$/ },
    { method: 'GET',   pattern: /^\/database-documents$/ },
    { method: 'GET',   pattern: /^\/database-documents\/\d+\/download$/ },
  ];

  const rules = role === 'gestionnaire_stock'
    ? gestStockRules
    : (role === 'gestionnaire_stock_zone'
      ? zoneStockManagerRules
    : (role === 'chef_chantier_site'
      ? siteChiefRules
      : (role === 'controle_achat' ? procurementReviewerRules : commisRules)));
  const isAllowed = rules.some(rule => rule.method === method && rule.pattern.test(pathName));
  if (isAllowed) {
    return next();
  }

  return res.status(403).json({ error: 'Acces refuse pour ce role' });
}

function hashTrackingToken(rawToken) {
  return crypto.createHash('sha256').update(String(rawToken || '')).digest('hex');
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

  const requestedUsername = String(username || '').trim();
  let user = await get('SELECT * FROM users WHERE username = ?', [requestedUsername]);
  if (!user && requestedUsername === ZONE_STOCK_MANAGER_ALIAS_USERNAME) {
    user = await get('SELECT * FROM users WHERE username = ?', [ZONE_STOCK_MANAGER_USERNAME]);
  }
  if (!user) {
    return res.status(401).json({ error: 'Utilisateur ou mot de passe invalide' });
  }

  let valid = await bcrypt.compare(password, user.password);
  if (!valid && requestedUsername === ZONE_STOCK_MANAGER_ALIAS_USERNAME) {
    const fallbackZonePassword = 'gestadzope@2026';
    valid = String(password || '') === String(ZONE_STOCK_MANAGER_PASSWORD || '')
      || String(password || '') === fallbackZonePassword;
  }
  if (!valid) {
    return res.status(401).json({ error: 'Utilisateur ou mot de passe invalide' });
  }

  const normalizedUsername = requestedUsername || String(user.username || '').trim();
  const effectiveRole = normalizedUsername === SITE_CHIEF_USERNAME
    ? 'chef_chantier_site'
    : (ZONE_STOCK_MANAGER_ALLOWED_USERNAMES.has(normalizedUsername)
      ? 'gestionnaire_stock_zone'
      : user.role);

  const token = jwt.sign({ id: user.id, username: normalizedUsername, role: effectiveRole }, JWT_SECRET, {
    expiresIn: '6h'
  });

  res.json({ token, username: normalizedUsername });
});

app.get('/api/auth/me', authenticateToken, async (req, res) => {
  const scopedProjectIds = await getScopedProjectIdsForUser(req.user);
  const scopeConfig = getUserOperationalScope(req.user);
  const scope = scopedProjectIds === null
    ? null
    : {
        projectIds: scopedProjectIds,
        warehouseId: scopeConfig?.warehouseId || null,
        projectName: scopeConfig?.projectName || null,
        zoneName: scopeConfig?.zoneName || null,
        siteNumber: scopeConfig?.siteNumber || null,
      };

  res.json({ username: req.user.username, role: req.user.role, scope });
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
    demandeur,
    etapeApprovisionnement = '',
    warehouseId = '',
    description = '',
    dateDemande = null,
    lines = [],
    zoneName = null,
    nomProjet = null,
  } = req.body || {};

  let projectId = Number(projetId || 0);
  
  // Resolve project from zone if needed
  if (!projectId && zoneName && nomProjet) {
    const zoneProject = await ensureZoneStockProject(nomProjet, zoneName);
    if (zoneProject) {
      projectId = Number(zoneProject.id || 0) || null;
    }
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

  if (!(await isProjectAllowedForUser(req.user, projectId))) {
    return res.status(403).json({ error: 'Acces refuse: ce compte ne peut agir que sur son site' });
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
  const nowIso = dateDemande ? new Date(dateDemande).toISOString() : new Date().toISOString();
  const createdRequests = [];
  let nextMaterialRequestIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM material_requests');
  let nextMaterialRequestId = Number(nextMaterialRequestIdRow?.nextId || nextMaterialRequestIdRow?.nextid || 1);

  for (const entry of sourceLines) {
    const inserted = await run(
      'INSERT INTO material_requests (id, projetId, demandeur, etapeApprovisionnement, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut, groupId, warehouseId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        nextMaterialRequestId,
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
    nextMaterialRequestId += 1;
  }
  const createdRows = await all(
    `SELECT mr.*, p.nomProjet as projetNom, p.numeroMaison, p.typeMaison
     FROM material_requests mr
     JOIN projects p ON p.id = mr.projetId
     WHERE mr.groupId = ?
     ORDER BY mr.id ASC`,
    [groupId]
  );

  res.status(201).json({
    groupId,
    stage: stageRaw,
    projetId: projectId,
    createdRequests: createdRows,
    message: `Demande d'approvisionnement generee automatiquement pour l'etape ${stageRaw}. Disponible dans les propositions depuis les demandes.`,
  });
});

app.post('/api/material-requests/create-po-from-group', async (req, res) => {
  const { requestIds } = req.body || {};
  const ids = (Array.isArray(requestIds) ? requestIds : []).map(Number).filter(id => id > 0);
  if (!ids.length) return res.status(400).json({ error: 'requestIds est obligatoire' });

  const placeholders = ids.map(() => '?').join(',');
  const requests = await all(
    `SELECT mr.*, p.nomProjet, p.numeroMaison FROM material_requests mr JOIN projects p ON p.id = mr.projetId WHERE mr.id IN (${placeholders})`,
    ids
  );
  if (!requests.length) return res.status(404).json({ error: 'Demandes introuvables' });

  // Check if any request is already linked to a PO
  const linkedItems = await all(
    `SELECT DISTINCT materialRequestId FROM purchase_order_items WHERE materialRequestId IN (${placeholders})`,
    ids
  );
  const linkedIds = new Set(linkedItems.map(r => Number(r.materialRequestId)));
  const alreadyLinked = ids.filter(id => linkedIds.has(id));
  if (alreadyLinked.length) {
    return res.status(409).json({ error: 'Ces demandes sont déjà liées à un bon de commande existant' });
  }

  const first = requests[0];
  const projectId = Number(first.projetId);
  const projectFolder = String(first.nomProjet || '').trim();
  const stageRaw = String(first.etapeApprovisionnement || '').trim();
  const warehouseId = String(first.warehouseId || '').trim();
  const nowIso = new Date().toISOString();
  const createdBy = req.user ? req.user.username : 'system';

  // Load catalog for price info
  const catalogRows = await all(
    'SELECT * FROM building_material_catalog WHERE projectFolder = ?',
    [projectFolder]
  );
  const catalogByName = new Map(
    (catalogRows || []).map(entry => [String(entry.materialName || '').trim().toLowerCase(), entry])
  );

  const orderLines = requests.map(request => {
    const catalogEntry = catalogByName.get(String(request.itemName || '').trim().toLowerCase());
    const prixUnitaire = Number(catalogEntry?.prixUnitaire || 0);
    const quantite = Number(request.quantiteDemandee || 0);
    return {
      materialRequestId: Number(request.id),
      article: String(request.itemName || ''),
      quantite,
      prixUnitaire,
      totalLigne: quantite * prixUnitaire,
    };
  });

  const computedTotal = orderLines.reduce((sum, line) => sum + line.totalLigne, 0);
  const firstRequestId = orderLines[0].materialRequestId;

  const nextPurchaseOrderIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_orders');
  const nextPurchaseOrderId = Number(nextPurchaseOrderIdRow?.nextId || nextPurchaseOrderIdRow?.nextid || 1);

  const orderInsert = await run(
    'INSERT INTO purchase_orders (id, materialRequestId, creePar, fournisseur, montantTotal, quantiteCommandee, prixUnitaire, dateCommande, dateLivraisonPrevue, statut, statutValidation, projetId, nomProjetManuel, siteId, nomSiteManuel, warehouseId, etapeApprovisionnement, signatureName, signatureRole) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [nextPurchaseOrderId, firstRequestId, createdBy, 'FOURNISSEUR_A_DEFINIR', computedTotal, Number(orderLines[0].quantite || 0), Number(orderLines[0].prixUnitaire || 0), nowIso, null, 'EN_COURS', 'EN_COURS', projectId, projectFolder || null, projectId, null, warehouseId, stageRaw, null, null]
  );

  for (const line of orderLines) {
    const nextItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_order_items');
    const nextItemId = Number(nextItemIdRow?.nextId || nextItemIdRow?.nextid || 1);
    await run(
      `INSERT INTO purchase_order_items (id, purchaseOrderId, materialRequestId, article, details, quantite, prixUnitaire, totalLigne) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [nextItemId, Number(orderInsert.lastID), line.materialRequestId, line.article, "BC depuis demande d'approvisionnement", Number(line.quantite || 0), Number(line.prixUnitaire || 0), Number(line.totalLigne || 0)]
    );
    await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['EN_COURS', line.materialRequestId]);
  }

  await runPurchaseOrderSideEffects(Number(orderInsert.lastID), {
    fournisseur: 'FOURNISSEUR_A_DEFINIR',
    dateCommande: nowIso,
    createdBy,
    projetId: projectId,
  });

  const createdOrder = await getPurchaseOrderById(Number(orderInsert.lastID));
  res.status(201).json({
    purchaseOrder: createdOrder,
    message: `Bon de commande #${Number(orderInsert.lastID)} créé automatiquement.`,
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

  const nextIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM project_catalog');
  const nextId = Number(nextIdRow?.nextId || nextIdRow?.nextid || 1);

  const result = await run(
    'INSERT INTO project_catalog (id, nomProjet, typeProjet, description, createdAt) VALUES (?, ?, ?, ?, ?)',
    [nextId, projectName, String(typeProjet || '').trim(), String(description || '').trim(), new Date().toISOString()]
  );

  const created = await get('SELECT * FROM project_catalog WHERE id = ?', [result.lastID]);
  res.status(201).json(created);
});

app.get('/api/project-catalog', async (_req, res) => {
  const rows = await all('SELECT * FROM project_catalog ORDER BY id DESC');
  res.json(rows);
});

app.delete('/api/project-catalog/:id', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse' });
  }

  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: 'Identifiant de projet invalide' });
  }

  const catalogItem = await get('SELECT id, nomProjet FROM project_catalog WHERE id = ?', [id]);
  if (!catalogItem) {
    return res.status(404).json({ error: 'Projet catalogue introuvable' });
  }

  const projectName = String(catalogItem.nomProjet || '').trim();
  const folderUsage = await get('SELECT id FROM project_folders WHERE LOWER(nomProjet) = LOWER(?) LIMIT 1', [projectName]);
  const siteUsage = await get('SELECT id FROM projects WHERE LOWER(nomProjet) = LOWER(?) LIMIT 1', [projectName]);

  if (folderUsage || siteUsage) {
    return res.status(409).json({ error: 'Impossible de supprimer ce projet: des zones ou des sites y sont rattaches' });
  }

  const result = await run('DELETE FROM project_catalog WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Projet catalogue introuvable' });
  }

  res.json({ message: 'Projet catalogue supprimé', id });
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
    const nextIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM project_folders');
    const nextId = Number(nextIdRow?.nextId || nextIdRow?.nextid || 1);

    result = await run(
      'INSERT INTO project_folders (id, projectId, nomProjet, prefecture, nomSite, description, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [nextId, Number.isInteger(projectIdValue) && projectIdValue > 0 ? projectIdValue : null, projectName, prefectureName, '', String(description || '').trim(), new Date().toISOString()]
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

app.get('/api/project-folders', async (req, res) => {
  let rows = await all('SELECT pf.*, pc.typeProjet FROM project_folders pf LEFT JOIN project_catalog pc ON pc.id = pf.projectId ORDER BY pf.id DESC');
  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const scopedProjects = await all(
      `SELECT id, nomProjet, prefecture
       FROM projects
       WHERE id IN (${scopedIds.length ? scopedIds.map(() => '?').join(',') : '0'})`,
      scopedIds.length ? scopedIds : [0]
    );
    const allowedFolderKeys = new Set(
      scopedProjects.map(row => `${normalizeScopeText(row.nomProjet)}::${normalizeScopeText(row.prefecture)}`)
    );
    rows = rows.filter(row => allowedFolderKeys.has(`${normalizeScopeText(row.nomProjet)}::${normalizeScopeText(row.prefecture)}`));
  }
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

  const nextIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM projects');
  const nextId = Number(nextIdRow?.nextId || nextIdRow?.nextid || 1);

  const result = await run(
    'INSERT INTO projects (id, nomProjet, prefecture, nomSite, typeMaison, numeroMaison, description, etapeConstruction, statutConstruction, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextId,
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

  const project = await get('SELECT * FROM projects WHERE id = ?', [result.lastID]);
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

  for (const siteName of generatedNames) {
    const nextIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM projects');
    const nextId = Number(nextIdRow?.nextId || nextIdRow?.nextid || 1);

    const result = await run(
      'INSERT INTO projects (id, nomProjet, prefecture, nomSite, typeMaison, numeroMaison, description, etapeConstruction, statutConstruction, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        nextId,
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
    createdIds.push(result.lastID);
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
  let rows = await all(`
    SELECT *
    FROM projects
    WHERE UPPER(COALESCE(typeMaison, '')) != 'ZONE_STOCK'
    ORDER BY id DESC
  `);
  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.id)));
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
    zoneName = null,
    nomProjet = null,
  } = req.body;

  // Zone-based creation: resolve project from zone
  let resolvedProjetId = projetId ? Number(projetId) : null;
  if (!resolvedProjetId && zoneName && nomProjet) {
    const zoneProject = await ensureZoneStockProject(nomProjet, zoneName);
    if (zoneProject) {
      resolvedProjetId = Number(zoneProject.id || 0) || null;
    }
  }

  if (!resolvedProjetId || !demandeur || quantiteDemandee == null) {
    return res.status(400).json({ error: 'projetId (ou zone+nomProjet), demandeur et quantiteDemandee sont obligatoires' });
  }

  const quantite = Number(quantiteDemandee);
  if (Number.isNaN(quantite) || quantite <= 0) {
    return res.status(400).json({ error: 'quantiteDemandee doit etre un nombre positif' });
  }

  const projet = await get('SELECT id, nomProjet FROM projects WHERE id = ?', [resolvedProjetId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  if (!(await isProjectAllowedForUser(req.user, resolvedProjetId))) {
    return res.status(403).json({ error: 'Acces refuse: ce compte ne peut agir que sur son site' });
  }

  const requestedStageRaw = String(etapeApprovisionnement || '').trim();
  const requestedStageKey = normalizeStageLabel(requestedStageRaw);
  if (requestedStageKey) {
    const existingRows = await all(
      'SELECT id, etapeApprovisionnement, groupId, statut FROM material_requests WHERE projetId = ? AND statut != ?',
      [resolvedProjetId, 'REJETEE']
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
        [resolvedProjetId, requestedItemName, 'REJETEE']
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
        [resolvedProjetId, requestedItemName, 'REJETEE']
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

  const nextRequestIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM material_requests');
  const nextRequestId = Number(nextRequestIdRow?.nextId || nextRequestIdRow?.nextid || 1);

  const result = await run(
    'INSERT INTO material_requests (id, projetId, demandeur, etapeApprovisionnement, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut, groupId, warehouseId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextRequestId,
      resolvedProjetId,
      demandeur,
      String(etapeApprovisionnement || '').trim(),
      String(itemName).trim() || 'Materiel divers',
      description || '',
      quantite,
      quantite,
      dateDemande ? new Date(dateDemande).toISOString() : new Date().toISOString(),
      'EN_ATTENTE',
      groupId ? String(groupId).trim() : null,
      warehouseId ? String(warehouseId).trim() : null,
    ]
  );

  const request = await get('SELECT * FROM material_requests WHERE id = ?', [result.lastID]);
  res.status(201).json(request);
});

app.get('/api/material-requests', async (req, res) => {
  let rows = await all(`
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
  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.projetId)));
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
      dateDemande ? new Date(dateDemande).toISOString() : new Date().toISOString(),
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
    zoneName = null,
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
  let resolvedZoneName = String(zoneName || '').trim() || null;

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
    const proj = await get('SELECT nomProjet, nomSite, numeroMaison, prefecture, typeMaison FROM projects WHERE id = ?', [Number(resolvedProjetId)]);
    if (proj) {
      resolvedNomProjet = proj.nomProjet || null;
      if (!resolvedZoneName) {
        resolvedZoneName = String(proj.prefecture || '').trim() || null;
      }
      if (!resolvedNomSite) {
        resolvedNomSite = isZoneStockProjectRow(proj)
          ? `Zone ${String(proj.prefecture || '').trim() || 'Non renseignee'}`
          : ([proj.nomSite, proj.numeroMaison].filter(Boolean).join(' ') || null);
      }
    }
  }

  if (!resolvedSiteId && resolvedNomProjet && resolvedZoneName) {
    const zoneStockProject = await ensureZoneStockProject(resolvedNomProjet, resolvedZoneName);
    if (zoneStockProject) {
      resolvedSiteId = Number(zoneStockProject.id || 0) || null;
      resolvedProjetId = resolvedSiteId;
      resolvedNomSite = `Zone ${resolvedZoneName}`;
    }
  }

  const siteLookupId = Number(resolvedSiteId || resolvedProjetId || 0);
  if (siteLookupId && (!resolvedNomProjet || !resolvedNomSite)) {
    const siteRow = await get('SELECT nomProjet, nomSite, numeroMaison, prefecture, typeMaison FROM projects WHERE id = ?', [siteLookupId]);
    if (siteRow) {
      if (!resolvedNomProjet) {
        resolvedNomProjet = siteRow.nomProjet || null;
      }
      if (!resolvedZoneName) {
        resolvedZoneName = String(siteRow.prefecture || '').trim() || null;
      }
      if (!resolvedNomSite) {
        resolvedNomSite = isZoneStockProjectRow(siteRow)
          ? `Zone ${String(siteRow.prefecture || '').trim() || 'Non renseignee'}`
          : ([siteRow.nomSite, siteRow.numeroMaison].filter(Boolean).join(' ') || null);
      }
    }
  }

  if (!resolvedSiteId) {
    return res.status(400).json({ error: 'Selectionne un site ou une zone pour creer un bon de commande.' });
  }

  const siteProject = resolvedSiteId ? await get('SELECT typeMaison FROM projects WHERE id = ? LIMIT 1', [resolvedSiteId]) : null;
  const resolvedIsZoneOnly = Boolean(siteProject && isZoneStockProjectRow(siteProject));

  if (!resolvedEtape && !resolvedIsZoneOnly) {
    return res.status(400).json({ error: 'L\'etape d\'approvisionnement est obligatoire pour un bon de commande sur site.' });
  }

  const resolvedStageKey = normalizeStageLabel(resolvedEtape);
  const resolvedSiteKey = Number(resolvedSiteId || 0);
  if (resolvedStageKey && resolvedSiteKey) {
    const poColumns = await getTableColumns('purchase_orders');
    const siteSelector = poColumns.has('siteId')
      ? 'COALESCE(po.siteId, po.projetId)'
      : 'po.projetId';
    const stageOrders = await all(
      `SELECT DISTINCT
        po.id,
        po.etapeApprovisionnement,
        po.statut,
        mr.etapeApprovisionnement AS requestStage
       FROM purchase_orders po
       LEFT JOIN purchase_order_items poi ON poi.purchaseOrderId = po.id
       LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
       WHERE ${siteSelector} = ?
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

  const nextPurchaseOrderIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_orders');
  const nextPurchaseOrderId = Number(nextPurchaseOrderIdRow?.nextId || nextPurchaseOrderIdRow?.nextid || 1);

  const result = await run(
    'INSERT INTO purchase_orders (id, materialRequestId, creePar, fournisseur, montantTotal, quantiteCommandee, prixUnitaire, dateCommande, dateLivraisonPrevue, statut, statutValidation, projetId, nomProjetManuel, siteId, nomSiteManuel, warehouseId, etapeApprovisionnement, signatureName, signatureRole) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [
      nextPurchaseOrderId,
      firstRequestId,
      String(creePar || 'admin').trim() || 'admin',
      String(fournisseur).trim(),
      computedTotal,
      preparedItems[0].quantite,
      preparedItems[0].prixUnitaire,
      dateCommande ? new Date(dateCommande).toISOString() : new Date().toISOString(),
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
    const nextItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_order_items');
    const nextItemId = Number(nextItemIdRow?.nextId || nextItemIdRow?.nextid || 1);
    await run(
      'INSERT INTO purchase_order_items (id, purchaseOrderId, materialRequestId, article, details, quantite, prixUnitaire, totalLigne) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [nextItemId, result.lastID, item.materialRequestId || null, item.article, item.details, item.quantite, item.prixUnitaire, item.totalLigne]
    );
    if (item.materialRequestId) {
      await run('UPDATE material_requests SET statut = ? WHERE id = ?', ['EN_COURS', item.materialRequestId]);
    }
  }

  await runPurchaseOrderSideEffects(result.lastID, {
    fournisseur: String(fournisseur).trim(),
    dateCommande: dateCommande ? new Date(dateCommande).toISOString() : new Date().toISOString(),
    createdBy: req.user ? req.user.username : 'admin',
  });

  const order = await getPurchaseOrderById(result.lastID);
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

  let authorizationDocs = [];
  if (statut === 'VALIDEE' || statut === 'ANNULEE') {
    const authorizationDecision = statut === 'ANNULEE' ? 'REJETEE' : 'VALIDEE';
    authorizationDocs = await archiveAuthorizationsForValidatedOrder(
      id,
      String(signatureName || '').trim(),
      String(signatureRole || '').trim(),
      signedAt ? new Date(signedAt).toISOString() : new Date().toISOString(),
      authorizationDecision
    );
  }

  const order = await getPurchaseOrderById(id);
  res.json({ ...order, authorizationDocs });
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

  const rows = await all(
    'SELECT id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt FROM generated_documents WHERE entityType = ? AND entityId = ? ORDER BY updatedAt DESC, id DESC',
    ['material_request_authorization', id]
  );

  const docs = (rows || []).map(row => ({
    ...row,
    fileUrl: `/archives/${String(row.relativePath || '').replace(/\\/g, '/')}`,
  }));
  res.json(docs);
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
      'SELECT id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt FROM generated_documents WHERE entityType = ? AND entityId = ? ORDER BY updatedAt DESC, id DESC',
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
  let rows = await all(`
    SELECT
      po.id,
      po.fournisseur,
      po.statut,
      po.dateCommande,
      po.dateReception,
      po.montantTotal,
      po.warehouseId,
      po.siteId,
      po.nomSiteManuel,
      po.etapeApprovisionnement AS poEtape,
      poi.article,
      poi.quantite,
      poi.materialRequestId,
      mr.projetId,
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

  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.siteId)) || allowed.has(Number(row.projetId)));
  }

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
        nomSiteManuel: row.nomSiteManuel || null,
        numeroMaison: row.numeroMaison || null,
        zoneName: row.prefecture || null,
        isZoneOrder: isZoneStockProjectRow(row),
        etapeApprovisionnement: row.poEtape || null,
        projects: new Set(),
        items: [],
      });
    }

    const order = byOrder.get(orderId);
    if (row.nomProjet) {
      order.projects.add(row.nomProjet);
    }
    if (row.article) {
      order.items.push({
        article: row.article,
        quantite: Number(row.quantite || 0),
        materialRequestId: Number(row.materialRequestId || 0),
        etapeApprovisionnement: row.itemEtape || null,
        isZoneStock: isZoneStockProjectRow(row),
      });
    }
  }

  const result = Array.from(byOrder.values()).map(order => ({
    ...order,
    projects: Array.from(order.projects),
  }));
  res.json(result);
});

app.patch('/api/stock-management/orders/:id/arrive', async (req, res) => {
  const orderId = Number(req.params.id);
  if (!orderId) {
    return res.status(400).json({ error: 'ID commande invalide' });
  }

  const orderScopeRow = await get(
    `SELECT po.id, po.warehouseId, po.siteId, COALESCE(MAX(mr.projetId), 0) AS projetId
     FROM purchase_orders po
     LEFT JOIN purchase_order_items poi ON poi.purchaseOrderId = po.id
     LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
     WHERE po.id = ?
     GROUP BY po.id, po.warehouseId, po.siteId`,
    [orderId]
  );

  if (!orderScopeRow) {
    return res.status(404).json({ error: 'Commande introuvable' });
  }

  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    const siteId = Number(orderScopeRow.siteId || 0);
    const projectId = Number(orderScopeRow.projetId || 0);
    if (!allowed.has(siteId) && !allowed.has(projectId)) {
      return res.status(403).json({ error: 'Acces refuse: commande hors zone autorisee' });
    }
  }

  if (!isWarehouseAllowedForUser(req.user, orderScopeRow.warehouseId)) {
    return res.status(403).json({ error: 'Acces refuse: entrepot hors scope' });
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

app.delete('/api/stock-management/orders/:id', async (req, res) => {
  const orderId = Number(req.params.id);
  if (!orderId) {
    return res.status(400).json({ error: 'ID commande invalide' });
  }

  const orderScopeRow = await get(
    `SELECT po.id, po.warehouseId, po.siteId, COALESCE(MAX(mr.projetId), 0) AS projetId
     FROM purchase_orders po
     LEFT JOIN purchase_order_items poi ON poi.purchaseOrderId = po.id
     LEFT JOIN material_requests mr ON mr.id = poi.materialRequestId
     WHERE po.id = ?
     GROUP BY po.id, po.warehouseId, po.siteId`,
    [orderId]
  );

  if (!orderScopeRow) {
    return res.status(404).json({ error: 'Commande introuvable' });
  }

  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    const siteId = Number(orderScopeRow.siteId || 0);
    const projectId = Number(orderScopeRow.projetId || 0);
    if (!allowed.has(siteId) && !allowed.has(projectId)) {
      return res.status(403).json({ error: 'Acces refuse: commande hors zone autorisee' });
    }
  }

  if (!isWarehouseAllowedForUser(req.user, orderScopeRow.warehouseId)) {
    return res.status(403).json({ error: 'Acces refuse: entrepot hors scope' });
  }

  const docs = await all('SELECT relativePath FROM generated_documents WHERE entityType = ? AND entityId = ?', ['purchase_order', orderId]);
  await run('DELETE FROM purchase_orders WHERE id = ?', [orderId]);
  try { await run('DELETE FROM expenses WHERE purchaseOrderId = ?', [orderId]); } catch (e) {}
  try { await run('DELETE FROM generated_documents WHERE entityType = ? AND entityId = ?', ['purchase_order', orderId]); } catch (e) {}
  for (const doc of docs) {
    const filePath = path.join(ARCHIVE_ROOT, doc.relativePath);
    if (fs.existsSync(filePath)) {
      try { await fs.promises.unlink(filePath); } catch (e) {}
    }
  }

  res.json({ message: 'Commande supprimée définitivement' });
});

app.get('/api/stock-management/available', async (req, res) => {
  let rows = await all(`
    SELECT mr.id, mr.projetId, p.nomProjet, p.prefecture, p.nomSite, p.numeroMaison, p.typeMaison, mr.itemName, mr.quantiteDemandee, mr.quantiteRestante, mr.statut, mr.etapeApprovisionnement, mr.warehouseId
    FROM material_requests mr
    JOIN projects p ON p.id = mr.projetId
    WHERE mr.statut IN ('EN_STOCK', 'EPUISE')
    ORDER BY p.nomProjet ASC, mr.itemName ASC, mr.id DESC
  `);
  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.projetId)));
  }
  res.json(rows.map(row => ({
    ...row,
    sourceType: isZoneStockProjectRow(row) ? 'ZONE_STOCK' : 'SITE_STOCK',
    zoneName: row.prefecture || null,
    quantiteDemandee: Number(row.quantiteDemandee || 0),
    quantiteRestante: Number(row.quantiteRestante || 0),
  })));
});

app.get('/api/stock-management/issues', async (req, res) => {
  let rows = await all(`
    SELECT si.*,
           COALESCE(NULLIF(TRIM(si.issueType), ''), CASE WHEN si.note LIKE 'Consommation chantier%' THEN 'CONSUMPTION' ELSE 'SITE_TRANSFER' END) AS issueType,
           mr.itemName, p.nomProjet, p.numeroMaison
    FROM stock_issues si
    LEFT JOIN material_requests mr ON mr.id = si.materialRequestId
    LEFT JOIN projects p ON p.id = si.projetId
    ORDER BY si.issuedAt DESC, si.id DESC
    LIMIT 100
  `);
  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.projetId)));
  }
  res.json(rows);
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
  let rows = await all(
    `SELECT
      sia.*,
      mr.quantiteRestante,
      mr.quantiteDemandee,
      mr.etapeApprovisionnement AS requestEtape,
      p.nomProjet,
      p.numeroMaison,
      p.prefecture,
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

  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.projetId)));
  }

  const authorizationIds = (rows || []).map(row => Number(row.id)).filter(Boolean);
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

  res.json((rows || []).map(row => {
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

  const authorization = await get(
    `SELECT sia.*, p.nomProjet, p.numeroMaison, mr.itemName AS requestItemName, mr.etapeApprovisionnement AS requestEtape
     FROM stock_issue_authorizations sia
     LEFT JOIN projects p ON p.id = sia.projetId
     LEFT JOIN material_requests mr ON mr.id = sia.materialRequestId
     WHERE sia.id = ?`,
    [id]
  );

  if (!authorization) {
    return res.status(404).json({ error: 'Demande de sortie introuvable' });
  }

  const items = await all(
    `SELECT materialRequestId, projetId, itemName, quantiteSortie, etapeApprovisionnement, warehouseId
     FROM stock_issue_authorization_items
     WHERE authorizationId = ?
     ORDER BY id ASC`,
    [id]
  );

  const requestPayload = {
    id: Number(authorization.materialRequestId || 0) || Number(authorization.id),
    numeroMaison: authorization.numeroMaison || '-',
    nomSite: authorization.nomProjet || '',
    itemName: authorization.requestItemName || authorization.itemName || 'Article',
    etapeApprovisionnement: authorization.requestEtape || authorization.etapeApprovisionnement || 'Etape',
  };

  const docMeta = await archiveStockIssueAuthorizationPdf({
    authorization: {
      ...authorization,
      items: (Array.isArray(items) && items.length) ? items : [{
        itemName: authorization.itemName || authorization.requestItemName || 'Article',
        quantiteSortie: Number(authorization.quantiteSortie || 0),
      }],
    },
    request: requestPayload,
    signatureName: String(authorization.signatureName || '').trim(),
    signatureRole: String(authorization.signatureRole || '').trim(),
    signedAt: authorization.decidedAt || authorization.requestedAt || new Date().toISOString(),
    decisionStatus: String(authorization.status || 'VALIDEE').toUpperCase(),
  });

  // Prefer reading pdf_data stored in DB (survives Railway redeploys)
  const freshDoc = await get('SELECT pdf_data, fileName FROM generated_documents WHERE entityType = ? AND entityId = ?', ['stock_issue_authorization', id]);
  if (freshDoc && freshDoc.pdf_data) {
    const pdfBuffer = Buffer.isBuffer(freshDoc.pdf_data) ? freshDoc.pdf_data : Buffer.from(freshDoc.pdf_data);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${freshDoc.fileName || docMeta.fileName || `autorisation-sortie-${id}.pdf`}"`);
    return res.end(pdfBuffer);
  }
  // Fallback: read from disk (local dev)
  const absolutePath = path.join(ARCHIVE_ROOT, docMeta.relativePath);
  const pdfBuffer = await fs.promises.readFile(absolutePath);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${docMeta.fileName || `autorisation-sortie-${id}.pdf`}"`);
  res.end(pdfBuffer);
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
  let resolvedTargetProjectId = Number(targetProjetId || 0) || null;
  let resolvedWarehouseId = '';
  for (const entry of normalizedEntries) {
    const requestId = Number(entry?.materialRequestId || 0);
    const outQty = Number(entry?.quantiteSortie || 0);
    if (!requestId || Number.isNaN(outQty) || outQty <= 0) {
      return res.status(400).json({ error: 'Chaque ligne doit contenir materialRequestId et quantiteSortie valides' });
    }

    const requestRow = await get(
      `SELECT mr.id, mr.projetId, mr.itemName, mr.etapeApprovisionnement, mr.quantiteRestante, mr.warehouseId, p.nomProjet, p.numeroMaison, p.prefecture, p.typeMaison
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

    const isZoneSource = isZoneStockProjectRow(requestRow);
    if (resolvedTargetProjectId == null) {
      resolvedTargetProjectId = isZoneSource ? null : (Number(requestRow.projetId || 0) || null);
      resolvedWarehouseId = String(requestRow.warehouseId || '').trim();
    }
    if (!isZoneSource && resolvedTargetProjectId != null && Number(requestRow.projetId || 0) !== Number(resolvedTargetProjectId || 0)) {
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

  const requestedBy = req.user ? req.user.username : 'admin';
  const requestedAt = new Date().toISOString();
  const totalQty = prepared.reduce((sum, row) => sum + Number(row.outQty || 0), 0);
  const resolvedStage = String(etapeApprovisionnement || prepared[0]?.requestRow?.etapeApprovisionnement || '').trim() || null;
  const firstRow = prepared[0]?.requestRow || {};
  const hasZoneSource = prepared.some(row => isZoneStockProjectRow(row.requestRow));

  if (hasZoneSource && !resolvedTargetProjectId) {
    return res.status(400).json({ error: 'Selectionne le site destinataire pour sortir un stock de zone' });
  }

  if (resolvedTargetProjectId && !(await isProjectAllowedForUser(req.user, resolvedTargetProjectId))) {
    return res.status(403).json({ error: 'Acces refuse: ce compte ne peut sortir que vers son site' });
  }

  for (const row of prepared) {
    const isZoneSource = isZoneStockProjectRow(row.requestRow);
    if (!isZoneSource && !(await isProjectAllowedForUser(req.user, row.requestRow.projetId))) {
      return res.status(403).json({ error: 'Acces refuse: materiel hors périmetre autorise' });
    }
  }

  // ── Guards: EN_ATTENTE duplicate and catalog-quantity exhaustion ──────────
  if (resolvedTargetProjectId && resolvedStage) {
    // 1. Block if a pending authorization already exists for this project + stage
    const existingPending = await get(
      `SELECT id FROM stock_issue_authorizations
       WHERE projetId = ? AND etapeApprovisionnement = ? AND status = 'EN_ATTENTE' LIMIT 1`,
      [resolvedTargetProjectId, resolvedStage]
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
          [resolvedTargetProjectId, itemName]
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
  const nextAuthorizationId = Number(nextAuthorizationIdRow?.nextId || nextAuthorizationIdRow?.nextid || 1);
  const insert = await run(
    `INSERT INTO stock_issue_authorizations
      (id, materialRequestId, projetId, warehouseId, itemName, etapeApprovisionnement, quantiteSortie, note, status, requestedBy, requestedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextAuthorizationId,
      prepared[0].requestId,
      resolvedTargetProjectId,
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
    const nextAuthItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issue_authorization_items');
    const nextAuthItemId = Number(nextAuthItemIdRow?.nextId || nextAuthItemIdRow?.nextid || 1);
    await run(
      `INSERT INTO stock_issue_authorization_items
        (id, authorizationId, materialRequestId, projetId, itemName, quantiteSortie, etapeApprovisionnement, warehouseId, createdAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        nextAuthItemId,
        Number(insert.lastID),
        row.requestId,
        resolvedTargetProjectId,
        String(row.requestRow.itemName || '').trim() || 'Article',
        row.outQty,
        String(resolvedStage || row.requestRow.etapeApprovisionnement || '').trim() || null,
        String(row.requestRow.warehouseId || '').trim() || null,
        requestedAt,
      ]
    );
  }

  res.status(201).json({ created: [{
    id: Number(insert.lastID),
    materialRequestId: prepared[0].requestId,
    projetId: resolvedTargetProjectId,
    warehouseId: resolvedWarehouseId || null,
    itemName: prepared.length > 1 ? `${prepared.length} articles` : firstRow.itemName,
    etapeApprovisionnement: resolvedStage,
    quantiteSortie: totalQty,
    status: 'EN_ATTENTE',
    requestedBy,
    requestedAt,
    nomProjet: firstRow.nomProjet,
    numeroMaison: hasZoneSource ? null : firstRow.numeroMaison,
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
  if (req.user && (req.user.role === 'commis' || req.user.role === 'chef_chantier_site')) {
    return res.status(403).json({ error: 'Ce profil ne peut pas valider/rejeter une sortie' });
  }
  if (decision === 'VALIDEE' && (!String(signatureName || '').trim() || !String(signatureRole || '').trim())) {
    return res.status(400).json({ error: 'Signature et fonction obligatoires pour valider' });
  }

  const authRow = await get(
    `SELECT sia.*, mr.quantiteRestante, mr.statut as requestStatus,
            p.nomProjet, p.numeroMaison,
            src.projetId AS sourceProjetId, src.warehouseId AS sourceWarehouseId,
            psrc.prefecture AS sourcePrefecture, psrc.typeMaison AS sourceTypeMaison
     FROM stock_issue_authorizations sia
     LEFT JOIN material_requests mr ON mr.id = sia.materialRequestId
     LEFT JOIN projects p ON p.id = sia.projetId
     LEFT JOIN material_requests src ON src.id = sia.materialRequestId
     LEFT JOIN projects psrc ON psrc.id = src.projetId
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
    `SELECT si.*, mr.quantiteRestante, mr.statut AS requestStatus, mr.projetId AS sourceProjetId, mr.warehouseId AS sourceWarehouseId,
            psrc.prefecture AS sourcePrefecture, psrc.typeMaison AS sourceTypeMaison
     FROM stock_issue_authorization_items si
     LEFT JOIN material_requests mr ON mr.id = si.materialRequestId
     LEFT JOIN projects psrc ON psrc.id = mr.projetId
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

  const normalizedDecisionNote = String(decisionNote || '').trim();

  if (decision === 'VALIDEE') {
    const insufficientItem = effectiveItems.find(item => {
      const remaining = Number(item.quantiteRestante || 0);
      const outQty = Number(item.quantiteSortie || 0);
      return outQty > remaining;
    });

    if (insufficientItem) {
      const remaining = Number(insufficientItem.quantiteRestante || 0);
      const outQty = Number(insufficientItem.quantiteSortie || 0);
      const autoRejectReason = `Refus automatique: stock insuffisant pour ${insufficientItem.itemName} (disponible: ${remaining.toFixed(2)}, demande: ${outQty.toFixed(2)}).`;
      const finalDecisionNote = normalizedDecisionNote ? `${normalizedDecisionNote} | ${autoRejectReason}` : autoRejectReason;

      await run(
        `UPDATE stock_issue_authorizations
         SET status = ?, decidedBy = ?, decidedAt = ?, decisionNote = ?, signatureName = ?, signatureRole = ?
         WHERE id = ?`,
        ['REJETEE', decidedBy, decidedAt, finalDecisionNote, '', '', id]
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
          status: 'REJETEE',
          decisionNote: finalDecisionNote,
          quantiteSortie: Number(authRow.quantiteSortie || 0),
          items: effectiveItems,
        },
        request: requestRow || authRow,
        signatureName: '',
        signatureRole: '',
        signedAt: decidedAt,
        decisionStatus: 'REJETEE',
      });

      const updated = await get('SELECT * FROM stock_issue_authorizations WHERE id = ?', [id]);
      return res.status(409).json({ ...updated, doc, autoRejected: true, error: autoRejectReason });
    }

    for (const item of effectiveItems) {
      const remaining = Number(item.quantiteRestante || 0);
      const outQty = Number(item.quantiteSortie || 0);

      const newRemaining = Math.max(0, remaining - outQty);
      const newStatus = newRemaining > 0 ? 'EN_STOCK' : 'EPUISE';
      await run('UPDATE material_requests SET quantiteRestante = ?, statut = ? WHERE id = ?', [newRemaining, newStatus, Number(item.materialRequestId)]);

      if (isZoneStockProjectRow({ typeMaison: item.sourceTypeMaison || authRow.sourceTypeMaison })) {
        const destinationProjectId = Number(item.projetId || authRow.projetId || 0);
        if (!destinationProjectId) {
          return res.status(400).json({ error: 'Site destinataire introuvable pour cette sortie de stock zone' });
        }

        const destinationWarehouseId = String(item.warehouseId || authRow.warehouseId || item.sourceWarehouseId || '').trim() || null;
        const destinationStage = String(item.etapeApprovisionnement || authRow.etapeApprovisionnement || '').trim() || 'TRANSFERT_ZONE';
        const existingTargetStock = await get(
          `SELECT id, quantiteDemandee, quantiteRestante
           FROM material_requests
           WHERE projetId = ?
             AND COALESCE(warehouseId, '') = COALESCE(?, '')
             AND itemName = ?
             AND etapeApprovisionnement = ?
             AND statut IN ('EN_STOCK', 'EPUISE', 'EN_COURS')
           ORDER BY id DESC
           LIMIT 1`,
          [destinationProjectId, destinationWarehouseId, String(item.itemName || '').trim(), destinationStage]
        );

        if (existingTargetStock) {
          await run(
            'UPDATE material_requests SET quantiteDemandee = ?, quantiteRestante = ?, statut = ? WHERE id = ?',
            [
              Number(existingTargetStock.quantiteDemandee || 0) + outQty,
              Number(existingTargetStock.quantiteRestante || 0) + outQty,
              'EN_STOCK',
              Number(existingTargetStock.id),
            ]
          );
        } else {
          const nextRequestIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM material_requests');
          const nextRequestId = Number(nextRequestIdRow?.nextId || nextRequestIdRow?.nextid || 1);
          await run(
            'INSERT INTO material_requests (id, projetId, demandeur, etapeApprovisionnement, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut, warehouseId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
              nextRequestId,
              destinationProjectId,
              decidedBy,
              destinationStage,
              String(item.itemName || '').trim() || 'Article',
              `Transfert depuis stock zone (${String(item.sourcePrefecture || authRow.sourcePrefecture || '').trim() || 'zone'})`,
              outQty,
              outQty,
              decidedAt,
              'EN_STOCK',
              destinationWarehouseId,
            ]
          );
        }
      }

      const nextStockIssueIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issues');
      const nextStockIssueId = Number(nextStockIssueIdRow?.nextId || nextStockIssueIdRow?.nextid || 1);
      await run(
        'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [
          nextStockIssueId,
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
    [decision, decidedBy, decidedAt, normalizedDecisionNote, String(signatureName || '').trim(), String(signatureRole || '').trim(), id]
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
    `SELECT mr.id, mr.projetId, mr.quantiteRestante, mr.statut, mr.itemName, mr.etapeApprovisionnement, mr.warehouseId,
            p.prefecture, p.typeMaison
     FROM material_requests mr
     JOIN projects p ON p.id = mr.projetId
     WHERE mr.id = ?`,
    [requestId]
  );
  if (!requestRow) {
    return res.status(404).json({ error: 'Matériel introuvable' });
  }

  if (!(await isProjectAllowedForUser(req.user, requestRow.projetId))) {
    return res.status(403).json({ error: 'Acces refuse: ce compte ne peut agir que sur son site' });
  }

  if (isZoneStockManagerRole(req.user?.role)) {
    const scopeZone = normalizeScopeText(ZONE_STOCK_MANAGER_ZONE_NAME);
    const requestZone = normalizeScopeText(requestRow.prefecture);
    if (!isZoneStockProjectRow(requestRow) || (scopeZone && requestZone !== scopeZone)) {
      return res.status(403).json({ error: 'Acces refuse: seules les sorties depuis le stock de zone Adzope sont autorisees' });
    }
  }

  const remaining = Number(requestRow.quantiteRestante || 0);
  if (outQty > remaining) {
    return res.status(400).json({ error: 'Quantité sortie supérieure au stock restant' });
  }

  const newRemaining = Math.max(0, remaining - outQty);
  const newStatus = newRemaining > 0 ? 'EN_STOCK' : 'EPUISE';
  await run('UPDATE material_requests SET quantiteRestante = ?, statut = ? WHERE id = ?', [newRemaining, newStatus, requestId]);

  const now = new Date().toISOString();
  const actor = req.user ? req.user.username : 'admin';
  const normalizedNote = String(note || '').trim();

  const nextStockIssueIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issues');
  const nextStockIssueId = Number(nextStockIssueIdRow?.nextId || nextStockIssueIdRow?.nextid || 1);
  await run(
    'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    [nextStockIssueId, requestId, requestRow.projetId || null, outQty, 'SITE_TRANSFER', normalizedNote, actor, now]
  );

  if (isZoneStockManagerRole(req.user?.role)) {
    const nextAuthIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issue_authorizations');
    const nextAuthId = Number(nextAuthIdRow?.nextId || nextAuthIdRow?.nextid || 1);
    await run(
      `INSERT INTO stock_issue_authorizations
       (id, materialRequestId, projetId, itemName, quantiteSortie, etapeApprovisionnement, warehouseId,
        requestType, requestedBy, requestedAt, status, decidedBy, decidedAt, decisionNote)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        nextAuthId,
        requestId,
        requestRow.projetId || null,
        String(requestRow.itemName || '').trim() || 'Article',
        outQty,
        String(requestRow.etapeApprovisionnement || '').trim() || null,
        String(requestRow.warehouseId || '').trim() || null,
        'AUTO_STOCK_ISSUE',
        actor,
        now,
        'VALIDEE',
        actor,
        now,
        normalizedNote || 'Sortie de stock validée automatiquement depuis Gestion de stock (gestionnaire zone).',
      ]
    );

    const nextAuthItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issue_authorization_items');
    const nextAuthItemId = Number(nextAuthItemIdRow?.nextId || nextAuthItemIdRow?.nextid || 1);
    await run(
      `INSERT INTO stock_issue_authorization_items
       (id, authorizationId, materialRequestId, projetId, itemName, quantiteSortie, etapeApprovisionnement, warehouseId)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        nextAuthItemId,
        nextAuthId,
        requestId,
        requestRow.projetId || null,
        String(requestRow.itemName || '').trim() || 'Article',
        outQty,
        String(requestRow.etapeApprovisionnement || '').trim() || null,
        String(requestRow.warehouseId || '').trim() || null,
      ]
    );
  }

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

  const result = await run(
    'UPDATE purchase_orders SET fournisseur = ?, montantTotal = ?, quantiteCommandee = ?, prixUnitaire = ?, dateLivraisonPrevue = ?, dateCommande = COALESCE(?, dateCommande) WHERE id = ?',
    [
      String(fournisseur).trim(),
      totalAmount,
      preparedItems && preparedItems.length ? preparedItems[0].quantite : 1,
      preparedItems && preparedItems.length ? preparedItems[0].prixUnitaire : totalAmount,
      dateLivraisonPrevue,
      dateCommande ? new Date(dateCommande).toISOString() : null,
      id,
    ]
  );

  if (result.changes === 0) {
    return res.status(404).json({ error: 'Bon de commande non trouve' });
  }

  if (preparedItems && preparedItems.length) {
    await run('DELETE FROM purchase_order_items WHERE purchaseOrderId = ?', [id]);
    for (const item of preparedItems) {
      const nextItemIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM purchase_order_items');
      const nextItemId = Number(nextItemIdRow?.nextId || nextItemIdRow?.nextid || 1);
      await run(
        'INSERT INTO purchase_order_items (id, purchaseOrderId, materialRequestId, article, details, quantite, prixUnitaire, totalLigne) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [nextItemId, id, item.materialRequestId, item.article, item.details, item.quantite, item.prixUnitaire, item.totalLigne]
      );
    }
  }

  await runPurchaseOrderSideEffects(id, {
    fournisseur: String(fournisseur).trim(),
    dateCommande: dateCommande ? new Date(dateCommande).toISOString() : undefined,
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
    const rows = sectionCode
      ? await all('SELECT id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt FROM generated_documents WHERE sectionCode = ? ORDER BY updatedAt DESC, id DESC', [sectionCode])
      : await all('SELECT id, sectionCode, sectionLabel, entityType, entityId, title, fileName, relativePath, createdAt, updatedAt FROM generated_documents ORDER BY updatedAt DESC, id DESC');

    if (!rows || !Array.isArray(rows)) {
      return res.json([]);
    }

    const normalizedRows = [];
    for (const row of rows) {
      try {
        if (!(await isGeneratedDocumentAllowedForUser(req.user, row))) {
          continue;
        }

        let resolvedTitle = String(row.title || '').trim();
        const entityType = String(row.entityType || '').trim().toLowerCase();
        const entityId = Number(row.entityId || 0);
        let projectId = 0;

        if ((entityType === 'manual_upload' || entityType === 'upload') && Number.isFinite(entityId) && entityId > 0) {
          projectId = entityId;
        } else if (entityType === 'purchase_order' && Number.isFinite(entityId) && entityId > 0) {
          try {
            const order = await getPurchaseOrderById(entityId);
            if (order) {
              resolvedTitle = buildPurchaseOrderDocumentTitle(order);
            }
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
  if (!id) return res.status(400).json({ error: 'ID invalide' });

  const row = await get('SELECT * FROM generated_documents WHERE id = ?', [id]);
  if (!row) return res.status(404).json({ error: 'Document introuvable' });
  if (!(await isGeneratedDocumentAllowedForUser(req.user, row))) {
    return res.status(403).json({ error: 'Acces refuse pour ce document' });
  }

  const safeFileName = String(row.fileName || 'document.pdf').replace(/[^a-zA-Z0-9._-]/g, '_');

  // Serve stored binary from DB (persists across redeploys)
  if (row.pdf_data) {
    const buffer = Buffer.isBuffer(row.pdf_data) ? row.pdf_data : Buffer.from(row.pdf_data);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${safeFileName}"`);
    return res.send(buffer);
  }

  // Fallback: try file on disk
  const filePath = path.join(ARCHIVE_ROOT, String(row.relativePath || ''));
  if (fs.existsSync(filePath)) {
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${safeFileName}"`);
    return res.sendFile(filePath);
  }

  // Fallback: regenerate from entityType/entityId
  const entityType = String(row.entityType || '').toLowerCase();
  const entityId = Number(row.entityId || 0);

  if (entityType === 'purchase_order' && entityId > 0) {
    try {
      const order = await getPurchaseOrderById(entityId);
      if (order) {
        const buffer = await generatePurchaseOrderPdfBuffer(order);
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename="${safeFileName}"`);
        return res.send(buffer);
      }
    } catch (e) { /* fallthrough */ }
  }

  if (entityType === 'stock_issue_authorization' && entityId > 0) {
    try {
      const authorization = await get(
        `SELECT sia.*, p.nomProjet, p.numeroMaison, mr.itemName AS requestItemName, mr.etapeApprovisionnement AS requestEtape
         FROM stock_issue_authorizations sia
         LEFT JOIN projects p ON p.id = sia.projetId
         LEFT JOIN material_requests mr ON mr.id = sia.materialRequestId
         WHERE sia.id = ?`,
        [entityId]
      );
      if (authorization) {
        const items = await all(
          `SELECT materialRequestId, projetId, itemName, quantiteSortie, etapeApprovisionnement, warehouseId
           FROM stock_issue_authorization_items
           WHERE authorizationId = ?
           ORDER BY id ASC`,
          [entityId]
        );
        const requestPayload = {
          id: Number(authorization.materialRequestId || 0) || Number(authorization.id),
          numeroMaison: authorization.numeroMaison || '-',
          nomSite: authorization.nomProjet || '',
          itemName: authorization.requestItemName || authorization.itemName || 'Article',
          etapeApprovisionnement: authorization.requestEtape || authorization.etapeApprovisionnement || 'Etape',
        };
        const docMeta = await archiveStockIssueAuthorizationPdf({
          authorization: {
            ...authorization,
            items: (Array.isArray(items) && items.length) ? items : [{
              itemName: authorization.itemName || authorization.requestItemName || 'Article',
              quantiteSortie: Number(authorization.quantiteSortie || 0),
            }],
          },
          request: requestPayload,
          signatureName: String(authorization.signatureName || '').trim(),
          signatureRole: String(authorization.signatureRole || '').trim(),
          signedAt: authorization.decidedAt || authorization.requestedAt || new Date().toISOString(),
          decisionStatus: String(authorization.status || 'VALIDEE').toUpperCase(),
        });
        // Retrieve freshly stored pdf_data from DB
        const freshRow = await get('SELECT pdf_data, fileName FROM generated_documents WHERE entityType = ? AND entityId = ?', ['stock_issue_authorization', entityId]);
        if (freshRow && freshRow.pdf_data) {
          const buf = Buffer.isBuffer(freshRow.pdf_data) ? freshRow.pdf_data : Buffer.from(freshRow.pdf_data);
          const fname = String(freshRow.fileName || safeFileName).replace(/[^a-zA-Z0-9._-]/g, '_');
          res.setHeader('Content-Type', 'application/pdf');
          res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
          return res.send(buf);
        }
      }
    } catch (e) { console.error('Erreur régénération PDF autorisation:', e); }
  }

  return res.status(404).json({ error: 'Fichier PDF non disponible. Regénérez le document depuis son module.' });
});

app.delete('/api/material-requests/:id', async (req, res) => {
  const id = Number(req.params.id);
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
  if (isSiteChiefRole(_req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse' });
  }
  await run('DELETE FROM expenses');
  res.json({ message: 'Toutes les dépenses ont été supprimées' });
});

app.delete('/api/expenses/:id', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse' });
  }
  const id = Number(req.params.id);
  const result = await run('DELETE FROM expenses WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Dépense non trouvée' });
  }
  res.json({ message: 'Dépense supprimée' });
});

app.post('/api/expenses', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse' });
  }

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

app.get('/api/expenses', async (req, res) => {
  let rows = await all(`
    SELECT e.*, m.nom as materialNom, p.nomProjet as projetNom
    FROM expenses e
    LEFT JOIN materials m ON m.id = e.materialId
    LEFT JOIN projects p ON p.id = e.projetId
    ORDER BY e.dateExpense DESC
  `);

  const scopedIds = await getScopedProjectIdsForUser(req.user);
  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.projetId)));
  }

  res.json(rows);
});

app.patch('/api/expenses/:id/status', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse' });
  }

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

  const result = await run(
    'INSERT INTO revenues (projetId, description, amount, dateRevenue, createdBy) VALUES (?, ?, ?, ?, ?)',
    [numericProjectId, String(description).trim(), numericAmount, dateRevenue ? new Date(dateRevenue).toISOString() : new Date().toISOString(), req.user.username]
  );

  await archiveRevenueInvoicePdf(result.lastID);

  const revenue = await get('SELECT * FROM revenues WHERE id = ?', [result.lastID]);
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

  const projet = await get('SELECT id FROM projects WHERE id = ?', [progressProjectId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouve' });
  }

  if (!(await isProjectAllowedForUser(req.user, progressProjectId))) {
    return res.status(403).json({ error: 'Acces refuse: ce compte ne peut agir que sur son site' });
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
  if (dateEtape) {
    const parsedDate = new Date(dateEtape);
    if (Number.isNaN(parsedDate.getTime())) {
      return res.status(400).json({ error: 'Date etape invalide' });
    }
    createdAt = parsedDate.toISOString();
  }

  const progressColumns = await getTableColumns('project_progress_updates');
  const usageDetailsColumn = progressColumns.has('materialUsageDetails')
    ? 'materialUsageDetails'
    : (progressColumns.has('materialusagedetails') ? 'materialusagedetails' : null);

  const nextProgressIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM project_progress_updates');
  const nextProgressId = Number(nextProgressIdRow?.nextId || nextProgressIdRow?.nextid || 1);

  const insertColumns = ['id', 'projectId', 'stage', 'title', 'note', 'materialUsedQty'];
  const insertValues = [
    nextProgressId,
    progressProjectId,
    stageLabel,
    String(title || '').trim(),
    noteValue,
    normalizedMaterialUsedQty,
  ];

  if (usageDetailsColumn) {
    insertColumns.push(usageDetailsColumn);
    insertValues.push(JSON.stringify(normalizedUsageLines));
  }

  insertColumns.push('progressPercent', 'createdBy', 'createdAt');
  insertValues.push(normalizedPercent, req.user ? req.user.username : 'admin', createdAt);

  const placeholders = insertColumns.map(() => '?').join(', ');
  const result = await run(
    `INSERT INTO project_progress_updates (${insertColumns.join(', ')}) VALUES (${placeholders})`,
    insertValues
  );

  if (normalizedUsageLines.length > 0) {
    for (const usageLine of normalizedUsageLines) {
      let remainingToIssue = usageLine.quantite;
      const availableRows = availableRowsByMaterial.get(usageLine.itemName.toLowerCase()) || [];

      for (const row of availableRows) {
        if (remainingToIssue <= 0) break;
        const currentRemaining = Number(row.siteRemaining || 0);
        if (currentRemaining <= 0) continue;

        const outQty = Math.min(currentRemaining, remainingToIssue);
        const nextStockIssueIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issues');
        const nextStockIssueId = Number(nextStockIssueIdRow?.nextId || nextStockIssueIdRow?.nextid || 1);
        await run(
          'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
          [
            nextStockIssueId,
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
    for (const row of availableRowsByMaterial.get('__legacy__') || []) {
      if (remainingToIssue <= 0) break;
      const currentRemaining = Number(row.siteRemaining || 0);
      if (currentRemaining <= 0) continue;

      const outQty = Math.min(currentRemaining, remainingToIssue);
      const nextStockIssueIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM stock_issues');
      const nextStockIssueId = Number(nextStockIssueIdRow?.nextId || nextStockIssueIdRow?.nextid || 1);
      await run(
        'INSERT INTO stock_issues (id, materialRequestId, projetId, quantiteSortie, issueType, note, issuedBy, issuedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [
          nextStockIssueId,
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
  const scopedIds = await getScopedProjectIdsForUser(req.user);

  if (scopedIds !== null && projectId && !scopedIds.includes(projectId)) {
    return res.json([]);
  }

  const baseQuery = `
    SELECT ppu.*, p.nomProjet, p.nomSite, p.numeroMaison, p.typeMaison
    FROM project_progress_updates ppu
    JOIN projects p ON p.id = ppu.projectId
  `;

  let rows = projectId
    ? await all(`${baseQuery} WHERE p.id = ? ORDER BY ppu.createdAt DESC, ppu.id DESC`, [projectId])
    : await all(`${baseQuery} ORDER BY ppu.createdAt DESC, ppu.id DESC`);

  if (scopedIds !== null) {
    const allowed = new Set(scopedIds);
    rows = rows.filter(row => allowed.has(Number(row.projectId)));
  }

  res.json(rows.map(formatProjectProgressRow));
});

app.delete('/api/project-progress', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse' });
  }

  await run('DELETE FROM project_progress_updates');
  res.json({ message: 'Tous les suivis de progression ont été supprimés' });
});

app.delete('/api/project-progress/:id', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse' });
  }

  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: 'Identifiant invalide' });
  }

  const result = await run('DELETE FROM project_progress_updates WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Suivi non trouvé' });
  }

  res.json({ message: 'Suivi supprimé', id });
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
  let rows = folder
    ? await all('SELECT * FROM building_material_catalog WHERE projectFolder = ? ORDER BY materialName ASC', [folder])
    : await all('SELECT * FROM building_material_catalog ORDER BY projectFolder ASC, materialName ASC');

  if (isSiteChiefRole(req.user?.role)) {
    rows = rows.filter(row => normalizeScopeText(row.projectFolder || '') === normalizeScopeText(SITE_CHIEF_PROJECT_NAME));
  }

  res.json(rows);
});

app.post('/api/material-catalog', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse: consultation uniquement pour le debourse sec' });
  }

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
  const nextIdRow = await get('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM building_material_catalog');
  const nextId = Number(nextIdRow?.nextId || nextIdRow?.nextid || 1);
  const result = await run(
    `INSERT INTO building_material_catalog
      (id, projectFolder, materialName, unite, quantiteParBatiment, prixUnitaire, notes, createdAt, updatedAt)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      nextId,
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

  const row = await get('SELECT * FROM building_material_catalog WHERE id = ?', [result.lastID]);
  res.status(201).json(row);
});

app.patch('/api/material-catalog/:id', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse: consultation uniquement pour le debourse sec' });
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

app.delete('/api/material-catalog/:id', async (req, res) => {
  if (isSiteChiefRole(req.user?.role)) {
    return res.status(403).json({ error: 'Acces refuse: consultation uniquement pour le debourse sec' });
  }

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

