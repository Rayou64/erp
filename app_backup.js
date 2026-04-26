// app.js
// API Construction & Logistique avec une base de données SQLite et authentification

const express = require('express');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const DB_FILE = path.join(__dirname, 'data.db');
const JWT_SECRET = process.env.JWT_SECRET || 'erp-secret-2026';
const PORT = process.env.PORT || 4000;

const db = new sqlite3.Database(DB_FILE, err => {
  if (err) {
    console.error('Impossible d’ouvrir la base de données', err);
    process.exit(1);
  }
});

db.serialize(() => {
  db.run('PRAGMA foreign_keys = ON');
});

function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) {
        reject(err);
      } else {
        resolve(this);
      }
    });
  });
}

function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
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
    nomSite TEXT NOT NULL,
    etapeConstruction TEXT,
    statutConstruction TEXT,
    createdAt TEXT NOT NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS material_requests (
    id INTEGER PRIMARY KEY,
    projetId INTEGER NOT NULL,
    demandeur TEXT NOT NULL,
    itemName TEXT NOT NULL,
    description TEXT,
    quantiteDemandee REAL NOT NULL DEFAULT 0,
    quantiteRestante REAL NOT NULL DEFAULT 0,
    dateDemande TEXT NOT NULL,
    statut TEXT NOT NULL,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE CASCADE
  )`);

  try {
    await run('ALTER TABLE material_requests ADD COLUMN itemName TEXT NOT NULL DEFAULT ""');
  } catch (e) {}
  try {
    await run('ALTER TABLE material_requests ADD COLUMN quantiteDemandee REAL NOT NULL DEFAULT 0');
  } catch (e) {}
  try {
    await run('ALTER TABLE material_requests ADD COLUMN quantiteRestante REAL NOT NULL DEFAULT 0');
  } catch (e) {}

  await run(`CREATE TABLE IF NOT EXISTS purchase_orders (
    id INTEGER PRIMARY KEY,
    materialRequestId INTEGER NOT NULL,
    creePar TEXT NOT NULL,
    fournisseur TEXT,
    montantTotal REAL,
    dateCommande TEXT NOT NULL,
    statutValidation TEXT NOT NULL,
    FOREIGN KEY(materialRequestId) REFERENCES material_requests(id) ON DELETE CASCADE
  )`);

  await run(`CREATE TABLE IF NOT EXISTS materials (
    id INTEGER PRIMARY KEY,
    nom TEXT NOT NULL,
    categorie TEXT NOT NULL,
    unite TEXT NOT NULL,
    prixUnitaire REAL NOT NULL,
    description TEXT,
    createdAt TEXT NOT NULL
  )`);

  await run(`CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY,
    materialId INTEGER,
    projetId INTEGER,
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
    FOREIGN KEY(projetId) REFERENCES projects(id)
  )`);

  await run(`CREATE TABLE IF NOT EXISTS project_assignments (
    id INTEGER PRIMARY KEY,
    projetId INTEGER NOT NULL,
    userId INTEGER NOT NULL,
    role TEXT NOT NULL,
    assignedAt TEXT NOT NULL,
    FOREIGN KEY(projetId) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY(userId) REFERENCES users(id) ON DELETE CASCADE
  )`);

  await run(`CREATE TABLE IF NOT EXISTS revenues (
    id INTEGER PRIMARY KEY,
    projetId INTEGER,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    dateRevenue TEXT NOT NULL,
    createdBy TEXT NOT NULL,
    FOREIGN KEY(projetId) REFERENCES projects(id)
  )`);

  const admin = await get('SELECT * FROM users WHERE username = ?', ['admin']);
  if (!admin) {
    const passwordHash = await bcrypt.hash('admin123', 10);
    await run('INSERT INTO users (username, password, role, createdAt) VALUES (?, ?, ?, ?)', [
      'admin',
      passwordHash,
      'admin',
      new Date().toISOString(),
    ]);
    console.log('Utilisateur admin créé avec mot de passe admin123');
  }
}

initDb().catch(error => {
  console.error('Erreur d’initialisation de la base de données', error);
  process.exit(1);
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

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: 'Nom d’utilisateur et mot de passe sont obligatoires' });
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
    expiresIn: '6h',
  });

  res.json({ token, username: user.username });
});

app.get('/api/auth/me', async (req, res) => {
  res.json({ username: req.user.username, role: req.user.role });
});

app.use('/api', authenticateToken);

app.get('/api/users', async (req, res) => {
  const rows = await all('SELECT id, username, role FROM users');
  res.json(rows);
});

app.post('/api/projects', async (req, res) => {
  const { nomProjet, nomSite, etapeConstruction = '', statutConstruction = '' } = req.body;
  if (!nomProjet || !nomSite) {
    return res.status(400).json({ error: 'nomProjet et nomSite sont obligatoires' });
  }

  const result = await run(
    'INSERT INTO projects (nomProjet, nomSite, etapeConstruction, statutConstruction, createdAt) VALUES (?, ?, ?, ?, ?)',
    [nomProjet, nomSite, etapeConstruction, statutConstruction, new Date().toISOString()]
  );

  const projet = await get('SELECT * FROM projects WHERE id = ?', [result.lastID]);
  res.status(201).json(projet);
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

  const montantTotal = expenseQuantity * expenseUnitPrice;

  const result = await run(
    'INSERT INTO expenses (materialId, projetId, description, quantite, prixUnitaire, montantTotal, dateExpense, fournisseur, categorie, statut, createdBy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    [materialId || null, expenseProjectId, expenseDescription, expenseQuantity, expenseUnitPrice, montantTotal, new Date().toISOString(), fournisseur, expenseCategory, 'EN_ATTENTE', req.user.username]
  );

  const expense = await get('SELECT * FROM expenses WHERE id = ?', [result.lastID]);
  res.status(201).json(expense);
});

app.get('/api/projects', async (_req, res) => {
  const rows = await all('SELECT * FROM projects ORDER BY id DESC');
  res.json(rows);
});

app.delete('/api/projects/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM projects WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Projet non trouvé' });
  }
  res.json({ message: 'Projet supprimé avec succès' });
});

app.post('/api/material-requests', async (req, res) => {
  const { projetId, demandeur, itemName = '', description = '', quantiteDemandee } = req.body;
  if (!projetId || !demandeur || !itemName || quantiteDemandee == null) {
    return res.status(400).json({ error: 'projetId, demandeur, itemName et quantiteDemandee sont obligatoires' });
  }

  const quantite = Number(quantiteDemandee);
  if (Number.isNaN(quantite) || quantite <= 0) {
    return res.status(400).json({ error: 'quantiteDemandee doit être un nombre positif' });
  }

  const projet = await get('SELECT * FROM projects WHERE id = ?', [projetId]);
  if (!projet) {
    return res.status(404).json({ error: 'Projet non trouvé' });
  }

  const result = await run(
    'INSERT INTO material_requests (projetId, demandeur, itemName, description, quantiteDemandee, quantiteRestante, dateDemande, statut) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    [projetId, demandeur, itemName, description, quantite, quantite, new Date().toISOString(), 'EN_ATTENTE']
  );

  const demande = await get('SELECT * FROM material_requests WHERE id = ?', [result.lastID]);
  res.status(201).json(demande);
});

app.get('/api/material-requests', async (_req, res) => {
  const rows = await all(
    `SELECT mr.*, p.nomProjet AS projetNom, p.nomSite AS projetSite
     FROM material_requests mr
     LEFT JOIN projects p ON p.id = mr.projetId
     ORDER BY mr.id DESC`
  );

  const result = rows.map(row => ({
    id: row.id,
    projetId: row.projetId,
    demandeur: row.demandeur,
    itemName: row.itemName,
    description: row.description,
    quantiteDemandee: row.quantiteDemandee,
    quantiteRestante: row.quantiteRestante,
    dateDemande: row.dateDemande,
    statut: row.statut,
    projet: row.projetId ? { id: row.projetId, nomProjet: row.projetNom, nomSite: row.projetSite } : null,
  }));

  res.json(result);
});

app.patch('/api/material-requests/:id/remaining', async (req, res) => {
  const id = Number(req.params.id);
  const { quantiteRestante } = req.body;

  if (quantiteRestante == null) {
    return res.status(400).json({ error: 'quantiteRestante est obligatoire' });
  }

  const valeur = Number(quantiteRestante);
  if (Number.isNaN(valeur) || valeur < 0) {
    return res.status(400).json({ error: 'quantiteRestante invalide' });
  }

  const demande = await get('SELECT quantiteDemandee FROM material_requests WHERE id = ?', [id]);
  if (!demande) {
    return res.status(404).json({ error: 'Demande non trouvée' });
  }

  if (valeur > demande.quantiteDemandee) {
    return res.status(400).json({ error: 'La quantité restante ne peut pas dépasser la quantité demandée' });
  }

  await run('UPDATE material_requests SET quantiteRestante = ? WHERE id = ?', [valeur, id]);
  const updated = await get('SELECT * FROM material_requests WHERE id = ?', [id]);
  res.json(updated);
});

app.patch('/api/material-requests/:id/statut', async (req, res) => {
  const id = Number(req.params.id);
  const { statut } = req.body;
  const valeursAutorisees = ['EN_ATTENTE', 'EN_NEGOCIATION', 'APPROUVEE', 'REFUSEE'];

  if (!valeursAutorisees.includes(statut)) {
    return res.status(400).json({ error: 'Statut invalide' });
  }

  const result = await run('UPDATE material_requests SET statut = ? WHERE id = ?', [statut, id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Demande non trouvée' });
  }

  const demande = await get('SELECT * FROM material_requests WHERE id = ?', [id]);
  res.json(demande);
});

app.post('/api/purchase-orders', async (req, res) => {
  const { materialRequestId, creePar, fournisseur = '', montantTotal = null } = req.body;
  if (!materialRequestId || !creePar) {
    return res.status(400).json({ error: 'materialRequestId et creePar sont obligatoires' });
  }

  const demande = await get('SELECT * FROM material_requests WHERE id = ?', [materialRequestId]);
  if (!demande) {
    return res.status(404).json({ error: 'Demande de matériel non trouvée' });
  }

  const result = await run(
    'INSERT INTO purchase_orders (materialRequestId, creePar, fournisseur, montantTotal, dateCommande, statutValidation) VALUES (?, ?, ?, ?, ?, ?)',
    [materialRequestId, creePar, fournisseur, montantTotal != null ? Number(montantTotal) : null, new Date().toISOString(), 'EN_ATTENTE']
  );

  const po = await get('SELECT * FROM purchase_orders WHERE id = ?', [result.lastID]);
  res.status(201).json(po);
});

app.get('/api/purchase-orders', async (_req, res) => {
  const rows = await all(
    `SELECT po.*, mr.demandeur AS demandeur, mr.description AS demandeDescription, mr.itemName AS itemName, mr.projetId AS projetId,
            p.nomProjet AS projetNom, p.nomSite AS projetSite
     FROM purchase_orders po
     LEFT JOIN material_requests mr ON mr.id = po.materialRequestId
     LEFT JOIN projects p ON p.id = mr.projetId
     ORDER BY po.id DESC`
  );

  const result = rows.map(row => ({
    ...row,
    materialRequest: row.materialRequestId
      ? {
          id: row.materialRequestId,
          demandeur: row.demandeur,
          description: row.demandeDescription,
          projet: row.projetId ? { id: row.projetId, nomProjet: row.projetNom, nomSite: row.projetSite } : null,
        }
      : null,
  }));

  res.json(result);
});

app.patch('/api/purchase-orders/:id/validation', async (req, res) => {
  const id = Number(req.params.id);
  const { statutValidation } = req.body;
  const valeursAutorisees = ['EN_ATTENTE', 'VALIDE', 'REJETE'];

  if (!valeursAutorisees.includes(statutValidation)) {
    return res.status(400).json({ error: 'Statut de validation invalide' });
  }

  const result = await run('UPDATE purchase_orders SET statutValidation = ? WHERE id = ?', [statutValidation, id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Bon de commande non trouvé' });
  }

  const po = await get('SELECT * FROM purchase_orders WHERE id = ?', [id]);
  res.json(po);
});

app.delete('/api/expenses/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM expenses WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Dépense non trouvée' });
  }
  res.json({ message: 'Dépense supprimée' });
});

app.delete('/api/project-assignments/:id', async (req, res) => {
  const id = Number(req.params.id);
  const result = await run('DELETE FROM project_assignments WHERE id = ?', [id]);
  if (result.changes === 0) {
    return res.status(404).json({ error: 'Assignation non trouvée' });
  }
  res.json({ message: 'Assignation supprimée' });
});

// --- ROUTES TABLEAU DE BORD ADMIN ---

app.get('/api/admin/dashboard', async (req, res) => {
  if (req.user.role !== 'admin') {
    return res.status(403).json({ error: 'Accès réservé aux administrateurs' });
  }

  const [projects, requests, orders, expenses, revenues, assignments] = await Promise.all([
    all('SELECT * FROM projects ORDER BY createdAt DESC'),
    all('SELECT COUNT(*) as count FROM material_requests'),
    all('SELECT COUNT(*) as count FROM purchase_orders'),
    all('SELECT SUM(montantTotal) as total FROM expenses'),
    all('SELECT SUM(amount) as total FROM revenues'),
    all(`
      SELECT pa.*, p.nomProjet, u.username
      FROM project_assignments pa
      JOIN projects p ON p.id = pa.projetId
      JOIN users u ON u.id = pa.userId
      ORDER BY pa.assignedAt DESC
    `)
  ]);

  const totalExpenses = Number(expenses[0]?.total || 0);
  const totalRevenue = Number(revenues[0]?.total || 0);
  const profit = totalRevenue - totalExpenses;
  const countExpenses = (await all('SELECT COUNT(*) AS count FROM expenses'))[0]?.count || 0;
  const lastExpense = (await all('SELECT montantTotal FROM expenses ORDER BY dateExpense DESC LIMIT 1'))[0];
  const marginalCost = lastExpense ? Number(lastExpense.montantTotal) : 0;
  const averageCost = countExpenses ? totalExpenses / countExpenses : 0;
  const variableCost = totalExpenses * 0.65;
  const fixedCost = totalExpenses * 0.35;

  res.json({
    projects: projects || [],
    totalRequests: requests[0]?.count || 0,
    totalOrders: orders[0]?.count || 0,
    totalExpenses,
    totalRevenue,
    profit,
    countExpenses,
    countRevenues: (await all('SELECT COUNT(*) AS count FROM revenues'))[0]?.count || 0,
    averageCost,
    marginalCost,
    variableCost,
    fixedCost,
    chartData: {
      revenueVsExpenses: [totalRevenue, totalExpenses],
      profitVsExpenses: [profit, totalExpenses],
      costTypes: [averageCost, marginalCost, variableCost, fixedCost],
    },
    assignments: assignments || []
  });
});

// --- ROUTES COMPTABILITÉ ---

app.post('/api/materials', async (req, res) => {
  const { nom, categorie, unite, prixUnitaire, description = '' } = req.body;
  if (!nom || !categorie || !unite || prixUnitaire == null) {
    return res.status(400).json({ error: 'Tous les champs sont obligatoires' });
  }

  const result = await run(
    'INSERT INTO materials (nom, categorie, unite, prixUnitaire, description, createdAt) VALUES (?, ?, ?, ?, ?, ?)',
    [nom, categorie, unite, prixUnitaire, description, new Date().toISOString()]
  );

  const material = await get('SELECT * FROM materials WHERE id = ?', [result.lastID]);
  res.status(201).json(material);
});

app.get('/api/materials', async (_req, res) => {
  const rows = await all('SELECT * FROM materials ORDER BY categorie, nom');
  res.json(rows);
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
  const { projetId, description = '', amount, createdBy = req.user.username } = req.body;
  if (!description || amount == null) {
    return res.status(400).json({ error: 'description et amount sont obligatoires' });
  }

  const montant = Number(amount);
  if (Number.isNaN(montant) || montant <= 0) {
    return res.status(400).json({ error: 'amount doit être un nombre positif' });
  }

  const result = await run(
    'INSERT INTO revenues (projetId, description, amount, dateRevenue, createdBy) VALUES (?, ?, ?, ?, ?)',
    [projetId || null, description, montant, new Date().toISOString(), createdBy]
  );

  const revenue = await get('SELECT * FROM revenues WHERE id = ?', [result.lastID]);
  res.status(201).json(revenue);
});

app.get('/api/revenues', async (_req, res) => {
  const rows = await all(
    `SELECT r.*, p.nomProjet AS projectName
     FROM revenues r
     LEFT JOIN projects p ON p.id = r.projetId
     ORDER BY r.dateRevenue DESC`
  );
  res.json(rows);
});

// --- ROUTES ASSIGNATIONS PROJETS ---

app.post('/api/project-assignments', async (req, res) => {
  if (req.user.role !== 'admin') {
    return res.status(403).json({ error: 'Accès réservé aux administrateurs' });
  }

  const { projetId, userId, role } = req.body;
  if (!projetId || !userId || !role) {
    return res.status(400).json({ error: 'Tous les champs sont obligatoires' });
  }

  const result = await run(
    'INSERT INTO project_assignments (projetId, userId, role, assignedAt) VALUES (?, ?, ?, ?)',
    [projetId, userId, role, new Date().toISOString()]
  );

  const assignment = await get(`
    SELECT pa.*, p.nomProjet, u.username
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projetId
    JOIN users u ON u.id = pa.userId
    WHERE pa.id = ?
  `, [result.lastID]);

  res.status(201).json(assignment);
});

app.get('/api/project-assignments', async (_req, res) => {
  const rows = await all(`
    SELECT pa.*, p.nomProjet, u.username
    FROM project_assignments pa
    JOIN projects p ON p.id = pa.projetId
    JOIN users u ON u.id = pa.userId
    ORDER BY pa.assignedAt DESC
  `);
  res.json(rows);
});

app.listen(PORT, () => {
  console.log(`API Construction & Logistique démarrée sur http://localhost:${PORT}`);
});
