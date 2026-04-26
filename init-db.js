const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const DB_PATH = path.join(__dirname, 'erp.db');
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('✗ Erreur connexion:', err.message);
    process.exit(1);
  }
  console.log('✓ Connecté à la base de données');
});

function run(sql) {
  return new Promise((resolve, reject) => {
    db.run(sql, function(err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

async function init() {
  try {
    // Vérifier si la table existe
    console.log('Création de la table generated_documents...');
    
    await run(`DROP TABLE IF EXISTS generated_documents`);
    console.log('✓ Ancienne table supprimée');
    
    await run(`CREATE TABLE generated_documents (
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
    console.log('✓ Nouvelle table créée');
    
    // Vérifier la création
    db.all('SELECT COUNT(*) as count FROM generated_documents', [], (err, rows) => {
      if (err) {
        console.error('✗ Erreur vérification:', err.message);
      } else {
        console.log(`✓ Table vérifiée: ${rows[0].count} documents`);
      }
      db.close(() => {
        console.log('✓ Terminé');
        process.exit(0);
      });
    });
  } catch (err) {
    console.error('✗ Erreur:', err.message);
    db.close(() => {
      process.exit(1);
    });
  }
}

init();
