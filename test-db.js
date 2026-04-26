const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const DB_PATH = path.join(__dirname, 'erp.db');
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('Erreur connexion:', err.message);
    process.exit(1);
  }
  console.log('✓ Connecté à la base de données');
});

// Test la table generated_documents
db.all('SELECT COUNT(*) as count FROM generated_documents', [], (err, rows) => {
  if (err) {
    console.error('✗ Erreur requête:', err.message);
  } else {
    console.log(`✓ Table generated_documents: ${rows[0].count} documents`);
    
    // Affiche quelques documents
    db.all('SELECT id, title, fileName, entityType FROM generated_documents LIMIT 3', [], (err, rows) => {
      if (err) {
        console.error('Erreur:', err.message);
      } else {
        console.log('Premiers documents:');
        rows.forEach(r => {
          console.log(`  - ${r.id}: ${r.title || r.fileName} (${r.entityType})`);
        });
      }
      db.close(() => {
        console.log('✓ Terminé');
      });
    });
  }
});
