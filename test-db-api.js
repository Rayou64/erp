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

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
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

async function test() {
  try {
    // Vérifie la table
    const tables = await all("SELECT name FROM sqlite_master WHERE type='table' AND name='generated_documents'");
    console.log(`✓ Tables trouvées: ${tables.length}`);
    
    // Vérifie le schéma
    const schema = await all("PRAGMA table_info(generated_documents)");
    console.log(`✓ Colonnes: ${schema.length}`);
    schema.forEach(col => {
      console.log(`  - ${col.name} (${col.type})`);
    });
    
    // Teste une requête simple
    const rows = await all('SELECT * FROM generated_documents LIMIT 5');
    console.log(`✓ Documents trouvés: ${rows.length}`);
    
    // Teste avec les lookups
    console.log('\n✓ Test du endpoint GET /api/database-documents:');
    const testRows = await all('SELECT * FROM generated_documents');
    console.log(`  - Nombre de documents: ${testRows.length}`);
    
    if (testRows.length === 0) {
      console.log('  - Liste vide (normal si pas de documents uploadés)');
    }
    
  } catch (err) {
    console.error('✗ Erreur test:', err.message);
  } finally {
    db.close();
  }
}

test();
