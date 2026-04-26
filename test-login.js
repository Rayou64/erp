// test-login.js - Script pour tester la connexion
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const path = require('path');

const DB_FILE = path.join(__dirname, 'data.db');
const db = new sqlite3.Database(DB_FILE);

db.get('SELECT * FROM users WHERE username = ?', ['admin'], async (err, user) => {
  if (err) {
    console.error('Erreur DB:', err);
    return;
  }

  if (!user) {
    console.log('Utilisateur admin non trouvé');
    return;
  }

  console.log('Utilisateur trouvé:', user.username);
  console.log('Role:', user.role);

  const valid = await bcrypt.compare('admin123', user.password);
  console.log('Mot de passe valide:', valid);

  db.close();
});