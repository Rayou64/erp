const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('data.db');

db.all(`SELECT id, nomProjet, prefecture, typeMaison FROM projects WHERE typeMaison = 'ZONE_STOCK' ORDER BY id`, (err, rows) => {
  if(err) {
    console.error('Error:', err);
  } else {
    console.log(JSON.stringify(rows, null, 2));
  }
  db.close();
});
