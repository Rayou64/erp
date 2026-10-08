const sqlite3 = require('sqlite3');
const path = require('path');
const dbPath = path.join(process.cwd(), 'db.sqlite');
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, (err) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
});

db.all("SELECT username, role FROM users WHERE LOWER(username) IN ('boga','cissoko') ORDER BY username", (err, rows) => {
  if (err) {
    console.error(err);
    db.close(() => process.exit(1));
    return;
  }
  console.log(JSON.stringify(rows));
  db.close();
});
