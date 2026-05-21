const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('data.db');

db.run('ALTER TABLE projects ADD COLUMN villaType TEXT DEFAULT ""', (err) => {
  if (err && err.message.includes('duplicate')) {
    console.log('Column already exists');
  } else if (err) {
    console.error(err);
  } else {
    console.log('Column villaType added successfully');
  }
  db.close();
});
