/**
 * fix-pg-sql-strings.js  v2
 * Fixes JavaScript SQL strings that use double-quotes for SQL string literals.
 * PostgreSQL requires single quotes for string literals (double quotes = identifiers).
 *
 * Problem: run('ALTER TABLE foo ADD COLUMN bar TEXT DEFAULT ""')
 *   -> PostgreSQL error: "" is treated as an empty identifier
 *
 * Fix: run("ALTER TABLE foo ADD COLUMN bar TEXT DEFAULT ''")
 *   -> PostgreSQL OK: '' is an empty string
 *
 * Strategy: For each line containing run(  or similar with a SQL string,
 * if it has DEFAULT "..." or SET col = "...", rewrite using template literals.
 */

const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'app.js');
let content = fs.readFileSync(filePath, 'utf8');

const lines = content.split('\n');
const fixed = lines.map((line, i) => {
  // Only target lines that are SQL strings inside run() calls
  // Look for: run('...DEFAULT "..."...') or run('...COALESCE(..., "...")...')
  // Pattern: the JS string uses single-quote delimiters AND contains SQL double-quoted literals

  // Match lines like: await run('ALTER TABLE ... DEFAULT "something"');
  // or: await run('UPDATE ... SET col = COALESCE(col, "something")');
  
  // We'll detect: single-quoted JS string containing SQL DEFAULT/COALESCE with "..."
  const sqlStringPattern = /^(\s*(?:await\s+)?(?:(?:try\s*\{\s*)?(?:await\s+)?)?run\()('.*?DEFAULT\s+)"(.*?)"/;
  const coalescePattern = /^(\s*(?:await\s+)?(?:(?:try\s*\{\s*)?(?:await\s+)?)?run\()('.*?")/;

  // Simpler approach: find lines where:
  // 1. The run() call is wrapped in single quotes
  // 2. Inside the SQL, there are double-quoted string literals (for DEFAULT values)
  
  // The specific broken patterns after the failed replacement:
  // 1. `run("ALTER TABLE ... DEFAULT ''` -- opening quote changed to ", closing still '
  // 2. `run('ALTER TABLE ... DEFAULT ''` -- both changed but syntax broken

  // Fix pattern 1: run("... DEFAULT ''...') -- mixed quotes  
  // These lines have opening " but closing '
  if (/run\(".*DEFAULT ''.*'\)/.test(line) || /run\(".*DEFAULT '(?:EN_COURS|EN_ATTENTE|Non renseigne|system|admin|smartphone|SITE_TRANSFER|manual|offline)'.*'\)/.test(line)) {
    // Change closing ' to "
    line = line.replace(/'\);(\s*(\/\/)?.*)$/, '");$1');
    line = line.replace(/'\);$/, '");');
    return line;
  }

  // Fix pattern 2: mixed single/double quotes in same line from previous failed attempt
  // Lines that now have: run("ALTER TABLE ... INTEGER');  (opening changed, closing not)
  if (/run\(".*'\)/.test(line) && !line.includes('DEFAULT') && !line.includes("''")) {
    // If the content looks like SQL ALTER TABLE with no DEFAULT, just fix closing quote
    if (/run\("ALTER TABLE|run\("UPDATE/.test(line)) {
      line = line.replace(/'\)(\s*;)/, '")$1');
      return line;
    }
  }

  return line;
});

content = fixed.join('\n');
fs.writeFileSync(filePath, content, 'utf8');
console.log('Phase 1 done');

// Now do the remaining double-quote to single-quote SQL conversions
// for lines that still have the original double-quote form
content = fs.readFileSync(filePath, 'utf8');

// Process line by line
const lines2 = content.split('\n');
const fixed2 = lines2.map(line => {
  // Target: run('...') where ... contains SQL double-quoted string literals
  // These look like: run('ALTER TABLE foo ADD COLUMN bar TEXT DEFAULT ""')
  // or:             run('UPDATE foo SET bar = COALESCE(bar, "val")')
  
  // If line contains run(' and has " inside SQL keywords
  if (!line.includes("run('")) return line;

  // Check if there are double-quoted SQL literals inside
  // SQL double-quoted literals appear after: DEFAULT, VALUES, COALESCE(...,
  const hasSqlDoubleQuotes = /"[^"]*"/.test(line) && 
    (line.includes('DEFAULT "') || line.includes('COALESCE(') || line.includes('SET '));
  
  if (!hasSqlDoubleQuotes) return line;

  // Strategy: convert the entire run('...') to use backtick template literal
  // run('...sql with "val"...') -> run(`...sql with 'val'...`)
  // Find the JS string content and rewrap it
  
  // Match: run('...') 
  return line.replace(/run\('((?:[^'\\]|\\.)*)'\)/, (match, sql) => {
    // Replace double-quoted SQL string literals with single-quoted ones
    const fixedSql = sql.replace(/"([^"]*)"/g, "'$1'");
    return `run(\`${fixedSql}\`)`;
  });
});

content = fixed2.join('\n');
fs.writeFileSync(filePath, content, 'utf8');
console.log('Phase 2 done');
