/**
 * Fixes broken SQL strings in app.js caused by mixed JS/SQL quote styles.
 * 
 * Problem: run('ALTER TABLE ... DEFAULT ""') fails on PostgreSQL.
 * Previous attempt left some lines with mixed delimiters like run("...').
 * 
 * This script:
 * 1. Finds lines with run() containing SQL strings
 * 2. Extracts the SQL content regardless of delimiter state
 * 3. Rewrites as run(`...`) template literals with single-quoted SQL strings
 */

const fs = require('fs');
const path = require('path');

const appPath = path.join(__dirname, '..', 'app.js');
const content = fs.readFileSync(appPath, 'utf8');
const lines = content.split('\n');

let fixedCount = 0;

const result = lines.map((line, lineNum) => {
  // Skip lines that don't have run(
  if (!line.includes('run(')) return line;
  
  // Skip backtick template literals - already fine
  if (/run\(`/.test(line)) return line;
  
  // Find the run( position
  const match = line.match(/^(\s*)((?:await\s+)?run\()(.*)$/);
  if (!match) return line;

  const [, indent, callPrefix, afterParen] = match;

  // afterParen starts with the string delimiter (', ") or something else
  const delim = afterParen[0];
  if (delim !== "'" && delim !== '"') return line;

  // Try to extract the SQL content between delimiters
  // The string may be broken (mismatched delimiters), so we extract heuristically:
  // Find the last ); in the line, which is the end of run(...)
  const lastParen = line.lastIndexOf(');');
  if (lastParen === -1) return line;

  // Raw argument: everything between run( and );
  const runStart = line.indexOf('run(') + 4;
  let rawArg = line.slice(runStart, lastParen);

  // Strip outer string delimiters (they may be mismatched)
  // Remove first char if it's ' or "
  if (rawArg.length > 0 && (rawArg[0] === "'" || rawArg[0] === '"')) {
    rawArg = rawArg.slice(1);
  }
  // Remove last char if it's ' or "
  if (rawArg.length > 0 && (rawArg[rawArg.length - 1] === "'" || rawArg[rawArg.length - 1] === '"')) {
    rawArg = rawArg.slice(0, -1);
  }

  // Check if this SQL contains double-quoted literals that need fixing
  // (PostgreSQL SQL double quotes = identifiers, not strings)
  const hasDqLiteral = /"[^"]*"/.test(rawArg) && 
    (rawArg.includes('DEFAULT "') || /COALESCE\([^)]*"/.test(rawArg) || 
     rawArg.includes('SET ') || rawArg.includes('VALUES '));
  
  // Also check if the line has mismatched delimiters (broken from previous attempt)
  const openDelim = afterParen[0];
  const closeDelimMatch = line.match(/(['"]);?\s*$/) || line.match(/(['"]);/);
  const closeDelim = closeDelimMatch ? closeDelimMatch[1] : null;
  const hasMismatch = openDelim && closeDelim && openDelim !== closeDelim;

  if (!hasDqLiteral && !hasMismatch) return line;

  // Fix the SQL: replace double-quoted SQL string literals with single quotes
  const fixedSql = rawArg.replace(/"([^"]*)"/g, "'$1'");

  // Reconstruct the line using template literal
  // Preserve any trailing comment
  const trailingComment = line.match(/(\/\/[^]*)?$/)?.[0]?.trim() || '';
  const newLine = indent + 'await run(`' + fixedSql + '`);';
  
  if (newLine !== line) {
    fixedCount++;
    console.log(`Line ${lineNum + 1}: Fixed`);
  }
  return newLine;
});

fs.writeFileSync(appPath, result.join('\n'), 'utf8');
console.log(`\nTotal fixed: ${fixedCount} lines`);
