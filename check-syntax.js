const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.join(__dirname, 'public', 'erp.html');
const content = fs.readFileSync(htmlPath, 'utf8');

// Extract JavaScript
const scriptStart = content.indexOf('<script>');
const scriptEnd = content.lastIndexOf('</script>');

if (scriptStart >= 0 && scriptEnd > scriptStart) {
  const jsCode = content.substring(scriptStart + 8, scriptEnd);
  console.log(`JavaScript code length: ${jsCode.length}`);
  
  // Try to evaluate it to catch errors
  try {
    // This won't actually execute, just check syntax
    new Function(jsCode);
    console.log('✓ Syntax is valid!');
  } catch (err) {
    console.error('✗ Syntax error found:');
    console.error(err.message);
    console.error('Line info:', err.stack.split('\n')[0]);
  }
} else {
  console.log('Script tags not found');
}
