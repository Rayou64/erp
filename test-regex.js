const pattern = /^\/stock-issue-authorizations$/;
const path1 = '/stock-issue-authorizations';
const path2 = '/api/stock-issue-authorizations';
console.log(`Path '${path1}' matches pattern: ${pattern.test(path1)}`);
console.log(`Path '${path2}' matches pattern: ${pattern.test(path2)}`);
