const assert = require('assert');
const { normalizeHrUsernameCandidate, ensureHrEmployeeUserAccount, reconcileHrEmployeesToUserAccounts } = require('../lib/hr-user-account');

(async () => {
  const base = normalizeHrUsernameCandidate(' Jean Dupont ', 'Jean Dupont');
  assert.strictEqual(base, 'jeandupont');

  const store = new Map();
  const get = async (query, params = []) => {
    const username = String(params[0] || '').trim().toLowerCase();
    if (query.includes('FROM users')) {
      return store.has(username) ? { id: 1, username } : null;
    }
    if (query.includes('FROM hr_employees')) {
      return null;
    }
    return null;
  };
  const run = async (query, params = []) => {
    const username = String(params[1] || '').trim().toLowerCase();
    if (query.includes('INSERT INTO users')) {
      store.set(username, { id: params[0], username, role: params[3] });
    }
    return { changes: 1, lastID: params[0] };
  };

  const result = await ensureHrEmployeeUserAccount({
    get,
    run,
    getNextTableId: async () => 7,
    username: 'Jean Dupont',
    fullName: 'Jean Dupont',
    createdBy: 'admin',
  });

  assert.strictEqual(result.created, true);
  assert.strictEqual(result.username, 'jeandupont');
  assert.ok(result.initialPassword.endsWith('@2026'));

  let nextId = 8;
  const createdAccounts = await reconcileHrEmployeesToUserAccounts({
    get,
    run,
    getNextTableId: async () => nextId++,
    createdBy: 'admin',
    employees: [
      { id: 1, fullName: 'Jean Dupont', username: 'jeandupont' },
      { id: 2, fullName: 'Boga', username: 'boga' },
      { id: 3, fullName: 'Cissoko', username: 'cissoko' },
    ],
  });

  assert.strictEqual(createdAccounts.length, 2);
  assert.deepStrictEqual(createdAccounts.map(entry => entry.username), ['boga', 'cissoko']);
  assert.strictEqual(store.get('boga').role, 'employe_standard');
  console.log('hr user account test passed');
})().catch(err => {
  console.error(err);
  process.exit(1);
});
