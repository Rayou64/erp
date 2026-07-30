function normalizeHrUsernameCandidate(rawUsername, fallbackName = '') {
  const base = String(rawUsername || fallbackName || '').trim();
  if (!base) return '';
  const normalized = base
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '');
  return normalized;
}

async function ensureHrEmployeeUserAccount({
  get,
  run,
  getNextTableId,
  username,
  fullName,
  createdBy,
  role = 'employe_standard',
}) {
  const usernameValue = normalizeHrUsernameCandidate(username, fullName);
  if (!usernameValue) {
    return { created: false, username: '', initialPassword: '', role };
  }

  const existingUser = await get('SELECT id, username, role FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))', [usernameValue]);
  if (existingUser) {
    return { created: false, username: usernameValue, initialPassword: '', role: String(existingUser.role || role) };
  }

  const nextUserId = await getNextTableId('users');
  const initialPassword = `${usernameValue}@2026`;
  const hashedPassword = require('bcryptjs').hashSync(initialPassword, 10);
  await run(
    'INSERT INTO users (id, username, password, role, createdAt) VALUES (?, ?, ?, ?, ?)',
    [nextUserId, usernameValue, hashedPassword, role, new Date().toISOString()]
  );

  return { created: true, username: usernameValue, initialPassword, role };
}

async function reconcileHrEmployeesToUserAccounts({
  get,
  run,
  getNextTableId,
  employees = [],
  createdBy = 'admin',
  role = 'employe_standard',
}) {
  const createdAccounts = [];

  for (const employee of employees || []) {
    const employeeId = Number(employee?.id || 0);
    const fullName = String(employee?.fullName || '').trim();
    const username = String(employee?.username || '').trim() || normalizeHrUsernameCandidate(fullName);
    if (!employeeId || !username) continue;

    const existingUser = await get('SELECT id FROM users WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))', [username]);
    if (existingUser) continue;

    const account = await ensureHrEmployeeUserAccount({
      get,
      run,
      getNextTableId,
      username,
      fullName,
      createdBy,
      role,
    });

    if (account.created) {
      createdAccounts.push({ id: employeeId, username: account.username, initialPassword: account.initialPassword });
    }
  }

  return createdAccounts;
}

module.exports = { normalizeHrUsernameCandidate, ensureHrEmployeeUserAccount, reconcileHrEmployeesToUserAccounts };
