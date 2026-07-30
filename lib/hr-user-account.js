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

module.exports = { normalizeHrUsernameCandidate, ensureHrEmployeeUserAccount };
