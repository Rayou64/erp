const USER_RESTORE_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
let profileChangesPending = Promise.resolve();

async function withUserProfileChanges(operation) {
  const previous = profileChangesPending;
  let release;
  profileChangesPending = new Promise(resolve => { release = resolve; });
  await previous;
  try {
    return await operation();
  } finally {
    release();
  }
}

function linkedEmployeeJoin(userAlias, employeeAlias) {
  return `LEFT JOIN hr_employees ${employeeAlias} ON ${employeeAlias}.id = (
    SELECT candidate.id FROM hr_employees candidate
    WHERE LOWER(TRIM(candidate.username)) = LOWER(TRIM(${userAlias}.username))
    ORDER BY CASE
      WHEN TRIM(candidate.fullName) = '' THEN 2
      WHEN LOWER(TRIM(candidate.fullName)) = LOWER(TRIM(candidate.jobTitle))
        OR LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(candidate.fullName), ' ', ''), '_', ''), '-', ''), '.', ''))
         = LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(candidate.username), ' ', ''), '_', ''), '-', ''), '.', ''))
      THEN 1 ELSE 0 END, candidate.id DESC LIMIT 1
  )`;
}

const INTERNAL_MAIL_IDENTITIES = `(SELECT id, username, role FROM users
  UNION ALL SELECT userId AS id, username, '' AS role FROM deleted_user_accounts
  WHERE NOT EXISTS (SELECT 1 FROM users WHERE users.id = deleted_user_accounts.userId))`;

async function ensureProjectAssignmentRetention(db) {
  if (db.driver === 'postgres') {
    const constraints = await db.all(`SELECT conname AS name FROM pg_constraint
      WHERE conrelid = 'project_assignments'::regclass AND confrelid = 'users'::regclass
      AND contype = 'f' AND confdeltype <> 'n'`);
    for (const constraint of constraints) {
      const name = '"' + constraint.name.replace(/"/g, '""') + '"';
      await db.run(`ALTER TABLE project_assignments
        ALTER COLUMN userId DROP NOT NULL, DROP CONSTRAINT ${name},
        ADD CONSTRAINT project_assignments_user_retention FOREIGN KEY (userId) REFERENCES users(id) ON DELETE SET NULL`);
    }
    return;
  }
  const foreignKeys = await db.all('PRAGMA foreign_key_list(project_assignments)');
  if (!foreignKeys.some(key => key.from === 'userId' && key.table === 'users' && key.on_delete !== 'SET NULL')) return;
  const table = await db.get("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'project_assignments'");
  const schema = table.sql
    .replace(/^(CREATE TABLE(?: IF NOT EXISTS)?)\s+"?project_assignments"?/i, '$1 project_assignments_retained')
    .replace(/("?userId"?\s+INTEGER)\s+NOT NULL/i, '$1')
    .replace(/(FOREIGN KEY\s*\(\s*"?userId"?\s*\)\s*REFERENCES\s+"?users"?\s*\(\s*"?id"?\s*\)\s*ON DELETE)\s+CASCADE/i, '$1 SET NULL');
  if (!schema.includes('project_assignments_retained') || !/ON DELETE SET NULL/i.test(schema)
    || /"?userId"?\s+INTEGER\s+NOT NULL/i.test(schema)) {
    throw new Error('Migration de conservation des assignations impossible : schéma inattendu');
  }
  const dependents = await db.all(`SELECT sql FROM sqlite_master WHERE tbl_name = 'project_assignments'
    AND type IN ('index', 'trigger') AND sql IS NOT NULL`);
  await db.run('BEGIN IMMEDIATE');
  try {
    await db.run(schema);
    await db.run('INSERT INTO project_assignments_retained SELECT * FROM project_assignments');
    await db.run('DROP TABLE project_assignments');
    await db.run('ALTER TABLE project_assignments_retained RENAME TO project_assignments');
    for (const dependent of dependents) await db.run(dependent.sql);
    await db.run('COMMIT');
  } catch (error) {
    await db.run('ROLLBACK');
    throw error;
  }
}

async function setUserProfileState({ run }, userId, employeeId, state) {
  await run(`INSERT INTO user_profile_states (employeeId, userId, deactivatedAt, deleteAfter, deletedAt)
    VALUES (?, ?, ?, ?, ?) ON CONFLICT(employeeId) DO UPDATE SET
    userId = excluded.userId, deactivatedAt = excluded.deactivatedAt,
    deleteAfter = excluded.deleteAfter, deletedAt = excluded.deletedAt`,
  [employeeId, userId, state.deactivatedAt || '', state.deleteAfter || '', state.deletedAt || '']);
}

async function deleteUserAccount(db, user, now = new Date().toISOString()) {
  const employees = await db.all('SELECT id FROM hr_employees WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))', [user.username]);
  for (const employee of employees) {
    await setUserProfileState(db, user.id, employee.id, { deletedAt: now });
  }
  await db.run(`INSERT INTO deleted_user_accounts (userId, username, deletedAt) VALUES (?, ?, ?)
    ON CONFLICT(userId) DO NOTHING`, [user.id, user.username, now]);
  await db.run('DELETE FROM push_subscriptions WHERE userId = ?', [user.id]);
  await db.run('DELETE FROM password_reset_requests WHERE userId = ?', [user.id]);
  await db.run('DELETE FROM user_access_profiles WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))', [user.username]);
  await db.run('DELETE FROM users WHERE id = ?', [user.id]);
}

async function syncUserProfileAccess(db, user, now = new Date().toISOString()) {
  const profiles = await db.all(`SELECT he.id, COALESCE(ps.deactivatedAt, '') AS deactivatedAt,
    COALESCE(ps.deleteAfter, '') AS deleteAfter, COALESCE(ps.deletedAt, '') AS deletedAt
    FROM hr_employees he LEFT JOIN user_profile_states ps ON ps.employeeId = he.id AND ps.userId = ?
    WHERE LOWER(TRIM(he.username)) = LOWER(TRIM(?))`, [user.id, user.username]);
  const remaining = profiles.filter(profile => !profile.deletedAt);
  if (!remaining.length) {
    await deleteUserAccount(db, user, now);
    return { deleted: true, sharedAccountActive: false };
  }
  if (remaining.some(profile => !profile.deactivatedAt)) {
    await db.run(`UPDATE users SET deactivatedAt = '', deleteAfter = '', tokenVersion = tokenVersion + 1
      WHERE id = ? AND deactivatedAt <> ''`, [user.id]);
    return { deleted: false, sharedAccountActive: true };
  }
  const deleteAfter = remaining.map(profile => profile.deleteAfter).sort().at(-1);
  await db.run(`UPDATE users SET deactivatedAt = ?, deleteAfter = ?,
    tokenVersion = tokenVersion + CASE WHEN deactivatedAt = '' THEN 1 ELSE 0 END WHERE id = ?`,
  [remaining[0].deactivatedAt, deleteAfter, user.id]);
  await db.run('DELETE FROM push_subscriptions WHERE userId = ?', [user.id]);
  await db.run('DELETE FROM password_reset_requests WHERE userId = ?', [user.id]);
  return { deleted: false, sharedAccountActive: false };
}

async function purgeExpiredUserAccountsUnlocked(db, now) {
  const expiredProfiles = await db.all(`SELECT ps.employeeId, u.id, u.username FROM user_profile_states ps
    JOIN users u ON u.id = ps.userId
    WHERE ps.deletedAt = '' AND ps.deactivatedAt <> '' AND ps.deleteAfter <> '' AND ps.deleteAfter <= ?`, [now]);
  for (const profile of expiredProfiles) {
    await setUserProfileState(db, profile.id, profile.employeeId, { deletedAt: now });
    await syncUserProfileAccess(db, profile, now);
  }
  const expired = await db.all("SELECT id, username FROM users WHERE deactivatedAt <> '' AND deleteAfter <> '' AND deleteAfter <= ?", [now]);
  for (const user of expired) await deleteUserAccount(db, user, now);
  return expired.length + expiredProfiles.length;
}

function purgeExpiredUserAccounts(db, now = new Date().toISOString()) {
  return withUserProfileChanges(() => purgeExpiredUserAccountsUnlocked(db, now));
}

module.exports = {
  USER_RESTORE_WINDOW_MS, linkedEmployeeJoin, INTERNAL_MAIL_IDENTITIES, ensureProjectAssignmentRetention,
  withUserProfileChanges, setUserProfileState, deleteUserAccount, syncUserProfileAccess, purgeExpiredUserAccounts,
};
