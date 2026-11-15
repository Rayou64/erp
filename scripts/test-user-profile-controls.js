const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'erp.html'), 'utf8');
const source = html.slice(html.indexOf('    function renderUsersModuleTable('), html.indexOf('    async function loadUsersModule('));

function frontend({ admin = true, confirm = true, fail = false } = {}) {
  const elements = new Map();
  const getElementById = id => {
    if (!elements.has(id)) elements.set(id, { innerHTML: '', value: '', querySelectorAll: () => [] });
    return elements.get(id);
  };
  const calls = [];
  const messages = [];
  const confirmations = [];
  const rows = [
    { id: 8, username: 'duplicate', linkedEmployeeId: 10, linkedEmployeeName: 'Employé A', hasUserAccount: 1, linkedProfileCount: 2 },
    { id: 8, username: 'duplicate', linkedEmployeeId: 11, linkedEmployeeName: 'Employé B', hasUserAccount: 1, linkedProfileCount: 2, deactivatedAt: '2026-11-15', deleteAfter: '2026-12-15' },
    { id: 1, username: 'admin', linkedEmployeeId: 0, hasUserAccount: 1 },
  ];
  const context = vm.createContext({
    document: { getElementById },
    usersAdminCache: rows,
    currentUserUsername: 'admin',
    usersLastLoadedAt: null, usersPageSize: 10, usersCurrentPage: 1,
    isAdmin: () => admin,
    getRoleDisplayLabel: value => value || '-',
    normalizeText: value => value,
    escapeHtml: value => String(value),
    formatDateWithTime: value => value,
    window: { confirm: message => { confirmations.push(message); return confirm; } },
    showMessage: (...args) => messages.push(args),
    fetchJson: async (...args) => {
      calls.push(args);
      if (fail) throw new Error('Échec de validation');
      return { sharedAccountActive: true };
    },
    loadUsersModule: async () => {},
  });
  vm.runInContext(source, context);
  return { context, elements, calls, messages, confirmations };
}

test('all duplicate rows remain visible and lifecycle buttons stay in their approved columns', () => {
  const browser = frontend();
  browser.context.renderUsersModuleTable();
  const markup = browser.elements.get('users-table-body').innerHTML;
  const rows = [...markup.matchAll(/<tr>([\s\S]*?)<\/tr>/g)].map(match => match[1]);
  assert.equal(rows.length, 3);
  for (const [index, employeeId] of [[1, 10], [2, 11]]) {
    const columns = [...rows[index].matchAll(/<td>([\s\S]*?)<\/td>/g)].map(match => match[1]);
    assert.equal(columns.length, 8);
    assert.match(columns[6], new RegExp(`data-user-lifecycle="${employeeId === 10 ? 'deactivate' : 'restore'}"`));
    assert.match(columns[6], /users-lifecycle-button/);
    assert.doesNotMatch(columns[6], /data-user-lifecycle="delete"/);
    assert.match(columns[7], /data-user-lifecycle="delete"/);
    assert.match(columns[7], new RegExp(`data-employee-id="${employeeId}"`));
    assert.match(columns[0], new RegExp(`fiche RH ${employeeId}`));
  }
  assert.match(rows[0], /data-user-lifecycle="delete"[^>]+disabled/);
  assert.match(rows[0], /data-user-lifecycle="deactivate"[^>]+disabled/);
});

test('non-admin users cannot see lifecycle actions', () => {
  const browser = frontend({ admin: false });
  browser.context.renderUsersModuleTable();
  assert.doesNotMatch(browser.elements.get('users-table-body').innerHTML, /data-user-lifecycle=/);
});

test('cancelled irreversible deletion never sends an API request', async () => {
  const browser = frontend({ confirm: false });
  await browser.context.changeUserLifecycle(8, 11, 'delete');
  assert.equal(browser.calls.length, 0);
  assert.match(browser.confirmations[0], /Employé B/);
  assert.match(browser.confirmations[0], /fiche RH 11/);
  assert.match(browser.confirmations[0], /irréversible/);
});

test('deactivation, restoration and deletion target the exact duplicate with distinct API methods', async () => {
  const browser = frontend();
  await browser.context.changeUserLifecycle(8, 10, 'deactivate');
  await browser.context.changeUserLifecycle(8, 11, 'restore');
  await browser.context.changeUserLifecycle(8, 11, 'delete');
  assert.deepEqual(browser.calls.map(([url, options]) => [url, options.method, JSON.parse(options.body)]), [
    ['/api/admin/users/8/deactivate', 'PATCH', { employeeId: 10 }],
    ['/api/admin/users/8/restore', 'PATCH', { employeeId: 11 }],
    ['/api/admin/users/8', 'DELETE', { employeeId: 11, confirm: 'DELETE_USER_PERMANENTLY' }],
  ]);
  assert.match(browser.confirmations[0], /30 jours/);
  assert.match(browser.confirmations[0], /Seule la ligne choisie/);
  assert.match(browser.messages[2][0], /sans réhabilitation/);
});

test('failed actions surface errors and never claim success', async () => {
  const browser = frontend({ fail: true });
  await browser.context.changeUserLifecycle(8, 11, 'delete');
  assert.deepEqual(browser.messages, [['Échec de validation', 'error']]);
});
