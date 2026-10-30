const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const server = fs.readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'erp.html'), 'utf8');
const roles = ['admin', 'directeur_rh', 'dirigeant', 'commis', 'gestionnaire_stock',
  'gestionnaire_stock_zone', 'gestionnaire_stock_songon', 'chef_chantier_site',
  'controle_achat', 'controle_achat_global', 'achat', 'employe_standard', 'chauffeur', 'ops_reviewer'];

function extract(source, start, end) {
  const first = source.indexOf(start);
  const last = source.indexOf(end, first);
  assert.ok(first >= 0 && last > first, `Missing source: ${start}`);
  return source.slice(first, last);
}

function backend(extra = {}) {
  const context = vm.createContext(extra);
  vm.runInContext(extract(server, 'const RESTRICTED_DEFAULT_ALLOWED_MODULES =', 'async function getUserAccessProfileByUsername('), context);
  return context;
}

test('every role baseline and effective profile includes internal messaging', () => {
  const context = backend();
  for (const role of roles) {
    assert.ok(context.getAccessProfileBaselineModules(role).has('admin-mail'), role);
    for (const profile of [
      {},
      { username: 'test', roleSnapshot: role, accreditationLevel: 'restricted' },
      { allowedModules: ['inventory'], deniedModules: ['admin-mail', 'projects'], forcedModule: 'projects' },
    ]) {
      const modules = context.computeEffectiveModulesForAccessProfile(profile, role);
      assert.ok(modules.has('admin-mail'), `${role}: messaging must remain accessible`);
    }
  }
});

test('mandatory messaging does not expand unrelated permissions or remove standard fallback', () => {
  const context = backend();
  const restricted = context.computeEffectiveModulesForAccessProfile({
    allowedModules: ['inventory', 'projects'], deniedModules: ['projects', 'admin-mail'],
  }, 'commis');
  assert.deepEqual(Array.from(restricted).sort(), ['admin-mail', 'inventory']);
  const standard = context.computeEffectiveModulesForAccessProfile({}, 'employe_standard');
  for (const key of ['hr-employees', 'hr-employee-search', 'hr-attendance', 'hr-calendar', 'hr-leave', 'guide-erp', 'admin-mail']) {
    assert.ok(standard.has(key), key);
  }
  assert.ok(!standard.has('access-profiles'));
  assert.ok(!standard.has('users'));
});

test('all roles including drivers can use mail APIs without opening admin APIs', () => {
  const context = vm.createContext();
  vm.runInContext(extract(server, 'function authorizeRoleAccess(', 'function hashTrackingToken('), context);
  for (const role of roles) {
    for (const [method, apiPath] of [
      ['GET', '/internal-mail/users'], ['GET', '/internal-mail/conversations'],
      ['POST', '/internal-mail/conversations'], ['POST', '/internal-mail/conversations/1/replies'],
      ['PATCH', '/internal-mail/conversations/1'], ['GET', '/internal-mail/attachments/1/download'],
    ]) {
      let permitted = false;
      context.authorizeRoleAccess({ user: { role }, method, path: apiPath }, {
        status: () => { throw new Error(`Mail denied: ${role} ${apiPath}`); },
      }, () => { permitted = true; });
      assert.ok(permitted);
    }
  }
  for (const apiPath of ['/admin/access-profiles', '/users', '/projects']) {
    let status;
    context.authorizeRoleAccess({ user: { role: 'chauffeur' }, method: 'GET', path: apiPath }, {
      status: code => { status = code; return { json: () => {} }; },
    }, () => { throw new Error(`Driver unexpectedly allowed: ${apiPath}`); });
    assert.equal(status, 403);
  }
});

test('frontend shows mail for all profiles including legacy restricted profiles', () => {
  for (const role of roles) {
    const context = vm.createContext({
      currentUserRole: role,
      currentUserAccessProfile: { effectiveModules: ['inventory'] },
      isAdmin: () => role === 'admin',
      normalizeAccessProfileModuleList: list => list,
      ADMIN_ONLY_MODULES: new Set(['access-profiles']),
      RESTRICTED_DEFAULT_ALLOWED_MODULES: new Set(['guide-erp']),
    });
    vm.runInContext(extract(html, '    function getAllowedModules()', '    function applyExecutiveReadOnlyState('), context);
    assert.ok(context.getAllowedModules().has('admin-mail'), role);
    if (role !== 'admin') assert.ok(!context.getAllowedModules().has('access-profiles'));
  }
  assert.ok(html.includes('const ADMIN_ONLY_MODULES = new Set([\'access-profiles\']);'));
  assert.ok(html.includes('data-module="admin-mail">✉️ Messagerie interne'));
});

test('admin dossier pickers lock mail as allowed and exclude denial even for old restrictions', () => {
  const elements = new Map();
  const getElementById = id => {
    if (!elements.has(id)) elements.set(id, { innerHTML: '', querySelectorAll: () => [] });
    return elements.get(id);
  };
  const context = vm.createContext({
    document: { getElementById },
    refreshAccessProfileModuleCatalog: () => [{ key: 'admin-mail', label: 'Messagerie interne' }, { key: 'inventory', label: 'Inventaire' }],
    normalizeAccessProfileModuleList: value => Array.isArray(value) ? value : String(value || '').split(',').filter(Boolean),
    getAccessProfileBaselineModules: () => new Set(['inventory', 'admin-mail']),
    escapeHtml: value => String(value),
  });
  vm.runInContext(extract(html, '    function renderAccessProfileModulePickers(', '    function renderAccessProfilesList('), context);
  context.renderAccessProfileModulePickers({ role: 'commis', allowedModules: ['inventory'], deniedModules: ['admin-mail'] });
  const allowedHtml = getElementById('access-profile-allowed-modules-list').innerHTML;
  const deniedHtml = getElementById('access-profile-denied-modules-list').innerHTML;
  assert.match(allowedHtml, /id="access-profile-allowed-admin-mail"[^>]*checked disabled/);
  assert.match(deniedHtml, /id="access-profile-denied-admin-mail"[^>]*disabled/);
  assert.doesNotMatch(deniedHtml, /id="access-profile-denied-admin-mail"[^>]*checked/);
  assert.match(allowedHtml, /Toujours autorisée pour tous les profils/);
});

test('saving admin profile persists other restrictions but cannot deny messaging', async () => {
  let handler;
  let saved;
  const context = backend({
    app: { patch: (_url, callback) => { handler = callback; } },
    ensureAccessProfileSchema: async () => {},
    get: async sql => sql.includes('FROM users') ? { id: 2, username: 'test', role: 'commis' } : { id: 3 },
    run: async (sql, params) => {
      if (sql.includes('UPDATE user_access_profiles')) {
        saved = { allowedModules: params[2], deniedModules: params[3], roleSnapshot: params[0] };
      }
    },
    getNextTableId: async () => 1,
    getUserAccessProfileByUsername: async () => saved,
  });
  vm.runInContext(extract(server, "app.patch('/api/admin/access-profiles/:username',", "app.post('/api/admin/access-profiles/restore-all',"), context);
  let response;
  await handler({
    user: { role: 'admin', username: 'admin' }, params: { username: 'test' },
    body: { allowedModules: ['inventory'], deniedModules: ['admin-mail', 'projects'] },
  }, {
    json: value => { response = value; },
    status: code => { throw new Error(`Unexpected status ${code}`); },
  });
  assert.equal(saved.deniedModules, 'projects');
  assert.equal(saved.allowedModules, 'inventory');
  assert.ok(response.effectiveModules.includes('admin-mail'));
  assert.ok(response.deniedModules.includes('projects'));
});

test('frontend role baseline and profile catalog label include messaging', () => {
  const context = vm.createContext({
    normalizeText: value => String(value || '').toLowerCase(),
    normalizeAccessProfileModuleList: value => value,
    getMenuModuleKeys: () => [],
    KOKAN_ALLOWED_MODULES: new Set(['inventory']),
  });
  for (const name of ['HR_DIRECTOR', 'EXECUTIVE', 'ACHAT', 'PROCUREMENT_REVIEWER', 'COMMIS',
    'GEST_STOCK', 'ZONE_STOCK_MANAGER', 'SITE_CHIEF', 'STANDARD_EMPLOYEE']) {
    context[`${name}_ALLOWED_MODULES`] = new Set();
  }
  vm.runInContext(extract(html, '    function getAccessProfileBaselineModules(', '    const AUTO_MAP_REFRESH_MS'), context);
  for (const role of roles) assert.ok(context.getAccessProfileBaselineModules(role, 'test').has('admin-mail'), role);
  assert.ok(context.getAccessProfileBaselineModules('gestionnaire_stock_songon', 'Kokan_SK').has('admin-mail'));
  assert.ok(html.includes("'admin-mail': 'Messagerie interne'"));
});
