const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');
const sqlite3 = require('sqlite3');

const server = fs.readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'erp.html'), 'utf8');
const kokan = { username: 'Kokan_SK', role: 'gestionnaire_stock_songon' };

function extract(source, start, end) {
  const first = source.indexOf(start);
  const last = source.indexOf(end, first);
  assert.ok(first >= 0 && last > first, `Missing source: ${start}`);
  return source.slice(first, last);
}

function scopeContext(extra = {}) {
  const context = vm.createContext(extra);
  vm.runInContext(extract(server, 'function extractSiteNumericToken(', 'function isProcurementReviewerRole('), context);
  return context;
}

test('Kokan sees Songon Kassemble and its building types, not other projects', () => {
  const context = scopeContext();
  for (const nomProjet of ['Songon Kassemble', 'Songon Kassemble - T1', 'Songon Kassemble - T2', ' SONGON KASSEMBLÉ - T3 ']) {
    assert.equal(context.isInUserProjectScope(kokan, { nomProjet }), true);
  }
  for (const row of [
    { nomProjet: 'Adzope', prefecture: 'Songon' },
    { nomProjet: 'Songon autre' },
    { nomProjet: 'Songon Kassemblement' },
    {},
  ]) {
    assert.equal(context.isInUserProjectScope(kokan, row), false);
  }
  assert.equal(context.isInUserProjectScope(kokan, { projetNom: 'Songon Kassemble' }), true);
});

test('other stock managers, site chiefs and administrators retain their scopes', () => {
  const context = scopeContext();
  assert.equal(context.isInUserProjectScope({ role: 'admin' }, { nomProjet: 'Adzope' }), true);
  assert.equal(context.isInUserProjectScope({ role: 'gestionnaire_stock_songon', username: 'autre' }, { nomProjet: 'Songon autre' }), true);
  const chief = { role: 'chef_chantier_site', username: 'site_02' };
  assert.equal(context.isInUserProjectScope(chief, { numeroMaison: '2' }), true);
  assert.equal(context.isInUserProjectScope(chief, { numeroMaison: '3' }), false);
});

test('material requests API returns real project metadata and scoped inventory from SQLite', async () => {
  const db = new sqlite3.Database(':memory:');
  const exec = sql => new Promise((resolve, reject) => db.exec(sql, error => error ? reject(error) : resolve()));
  const all = sql => new Promise((resolve, reject) => db.all(sql, (error, rows) => error ? reject(error) : resolve(rows)));
  try {
    await exec(`
      CREATE TABLE projects (id INTEGER, nomProjet TEXT, nomSite TEXT, prefecture TEXT, numeroMaison TEXT, typeMaison TEXT);
      CREATE TABLE material_requests (id INTEGER, projetId INTEGER, warehouseId TEXT, dateDemande TEXT, itemName TEXT);
      CREATE TABLE purchase_orders (id INTEGER, materialRequestId INTEGER, statut TEXT, statutValidation TEXT, dateReception TEXT, dateCommande TEXT, warehouseId TEXT);
      CREATE TABLE purchase_order_items (purchaseOrderId INTEGER, materialRequestId INTEGER);
      INSERT INTO projects VALUES (1, 'Songon Kassemble', 'Lot 1', 'Songon', '1', 'T1');
      INSERT INTO projects VALUES (2, 'Adzope', 'Lot 2', 'Adzope', '2', 'T1');
      INSERT INTO projects VALUES (3, 'Songon autre', 'Lot 3', 'Songon', '3', 'T2');
      INSERT INTO material_requests VALUES (1, 1, 'entrepot-songon-2', '2026-10-29', 'Ciment');
      INSERT INTO material_requests VALUES (2, 2, 'entrepot-plateau', '2026-10-29', 'Sable');
      INSERT INTO material_requests VALUES (3, 3, 'entrepot-songon-2', '2026-10-29', 'Gravier');
    `);
    let handler;
    const context = scopeContext({ all, app: { get: (_url, callback) => { handler = callback; } } });
    vm.runInContext(extract(server, "app.get('/api/material-requests',", "app.patch('/api/material-requests/:id/remaining'"), context);
    let rows;
    const res = { json: result => { rows = result; } };
    await handler({ user: kokan }, res);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].itemName, 'Ciment');
    assert.equal(rows[0].nomProjet, 'Songon Kassemble');
    assert.equal(rows[0].projetNom, 'Songon Kassemble');
    assert.equal(rows[0].prefecture, 'Songon');
    assert.equal(rows[0].nomSite, 'Lot 1');
    await handler({ user: { role: 'admin' } }, res);
    assert.equal(rows.length, 3);
  } finally {
    await new Promise((resolve, reject) => db.close(error => error ? reject(error) : resolve()));
  }
});

test('project catalog and folders use the same project access scope', async () => {
  for (const [start, end] of [
    ["app.get('/api/project-catalog',", "app.delete('/api/project-catalog/:id'"],
    ["app.get('/api/project-folders',", 'async function handleDeleteProjectFolder('],
  ]) {
    let handler;
    const context = scopeContext({
      all: async () => [{ nomProjet: 'Songon Kassemble' }, { nomProjet: 'Songon autre' }, { nomProjet: 'Adzope' }],
      app: { get: (_url, callback) => { handler = callback; } },
    });
    vm.runInContext(extract(server, start, end), context);
    let rows;
    await handler({ user: kokan }, { json: result => { rows = result; } });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].nomProjet, 'Songon Kassemble');
  }
});

test('frontend project filters retain all Songon Kassemble lots only', () => {
  const context = vm.createContext({
    isZoneStockManager: () => true,
    getEffectiveZoneManagerScope: () => ({ projectName: 'Songon Kassemble', zoneName: 'Songon', projectIds: [] }),
    normalizeText: value => String(value || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, ''),
    projectsCache: [
      { id: 1, nomProjet: 'Songon Kassemble', prefecture: 'Songon' },
      { id: 2, nomProjet: 'Songon Kassemble - T2', prefecture: 'Songon' },
      { id: 3, nomProjet: 'Songon autre', prefecture: 'Songon' },
      { id: 4, nomProjet: 'Adzope', prefecture: 'Adzope' },
    ],
  });
  vm.runInContext(extract(html, '    function getScopedProjects(', '    function getScopedProjectFolders('), context);
  assert.deepEqual(Array.from(context.getScopedProjects(), project => project.id), [1, 2]);
});

test('Kokan inventory API reads are allowed by role authorization', () => {
  const context = vm.createContext();
  vm.runInContext(extract(server, 'function authorizeRoleAccess(', 'function hashTrackingToken('), context);
  for (const apiPath of ['/projects', '/material-requests', '/stock-management/orders', '/stock-management/issues', '/stock-management/available', '/stock-issue-authorizations']) {
    let allowed = false;
    context.authorizeRoleAccess({ user: kokan, method: 'GET', path: apiPath }, {
      status: () => { throw new Error(`Denied inventory API: ${apiPath}`); },
    }, () => { allowed = true; });
    assert.equal(allowed, true);
  }
});
