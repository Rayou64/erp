const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'erp.html'), 'utf8');
const start = html.indexOf('    async function loadInventory() {');
const end = html.indexOf('    window.selectInventoryHouse', start);
assert.ok(start >= 0 && end > start, 'Inventory loader must exist');

function createInventory() {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, { value: '', innerHTML: '', textContent: '', style: {} });
    return elements.get(id);
  };
  const projects = [
    { id: 101, nomProjet: 'Projet A', numeroMaison: '1' },
    { id: 202, nomProjet: 'Projet A', numeroMaison: '2' },
  ];
  const material = (id, projetId, itemName, quantiteRestante, stage = 'Fondation') => ({
    id, projetId, itemName, quantiteRestante, quantiteDemandee: 30,
    statut: quantiteRestante > 0 ? 'EN_STOCK' : 'EPUISE',
    etapeApprovisionnement: stage, unite: 'sac', prixUnitaire: 10,
  });
  const materials = [
    material(1, 101, 'Ciment', 0),
    material(2, 101, 'Ciment', 2),
    material(3, 101, 'Sable', 0),
    material(4, 101, 'Non transfere', 50),
    material(5, 202, 'Ciment', 0),
    material(6, 101, 'Ciment', 0, 'Toiture'),
    material(7, 101, 'Gravier', 0),
  ];
  const issue = (materialRequestId, quantiteSortie, issueType, note = '') => ({
    materialRequestId, quantiteSortie, issueType, note,
  });
  const issues = [
    issue(1, 6, 'SITE_TRANSFER'), issue(1, 4, 'SITE_TRANSFER'),
    issue(1, 4, 'CONSUMPTION'), issue(2, 3, 'SITE_TRANSFER'),
    issue(2, 1, 'CONSUMPTION'), issue(3, 20, 'SITE_TRANSFER'),
    issue(3, 20, '', 'Consommation chantier - sable'),
    issue(5, 7, 'SITE_TRANSFER'), issue(5, 2, 'CONSUMPTION'),
    issue(6, 2, 'SITE_TRANSFER'), issue(6, 1, 'CONSUMPTION'),
    issue(7, 0.3, 'SITE_TRANSFER'), issue(7, 0.1, 'CONSUMPTION'),
  ];
  const orders = [{
    id: 1,
    items: materials.map(row => ({ materialRequestId: row.id, quantite: row.id === 1 ? 10 : 5 })),
  }];
  const errors = [];
  const context = vm.createContext({
    projectsCache: projects,
    inventoryActiveTab: 'site',
    inventoryViewMode: 'building',
    inventoryViewSearchTerm: '',
    selectedInventoryHouseId: '',
    document: {
      getElementById: element,
      querySelectorAll: () => [],
    },
    fetchJson: async url => {
      if (url === context.failedUrl) throw new Error('API indisponible');
      const responses = {
        '/api/material-requests': materials,
        '/api/projects': projects,
        '/api/stock-management/issues': issues,
        '/api/stock-management/orders': orders,
        '/api/stock-issue-authorizations': [],
        '/api/materials': [],
      };
      assert.ok(url in responses, `Unexpected API: ${url}`);
      return responses[url];
    },
    normalizeText: value => String(value || '').toLowerCase().trim(),
    escapeHtml: value => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    ensureWarehouseAssignmentsForRows: () => {},
    populateInventoryLocationFilters: (_projects, rows) => ({
      scopedMaterials: rows.filter(row => !context.selectedInventoryHouseId || String(row.projetId) === context.selectedInventoryHouseId),
      warehouseLabel: 'Entrepot',
    }),
    applyInventoryViewMode: mode => { context.inventoryViewMode = mode; },
    isSiteChief: () => false,
    getProjectById: id => projects.find(project => project.id === Number(id)),
    getWarehouseIdForRequest: () => 'warehouse',
    getWarehouseNameById: () => 'Entrepot',
    formatInventoryStatusLabel: status => status,
    getInventoryBadgeClass: () => '',
    renderInventoryFocusSummary: () => {},
    renderInventoryMovements: () => {},
    showMessage: (message, type) => errors.push({ message, type }),
  });
  vm.runInContext(html.slice(start, end), context);
  return {
    context, element, issues, errors,
    load: () => context.loadInventory(),
    rows: () => [...element('inventory-site-list').innerHTML.matchAll(/<tr>([\s\S]*?)<\/tr>/g)]
      .map(row => [...row[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/g)]
        .map(cell => cell[1].replace(/<[^>]*>/g, '').trim())),
  };
}

test('site inventory retains consumed products and separates lots and stages', async () => {
  const inventory = createInventory();
  await inventory.load();
  assert.deepEqual(inventory.errors, []);
  const rows = inventory.rows();
  assert.equal(rows.length, 5);
  assert.ok(rows.every(row => row.length === 12));
  const cement = rows.find(row => row[0] === 'Ciment' && row[1].includes('Site N°1') && row[2] === 'Fondation');
  assert.deepEqual(cement.slice(4, 9), ['15.00', '13.00', '5.00', '2.00', '8.00']);
  const sand = rows.find(row => row[0] === 'Sable');
  assert.deepEqual(sand.slice(5, 9), ['20.00', '20.00', '0.00', '0.00']);
  assert.equal(sand[10], 'EPUISE');
  const gravel = rows.find(row => row[0] === 'Gravier');
  assert.equal(gravel[8], '0.20');
  assert.equal(inventory.element('inv-kpi-products').textContent, '3');
  assert.equal(inventory.element('inv-kpi-stock').textContent, (14.2).toLocaleString('fr-FR', { maximumFractionDigits: 2 }));
  assert.equal(inventory.element('inventory-site-view').style.display, '');
  assert.equal(inventory.element('inventory-project-view-card').style.display, 'none');
});

test('reloading after journal consumption updates quantities without removing the product', async () => {
  const inventory = createInventory();
  inventory.context.selectedInventoryHouseId = '101';
  await inventory.load();
  inventory.issues.push({ materialRequestId: 1, quantiteSortie: 6, issueType: 'CONSUMPTION' });
  inventory.issues.push({ materialRequestId: 2, quantiteSortie: 2, issueType: 'CONSUMPTION' });
  await inventory.load();
  const cement = inventory.rows().find(row => row[0] === 'Ciment' && row[2] === 'Fondation');
  assert.deepEqual(cement.slice(5, 9), ['13.00', '13.00', '2.00', '0.00']);
  assert.equal(cement[10], 'EPUISE');
  assert.ok(inventory.rows().every(row => row[1].includes('Site N°1')));
  const submitStart = html.indexOf("document.getElementById('project-progress-form').addEventListener('submit'");
  const submitEnd = html.indexOf("document.getElementById('clear-progress-history-btn')", submitStart);
  assert.match(html.slice(submitStart, submitEnd), /await fetchJson\('\/api\/project-progress'[\s\S]*loadInventory\(\)/);
});

test('site stock filters use aggregated site balances, not warehouse balances', async () => {
  const inventory = createInventory();
  inventory.element('inventory-filter-stock').value = 'available';
  await inventory.load();
  assert.equal(inventory.rows().length, 4);
  assert.ok(inventory.rows().some(row => row[0] === 'Ciment' && row[1].includes('Site N°2')));
  inventory.element('inventory-filter-stock').value = 'empty';
  await inventory.load();
  assert.equal(inventory.rows().length, 1);
  assert.equal(inventory.rows()[0][0], 'Sable');
  inventory.element('inventory-filter-stock').value = '';
  inventory.element('inventory-filter-status').value = 'EPUISE';
  await inventory.load();
  assert.equal(inventory.rows().length, 1);
  assert.equal(inventory.rows()[0][0], 'Sable');
});

test('low stock, search and empty results render in the visible site table', async () => {
  const inventory = createInventory();
  inventory.context.inventoryActiveTab = 'low';
  await inventory.load();
  assert.equal(inventory.rows().length, 5);
  inventory.element('inventory-filter-search').value = 'sable';
  await inventory.load();
  assert.equal(inventory.rows().length, 1);
  inventory.element('inventory-filter-search').value = 'absent';
  await inventory.load();
  assert.match(inventory.element('inventory-site-list').innerHTML, /colspan="12".*Aucun produit/);
});

test('warehouse view preserves request quantities and warehouse filtering', async () => {
  const inventory = createInventory();
  inventory.context.inventoryActiveTab = 'warehouse';
  inventory.element('inventory-filter-stock').value = 'available';
  await inventory.load();
  assert.deepEqual(inventory.errors, []);
  assert.equal(inventory.element('inventory-site-view').style.display, 'none');
  assert.equal(inventory.element('inventory-project-view-card').style.display, '');
  assert.ok(!inventory.element('inventory-list').innerHTML.includes('Sable'));
  assert.equal(inventory.element('inv-kpi-stock').textContent, '52');
});

test('stock API failures are reported rather than silently treated as empty stock', async () => {
  for (const failedUrl of ['/api/stock-management/issues', '/api/stock-management/orders']) {
    const inventory = createInventory();
    inventory.context.failedUrl = failedUrl;
    await inventory.load();
    assert.equal(inventory.errors.length, 1);
    assert.equal(inventory.errors[0].type, 'error');
    assert.match(inventory.errors[0].message, /API indisponible/);
  }
});

test('all inline application scripts have valid syntax', () => {
  const scripts = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0);
  scripts.forEach((script, index) => new vm.Script(script[1], { filename: `erp-inline-${index}.js` }));
});
