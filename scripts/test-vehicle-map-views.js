const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const sqlite3 = require('sqlite3');
const { test } = require('node:test');

const root = path.join(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public', 'erp.html'), 'utf8');
const backend = fs.readFileSync(path.join(root, 'app.js'), 'utf8');

function extract(source, start, end) {
  const first = source.indexOf(start);
  const last = source.indexOf(end, first);
  assert.ok(first >= 0 && last > first, start);
  return source.slice(first, last);
}

function selectionContext(fetchJson) {
  const context = vm.createContext({
    selectedAutoLocationVehicleId: null,
    autoLocationSelectionRequest: 0,
    autoLocationHistoryCache: [],
    autoLocationRouteCache: [],
    autoMapLiveRefreshInFlight: false,
    document: { getElementById: () => null },
    URLSearchParams, fetchJson,
    renderAutoVehicleLocationHistory() {},
    renderAutoVehicleMap() {},
    renderMapsModule() {},
    errors: [],
    showMessage(message) { this.errors.push(message); },
  });
  vm.runInContext(extract(html, '    async function setSelectedAutoLocationVehicle(', '    async function captureCurrentBrowserLocation('), context);
  return context;
}

function positions(first, last, session = 0) {
  return Array.from({ length: first - last + 1 }, (_, index) => ({
    id: first - index, trackingSessionId: session,
    latitude: 5.3 + (first - index) / 10000, longitude: -4,
    recordedAt: new Date(Date.UTC(2026, 10, 13, 10, 0, first - index)).toISOString(),
  }));
}

test('all pages contribute to the chronological blue route, including unassigned GPS sessions', async () => {
  const calls = [];
  const context = selectionContext(async url => {
    calls.push(url);
    return calls.length === 1
      ? { locations: positions(40, 11), nextCursor: { id: 11, recordedAt: '2026-11-13T10:00:11Z' } }
      : { locations: positions(10, 1), nextCursor: null };
  });
  await context.setSelectedAutoLocationVehicle(7);
  assert.equal(context.autoLocationHistoryCache.length, 40);
  assert.deepEqual(Array.from(context.autoLocationRouteCache, row => row.id), Array.from({ length: 40 }, (_, i) => i + 1));
  assert.match(calls[1], /beforeRecordedAt=2026-11-13T10%3A00%3A11Z&beforeId=11/);
});

test('the route uses the entire latest tracking session rather than previous sessions', async () => {
  const context = selectionContext(async () => ({ locations: [...positions(60, 21, 9), ...positions(20, 1, 8)] }));
  await context.setSelectedAutoLocationVehicle(7);
  assert.equal(context.autoLocationRouteCache.length, 40);
  assert.ok(context.autoLocationRouteCache.every(row => row.trackingSessionId === 9));
  assert.equal(context.autoLocationHistoryCache.length, 60);
});

test('a session longer than the 20000-point API page is not truncated', async () => {
  let call = 0;
  const context = selectionContext(async () => ++call === 1
    ? { locations: positions(20001, 2, 9), nextCursor: { id: 2, recordedAt: '2026-11-13T10:00:02Z' } }
    : { locations: positions(1, 1, 9), nextCursor: null });
  await context.setSelectedAutoLocationVehicle(7);
  assert.equal(context.autoLocationRouteCache.length, 20001);
  assert.equal(context.autoLocationRouteCache[0].id, 1);
  assert.equal(context.autoLocationRouteCache[20000].id, 20001);
});

test('a late response cannot overwrite a newer A-B-A vehicle selection', async () => {
  let resolveFirst;
  let call = 0;
  const context = selectionContext(async () => {
    call += 1;
    if (call === 1) return new Promise(resolve => { resolveFirst = resolve; });
    return { locations: positions(call, call) };
  });
  const first = context.setSelectedAutoLocationVehicle(1);
  await context.setSelectedAutoLocationVehicle(2);
  await context.setSelectedAutoLocationVehicle(1);
  resolveFirst({ locations: positions(99, 99) });
  await first;
  assert.equal(context.autoLocationRouteCache[0].id, 3);
});

test('selected vehicle detail draws every route point and fits the entire blue line', () => {
  let polyline, fitBounds;
  const marker = coords => ({
    addTo() { return this; }, bindPopup() {}, on() {},
    getLatLng: () => coords, openPopup() {},
  });
  const context = vm.createContext({
    mapsModuleLeafletMap: {
      removeLayer() {}, fitBounds(bounds) { fitBounds = bounds; },
      getBounds: () => ({ contains: () => true }),
    },
    mapsModuleLeafletLayers: [], mapsModuleLeafletMarkersByVehicleId: new Map(),
    mapsModuleLeafletHistoryLayer: null, managerMapRebuilding: false,
    managerMapFitKeys: { maps: '' }, mapsDetailOpen: true,
    selectedAutoLocationVehicleId: 7,
    autoVehicleLocationsCache: [
      { id: 7, lastLocation: { latitude: 5.3, longitude: -4 } },
      { id: 8, lastLocation: { latitude: 10, longitude: 10 } },
    ],
    autoLocationRouteCache: positions(40, 1).reverse(),
    initializeMapsModuleLeafletMap: () => true,
    getMapsModuleFilteredVehicles: () => [],
    getAutoLocationStatusColor: () => '#00f', getAutoLocationStatusLabel: () => 'Actif',
    buildAutoVehicleLabel: () => 'Vehicule de test', escapeHtml: String,
    keepManagerPopup() {}, restoreManagerPopup() {}, applyLiveRouteLine() {},
    managerMapRouteKey: () => '7',
    L: {
      circleMarker: marker, marker, divIcon: () => ({}),
      polyline(coords, options) {
        polyline = { coords, options, addTo() { return this; } };
        return polyline;
      },
    },
  });
  vm.runInContext(extract(html, '    function renderMapsModuleLeafletMap()', '    function setMapsModuleMapMode('), context);
  context.renderMapsModuleLeafletMap();
  assert.equal(polyline.coords.length, 40);
  assert.equal(polyline.options.color, '#2878f5');
  assert.equal(fitBounds.length, 41);
  assert.deepEqual(Array.from(context.mapsModuleLeafletMarkersByVehicleId.keys()), [7]);
});

test('vehicle cards expose both map modes and mobile mail stays in its WebView', () => {
  const detail = extract(html, '    window.vehiclesOpenDetail =', '    window.vehiclesCloseDetail =');
  assert.match(detail, /vehiclesOpenOnMaps\(\$\{Number\(vehicle.id\)\}, 'google'\)/);
  assert.match(detail, /vehiclesOpenOnMaps\(\$\{Number\(vehicle.id\)\}, 'leaflet'\)/);
  for (const id of ['maps-detail-mode-google', 'maps-detail-mode-leaflet', 'maps-detail-leaflet-wrapper']) {
    assert.ok(html.includes(`id="${id}"`), id);
  }
  const calls = [];
  const context = vm.createContext({
    window: {
      matchMedia: () => ({ matches: true }),
      location: { assign: url => calls.push(url) },
      open() { throw new Error('Must not open a new WebView'); },
    },
  });
  vm.runInContext(extract(html, '    function openInternalMailApp()', '    function setupInternalMailModule('), context);
  context.openInternalMailApp();
  assert.deepEqual(calls, ['/messaging.html']);
});

test('GPS cursor pagination uses timestamp and id without gaps for matching timestamps', async () => {
  const db = new sqlite3.Database(':memory:');
  const run = (sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, error => error ? reject(error) : resolve()));
  const all = (sql, params) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
  try {
    await run(`CREATE TABLE auto_vehicle_locations (id INTEGER, vehicle_id INTEGER, latitude REAL, longitude REAL,
      speed_kph REAL, heading REAL, accuracy_meters REAL, source TEXT, status TEXT, note TEXT,
      recorded_at TEXT, trackingSessionId INTEGER, created_by INTEGER)`);
    for (const [id, timestamp] of [[4, '2026-11-13T10:00:00Z'], [2, '2026-11-13T10:00:00Z'], [9, '2026-11-12T10:00:00Z'], [1, '2026-11-11T10:00:00Z']]) {
      await run('INSERT INTO auto_vehicle_locations (id,vehicle_id,recorded_at) VALUES (?,7,?)', [id, timestamp]);
    }
    let handler;
    const context = vm.createContext({
      app: { get(route, callback) { handler = callback; } },
      get: async sql => sql.includes('auto_vehicles') ? { id: 7 } : null,
      all,
    });
    vm.runInContext(extract(backend, "app.get('/api/auto-vehicles/:id/locations',", "app.post('/api/auto-vehicles/:id/locations',"), context);
    const request = async query => {
      let payload, status = 200;
      await handler({ params: { id: '7' }, query }, {
        status(code) { status = code; return this; },
        json(data) { payload = data; },
      });
      return { payload, status };
    };
    const first = await request({ limit: '1' });
    const second = await request({ limit: '2', beforeRecordedAt: first.payload.nextCursor.recordedAt, beforeId: String(first.payload.nextCursor.id) });
    const third = await request({ limit: '2', beforeRecordedAt: second.payload.nextCursor.recordedAt, beforeId: String(second.payload.nextCursor.id) });
    assert.deepEqual([...first.payload.locations, ...second.payload.locations, ...third.payload.locations].map(row => row.id), [4, 2, 9, 1]);
    assert.equal(third.payload.nextCursor, null);
    assert.equal((await request({ limit: 'invalid' })).status, 400);
    assert.equal((await request({ beforeId: '4' })).status, 400);
    const incremental = await request({ afterId: '4' });
    assert.deepEqual(incremental.payload.locations.map(row => row.id), [9]);
  } finally {
    await new Promise((resolve, reject) => db.close(error => error ? reject(error) : resolve()));
  }
});
