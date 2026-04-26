const https = require('https');
const http = require('http');

const LOCAL = 'http://localhost:4000';
const RAIL = process.env.RAIL_URL || 'https://terrific-love-production-10ec.up.railway.app';
const ADMIN = { username: 'admin', password: 'admin123' };

function req(method, url, body, token) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === 'https:' ? https : http;
    const data = body ? JSON.stringify(body) : null;
    const options = {
      hostname: u.hostname,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: u.pathname + (u.search || ''),
      method,
      headers: {
        ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}),
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
    };

    const r = lib.request(options, (res) => {
      let raw = '';
      res.on('data', (c) => { raw += c; });
      res.on('end', () => {
        let parsed = raw;
        try { parsed = JSON.parse(raw); } catch {}
        resolve({ status: res.statusCode, body: parsed, raw });
      });
    });
    r.on('error', reject);
    if (data) r.write(data);
    r.end();
  });
}

async function login(base) {
  const out = await req('POST', `${base}/api/auth/login`, ADMIN);
  if (!out.body || !out.body.token) throw new Error(`Login failed on ${base}`);
  return out.body.token;
}

async function get(base, path, token) {
  const out = await req('GET', `${base}${path}`, null, token);
  if (out.status !== 200) throw new Error(`GET ${path} failed: ${out.status}`);
  let data = out.body;
  // Some endpoints can return JSON arrays encoded as strings (sometimes twice).
  for (let i = 0; i < 3 && typeof data === 'string'; i += 1) {
    try {
      data = JSON.parse(data);
    } catch {
      break;
    }
  }
  if (Array.isArray(data)) return data;
  if (data && typeof data === 'object') return data;
  return [];
}

async function post(base, path, token, body) {
  return req('POST', `${base}${path}`, body, token);
}

async function del(base, path, token) {
  return req('DELETE', `${base}${path}`, null, token);
}

function norm(v) { return String(v || '').trim().toLowerCase(); }

function findProjectIdByKey(projects, p) {
  const m = projects.find(r =>
    norm(r.nomProjet) === norm(p.nomProjet) &&
    norm(r.prefecture) === norm(p.prefecture) &&
    norm(r.nomSite) === norm(p.nomSite)
  );
  return m ? Number(m.id) : null;
}

(async () => {
  const lt = await login(LOCAL);
  const rt = await login(RAIL);

  // Cleanup accidental test catalog project if exists.
  const railCatalog = await get(RAIL, '/api/project-catalog', rt);
  const tmp = railCatalog.find(x => norm(x.nomProjet) === 'test-catalog-tmp');
  if (tmp) {
    const d = await del(RAIL, `/api/project-catalog/${tmp.id}`, rt);
    console.log(`cleanup catalog tmp: status ${d.status}`);
  }

  const localProjects = await get(LOCAL, '/api/projects', lt);
  const railProjects = await get(RAIL, '/api/projects', rt);

  // Reset-and-sync material catalog exactly.
  const railMaterials = await get(RAIL, '/api/material-catalog', rt);
  for (const row of railMaterials) {
    await del(RAIL, `/api/material-catalog/${row.id}`, rt);
  }
  const localMaterials = await get(LOCAL, '/api/material-catalog', lt);
  let matOk = 0;
  for (const m of localMaterials) {
    const out = await post(RAIL, '/api/material-catalog', rt, {
      projectFolder: m.projectFolder,
      materialName: m.materialName,
      unite: m.unite,
      quantiteParBatiment: Number(m.quantiteParBatiment || 0),
      prixUnitaire: Number(m.prixUnitaire || 0),
      notes: m.notes || '',
    });
    if (out.status === 201) matOk += 1;
    else console.log('material insert failed:', out.status, out.raw);
  }

  // Reset-and-sync project progress exactly.
  await del(RAIL, '/api/project-progress', rt);
  const localProgress = await get(LOCAL, '/api/project-progress', lt);
  let progOk = 0;
  for (const p of localProgress) {
    const lp = localProjects.find(x => Number(x.id) === Number(p.projectId));
    if (!lp) continue;
    const rid = findProjectIdByKey(railProjects, lp);
    if (!rid) continue;

    const out = await post(RAIL, '/api/project-progress', rt, {
      projectId: rid,
      stage: p.stage,
      title: p.title,
      note: p.note || '',
      percentage: Number(p.progressPercent || 0),
      materialsUsed: '',
      laborCount: 0,
      dateEtape: p.createdAt,
    });
    if (out.status === 201) progOk += 1;
    else console.log('progress insert failed:', out.status, out.raw);
  }

  // Reset-and-sync expenses exactly.
  await del(RAIL, '/api/expenses', rt);
  const localExpenses = await get(LOCAL, '/api/expenses', lt);
  let expOk = 0;
  for (const e of localExpenses) {
    const lp = localProjects.find(x => Number(x.id) === Number(e.projetId));
    const rid = lp ? findProjectIdByKey(railProjects, lp) : null;
    const out = await post(RAIL, '/api/expenses', rt, {
      projectId: rid,
      description: e.description,
      quantity: Number(e.quantite || 0),
      unitPrice: Number(e.prixUnitaire || 0),
      supplier: e.fournisseur || '',
      category: e.categorie || 'autres',
    });
    if (out.status === 201) expOk += 1;
    else console.log('expense insert failed:', out.status, out.raw);
  }

  // Reset-and-sync purchase orders exactly.
  const railOrders = await get(RAIL, '/api/purchase-orders', rt);
  for (const po of railOrders) {
    const id = Number(po && po.id);
    if (Number.isInteger(id) && id > 0) {
      await del(RAIL, `/api/purchase-orders/${id}`, rt);
    }
  }

  const localOrders = await get(LOCAL, '/api/purchase-orders', lt);
  let poOk = 0;
  for (const po of localOrders) {
    const localPoProject = localProjects.find(p => Number(p.id) === Number(po.projetId || po.siteId || 0));
    const mappedRailId = localPoProject ? findProjectIdByKey(railProjects, localPoProject) : null;

    const items = Array.isArray(po.items)
      ? po.items.map(i => ({
          materialRequestId: null,
          article: i.article,
          details: i.details || '',
          quantite: Number(i.quantite || 0),
          prixUnitaire: Number(i.prixUnitaire || 0),
        }))
      : [];

    const out = await post(RAIL, '/api/purchase-orders', rt, {
      materialRequestId: null,
      creePar: po.creePar || 'admin',
      fournisseur: po.fournisseur,
      quantiteCommandee: Number(po.quantiteCommandee || 0),
      prixUnitaire: Number(po.prixUnitaire || 0),
      montantTotal: Number(po.montantTotal || 0),
      dateLivraisonPrevue: po.dateLivraisonPrevue || null,
      dateCommande: po.dateCommande,
      dateReception: po.dateReception || null,
      projetId: mappedRailId,
      nomProjetManuel: po.nomProjetManuel || null,
      siteId: mappedRailId,
      nomSiteManuel: po.nomSiteManuel || null,
      zoneName: po.zoneName || null,
      warehouseId: po.warehouseId || null,
      etapeApprovisionnement: po.etapeApprovisionnement || 'Approvisionnement',
      signatureName: po.signatureName || null,
      signatureRole: po.signatureRole || null,
      items,
    });
    if (out.status === 201) poOk += 1;
    else console.log('purchase-order insert failed:', out.status, out.raw);
  }

  // Purchase-order creation can generate side-effect expenses.
  // Re-sync expenses at the end to guarantee strict parity.
  await del(RAIL, '/api/expenses', rt);
  expOk = 0;
  for (const e of localExpenses) {
    const lp = localProjects.find(x => Number(x.id) === Number(e.projetId));
    const rid = lp ? findProjectIdByKey(railProjects, lp) : null;
    const out = await post(RAIL, '/api/expenses', rt, {
      projectId: rid,
      description: e.description,
      quantity: Number(e.quantite || 0),
      unitPrice: Number(e.prixUnitaire || 0),
      supplier: e.fournisseur || '',
      category: e.categorie || 'autres',
    });
    if (out.status === 201) expOk += 1;
    else console.log('expense final insert failed:', out.status, out.raw);
  }

  const endpoints = [
    '/api/project-catalog',
    '/api/project-folders',
    '/api/projects',
    '/api/material-catalog',
    '/api/project-progress',
    '/api/purchase-orders',
    '/api/expenses',
  ];

  console.log(`sync summary => material:${matOk}, progress:${progOk}, expenses:${expOk}, purchaseOrders:${poOk}`);
  for (const ep of endpoints) {
    const l = await get(LOCAL, ep, lt);
    const r = await get(RAIL, ep, rt);
    console.log(`${ep} => local:${Array.isArray(l) ? l.length : 0} railway:${Array.isArray(r) ? r.length : 0}`);
  }
})();
