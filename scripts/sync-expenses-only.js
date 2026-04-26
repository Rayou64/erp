const https = require('https');
const http = require('http');

const LOCAL = 'http://localhost:4000';
const RAIL = 'https://terrific-love-production-10ec.up.railway.app';
const ADMIN = { username: 'admin', password: 'admin123' };

function req(method, url, body, token) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === 'https:' ? https : http;
    const data = body ? JSON.stringify(body) : null;
    const r = lib.request({
      hostname: u.hostname,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: u.pathname + (u.search || ''),
      method,
      headers: {
        ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}),
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
    }, (res) => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => {
        let bodyOut = raw;
        try { bodyOut = JSON.parse(raw); } catch {}
        resolve({ status: res.statusCode, body: bodyOut, raw });
      });
    });
    r.on('error', reject);
    if (data) r.write(data);
    r.end();
  });
}

async function login(base) {
  const out = await req('POST', `${base}/api/auth/login`, ADMIN);
  return out.body.token;
}

async function get(base, path, token) {
  const out = await req('GET', `${base}${path}`, null, token);
  if (typeof out.body === 'string') {
    try { return JSON.parse(out.body); } catch { return []; }
  }
  return out.body;
}

function norm(v) { return String(v || '').trim().toLowerCase(); }

(async () => {
  const lt = await login(LOCAL);
  const rt = await login(RAIL);

  const localProjects = await get(LOCAL, '/api/projects', lt);
  const railProjects = await get(RAIL, '/api/projects', rt);
  const localExpenses = await get(LOCAL, '/api/expenses', lt);

  await req('DELETE', `${RAIL}/api/expenses`, null, rt);

  function mapProjectId(localId) {
    const lp = localProjects.find(p => Number(p.id) === Number(localId));
    if (!lp) return null;
    const rp = railProjects.find(p =>
      norm(p.nomProjet) === norm(lp.nomProjet) &&
      norm(p.prefecture) === norm(lp.prefecture) &&
      norm(p.nomSite) === norm(lp.nomSite)
    );
    return rp ? Number(rp.id) : null;
  }

  let ok = 0;
  for (const e of localExpenses) {
    const out = await req('POST', `${RAIL}/api/expenses`, {
      projectId: mapProjectId(e.projetId),
      description: e.description,
      quantity: Number(e.quantite || 0),
      unitPrice: Number(e.prixUnitaire || 0),
      supplier: e.fournisseur || '',
      category: e.categorie || 'autres',
    }, rt);
    if (out.status === 201) ok += 1;
    else console.log('insert failed', out.status, out.raw);
  }

  const l = await get(LOCAL, '/api/expenses', lt);
  const r = await get(RAIL, '/api/expenses', rt);
  console.log(`expenses synced => inserted:${ok} local:${l.length} railway:${r.length}`);
})();
