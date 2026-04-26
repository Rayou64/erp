/**
 * migrate-local-to-railway.js
 * Exporte toutes les données du localhost et les importe sur Railway.
 * Usage: node scripts/migrate-local-to-railway.js
 */

const https = require('https');
const http = require('http');

const LOCAL = 'http://localhost:4000';
const RAILWAY = process.env.RAIL_URL || 'https://terrific-love-production-10ec.up.railway.app';
const ADMIN_USER = 'admin';
const ADMIN_PASS = 'admin123';

// ─── HTTP helpers ────────────────────────────────────────────────────────────

function request(method, url, body, token) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const isHttps = parsed.protocol === 'https:';
    const lib = isHttps ? https : http;
    const data = body ? JSON.stringify(body) : null;
    const options = {
      hostname: parsed.hostname,
      port: parsed.port || (isHttps ? 443 : 80),
      path: parsed.pathname + (parsed.search || ''),
      method,
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {}),
      },
    };
    const req = lib.request(options, (res) => {
      let raw = '';
      res.on('data', (chunk) => (raw += chunk));
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(raw); } catch { parsed = raw; }
        resolve({ status: res.statusCode, body: parsed });
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

async function login(base, username, password) {
  const res = await request('POST', `${base}/api/auth/login`, { username, password });
  if (!res.body?.token) throw new Error(`Login failed on ${base}: ${JSON.stringify(res.body)}`);
  return res.body.token;
}

async function get(base, path, token) {
  const res = await request('GET', `${base}${path}`, null, token);
  if (res.status !== 200) throw new Error(`GET ${path} failed: ${res.status} ${JSON.stringify(res.body)}`);
  let data = res.body;
  for (let i = 0; i < 3 && typeof data === 'string'; i += 1) {
    try {
      data = JSON.parse(data);
    } catch {
      break;
    }
  }
  return data;
}

async function post(base, path, body, token) {
  const res = await request('POST', `${base}${path}`, body, token);
  return res;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Main migration ──────────────────────────────────────────────────────────

async function main() {
  console.log('🔐 Authentification...');
  const localToken = await login(LOCAL, ADMIN_USER, ADMIN_PASS);
  const railToken = await login(RAILWAY, ADMIN_USER, ADMIN_PASS);
  console.log('✅ Tokens obtenus\n');

  // ── 1. Project catalog ──────────────────────────────────────────────────
  console.log('📚 Migration du catalogue projets...');
  const catalog = await get(LOCAL, '/api/project-catalog', localToken);
  console.log(`   ${catalog.length} entrée(s) catalogue trouvée(s)`);
  let catalogIdMap = {}; // localId -> railwayId
  for (const item of [...catalog].reverse()) {
    const res = await post(RAILWAY, '/api/project-catalog', {
      nomProjet: item.nomProjet,
      typeProjet: item.typeProjet || '',
      description: item.description || '',
    }, railToken);
    if (res.status === 201) {
      catalogIdMap[item.id] = res.body.id;
      console.log(`   ✅ Catalogue: ${item.nomProjet} (local id=${item.id} → railway id=${res.body.id})`);
    } else if (res.status === 409) {
      // Already exists — fetch to get railway id
      const existing = await get(RAILWAY, '/api/project-catalog', railToken);
      const match = existing.find(e => e.nomProjet?.toLowerCase() === item.nomProjet?.toLowerCase());
      if (match) {
        catalogIdMap[item.id] = match.id;
        console.log(`   ⚠️  Catalogue déjà présent: ${item.nomProjet} (railway id=${match.id})`);
      }
    } else {
      console.log(`   ❌ Erreur catalogue ${item.nomProjet}: ${res.status} ${JSON.stringify(res.body)}`);
    }
  }

  // ── 2. Project folders (zones) ──────────────────────────────────────────
  console.log('\n📂 Migration des zones (project_folders)...');
  const folders = await get(LOCAL, '/api/project-folders', localToken);
  console.log(`   ${folders.length} zone(s) trouvée(s)`);
  for (const folder of [...folders].reverse()) {
    const railCatalogId = catalogIdMap[folder.projectId];
    const res = await post(RAILWAY, '/api/project-folders', {
      nomProjet: folder.nomProjet,
      prefecture: folder.prefecture,
      description: folder.description || '',
      projectId: railCatalogId || null,
    }, railToken);
    if (res.status === 201) {
      console.log(`   ✅ Zone: ${folder.nomProjet} - ${folder.prefecture}`);
    } else if (res.status === 409) {
      console.log(`   ⚠️  Zone déjà présente: ${folder.nomProjet} - ${folder.prefecture}`);
    } else {
      console.log(`   ❌ Erreur zone ${folder.prefecture}: ${res.status} ${JSON.stringify(res.body)}`);
    }
  }

  // ── 3. Projects (sites) ─────────────────────────────────────────────────
  console.log('\n🏗️  Migration des sites (projects)...');
  const projects = await get(LOCAL, '/api/projects', localToken);
  console.log(`   ${projects.length} site(s) trouvé(s)`);
  let ok = 0, skip = 0, err = 0;
  for (const p of [...projects].reverse()) {
    const res = await post(RAILWAY, '/api/projects', {
      nomProjet: p.nomProjet,
      prefecture: p.prefecture,
      nomSite: p.nomSite || '',
      typeMaison: p.typeMaison || '',
      numeroMaison: p.numeroMaison || '',
      description: p.description || '',
      etapeConstruction: p.etapeConstruction || '',
      statutConstruction: p.statutConstruction || '',
      // No projectId → skip zone existence check on server
    }, railToken);
    if (res.status === 201) {
      ok++;
    } else if (res.status === 409) {
      skip++;
    } else {
      err++;
      console.log(`   ❌ Site ${p.nomSite} (${p.prefecture}): ${res.status} ${JSON.stringify(res.body)}`);
    }
    // Small throttle to avoid overwhelming Railway
    if ((ok + skip + err) % 10 === 0) await sleep(100);
  }
  console.log(`   ✅ ${ok} créés, ⚠️  ${skip} déjà présents, ❌ ${err} erreurs`);

  // ── 4. Project progress ─────────────────────────────────────────────────
  console.log('\n📊 Migration des suivis de progression...');
  const progress = await get(LOCAL, '/api/project-progress', localToken);
  console.log(`   ${progress.length} suivi(s) trouvé(s)`);

  // We need to map local project IDs to Railway project IDs
  const railProjects = await get(RAILWAY, '/api/projects', railToken);
  function findRailProjectId(localProject) {
    const match = railProjects.find(rp =>
      rp.nomProjet?.toLowerCase() === localProject.nomProjet?.toLowerCase() &&
      rp.prefecture?.toLowerCase() === localProject.prefecture?.toLowerCase() &&
      rp.nomSite?.toLowerCase() === localProject.nomSite?.toLowerCase()
    );
    return match?.id || null;
  }

  // Build local project lookup
  const localProjectMap = {};
  for (const p of projects) localProjectMap[p.id] = p;

  let progressOk = 0, progressErr = 0;
  for (const entry of [...progress].reverse()) {
    const localProject = localProjectMap[entry.projectId];
    if (!localProject) {
      console.log(`   ⚠️  Suivi sans projet local id=${entry.projectId}, ignoré`);
      continue;
    }
    const railProjectId = findRailProjectId(localProject);
    if (!railProjectId) {
      console.log(`   ⚠️  Projet Railway introuvable pour ${localProject.nomSite} (${localProject.prefecture}), ignoré`);
      continue;
    }
    const res = await post(RAILWAY, '/api/project-progress', {
      projectId: railProjectId,
      stage: entry.stage || '',
      title: entry.title || '',
      note: entry.note || '',
      percentage: entry.percentage || 0,
      materialsUsed: entry.materialsUsed || '',
      laborCount: entry.laborCount || 0,
      createdBy: entry.createdBy || 'admin',
    }, railToken);
    if (res.status === 201) {
      progressOk++;
    } else {
      progressErr++;
      console.log(`   ❌ Suivi projectId=${railProjectId}: ${res.status} ${JSON.stringify(res.body)}`);
    }
  }
  console.log(`   ✅ ${progressOk} créés, ❌ ${progressErr} erreurs`);

  // ── 5. Summary ──────────────────────────────────────────────────────────
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🎉 Migration terminée!');
  console.log(`\n📋 Vérification finale sur Railway:`);
  const finalProjects = await get(RAILWAY, '/api/projects', railToken);
  const finalFolders = await get(RAILWAY, '/api/project-folders', railToken);
  const finalCatalog = await get(RAILWAY, '/api/project-catalog', railToken);
  console.log(`   Catalogue: ${finalCatalog.length} entrée(s)`);
  console.log(`   Zones:     ${finalFolders.length} entrée(s)`);
  console.log(`   Sites:     ${finalProjects.length} site(s)`);
  console.log(`\n🌐 URL: ${RAILWAY}/erp.html`);
}

main().catch(err => {
  console.error('❌ Migration échouée:', err.message);
  process.exit(1);
});
