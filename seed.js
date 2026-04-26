const http = require('http');

function apiCall(method, path, body, token) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : '';
    const opts = {
      hostname: 'localhost',
      port: 4000,
      path,
      method,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        ...(token ? { 'Authorization': 'Bearer ' + token } : {})
      }
    };
    const req = http.request(opts, res => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(raw) }); }
        catch(e) { resolve({ status: res.statusCode, body: raw }); }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

async function main() {
  // Login
  const loginRes = await apiCall('POST', '/api/auth/login', { username: 'admin', password: 'admin123' });
  const token = loginRes.body.token;
  console.log('LOGIN OK');

  const P1 = 3; // EPP AIMAN ROGER
  const P2 = 4; // IRMA

  // --- Demandes d'approvisionnement ---
  const mr1Res = await apiCall('POST', '/api/material-requests', {
    projetId: P1, itemName: 'Ciment Portland', quantiteDemandee: 50,
    demandeur: 'Aiman Roger', description: 'Ciment pour fondations', dateDemande: '2026-04-15'
  }, token);
  const mr1 = mr1Res.body; console.log('MR1 id=' + mr1.id, mr1Res.status);

  const mr2Res = await apiCall('POST', '/api/material-requests', {
    projetId: P1, itemName: 'Fer a beton 12mm', quantiteDemandee: 30,
    demandeur: 'Aiman Roger', description: 'Fer pour charpente', dateDemande: '2026-04-15'
  }, token);
  const mr2 = mr2Res.body; console.log('MR2 id=' + mr2.id, mr2Res.status);

  const mr3Res = await apiCall('POST', '/api/material-requests', {
    projetId: P2, itemName: 'Briques creuses', quantiteDemandee: 1000,
    demandeur: 'IRMA Chef', description: 'Briques pour murs', dateDemande: '2026-04-15'
  }, token);
  const mr3 = mr3Res.body; console.log('MR3 id=' + mr3.id, mr3Res.status);

  const mr4Res = await apiCall('POST', '/api/material-requests', {
    projetId: P2, itemName: 'Peinture acrylique', quantiteDemandee: 20,
    demandeur: 'IRMA Chef', description: 'Peinture finition interieure', dateDemande: '2026-04-15'
  }, token);
  const mr4 = mr4Res.body; console.log('MR4 id=' + mr4.id, mr4Res.status);

  // --- Bons de commande ---
  const po1Res = await apiCall('POST', '/api/purchase-orders', {
    projetId: P1, fournisseur: 'Safari Industries', dateCommande: '2026-04-15',
    items: [
      { materialRequestId: mr1.id, article: 'Ciment Portland 50kg', quantite: 50, prixUnitaire: 4500, totalLigne: 225000 },
      { materialRequestId: mr2.id, article: 'Fer a beton 12mm (barre)', quantite: 30, prixUnitaire: 8000, totalLigne: 240000 }
    ]
  }, token);
  const po1 = po1Res.body; console.log('PO1 id=' + po1.id + ' total=' + po1.montantTotal, po1Res.status);
  if (po1Res.status >= 400) console.error('PO1 ERROR:', JSON.stringify(po1));

  const po2Res = await apiCall('POST', '/api/purchase-orders', {
    projetId: P2, fournisseur: 'BTP Materiel SARL', dateCommande: '2026-04-15',
    items: [
      { materialRequestId: mr3.id, article: 'Briques creuses 20x20x15', quantite: 1000, prixUnitaire: 350, totalLigne: 350000 },
      { materialRequestId: mr4.id, article: 'Peinture acrylique 20L Blanc', quantite: 20, prixUnitaire: 12000, totalLigne: 240000 }
    ]
  }, token);
  const po2 = po2Res.body; console.log('PO2 id=' + po2.id + ' total=' + po2.montantTotal, po2Res.status);
  if (po2Res.status >= 400) console.error('PO2 ERROR:', JSON.stringify(po2));

  // --- Dépenses ---
  const e1Res = await apiCall('POST', '/api/expenses', {
    projetId: P1, description: 'Achat ciment - EPP Aiman Roger',
    quantite: 50, prixUnitaire: 4500, categorie: 'materiaux', fournisseur: 'Safari Industries'
  }, token);
  const e1 = e1Res.body; console.log('EXP1 id=' + e1.id + ' montant=' + e1.montantTotal, e1Res.status);
  if (e1Res.status >= 400) console.error('EXP1 ERROR:', JSON.stringify(e1));

  const e2Res = await apiCall('POST', '/api/expenses', {
    projetId: P1, description: 'Achat fer a beton - EPP Aiman Roger',
    quantite: 30, prixUnitaire: 8000, categorie: 'materiaux', fournisseur: 'Safari Industries'
  }, token);
  const e2 = e2Res.body; console.log('EXP2 id=' + e2.id + ' montant=' + e2.montantTotal, e2Res.status);

  const e3Res = await apiCall('POST', '/api/expenses', {
    projetId: P2, description: 'Achat briques - IRMA',
    quantite: 1000, prixUnitaire: 350, categorie: 'materiaux', fournisseur: 'BTP Materiel SARL'
  }, token);
  const e3 = e3Res.body; console.log('EXP3 id=' + e3.id + ' montant=' + e3.montantTotal, e3Res.status);

  const e4Res = await apiCall('POST', '/api/expenses', {
    projetId: P2, description: 'Achat peinture - IRMA',
    quantite: 20, prixUnitaire: 12000, categorie: 'materiaux', fournisseur: 'BTP Materiel SARL'
  }, token);
  const e4 = e4Res.body; console.log('EXP4 id=' + e4.id + ' montant=' + e4.montantTotal, e4Res.status);

  console.log('\n=== RESUME ===');
  console.log('EPP AIMAN ROGER total depenses:', (50*4500 + 30*8000), 'XAF');
  console.log('IRMA total depenses:', (1000*350 + 20*12000), 'XAF');
  console.log('GRAND TOTAL:', (50*4500 + 30*8000 + 1000*350 + 20*12000), 'XAF');
  console.log('DONE');
}

main().catch(console.error);
