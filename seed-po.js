const http = require('http');
function apiCall(method, path, body, token) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : '';
    const opts = { hostname:'localhost', port:4000, path, method, headers:{ 'Content-Type':'application/json', 'Content-Length':Buffer.byteLength(data), ...(token?{'Authorization':'Bearer '+token}:{}) } };
    const req = http.request(opts, res => { let raw=''; res.on('data',c=>raw+=c); res.on('end',()=>{ try{resolve({status:res.statusCode,body:JSON.parse(raw)})}catch(e){resolve({status:res.statusCode,body:raw})} }); });
    req.on('error',reject); if(data) req.write(data); req.end();
  });
}
async function main() {
  const token = (await apiCall('POST','/api/auth/login',{username:'admin',password:'admin123'})).body.token;

  // Trouver les vraies demandes
  const mrsRes = await apiCall('GET','/api/material-requests',null,token);
  const mrs = mrsRes.body;
  console.log('Demandes:', mrs.map(m=>`id=${m.id} proj=${m.projetId} item=${m.itemName}`).join(' | '));

  // Grouper par projet
  const byProject = {};
  mrs.forEach(m=>{ (byProject[m.projetId]=byProject[m.projetId]||[]).push(m); });

  // Bon de commande EPP AIMAN ROGER (projetId=3)
  const p1mrs = byProject[3]||[];
  if(p1mrs.length) {
    const r = await apiCall('POST','/api/purchase-orders',{
      projetId:3, fournisseur:'Safari Industries', dateCommande:'2026-04-15',
      items: p1mrs.map(m=>({ materialRequestId:m.id, article:m.itemName, quantite:m.quantiteDemandee, prixUnitaire: m.itemName==='Ciment Portland'?4500:8000, totalLigne: m.quantiteDemandee*(m.itemName==='Ciment Portland'?4500:8000) }))
    }, token);
    console.log('PO1:', r.status, JSON.stringify(r.body).substring(0,120));
  }

  // Bon de commande IRMA (projetId=4)
  const p2mrs = byProject[4]||[];
  if(p2mrs.length) {
    const r = await apiCall('POST','/api/purchase-orders',{
      projetId:4, fournisseur:'BTP Materiel SARL', dateCommande:'2026-04-15',
      items: p2mrs.map(m=>({ materialRequestId:m.id, article:m.itemName, quantite:m.quantiteDemandee, prixUnitaire: m.itemName.includes('Bri')?350:12000, totalLigne: m.quantiteDemandee*(m.itemName.includes('Bri')?350:12000) }))
    }, token);
    console.log('PO2:', r.status, JSON.stringify(r.body).substring(0,120));
  }
}
main().catch(console.error);
