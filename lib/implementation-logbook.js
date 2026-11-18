const express = require('express');

const STAGES = [
  { id: 'etape-0', number: 0, title: "Étape d'implémentation préalable" },
  { id: 'etape-1', number: 1, title: "Première étape d'implimentation" },
];
const ACTIVE_STATUSES = new Set(['queued', 'preparing', 'publishing', 'building', 'verifying']);
const DEPLOYMENT_BLOCKERS = [
  "Le contenu final de l'étape 1 doit encore être validé.",
  'La procédure de publication et le périmètre de la version doivent encore être validés.',
];
const TEXT_FIELDS = {
  title: 160, summary: 400, beforeText: 4000, afterText: 4000,
  profiles: 1500, modules: 1500, rights: 4000, visibility: 4000,
  validation: 4000, source: 1000,
};

function failure(status, message) {
  return Object.assign(new Error(message), { status });
}

function isLocalRequest(req) {
  const address = req.socket?.remoteAddress;
  const loopback = address === '::1' || address === '127.0.0.1' || address === '::ffff:127.0.0.1';
  if (!loopback || !/^(localhost|127\.0\.0\.1|\[::1\])(?::\d+)?$/i.test(req.headers.host || '')) return false;
  if (req.headers['x-forwarded-for'] || req.headers['x-forwarded-host']) return false;
  if (!req.headers.origin) return true;
  try {
    const origin = new URL(req.headers.origin);
    return ['http:', 'https:'].includes(origin.protocol) && origin.host.toLowerCase() === req.headers.host.toLowerCase();
  } catch {
    return false;
  }
}

function normalizeEntry(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw failure(400, 'Fiche invalide.');
  const keys = new Set([...Object.keys(TEXT_FIELDS), 'kind', 'validationStatus', 'version']);
  if (Object.keys(body).some(key => !keys.has(key))) throw failure(400, 'La fiche contient un champ non reconnu.');
  if (!['module', 'profile'].includes(body.kind)) throw failure(400, 'Choisir Module ou Profil.');
  if (!['pending', 'verified'].includes(body.validationStatus)) throw failure(400, 'Statut de vérification invalide.');
  const entry = { kind: body.kind, validationStatus: body.validationStatus };
  for (const [field, limit] of Object.entries(TEXT_FIELDS)) {
    const value = body[field] ?? '';
    if (typeof value !== 'string' || value.length > limit) throw failure(400, `Champ ${field} invalide (maximum ${limit} caractères).`);
    entry[field] = value.trim();
  }
  for (const field of ['title', 'summary', 'beforeText', 'afterText', 'profiles', 'modules', 'rights', 'visibility']) {
    if (!entry[field]) throw failure(400, `Le champ ${field} est obligatoire.`);
  }
  if (entry.validationStatus === 'verified' && !entry.validation) throw failure(400, 'Décrire les vérifications avant de marquer la fiche vérifiée.');
  return entry;
}

function createImplementationLogbook({ db, enabled, publicOrigin = '', deployer = null }) {
  const router = express.Router();
  const fields = ['kind', ...Object.keys(TEXT_FIELDS), 'validationStatus'];
  const canPublish = req => Boolean(deployer) && !publicOrigin && isLocalRequest(req);
  const canAccess = req => {
    if (!enabled || req.user?.role !== 'admin') return false;
    if (!publicOrigin) return isLocalRequest(req);
    const origin = new URL(publicOrigin);
    return req.headers.host?.toLowerCase() === origin.host.toLowerCase() &&
      (!req.headers.origin || req.headers.origin === publicOrigin);
  };

  async function insertEntry(tx, entry, stageId, username, now) {
    if (tx.driver === 'postgres') await tx.run('LOCK TABLE local_implementation_entries IN EXCLUSIVE MODE');
    const { nextId } = await tx.get('SELECT COALESCE(MAX(id),0)+1 AS nextId FROM local_implementation_entries');
    await tx.run(`INSERT INTO local_implementation_entries
      (id, stageId, ${fields.join(', ')}, createdAt, updatedAt, createdBy, updatedBy)
      VALUES (?, ?, ${fields.map(() => '?').join(', ')}, ?, ?, ?, ?)`,
    [nextId, stageId, ...fields.map(field => entry[field]), now, now, username, username]);
    return tx.get('SELECT * FROM local_implementation_entries WHERE id = ?', [nextId]);
  }

  async function initialize() {
    if (!enabled) return;
    await db.transaction(async tx => {
      await tx.run(`CREATE TABLE IF NOT EXISTS local_implementation_stages (
        id TEXT PRIMARY KEY, title TEXT NOT NULL, createdAt TEXT NOT NULL
      )`);
      await tx.run(`CREATE TABLE IF NOT EXISTS local_implementation_entries (
        id INTEGER PRIMARY KEY, stageId TEXT NOT NULL REFERENCES local_implementation_stages(id),
        kind TEXT NOT NULL CHECK(kind IN ('module', 'profile')),
        title TEXT NOT NULL, summary TEXT NOT NULL, beforeText TEXT NOT NULL, afterText TEXT NOT NULL,
        profiles TEXT NOT NULL, modules TEXT NOT NULL, rights TEXT NOT NULL, visibility TEXT NOT NULL,
        validation TEXT NOT NULL, source TEXT NOT NULL,
        validationStatus TEXT NOT NULL CHECK(validationStatus IN ('pending', 'verified')),
        version INTEGER NOT NULL DEFAULT 1, createdAt TEXT NOT NULL, updatedAt TEXT NOT NULL,
        createdBy TEXT NOT NULL, updatedBy TEXT NOT NULL
      )`);
      const now = new Date().toISOString();
      for (const stage of STAGES) {
        await tx.run(`INSERT INTO local_implementation_stages (id, title, createdAt)
          VALUES (?, ?, ?) ON CONFLICT(id) DO NOTHING`, [stage.id, stage.title, now]);
      }
      // Retire only the setup record generated by the first version, not user-authored specifications.
      await tx.run(`DELETE FROM local_implementation_entries
        WHERE id = 1 AND stageId = ? AND createdBy = 'system' AND kind = 'module'
          AND title = 'Carnet de bord' AND modules = 'Admin > Carnet de bord'
          AND summary = ?`,
      ['etape-1', "Création du journal local et du dossier de la première étape."]);
      if (!await tx.get("SELECT id FROM local_implementation_entries WHERE stageId='etape-0'")) {
        await insertEntry(tx, normalizeEntry({
          kind: 'module', title: 'Carnet de bord — livraison préalable CHADA0',
          summary: 'Publier uniquement le Carnet de bord sur RyanERP pour vérifier son fonctionnement en ligne, sans publier l’étape 1.',
          beforeText: 'Le Carnet est disponible uniquement sur localhost.',
          afterText: 'Le Carnet sera accessible depuis Admin sur RyanERP après le déploiement confirmé de l’étape 0. Le site conserve ses données métier et ses droits existants.',
          profiles: 'Administrateurs authentifiés. Aucun nouveau droit pour les autres profils.',
          modules: 'Admin → Carnet de bord uniquement ; intégration de son menu, écran, API et tables dédiées.',
          rights: 'Déclenchement uniquement depuis localhost : bouton Déployer l’étape 0, code CHADA0 exact et confirmation du périmètre. Aucun lancement depuis le site public. CHADA1 reste verrouillé.',
          visibility: 'Dossiers Étape 0 et Étape 1 distincts. Seul le code du Carnet est livré ; les données et spécifications locales de l’étape 1 ne sont pas importées en production.',
          validation: 'Contrôler l’isolation des fichiers, les refus d’accès, les confirmations, le suivi des erreurs et le résultat HTTP après publication.',
          validationStatus: 'pending',
          source: 'Demande validée : dossier préalable, bouton Déployer l’étape 0 et code CHADA0. Aucun déploiement avant le clic et la confirmation.',
        }), 'etape-0', 'system', now);
      }
    });
    if (deployer) await deployer.initialize();
  }

  router.use((req, res, next) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!enabled) return res.status(404).json({ error: 'Carnet de bord indisponible sur cet environnement.' });
    if (!canAccess(req)) return res.status(403).json({ error: 'Carnet de bord réservé aux administrateurs sur cet environnement autorisé.' });
    next();
  });

  router.get('/', async (req, res) => {
    const stages = await db.all('SELECT * FROM local_implementation_stages ORDER BY id');
    const entries = await db.all('SELECT * FROM local_implementation_entries ORDER BY updatedAt DESC, id DESC');
    const job = deployer ? await deployer.latest() : null;
    const deployments = {
      'etape-0': {
        enabled: canPublish(req), confirmationCode: 'CHADA0', job,
        blockers: canPublish(req) ? [] : ['Le lancement de la publication est réservé au Carnet du localhost.'],
      },
      'etape-1': { enabled: false, confirmationCode: 'CHADA1', blockers: DEPLOYMENT_BLOCKERS },
    };
    res.json({
      environment: publicOrigin ? 'production' : 'local',
      stages: stages.map(stage => ({
        ...stage, number: STAGES.find(item => item.id === stage.id)?.number,
        status: stage.id === 'etape-0' && publicOrigin ? 'deployed'
          : stage.id === 'etape-0' && job?.status === 'deployed' ? 'deployed'
            : stage.id === 'etape-0' && ACTIVE_STATUSES.has(job?.status) ? 'deploying'
              : stage.id === 'etape-0' && job?.status === 'failed' ? 'failed' : 'local',
      })),
      entries,
      deployments,
      deployment: { ...deployments['etape-1'], stageId: 'etape-1' },
    });
  });

  router.use('/stages/:stageId', async (req, res, next) => {
    const stage = await db.get('SELECT id FROM local_implementation_stages WHERE id = ?', [req.params.stageId]);
    if (!stage) return res.status(404).json({ error: "Dossier d'étape introuvable." });
    next();
  });

  router.post('/stages/:stageId/entries', async (req, res) => {
    const entry = normalizeEntry(req.body);
    if (req.body.version !== undefined) throw failure(400, 'Une nouvelle fiche ne possède pas encore de version.');
    const now = new Date().toISOString();
    const saved = await db.transaction(tx => insertEntry(tx, entry, req.params.stageId, req.user.username, now));
    res.status(201).json(saved);
  });

  router.put('/stages/:stageId/entries/:id', async (req, res) => {
    const entry = normalizeEntry(req.body);
    const id = Number(req.params.id);
    if (!Number.isSafeInteger(id) || id < 1) throw failure(400, 'Identifiant de fiche invalide.');
    if (!Number.isSafeInteger(req.body.version) || req.body.version < 1) throw failure(400, 'Version de fiche obligatoire.');
    const saved = await db.transaction(async tx => {
      const result = await tx.run(`UPDATE local_implementation_entries
        SET ${fields.map(field => `${field} = ?`).join(', ')}, version = version + 1, updatedAt = ?, updatedBy = ?
        WHERE id = ? AND stageId = ? AND version = ?`,
      [...fields.map(field => entry[field]), new Date().toISOString(), req.user.username, id, req.params.stageId, req.body.version]);
      if (!result.changes) {
        const existing = await tx.get('SELECT id FROM local_implementation_entries WHERE id = ? AND stageId = ?', [id, req.params.stageId]);
        throw failure(existing ? 409 : 404, existing
          ? 'Cette fiche a été modifiée ailleurs. Copie tes changements, ferme le formulaire puis actualise avant de réessayer.'
          : 'Fiche introuvable dans cette étape.');
      }
      return tx.get('SELECT * FROM local_implementation_entries WHERE id = ?', [id]);
    });
    res.json(saved);
  });

  router.get('/stages/:stageId/deployment-check', async (req, res) => {
    if (req.params.stageId !== 'etape-0' || !canPublish(req)) throw failure(403, 'Préparation réservée à l’étape 0 sur localhost.');
    res.json(await deployer.check());
  });

  router.post('/stages/:stageId/deploy', async (req, res) => {
    const stage = STAGES.find(item => item.id === req.params.stageId);
    if (!stage) throw failure(400, 'Étape de publication inconnue.');
    const code = `CHADA${stage.number}`;
    if (req.body?.code !== code) throw failure(400, `Code incorrect. Saisir exactement ${code}.`);
    if (req.body?.confirmed !== true) throw failure(400, `La confirmation du périmètre de l’étape ${stage.number} est obligatoire.`);
    if (stage.number === 0) {
      if (!canPublish(req)) throw failure(403, 'Le déploiement se lance uniquement depuis le Carnet du localhost.');
      if (!/^[a-f0-9]{64}$/.test(req.body.releaseDigest || '')) throw failure(400, 'Vérifie la livraison isolée avant de confirmer.');
      const job = await deployer.start(req.user.username, req.body.releaseDigest);
      return res.status(202).json({ job });
    }
    res.status(409).json({
      code: 'DEPLOYMENT_LOCKED',
      error: "Code reconnu, mais publication verrouillée. Le contenu final et la procédure de l'étape 1 doivent être validés. Aucun déploiement lancé.",
      blockers: DEPLOYMENT_BLOCKERS,
    });
  });

  router.use((error, _req, res, _next) => {
    const status = Number.isInteger(error.status) ? error.status : 500;
    if (status >= 500) console.error('[implementation-logbook]', error);
    res.status(status).json({ error: status >= 500 ? 'Impossible de terminer cette opération du Carnet de bord.' : error.message });
  });

  return { initialize, router, canAccess };
}

module.exports = { createImplementationLogbook };
