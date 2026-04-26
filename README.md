# RyanERP

## Description

Application web simple pour gérer des projets, demandes d'approvisionnement et bons de commande.
Cette version utilise une base de données SQLite locale et une page de connexion.

## Identifiants de test

- Nom d'utilisateur : `admin`
- Mot de passe : `admin123`

## Démarrage

1. Ouvre un terminal dans le dossier :
   `c:\Users\koudo\OneDrive\travail personnel\ERP`
2. Installe les dépendances (une seule fois) :
   ```powershell
   npm install
   ```
3. Lance le serveur :
   ```powershell
   npm start
   ```
4. Ouvre le navigateur :
   ```text
   http://localhost:4000
   ```

## Fichiers principaux

- `app.js` : serveur Express, authentification et SQLite
- `data.db` : base SQLite créée automatiquement
- `package.json` : dépendances et script de démarrage
- `public/index.html` : page de connexion
- `public/app.html` : interface métier
- `public/style.css` : style de l'application

## Conseils

- Garde le terminal ouvert tant que tu utilises l'application.
- Pour partager en ligne, tu peux déployer ce projet sur Railway, Render ou Vercel.

## Sauvegardes fiables (DB + archives)

Pour garantir que tout est bien sauvegarde (base + PDF archives), utilise ces commandes:

```powershell
npm run backup:now
```

Cette commande cree un snapshot complet dans `backups/snapshot-YYYYMMDD-HHMMSS` avec:

- `data.db`
- dossier `archives/`
- `manifest.json` (taille + empreinte SHA256 de chaque fichier)

Verification d'integrite du dernier snapshot:

```powershell
npm run backup:verify
```

Parametres optionnels:

- `BACKUP_DIR` : dossier de sauvegarde (defaut: `./backups`)
- `BACKUP_KEEP` : nombre de snapshots conserves (defaut: `20`)

Sauvegarde automatique a chaque modification metier:

- Le backend declenche automatiquement une sauvegarde apres chaque requete API de type `POST`, `PATCH`, `PUT`, `DELETE` reussie (hors `/api/auth/*`).
- La sauvegarde est debouncee (anti-spam) pour eviter les duplications.

Variables de controle:

- `AUTO_BACKUP_ENABLED=1` (defaut)
- `AUTO_BACKUP_ON_MUTATION=1` (defaut)
- `AUTO_BACKUP_DEBOUNCE_MS=10000` (defaut 10s)

Sauvegarde automatique a chaque modification de code (watcher):

```powershell
npm run backup:auto
```

Ce watcher lance `backup:now` des qu un fichier source est modifie.

Lancement automatique au demarrage Windows (watcher backup):

- Script: `scripts/start-auto-backup.ps1`
- Lanceur startup: `%APPDATA%\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\RyanERP-AutoBackup.cmd`
- Logs: `logs/auto-backup.log`

Exemple:

```powershell
$env:BACKUP_KEEP=30
npm run backup:now
```

## Déploiement Railway

1. Crée un nouveau projet Railway depuis ce dossier (GitHub ou dépôt local).
2. Railway détecte automatiquement le Dockerfile et lance l'application.
3. Dans Railway, ajoute un Volume persistant et monte-le sur `/data`.
4. Configure ces variables d'environnement dans Railway :
   - `DB_FILE=/data/data.db`
   - `ARCHIVE_ROOT=/data/archives`
   - `JWT_SECRET=met_un_secret_long_et_unique`
   - `API_RATE_MAX=600`
   - `API_RATE_WINDOW_MS=60000`
   - `AUTH_RATE_MAX=25`
   - `AUTH_RATE_WINDOW_MS=900000`
   - `JSON_BODY_LIMIT=1mb`
5. Redéploie le service.

Avec cette configuration, la base SQLite et les archives PDF restent persistantes entre les redémarrages et déploiements.

### Déploiement automatique vers le lien public

Si tu veux que les modifications de code partent automatiquement vers Railway :

1. Installe les dépendances du projet (inclut le watcher) :
   ```powershell
   npm install
   ```
2. Vérifie que le dossier est lié au bon projet Railway :
   ```powershell
   railway status
   ```
3. Lance le mode auto-déploiement :
   ```powershell
   npm run deploy:auto
   ```

Quand cette commande tourne, chaque modification de `app.js`, `public/**/*`, `package.json` ou `railway.json` déclenche automatiquement un nouveau déploiement vers le lien public Railway.

### Lancement automatique au démarrage Windows

Le projet est configuré pour relancer automatiquement le watcher à l'ouverture de session Windows.

- Script watcher : `scripts/start-auto-deploy.ps1`
- Lanceur démarrage Windows : `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\RyanERP-AutoDeploy.cmd`
- Log d'exécution : `logs/auto-deploy.log`

Si tu redémarres le PC puis ouvres ta session, `npm run deploy:auto` repart automatiquement.

## Remarques

- Les mots de passe sont stockés de façon sécurisée dans SQLite.
- Les données restent dans `data.db` entre les redémarrages.

## Durcissement production ajouté

Sans changer la structure métier de l'ERP, le backend inclut maintenant :

- En-têtes de sécurité HTTP via Helmet.
- Limitation de débit API globale + limitation renforcée sur la connexion.
- Identifiant de requête (`X-Request-Id`) et logs JSON des requêtes.
- Endpoints de supervision :
   - `GET /healthz` (liveness)
   - `GET /readyz` (readiness, test DB)
- Arrêt propre du serveur sur signaux (`SIGINT`, `SIGTERM`).
- Optimisations SQLite (`WAL`, `busy_timeout`) et index SQL pour les requêtes fréquentes.

### Vérification rapide en local

Après `npm start`, tu peux tester :

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:4000/healthz
Invoke-WebRequest -UseBasicParsing http://localhost:4000/readyz
```

## Audit cloud grande entreprise

Un audit automatique est disponible pour savoir si la configuration actuelle est prete pour une informatique en nuage de grande envergure.

```powershell
npm run cloud:readiness
```

Le script renvoie un rapport JSON avec:

- `enterpriseReady`: `true` ou `false`
- `score`: score global de preparation
- `checks`: details par domaine (securite, base de donnees, stockage, fiabilite, observabilite)

Note importante:

- Tant que l application reste sur SQLite locale, le rapport ne peut pas etre `enterpriseReady=true` pour une cible grande entreprise multi-instance.

Fichiers de demarrage cloud:

- `.env.cloud.example` : base de configuration production/cloud
- `CLOUD-ENTERPRISE-RUNBOOK.md` : plan d execution par phases

Activation couche PostgreSQL (phase progressive):

- `DATABASE_DRIVER=postgres`
- `DATABASE_URL=postgres://...`

L application utilise maintenant une couche d acces unifiee (`run/get/all`) compatible SQLite et PostgreSQL.
