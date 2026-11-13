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

## Stock par lot

- Dans **Inventaire > Stock par lot**, les produits transférés au chantier restent visibles, même après consommation complète.
- Les quantités sont regroupées par produit, lot et étape : commandé, transféré, utilisé au chantier, restant en entrepôt et restant sur le lot.
- Le restant sur le lot est calculé automatiquement : **transféré - utilisé dans le journal de chantier**, avec un minimum de zéro. Une consommation ne déduit pas une seconde fois le stock de l'entrepôt.
- Après l'enregistrement du journal, l'inventaire est rechargé automatiquement. Les filtres de stock et de statut de cette vue portent sur le restant du lot.
- Vérification ciblée : `node --test scripts/test-inventory-site.js scripts/test-kokan-inventory.js`.
- Le compte **Kokan_SK** consulte l'inventaire du projet **Songon Kassemble**, y compris ses types de bâtiments et ses lots. Les demandes, mouvements et filtres de projet respectent ce périmètre.

## Messagerie interne et dossiers profils

- La **Messagerie interne** est accessible dans tous les profils, y compris les comptes restreints et les chauffeurs.
- Dans **Admin > Dossiers profils**, elle apparaît comme toujours autorisée et ne peut pas être interdite. Les anciennes restrictions sur ce module sont sans effet ; les autres restrictions sont conservées.
- Chaque compte conserve sa propre boîte de réception et les contrôles d'accès aux conversations restent inchangés.
- Dans l'application de messagerie, les dossiers colorés classent les messages : Rapports d'incidents, Informations générales, Documents projets, Réunions et Administration.
- Les équipes destinataires RH, Direction, Chantiers et Achats sont présentées séparément des dossiers. Elles restent indisponibles tant que leur gestion et leurs membres ne sont pas raccordés au module **Liste des employés** ; les rôles utilisateurs ne sont pas utilisés comme équipes.
- Sur téléphone (jusqu'à 780 px), la messagerie reprend la maquette validée : enveloppe unique, retour ERP, recherche arrondie, dossiers colorés défilants, menu latéral, lecture et rédaction plein écran, bouton Nouveau. L'ordinateur conserve sa présentation à trois panneaux ; les deux interfaces utilisent les mêmes API et données réelles.
- La rédaction conserve À/Cc/Cci, le gras, l'italique, le souligné, les brouillons, les pièces jointes avec retrait et les images. Les limites existantes restent applicables : 600 Ko par fichier, 250 Ko par image intégrée et 900 Ko pour la requête JSON.
- Le client Android charge le site distant configuré dans [mobile/capacitor.config.json](mobile/capacitor.config.json). Une publication web actualise cette interface sans reconstruire l'APK ; la messagerie s'ouvre dans la WebView courante sur mobile.
- Vérification ciblée : `node --test scripts/test-internal-mail-access.js scripts/test-messaging-teams.js scripts/test-messaging-mobile.js`.

## Cartes du profil Gestion du parc auto

- Dans la fiche d'un véhicule, **Vue Google** et **Vue technique · trajet bleu** ouvrent directement le véhicule correspondant dans les cartes. Les deux vues restent accessibles depuis le détail.
- La carte principale et la fiche véhicule sont deux vues exclusives. **Retour à la carte** réaffiche la carte principale de flotte et y replace la carte technique avec le trajet sélectionné ; les boutons Google/technique restent utilisables après ce retour.
- Google affiche la dernière position enregistrée. La vue technique affiche en bleu toutes les positions de la dernière session GPS, ou tout l'historique disponible en l'absence de session, sans limiter le trajet aux 25 derniers points. Une trace GPS superposée à Google nécessiterait une intégration Google Maps JavaScript API configurée ; l'iframe actuelle ne la fournit pas.
- Les deux cartes techniques utilisent l'URL HTTPS canonique OpenStreetMap, l'attribution visible et `strict-origin-when-cross-origin` sur leurs images de tuiles, conformément à la [politique du fournisseur](https://operations.osmfoundation.org/policies/tiles/). Le navigateur transmet seulement l'origine Ryan ERP, pas le chemin ni les paramètres. La politique globale `no-referrer` reste inchangée pour les autres requêtes ; aucun proxy, téléchargement en masse ou contournement du cache n'est ajouté. Un échec de chargement du fond de carte est signalé sans supprimer le tracé GPS.
- L'historique GPS est paginé par le couple `recordedAt/id` : `GET /api/auto-vehicles/:id/locations` renvoie `nextCursor` ; les pages suivantes utilisent `beforeRecordedAt` et `beforeId`. Les paramètres existants `afterId`, `fromTrackingStart` et `limit` restent disponibles. Les réponses d'une ancienne sélection ne remplacent pas le trajet d'un autre véhicule.
- Vérification ciblée : `node --test scripts/test-vehicle-map-views.js` (pagination SQLite, session de plus de 20 000 points, changements rapides de véhicule, trace et accès aux deux vues).

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
