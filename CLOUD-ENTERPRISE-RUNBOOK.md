# Runbook Cloud Grande Entreprise - ERP

## Objectif
Passer l ERP vers une architecture cloud capable de supporter une grande entreprise (haute disponibilite, securite, scalabilite, exploitation).

## Etat actuel
- Backend Node.js valide.
- Endpoints de sante disponibles (`/healthz`, `/readyz`).
- Blocage principal pour grande echelle: SQLite + stockage local.

## Phase 1 - Fondations (1 a 2 semaines)
1. Activer les variables de production depuis `.env.cloud.example`.
2. Mettre les secrets dans un secret manager (pas de valeurs par defaut).
3. Activer logs centralises et alerting.
4. Verifier readiness/liveness dans la plateforme cloud.

## Phase 2 - Donnees et stockage (2 a 4 semaines)
1. Creer PostgreSQL manag\u00e9 (HA) et activer sauvegardes.
2. Migrer schema + donnees SQLite vers PostgreSQL.
3. Basculer archives PDF vers object storage (Blob/S3/GCS).
4. Conserver un mode rollback tant que la validation n est pas complete.

## Phase 3 - Scalabilite (1 a 2 semaines)
1. Deployer plusieurs instances applicatives.
2. Activer autoscaling et policies de ressources.
3. Ajouter Redis pour cache distribue / file de travaux.
4. Realiser tests de charge (peak + endurance).

## Phase 4 - Exploitation entreprise (1 a 2 semaines)
1. Definir SLA/SLO et runbooks incident.
2. Tester PRA/PCA avec objectifs RTO/RPO.
3. Mettre en place traces distribuees et tableaux de bord ops.
4. Passer audit securite (vuln scans + revue acces).

## Critere de sortie (Go-Live Enterprise)
- Aucune dependance critique au disque local.
- Base PostgreSQL HA validee sous charge.
- Restauration testee en conditions reelles.
- Alertes production et rotation d astreinte operationnelles.
- Deploiement progressif avec rollback automatique.

## Commandes utiles
```powershell
npm run cloud:readiness
node --check app.js
```

## Migration SQLite vers PostgreSQL

Prerequis:

- `DATABASE_URL` pointe vers ta base PostgreSQL cible.
- `DATABASE_DRIVER` reste sur `sqlite` pendant la copie initiale.

Execution:

```powershell
npm run db:migrate:pg
```

Si tu veux vider les tables cible avant recopie:

```powershell
npm run db:migrate:pg:truncate
```

Apres verification des donnees, bascule l application:

- `DATABASE_DRIVER=postgres`
- `DATABASE_URL=...`
