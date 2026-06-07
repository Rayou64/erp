# CHANGES SUMMARY - 2026-06-07

## Scope
Daily consolidation of mobile UX, HR readability, and notifications (UI + system push) for Ryan ERP.

## Main outcomes
- Mobile navigation/dock/back flow redesigned and stabilized.
- HR employee dossier readability restored and reinforced (black text + light surfaces where needed).
- Full notifications stack implemented:
  - in-app notification center UI
  - mobile-open behavior for notification panel
  - service worker push handling
  - backend push subscription API + test route
  - server-side polling push broadcast for pending workflows
- Production app updated multiple times; latest deployment is ACTIVE.

## Commits delivered today (main)
- d878a3c - Refine mobile dock on module screens
- a2e3af6 - Fix mobile menu placement and HR employee dossier readability
- 4807a14 - Apply global mobile glass UI and align back/menu controls
- d39fde3 - Revert global color overrides and restore HR dossier readability
- 72f88be - feat: implement full mobile/system push notifications
- 31010fd - fix: force visible notifications and HR text readability
- 4373694 - fix: stabilize mobile notifications and improve employee list readability

## Files touched (key)
- public/erp.html
  - mobile nav/dock/back adjustments
  - notif panel behavior + readability
  - HR employee module readability hardening
- app.js
  - push notification backend wiring
  - push subscription endpoints
  - polling/broadcast loop for system push triggers
- public/sw.js
  - push event + notification click handling
- package.json, package-lock.json
  - added web-push dependency

## Production deployments (today)
- a67ee161 - commit d878a3c - SUPERSEDED
- 6e5cfdca - commit a2e3af6 - SUPERSEDED
- a00e8940 - commit 4807a14 - SUPERSEDED
- 0f2efe8b - commit d39fde3 - SUPERSEDED
- db07e3e2 - commit 72f88be - SUPERSEDED
- 4453faac - commit 31010fd - SUPERSEDED
- cf969474 - commit 4373694 - ACTIVE

## Current status
- Branch main contains all requested fixes from today.
- Latest production deployment for Ryan ERP is ACTIVE.
- Remaining untracked workspace files are temp/local artifacts and were intentionally not included in these commits.
