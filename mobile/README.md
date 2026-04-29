# Ryan ERP Mobile (Android)

Ce dossier contient l'application mobile Android (Capacitor) pour Ryan ERP.

## Profils inclus

- Profil 1: `gestionnaire_stock_zone`
- Profil 2: `chef_chantier_site`

Le choix du profil dans l'ecran mobile est un raccourci. La connexion reste geree par l'ecran login ERP (identifiants + mot de passe).

## Prerequis

- Node.js 18+
- Java JDK 17
- Android Studio (SDK + build tools)

## Commandes

Depuis ce dossier (`mobile/`):

- Installer dependances:
  - `npm install`
- Synchroniser le projet Android:
  - `npx cap sync android`
- Ouvrir dans Android Studio:
  - `npx cap open android`

## Build APK/AAB

Dans Android Studio:

1. `Build` -> `Build Bundle(s) / APK(s)` -> `Build APK(s)` pour un APK test.
2. `Build` -> `Generate Signed Bundle / APK` pour Play Store.

## Publication Play Store (resume)

1. Creer un compte Google Play Console.
2. Generer un AAB signe.
3. Completer fiche store (nom, captures, politique de confidentialite).
4. Publier en test interne, puis production.
