# Ryan ERP Mobile (Android)

Ce dossier contient l'application mobile Android (Capacitor) pour Ryan ERP.

## Connexion

L'application utilise les comptes du module **Utilisateurs** de Ryan ERP.

Le parcours « Mot de passe oublié » est validé par un administrateur :

1. l'utilisateur envoie une demande avec son identifiant depuis l'écran mobile ;
2. l'administrateur accepte ou refuse la demande dans le module **Utilisateurs** ;
3. après acceptation, la saisie de l'identifiant affiche automatiquement les deux champs de nouveau mot de passe ;
4. l'enregistrement met à jour le compte, termine la demande et invalide les anciennes sessions.

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

À chaque démarrage ou retour dans l'application Android, la page de connexion publiée
sur le serveur RyanERP est affichée et la session précédente est effacée. Le formulaire
local reste disponible comme solution de repli lorsque le téléphone est hors ligne.

L'application vérifie aussi `mobile-version.json` au démarrage et toutes les six heures.
Lorsqu'une version plus récente est disponible, Android affiche une notification
« Mise à jour RyanERP disponible ». L'action « Mettre à jour » télécharge l'APK depuis
le serveur, vérifie son empreinte SHA-256 et son identifiant d'application, puis ouvre
l'installateur Android.

Dans Android Studio:

1. `Build` -> `Build Bundle(s) / APK(s)` -> `Build APK(s)` pour un APK test.
2. `Build` -> `Generate Signed Bundle / APK` pour Play Store.

## Publication Play Store (resume)

1. Creer un compte Google Play Console.
2. Generer un AAB signe.
3. Completer fiche store (nom, captures, politique de confidentialite).
4. Publier en test interne, puis production.
