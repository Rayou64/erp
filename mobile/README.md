# Ryan ERP Mobile (Android)

Ce dossier contient deux variantes Android (Capacitor) :

- **Ryan ERP** (`com.ryan.erp`) : l'ERP complet, incluant la messagerie avec son logo-raccourci sur l'accueil.
- **Portail Safari** (`com.ryan.portailsafari`) : une installation facultative avec le logo de la messagerie, ouverte directement sur la boîte de réception après connexion. Elle ne demande pas les permissions GPS et ne lance pas le suivi des véhicules.

Les variantes et le site utilisent les mêmes comptes, API et messages. Installer Portail Safari n'est jamais nécessaire pour accéder à la messagerie depuis RyanERP.

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

Capacitor utilise le site HTTPS publié sur le serveur RyanERP.
Ryan ERP conserve son parcours de connexion au démarrage/retour.
Portail Safari réutilise sa session valide (expiration serveur inchangée, six heures)
et ouvre directement la messagerie ; une session expirée demande une nouvelle connexion.
Une notification conserve la conversation cible pendant cette connexion.
Le fond de lancement et celui de la WebView utilisent le bleu nuit RyanERP.

Ryan ERP demande d'abord l'autorisation Android d'afficher les notifications, puis
vérifie `mobile-version.json`. Elle effectue aussi une nouvelle vérification à chaque
retour dans l'application et toutes les six heures.
Lorsqu'une version plus récente est disponible, Android affiche une notification
« Mise à jour RyanERP disponible ». L'action « Mettre à jour » télécharge l'APK depuis
le serveur, vérifie son empreinte SHA-256 et son identifiant d'application, puis ouvre
l'installateur Android.

Dans Android Studio:

1. `Build` -> `Build Bundle(s) / APK(s)` -> `Build APK(s)` pour un APK test.
2. `Build` -> `Generate Signed Bundle / APK` pour Play Store.

### Firebase Cloud Messaging

1. Dans le même projet Firebase (FCM fonctionne sur le forfait Spark sans facturation),
   enregistrer les deux applications Android avec leurs identifiants ci-dessus.
2. Télécharger chaque **configuration Android** dans :
   - `android/app/src/erp/google-services.json`
   - `android/app/src/safari/google-services.json`
   Ces fichiers locaux sont ignorés par Git. Ils ne contiennent pas la clé privée du serveur.
3. Configurer le serveur avec `FIREBASE_SERVICE_ACCOUNT_JSON` (secret d'environnement)
   ou `FIREBASE_SERVICE_ACCOUNT_FILE` (fichier privé hors du site).
   Le compte de service doit pouvoir envoyer des messages FCM dans le même projet.
   Ne jamais copier cette clé dans les ressources Android.
4. Depuis `mobile`, exécuter `npm install`, puis `npx cap sync android`.
5. Depuis `mobile/android`, exécuter :

   ```powershell
   .\gradlew.bat :app:assembleSafariRelease :app:assembleErpRelease
   ```

Les APK sont dans `android/app/build/outputs/apk/safari/release` et
`android/app/build/outputs/apk/erp/release`. La signature utilise le fichier local
`android/keystore.properties`, jamais versionné. Sans celui-ci, la sortie release
est non signée et ne doit pas être distribuée telle quelle.

Dans la messagerie, toucher **Activer les alertes courriel**, puis **Tester sur cet appareil**.
Le canal Android **Messages internes** utilise le son système et la vibration ; ces choix
restent contrôlés par l'utilisateur (mode silencieux, Ne pas déranger, économie de batterie,
visibilité du contenu sur l'écran verrouillé).
Le message affiche le nom de l'employé, l'objet et l'aperçu. Au premier plan, une notification
locale relaie le même message ; en arrière-plan, Android affiche la notification FCM.
Un toucher ouvre la conversation, y compris après reconnexion.
La déconnexion retire l'inscription et les notifications affichées.
Les anciens APK doivent être mis à jour pour obtenir le plugin natif.
Un téléphone avec Google Play Services et Internet est nécessaire au test de réception.

### Téléchargement direct

- [Portail Safari](https://ryanerp-hn5zd.ondigitalocean.app/portail-safari.apk) :
  messagerie seule, facultative.
- [Ryan ERP](https://ryanerp-hn5zd.ondigitalocean.app/app-release.apk) :
  application complète avec les notifications natives.

Lors d'une publication, copier les APK release signées dans `public/portail-safari.apk`
et `public/app-release.apk`. Conserver `public/downloads/app-release.apk` identique
à cette dernière pour les liens existants. Mettre à jour `public/mobile-version.json`
avec la version ERP et le SHA-256 exact de son APK pour le mécanisme de mise à jour existant.
Publier simultanément les scripts web et le serveur compatibles ; le téléchargement
de l'APK ne configure pas Firebase à la place du serveur.

## Publication Play Store (resume)

1. Creer un compte Google Play Console.
2. Generer un AAB signe.
3. Completer fiche store (nom, captures, politique de confidentialite).
4. Publier en test interne, puis production.
