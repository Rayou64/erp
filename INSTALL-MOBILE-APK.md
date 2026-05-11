# 📱 Guide d'Installation - Ryan ERP Mobile

## Installation de l'APK signé sur Téléphone Android

L'application Ryan ERP est distribuée sous forme d'APK (Android Package) signé et prêt à l'emploi.

---

## 📥 Étape 1 : Récupérer l'APK

**Fichier APK** :
```
app-release.apk (3.02 MB)
Localisation : mobile/android/app/build/outputs/apk/release/
```

### Méthode A : Via OneDrive / Cloud
1. L'APK se trouve dans : `OneDrive\travail personnel\ERP\mobile\android\app\build\outputs\apk\release\`
2. Partagez le lien OneDrive avec le téléphone ou téléchargez-le directement

### Méthode B : Via Email / Mensagerie
- Envoyez l'APK par email depuis votre PC
- Ouvrez le lien de téléchargement sur le téléphone

### Méthode C : Via USB
1. Connectez le téléphone au PC par câble USB
2. Copiez `app-release.apk` dans le dossier **Téléchargements** du téléphone
3. Ouvrez le gestionnaire de fichiers → Téléchargements

---

## 🔓 Étape 2 : Autoriser les Sources Inconnues

**Android 12 et versions récentes** :
1. Allez dans **Paramètres**
2. **Applications** → **Applications spéciales** (ou **Paramètres avancés**)
3. **Installer des applications inconnues** (ou **Accès aux fichiers unknown**)
4. Cherchez votre **Gestionnaire de fichiers** ou **Google Chrome**
5. Activez le commutateur ✅

**Android 11 et antérieures** :
1. **Paramètres** → **Sécurité**
2. Activez **Sources inconnues** ✅

---

## 💾 Étape 3 : Installer l'APK

### Depuis le Gestionnaire de Fichiers :
1. Ouvrez **Gestionnaire de fichiers** (ou **Fichiers**)
2. Naviguez vers **Téléchargements**
3. Trouvez **app-release.apk**
4. Appuyez dessus → **Installer**
5. Attendez quelques secondes
6. ✅ **Installation terminée !**

### Depuis Gmail / Email :
1. Ouvrez l'email avec la pièce jointe APK
2. Appuyez sur l'APK
3. Choisissez **Installer**
4. Confirmez les permissions
5. ✅ **Installation terminée !**

---

## 🚀 Étape 4 : Lancer l'Application

### Depuis l'écran d'accueil :
1. Cherchez l'icône 🏗️ **Ryan ERP** 
2. Appuyez pour ouvrir

### Ou depuis les Applications :
1. **Paramètres** → **Applications** (ou **App Drawer**)
2. Trouvez **Ryan ERP**
3. Appuyez pour lancer

---

## 👥 Étape 5 : Choisir un Profil

Au démarrage, vous verrez deux options :

```
┌─────────────────────────────────┐
│    Ryan ERP Mobile              │
├─────────────────────────────────┤
│                                 │
│  📋 Profil 1 - Gestionnaire stock zone
│     Operations zone, stock,     │
│     autorisations               │
│                                 │
│  🔧 Profil 2 - Chef chantier site
│     Operations site et suivi    │
│     chantier                    │
│                                 │
│  ┌─────────────────────────────┐
│  │ Continuer avec le dernier   │
│  │ profil                      │
│  └─────────────────────────────┘
│  ┌─────────────────────────────┐
│  │ Ouvrir sans profil          │
│  └─────────────────────────────┘
└─────────────────────────────────┘
```

### Options :
- **Profil 1 ou 2** : Sélectionne un profil par défaut (raccourci)
- **Continuer avec le dernier** : Réutilise votre dernier choix
- **Ouvrir sans profil** : Accès à l'ERP classique

---

## 🔐 Étape 6 : Se Connecter

Après avoir choisi un profil, vous arrivez à l'écran de connexion ERP :

```
Nom d'utilisateur : [          ]
Mot de passe :     [          ]
                   [ SE CONNECTER ]
```

**Utilisez vos identifiants normaux** :
- Exemple : `admin` / `admin123`
- Ou tout autre compte ERP valide

---

## ✅ Vérification - Vous êtes connecté !

Une fois logué, vous voyez :
- ✅ Tableau de bord avec les cartes de statistiques
- ✅ Menu latéral (à gauche, swipez pour afficher sur mobile)
- ✅ Topbar bleue avec le titre et vos infos utilisateur

---

## 🔄 Mises à Jour Futures

Quand une nouvelle version de l'APK est disponible :

1. Téléchargez le nouvel `app-release.apk`
2. Installez le nouveau fichier
3. Android proposera de **remplacer l'ancienne version**
4. ✅ **C'est fini** - l'ancienne version sera automatiquement supprimée

---

## ⚠️ Dépannage

### L'APK ne s'installe pas
- ❌ **Erreur "sources inconnues"** : Vérifiez l'Étape 2
- ❌ **Erreur "fichier corrompu"** : Retéléchargez l'APK
- ❌ **Espace insuffisant** : Libérez au moins 50 MB

### L'app plante au démarrage
- Vérifiez votre connexion Internet (4G/WiFi)
- L'app a besoin d'accès à : `https://ryanerp-hn5zd.ondigitalocean.app`
- Tentez une reconnexion WiFi

### La connexion échoue
- Vérifiez que le serveur DigitalOcean est actif
- Vérifiez vos identifiants ERP
- Essayez en WiFi plutôt qu'en données mobiles

### Comment déconnecter
1. Menu latéral → **Déconnexion** (ou équivalent selon votre rôle)
2. L'app retourne à l'écran de sélection de profil

---

## 📞 Support

Pour plus d'aide, contactez votre administrateur ERP.

**Version APK** : 1.0  
**Date** : 4 Mai 2026  
**Backend** : https://ryanerp-hn5zd.ondigitalocean.app
