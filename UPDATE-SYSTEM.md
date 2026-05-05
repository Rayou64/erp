# 🔄 Système de Mise à Jour - Ryan ERP Mobile

## 📋 Vue d'ensemble

Le système de mise à jour est **automatisé et en trois couches** :

```
Code source (app.js, mobile/www/index.html)
    ↓ Git push
GitHub (Rayou64/erp)
    ↓ Auto-deploy webhook
DigitalOcean (https://ryanerp-hn5zd.ondigitalocean.app)
    ↓ Build new APK
APK disponible au téléchargement
```

---

## 🔐 3 Profils d'Authentification

L'application mobile **exige obligatoirement une connexion** avant d'accéder à l'application.

### Profils Disponibles :

| Profil | Username | Password | Rôle | Accès |
|--------|----------|----------|------|-------|
| **Admin** | `admin` | `admin123` | `admin` | ✅ Accès complet |
| **Achat** | `achat_user` | `achat@123` | `achat` | 📋 Achats & Commandes |
| **Chef Site** | `chef_chantier_site15` | `chefsite15@123` | `chef_chantier_site` | 🏗️ Chantier Site 15 |
| **Stock** | `commis_stock` | `stock123` | `commis` | 📦 Gestion stock zone |

---

## 🔄 Processus de Mise à Jour

### **Étape 1 : Modification du code source**
Effectuez vos modifications dans :
- `app.js` - Backend/API
- `mobile/www/index.html` - Interface mobile
- `public/erp.html` - Frontend principal

### **Étape 2 : Commit et Push**
```bash
cd "c:\Users\koudo\OneDrive\travail personnel\ERP"
git add .
git commit -m "Description de vos changements"
git push origin HEAD
```

### **Étape 3 : Redéploiement automatique**
- GitHub notifie DigitalOcean via webhook
- DigitalOcean lance une nouvelle **build** automatiquement
- L'application redémarre avec les nouvelles modifications
- **Temps**: ~3-5 minutes

### **Étape 4 : APK mise à jour** (optionnel)
Si vous modifiez `mobile/www/index.html`, l'APK doit être rebuildé :

```bash
# 1. Compiler une nouvelle version signée
$env:JAVA_HOME = "C:\Users\koudo\tools\jdk17"
$env:ANDROID_HOME = "C:\Users\koudo\tools\android-sdk"
Set-Location "c:\Users\koudo\OneDrive\travail personnel\ERP\mobile\android"
.\gradlew.bat assembleRelease --no-daemon

# 2. Copier dans public/
Copy-Item -Path "app\build\outputs\apk\release\app-release.apk" `
  -Destination "..\..\..\public\app-release.apk" -Force

# 3. Commit et Push
cd ..\..\..\..
git add public\app-release.apk
git commit -m "apk: Updated build"
git push origin HEAD
```

- Temps de build: ~5-10 minutes
- Disponible à: `https://ryanerp-hn5zd.ondigitalocean.app/app-release.apk`

---

## 📱 Installation Mobile

### Pour les utilisateurs finaux :

1. **Télécharger l'APK** :
   - Lien: https://ryanerp-hn5zd.ondigitalocean.app/app-release.apk
   - Taille: ~3 MB

2. **Installer sur le téléphone** :
   - Copier le fichier APK sur la mémoire interne
   - Ouvrir un gestionnaire de fichiers
   - Appuyer sur le fichier APK
   - Autoriser l'installation (Paramètres → Applications)

3. **Première utilisation** :
   - Sélectionner le profil souhaité
   - Remplir username + password
   - ✅ Connecté !

### Configuration d'entreprise :
- **Distribution USB** : Copier RyanERP.apk sur clés USB pour distribution en masse
- **Distribution WhatsApp/Teams** : Partager le lien direct
- **Management de flotte** : Utiliser Intune pour déploiement automatisé

---

## 🛠️ Architecture d'Authentification

### Flux de connexion :

```
Mobile App (index.html)
    ↓
Affiche: Écran de LOGIN
    ↓ (Utilisateur saisit username + password)
POST /api/auth/login
    ↓
Serveur vérifie credentials (bcrypt)
    ↓
JWT généré (valide 6 heures)
    ↓ localStorage.setItem('ryanerp_mobile_auth')
Affiche: Sélection de profil (4 options)
    ↓ (Utilisateur clique sur profil)
localStorage.setItem('ryanerp_mobile_profile')
    ↓
Redirige vers: /erp.html?mobileProfile=profil
```

### Sécurité :

✅ **JWT expiration** : 6 heures
✅ **Passwords hashés** : bcrypt (10 rounds)
✅ **HTTPS obligatoire** : Sur DigitalOcean
✅ **CORS configuré** : Cross-origin sécurisé
✅ **Rate limiting** : 25 tentatives par 15 minutes

---

## 📊 Monitoring des Mises à Jour

### Vérifier le déploiement en cours :

```bash
# Liste des déploiements
doctl apps list-deployments f10d7453-1577-4e69-b1b0-43bea7966c29 --no-header | head -1

# Voir les logs de build
doctl apps logs f10d7453-1577-4e69-b1b0-43bea7966c29 erp-web --type build --tail 50
```

### Tests locaux :

```bash
# Démarrer le serveur local
node app.js

# Tester la connexion
curl -X POST http://localhost:4000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}'

# Tester APK local
curl -I http://localhost:4000/app-release.apk
```

---

## 🎯 Résumé des Fonctionnalités

### Phase 1 ✅ Terminée
- ✅ APK signé et distribuable
- ✅ Interface mobile responsive (≤480px)
- ✅ Design moderne avec thème indigo

### Phase 2 ✅ Aujourd'hui
- ✅ Système de LOGIN obligatoire
- ✅ 4 profils disponibles (Admin, Achat, Zone, Chantier)
- ✅ Interface créative (cartes colorées, animations)
- ✅ Authentification JWT sécurisée

### Phase 3 📋 Prochaines étapes (optionnel)
- Intégration biométrique (fingerprint)
- Push notifications
- Téléchargement de données hors-ligne
- Cache localStorage avancé

---

## 📞 Support & Troubleshooting

### La connexion refuse
- **Vérifier** : Username/password corrects (case-sensitive)
- **Vérifier** : Connexion réseau active
- **Vérifier** : Serveur DigitalOcean en ligne (`curl https://ryanerp-hn5zd.ondigitalocean.app`)

### L'APK ne télécharge pas
- **Vérifier** : Espace libre sur le téléphone (min 50 MB)
- **Vérifier** : Téléchargement HTTPS autorisé
- **Fallback** : Télécharger via `downloads/RyanERP.apk` local

### Le profil ne se bascule pas
- **Vérifier** : localStorage autorisé dans le navigateur
- **Vérifier** : JavaScript activé
- **Fallback** : Actualiser la page (F5)

---

**Mis à jour le** : 05 Mai 2026
**Version** : 2.0 avec authentification
**Statut** : ✅ Production prête
