# 📱 Guide d'Installation - Ryan ERP Mobile App

## Distribution sans Play Store

L'application Ryan ERP est distribuée directement sous forme d'APK (Android Package) signé. Cela permet:
- ✅ Installation instantanée (pas d'attente Google Play)
- ✅ Contrôle total des versions
- ✅ Installation privée (pas visible publiquement)
- ✅ Mises à jour directes

---

## 📥 Installation sur le Téléphone Android

### Étape 1: Télécharger l'APK
1. Recevoir le fichier `app-release.apk`
2. Le télécharger sur le téléphone Android

### Étape 2: Autoriser les sources inconnues
1. Ouvrir **Paramètres** → **Sécurité**
2. Activer **"Sources inconnues"** (ou "Apps provenant de sources inconnues")
3. Confirmer l'avertissement

### Étape 3: Installer l'APK
1. Ouvrir le gestionnaire de fichiers
2. Naviguer vers le fichier `app-release.apk`
3. Appuyer dessus
4. Taper **"Installer"**
5. Attendre la fin de l'installation

### Étape 4: Lancer l'app
1. L'app **"Ryan ERP"** apparaît dans le menu
2. Appuyer pour lancer
3. L'app se connecte automatiquement à: `https://ryanerp-hn5zd.ondigitalocean.app`
4. Se connecter avec les identifiants habituel

---

## 🔄 Mises à Jour

### Option 1: Email Reçus
Recevoir l'APK mise à jour par email → Même installation que ci-dessus (remplace la version précédente)

### Option 2: Lien Cloud
Télécharger depuis un lien (OneDrive, Google Drive, etc.)

### Option 3: Serveur Web
Télécharger depuis: `https://ryanerp-hn5zd.ondigitalocean.app/downloads/app-release.apk`

---

## ✅ Vérification

Après installation, vous pouvez vérifier:
- L'app s'appelle **"Ryan ERP"**
- L'app affiche deux boutons ("Profil 1" et "Profil 2")
- Les deux boutons ouvrent la même app web: `https://ryanerp-hn5zd.ondigitalocean.app`

---

## 📋 Caractéristiques Techniques

| Propriété | Valeur |
|-----------|--------|
| **Package ID** | `com.ryan.erp` |
| **Version** | 1.0 |
| **Min SDK** | Android 7.0+ (API 24) |
| **Target SDK** | Android 15+ (API 36) |
| **Signature** | Certificat release signé |
| **Taille** | ~30-50 MB |

---

## 🔐 Sécurité

L'APK est **signé avec un certificat de release**:
- Certificat: `ryan-erp-release.keystore`
- Validité: 10 000 jours
- Alias: `ryan-erp`

Cela garantit que:
- ✅ L'app n'a pas été modifiée
- ✅ Seul le développeur peut publier des mises à jour
- ✅ Les mises à jour futures sont sûres

---

## ❓ Dépannage

### "Application non installée"
→ Vérifier que les sources inconnues sont activées

### "Application bloquée"
→ Vérifier la sécurité du téléphone (antivirus)
→ Autoriser l'app à accéder à Internet dans les paramètres

### "Impossible de se connecter"
→ Vérifier la connexion Internet
→ Vérifier que le serveur `ryanerp-hn5zd.ondigitalocean.app` est accessible

### Application plante
→ Redémarrer le téléphone
→ Télécharger la dernière version de l'APK

---

## 📧 Support

Pour une nouvelle version ou un dépannage:
- Contactez le support technique
- Fournissez les détails du problème (version Android, modèle téléphone, etc.)

---

**Créé**: 29 Avril 2026
**App**: Ryan ERP Mobile
**Distribution**: Entreprise uniquement
