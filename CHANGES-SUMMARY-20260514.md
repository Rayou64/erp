# Résumé des changements - 14 Mai 2026

## Sauvegarde de travail
- **Fichier**: `public/erp.html.backup.20260514-002259.working`
- **Intégrité**: SHA256 ADBF81E7094F417E7E51A3087DB81A4E6B01F4DFB4A11866A276C670AB02AA39
- **Date**: 14 Mai 2026 00:22:59

## Corrections apportées

### 1. Sauvegarde séquentielle du catalogue matériaux (FIX CRITIQUE)
**Fichier modifié**: `public/erp.html` (lignes 14739-14755)

#### Problème
Lors de l'enregistrement du déboursé sec avec 24 matériaux, le système envoyait tous les requêtes DELETE et POST en parallèle (`Promise.all`), ce qui causait des collisions d'ID et des erreurs HTTP 500.

#### Solution
Conversion du mode parallèle au mode séquentiel pour toutes les opérations d'enregistrement batch:

```javascript
// AVANT (parallèle - causait 500 errors):
await Promise.all(existingStageEntries.map(entry => fetchJson(...)));
await Promise.all(entries.map(entry => fetchJson(...)));

// APRÈS (séquentiel - stable):
for (const entry of existingStageEntries) {
  await fetchJson(...);
}
for (const entry of entries) {
  await fetchJson(...);
}
```

**Impact**: 
- ✅ Sauvegarde fiable sans erreur 500 pour les gros volumes (24+ matériaux)
- ✅ IDs auto-générés sans collision
- ✅ Transactions isolées et ordonnées

### 2. Intégrité du catalogue matériaux

Le changement de sauvegarde séquentielle s'applique à l'unique endroit de batch operations pour le catalogue matériaux:
- `#mc-save-list` click handler (enregistrement de déboursé sec)
- Gère: suppression des anciennes entrées + création des nouvelles

Les autres opérations du catalogue (recherche, filtrage, chargement) n'utilisaient pas de batch parallèles et n'ont pas besoin de modification.

## Validation fonctionnelle

### Données testées
Étape principale T1 "Montage élévation 10 rangs RDC - DUPLEX":
- 5 sous-étapes regroupées
- 24 matériaux distribués
- Total: 1 627 388 FCFA

### Vérifications après correction
- ✅ Éditeur: 5 sous-étapes, 24 matériaux affichés correctement
- ✅ Badges: counts précis
- ✅ Totaux: 231 655 + 230 155 + 336 000 + 603 500 + 226 078 = 1 627 388 FCFA
- ✅ Sauvegarde: 0 erreur 500, tous les enregistrements persistés
- ✅ Liste enregistrée: format sans colonne ACTIONS (lecture seule)
- ✅ Toutes les 5 sous-étapes visibles dans "Matériaux enregistrés"

## Fichiers modifiés dans cette session

| Fichier | Ligne(s) | Changement | Justification |
|---------|----------|-----------|---------------|
| `public/erp.html` | 14739-14755 | Promise.all → boucles for séquentiques | Fix 500 errors en batch save |

## Points de cohérence appliqués
1. Une seule approche de batch operations dans le catalogue (séquentiel)
2. Même logique appliquée partout où du batch save/delete de matériaux se fait
3. Confirmé: pas d'autre instance de Promise.all pour matériaux-catalog nécessitant ajustement

## Fichiers de secours
- `public/erp.html.backup.20260514-002259.working` - Version courante avec tous les fixes
- État prêt pour: sauvegarde supplémentaire sur cloud ou export de configuration
