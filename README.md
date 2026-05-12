# update-ephy

Synchronisation automatique des bases e-phy ANSES vers Supabase.

Vérifie toutes les 12h si data.gouv.fr a publié une nouvelle version des données.
Si oui : télécharge le ZIP, compare ligne par ligne, applique uniquement les changements.

## Tables mises à jour

| Table Supabase | Source CSV | Clé |
|---|---|---|
| `produits_phyto` | `produits_utf8.csv` | `amm` |
| `usages_phyto` | `produits_usages_utf8.csv` | `(amm, culture, type_traitement, nuisible)` |
| `produits_mfsc` | `mfsc_et_mixte_composition_utf8.csv` | `amm` |
| `usages_mfsc` | `mfsc_et_mixte_usage_utf8.csv` | `(amm, type_culture)` |

Les données e-phy sont publiées chaque semaine (nuit mardi → mercredi).

## Installation

```bash
npm install
```

Crée un fichier `.env` :

```
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJ...
```

## Utilisation

**Vérification manuelle (one-shot) :**
```bash
node update.mjs
```

**Daemon 12h :**
```bash
node index.mjs
```

**Reconstruction complète depuis les CSV :**
```bash
node update.mjs --rebuild
```

> Le rebuild vide `usages_phyto`, `produits_mfsc`, `usages_mfsc` puis les réimporte entièrement.
> `produits_phyto` ne peut pas être vidée (contrainte FK avec `interventions`) — elle est mise à jour ligne par ligne.
>
> Pour une réinitialisation complète (base vide), exécute d'abord dans le SQL Editor Supabase :
> ```sql
> TRUNCATE interventions, produits_phyto, usages_phyto, produits_mfsc, usages_mfsc RESTART IDENTITY;
> ```
> Puis lance `node update.mjs --rebuild`.

## Fichiers

| Fichier | Rôle |
|---|---|
| `update.mjs` | Toute la logique (téléchargement, diff, sync) |
| `index.mjs` | Scheduler 12h |
| `state.json` | URL du dernier ZIP traité (auto-généré) |
| `.env` | Credentials Supabase (ne pas commiter) |
