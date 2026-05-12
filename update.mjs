import fs, { createWriteStream } from 'fs'
import readline from 'readline'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import { mkdir, rm } from 'fs/promises'
import { execSync } from 'child_process'
import { createClient } from '@supabase/supabase-js'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

// ── Config ────────────────────────────────────────────────────────────────────

const ROOT  = dirname(fileURLToPath(import.meta.url))
const STATE = join(ROOT, 'state.json')
const DATASET_API =
  'https://www.data.gouv.fr/api/1/datasets/' +
  'donnees-ouvertes-du-catalogue-e-phy-des-produits-phytopharmaceutiques-' +
  'matieres-fertilisantes-et-supports-de-culture-adjuvants-produits-mixtes-et-melanges/'

function loadEnv() {
  const envPath = join(ROOT, '.env')
  if (!fs.existsSync(envPath)) return
  for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
    const m = line.match(/^([^#=\s]+)\s*=\s*(.+)/)
    if (m) process.env[m[1]] = m[2].trim()
  }
}
loadEnv()
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Manque SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY dans .env')
  process.exit(1)
}

const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY)

// ── Utilitaires ───────────────────────────────────────────────────────────────

const log = (msg) => console.log(`${new Date().toISOString()} ${msg}`)

function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE, 'utf8')) } catch { return {} }
}
function saveState(s) { fs.writeFileSync(STATE, JSON.stringify(s, null, 2)) }

function parseCSV(line) {
  const out = []
  let cur = '', inQuote = false
  for (const ch of line) {
    if (ch === '"') { inQuote = !inQuote; continue }
    if (ch === ';' && !inQuote) { out.push(cur.trim()); cur = '' } else cur += ch
  }
  out.push(cur.trim())
  return out
}

async function readCSV(csvPath, fn) {
  const rl = readline.createInterface({ input: fs.createReadStream(csvPath, { encoding: 'utf8' }) })
  let header = true
  for await (const line of rl) {
    if (header) { header = false; continue }
    if (line.trim()) fn(parseCSV(line))
  }
}

async function loadTable(table, cols) {
  const rows = []
  let from = 0
  for (;;) {
    const { data, error } = await db.from(table).select(cols).range(from, from + 999)
    if (error) throw new Error(`loadTable(${table}): ${error.message}`)
    rows.push(...data)
    if (data.length < 1000) break
    from += 1000
  }
  return rows
}

async function batchInsert(table, rows) {
  for (let i = 0; i < rows.length; i += 500) {
    const { error } = await db.from(table).insert(rows.slice(i, i + 500))
    if (error) throw new Error(`insert ${table}: ${error.message}`)
  }
}

async function batchUpdate(table, updates, keyCol) {
  for (let i = 0; i < updates.length; i += 20) {
    await Promise.all(
      updates.slice(i, i + 20).map(async ({ key, record }) => {
        const { error } = await db.from(table).update(record).eq(keyCol, key)
        if (error) throw new Error(`update ${table}: ${error.message}`)
      })
    )
  }
}

// Empreinte d'une ligne pour détecter les changements (exclut la/les clés)
function sig(obj, excludeKeys) {
  return Object.entries(obj)
    .filter(([k]) => !excludeKeys.includes(k))
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([, v]) => String(v ?? ''))
    .join('|')
}

const toDate  = (v) => { const m = v?.match(/^(\d{2})\/(\d{2})\/(\d{4})$/); return m ? `${m[3]}-${m[2]}-${m[1]}` : null }
const toInt   = (v) => { const n = parseInt(v, 10); return isNaN(n) ? null : n }
const toFloat = (v) => { const n = parseFloat(String(v).replace(',', '.')); return isNaN(n) ? null : n }
const str     = (v) => v || null

// ── Téléchargement ────────────────────────────────────────────────────────────

async function getLatestZipUrl() {
  const res = await fetch(DATASET_API, { headers: { 'X-fields': 'resources{url}' } })
  if (!res.ok) throw new Error(`data.gouv.fr API: ${res.status}`)
  const { resources } = await res.json()
  const resource = resources.find(({ url }) => {
    const u = url.toLowerCase()
    return u.includes('csv') && u.includes('utf8') && u.endsWith('.zip')
  })
  if (!resource) throw new Error('Ressource CSV UTF-8 introuvable dans le dataset e-phy')
  return resource.url
}

function findCSV(dir, test) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = join(dir, entry.name)
    if (entry.isDirectory()) {
      const found = findCSV(fullPath, test)
      if (found) return found
    } else if (entry.name.toLowerCase().endsWith('.csv') && test(entry.name.toLowerCase())) {
      return fullPath
    }
  }
}

async function downloadAndExtract(url) {
  const tmp = join(ROOT, `tmp-${Date.now()}`)
  await mkdir(tmp)
  try {
    const zip = join(tmp, 'ephy.zip')
    const res = await fetch(url)
    if (!res.ok) throw new Error(`Téléchargement échoué: ${res.status}`)
    await pipeline(Readable.fromWeb(res.body), createWriteStream(zip))
    execSync(`unzip -q -o ${JSON.stringify(zip)} -d ${JSON.stringify(tmp)}`)
    await rm(zip)
    return {
      tmp,
      produits:  findCSV(tmp, n => /^produits(_utf8)?\.csv$/.test(n)),
      usages:    findCSV(tmp, n => /^produits_usages(_utf8)?\.csv$/.test(n)),
      mfscCompo: findCSV(tmp, n => n.includes('mfsc') && n.includes('compos')),
      mfscUsage: findCSV(tmp, n => n.includes('mfsc') && n.includes('usage')),
    }
  } catch (err) {
    await rm(tmp, { recursive: true, force: true })
    throw err
  }
}

// ── Sync tables ───────────────────────────────────────────────────────────────

// produits_phyto — clé : amm
// CSV : 1:amm  2:nom  4:titulaire  11:fonctions  13:statut  14:date_retrait  15:date_amm
async function syncProduitsPhyto(csvPath) {
  const csv = new Map()
  await readCSV(csvPath, (f) => {
    const [amm, nom] = [f[1], f[2]]
    if (amm && nom) csv.set(amm, {
      amm,
      nom_commercial: nom,
      titulaire:    str(f[4]),
      type_produit: str(f[11]),
      statut:       str(f[13]),
      date_amm:     toDate(f[15]),
      date_retrait: toDate(f[14]),
    })
  })

  const existing = new Map(
    (await loadTable('produits_phyto', 'amm, nom_commercial, titulaire, type_produit, statut, date_amm, date_retrait'))
      .map(r => [r.amm, r])
  )

  const toInsert = [], toUpdate = []
  for (const [amm, rec] of csv) {
    if (!existing.has(amm)) toInsert.push(rec)
    else if (sig(rec, ['amm']) !== sig(existing.get(amm), ['amm'])) toUpdate.push({ key: amm, record: rec })
  }

  await batchInsert('produits_phyto', toInsert)
  await batchUpdate('produits_phyto', toUpdate, 'amm')
  return { inserted: toInsert.length, updated: toUpdate.length }
}

// usages_phyto — clé naturelle : (amm, culture_ephy, type_traitement, nuisible), update par id
// CSV : 0:amm  1:nom_produit  2:usage(culture*type*nuisible)  3:date_decision  ...
async function syncUsagesPhyto(csvPath) {
  const csv = new Map()
  await readCSV(csvPath, (f) => {
    const [amm, usage, etat] = [f[0], f[2], f[6]]
    if (!amm || !usage || !etat) return
    const [rawCulture, rawType, rawNuisible] = usage.split('*')
    const culture  = rawCulture?.trim()  || null
    const typeTrt  = rawType?.trim()     || null
    const nuisible = rawNuisible?.trim() || null
    if (!culture) return
    const key = [amm, culture, typeTrt, nuisible].map(v => v ?? '').join('|')
    csv.set(key, {
      amm, nom_produit: str(f[1]), culture_ephy: culture, type_traitement: typeTrt, nuisible,
      date_decision: toDate(f[3]), stade_bbch_min: str(f[4]), stade_bbch_max: str(f[5]),
      etat_usage: etat, dose_retenue: str(f[7]), dose_unite: str(f[8]),
      dar_jours: toInt(f[9]), dar_bbch: str(f[10]), nb_max_applications: toInt(f[11]),
      date_fin_distribution: toDate(f[12]), date_fin_utilisation: toDate(f[13]),
      condition_emploi: str(f[14]), znt_aquatique_m: toInt(f[15]),
      znt_arthropodes_m: toInt(f[16]), znt_plantes_m: toInt(f[17]),
      mentions: str(f[18]), intervalle_min_jours: toInt(f[19]),
    })
  })

  const dbRows = await loadTable('usages_phyto',
    'id, amm, culture_ephy, type_traitement, nuisible, nom_produit, date_decision, ' +
    'stade_bbch_min, stade_bbch_max, etat_usage, dose_retenue, dose_unite, dar_jours, ' +
    'dar_bbch, nb_max_applications, date_fin_distribution, date_fin_utilisation, ' +
    'condition_emploi, znt_aquatique_m, znt_arthropodes_m, znt_plantes_m, mentions, intervalle_min_jours'
  )
  const existing = new Map(
    dbRows.map(r => [[r.amm, r.culture_ephy, r.type_traitement, r.nuisible].map(v => v ?? '').join('|'), r])
  )

  const KEY_COLS = ['id', 'amm', 'culture_ephy', 'type_traitement', 'nuisible']
  const toInsert = [], toUpdate = []
  for (const [key, rec] of csv) {
    if (!existing.has(key)) toInsert.push(rec)
    else if (sig(rec, KEY_COLS) !== sig(existing.get(key), KEY_COLS)) toUpdate.push({ key: existing.get(key).id, record: rec })
  }

  await batchInsert('usages_phyto', toInsert)
  await batchUpdate('usages_phyto', toUpdate, 'id')
  return { inserted: toInsert.length, updated: toUpdate.length }
}

// produits_mfsc — clé : amm
// CSV : 0:type  1:amm  2:nom  3:composition
async function syncProduitsMfsc(csvPath) {
  const csv = new Map()
  await readCSV(csvPath, (f) => {
    const [amm, nom] = [f[1], f[2]]
    if (amm && nom) csv.set(amm, {
      amm,
      nom_produit:  nom,
      type_produit: str(f[0]),
      composition:  str(f[3]),
    })
  })

  const existing = new Map(
    (await loadTable('produits_mfsc', 'amm, nom_produit, type_produit, composition'))
      .map(r => [r.amm, r])
  )

  const toInsert = [], toUpdate = []
  for (const [amm, rec] of csv) {
    if (!existing.has(amm)) toInsert.push(rec)
    else if (sig(rec, ['amm']) !== sig(existing.get(amm), ['amm'])) toUpdate.push({ key: amm, record: rec })
  }

  await batchInsert('produits_mfsc', toInsert)
  await batchUpdate('produits_mfsc', toUpdate, 'amm')
  return { inserted: toInsert.length, updated: toUpdate.length }
}

// usages_mfsc — clé naturelle : (amm, type_culture), update par id
// CSV : 1:amm  2:nom  3:type_culture  4:dose_min  5:dose_min_unite  6:dose_max  7:dose_max_unite  10:etat  16:culture_commentaire
async function syncUsagesMfsc(csvPath) {
  const csv = new Map()
  await readCSV(csvPath, (f) => {
    const [amm, typeCulture, etat] = [f[1], f[3], f[10]]
    if (!amm || !typeCulture || !etat) return
    csv.set(`${amm}|${typeCulture}`, {
      amm, nom_produit: str(f[2]), type_culture: typeCulture,
      dose_min: toFloat(f[4]), dose_min_unite: str(f[5]),
      dose_max: toFloat(f[6]), dose_max_unite: str(f[7]),
      etat_usage: etat, culture_commentaire: str(f[16]),
    })
  })

  const dbRows = await loadTable('usages_mfsc',
    'id, amm, type_culture, nom_produit, dose_min, dose_min_unite, dose_max, dose_max_unite, etat_usage, culture_commentaire'
  )
  const existing = new Map(dbRows.map(r => [`${r.amm}|${r.type_culture}`, r]))

  const KEY_COLS = ['id', 'amm', 'type_culture']
  const toInsert = [], toUpdate = []
  for (const [key, rec] of csv) {
    if (!existing.has(key)) toInsert.push(rec)
    else if (sig(rec, KEY_COLS) !== sig(existing.get(key), KEY_COLS)) toUpdate.push({ key: existing.get(key).id, record: rec })
  }

  await batchInsert('usages_mfsc', toInsert)
  await batchUpdate('usages_mfsc', toUpdate, 'id')
  return { inserted: toInsert.length, updated: toUpdate.length }
}

// ── Orchestration ─────────────────────────────────────────────────────────────

function buildTableList(csvPaths) {
  return [
    { name: 'produits_phyto', fn: () => syncProduitsPhyto(csvPaths.produits) },
    { name: 'usages_phyto',   fn: () => syncUsagesPhyto(csvPaths.usages) },
    { name: 'produits_mfsc',  fn: () => syncProduitsMfsc(csvPaths.mfscCompo) },
    { name: 'usages_mfsc',    fn: () => syncUsagesMfsc(csvPaths.mfscUsage) },
  ]
}

async function truncate(table) {
  const { error } = await db.from(table).delete().not('id', 'is', null)
  if (error) throw new Error(`truncate ${table}: ${error.message}`)
}

async function runTables(csvPaths) {
  let ok = true
  for (const { name, fn } of buildTableList(csvPaths)) {
    log(`  ${name}...`)
    try {
      const { inserted, updated } = await fn()
      log(`  ${name} ✓  +${inserted} insérés  ~${updated} mis à jour`)
    } catch (err) {
      log(`  ${name} ✗  ${err.message}`)
      ok = false
    }
  }
  return ok
}

// Mise à jour incrémentale : ne fait rien si aucune nouvelle version disponible
export async function run() {
  const state = loadState()
  log('Vérification des mises à jour e-phy...')

  const url = await getLatestZipUrl()
  if (url === state.lastUrl) { log('Déjà à jour.'); return }

  log('Nouvelle version détectée, téléchargement...')
  const { tmp, ...csvPaths } = await downloadAndExtract(url)
  const ok = await runTables(csvPaths)
  await rm(tmp, { recursive: true, force: true })

  if (ok) {
    saveState({ lastUrl: url, lastRun: new Date().toISOString() })
    log('Terminé.')
  } else {
    log('Terminé avec des erreurs — état non sauvegardé, nouvelle tentative au prochain cycle.')
  }
}

// Reconstruction complète : vide les tables sans FK puis réimporte tout
// produits_phyto n'est pas vidée (FK avec interventions) — ses lignes sont mises à jour
export async function rebuild() {
  log('Rebuild — téléchargement des CSV e-phy...')
  const url = await getLatestZipUrl()
  const { tmp, ...csvPaths } = await downloadAndExtract(url)

  log('Suppression des données existantes...')
  for (const name of ['usages_phyto', 'produits_mfsc', 'usages_mfsc']) {
    await truncate(name)
    log(`  ${name} vidée`)
  }

  log('Réimport...')
  const ok = await runTables(csvPaths)
  await rm(tmp, { recursive: true, force: true })

  if (ok) {
    saveState({ lastUrl: url, lastRun: new Date().toISOString() })
    log('Rebuild terminé.')
  } else {
    log('Rebuild terminé avec des erreurs.')
  }
}

// node update.mjs           → mise à jour incrémentale
// node update.mjs --rebuild → reconstruction complète
if (process.argv[1].endsWith('update.mjs')) {
  const fn = process.argv.includes('--rebuild') ? rebuild : run
  fn().catch(err => { log(`ERREUR: ${err.message}`); process.exit(1) })
}
