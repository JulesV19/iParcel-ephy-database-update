import { run } from './update.mjs'

const INTERVAL = 12 * 60 * 60 * 1000

async function tick() {
  try { await run() } catch (err) { console.error(err.message) }
}

tick()
setInterval(tick, INTERVAL)
