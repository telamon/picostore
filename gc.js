import debug from 'debug'
import { toHex } from 'picofeed'
import { encode, decode } from 'cborg'
const D = debug('pico:gc')
const REG_TIMER = 'TIMR'

export default class GarbageCollector { // What's left is the scheduler
  intervalId = null
  constructor (repo) {
    this.repo = repo
    this.db = repo._db.sublevel(REG_TIMER, {
      keyEncoding: 'view',
      valueEncoding: 'view'
    })
  }

  /**
   * setTimeout over LevelDB
   * Schedules an event to be triggered on next sweep.
   * Used by 'mark()'-function to mark objects that should be removed.
   *
   * @param payload {Object} A state reference to check.
   * @param date {Number} When to run the check next time default to next run.
   */
  async schedule (root, blockRoot, id, date = Date.now()) {
    await this.setTimeout('gc', { root, blockRoot, id, date }, date, true)
  }

  /**
   * @param {string} type Accepted strings 'gc|pp|gcmp'
   * @param {any} memo Stored context that will be returned on trigger
   * @param {number} millis Milliseconds
   */
  async setTimeout (type, memo = {}, millis = 0, isTimestamp = false) {
    D('setTimeout', type, memo, millis)
    const key = mkKey(isTimestamp ? millis : Date.now() + millis)
    await this.db.put(key, encode({ type, memo }))
  }

  async tickQuery (now) {
    const query = {
      gt: new Uint8Array(10), // All zeroes
      lt: mkKey(now, 0xff)
    }
    D('sweep range:', toHex(query.gt), '...', toHex(query.lt))
    const iter = this.db.iterator(query)
    const result = []
    const batch = []
    while (true) {
      const res = await iter.next()
      if (!res) break
      const [key, value] = res
      result.push(decode(value))
      batch.push({ type: 'del', key })
    }
    await iter.close()
    await this.db.batch(batch)
    return result
  }

  async collectGarbage (now = Date.now(), picoStore) {
    D('Starting collecting garbage...')
    now = Math.floor(now) // Floor floats
    const pending = await this.tickQuery(now)
    D('Fetched pending from store:', pending.length)
    const evicted = []
    const mutated = new Set()
    for (const { type, memo } of pending) {
      const { root } = memo
      if (type !== 'gc') {
        console.info('Warn unhandled event', type, memo)
        continue
      }

      const collection = picoStore.roots[root]
      if (!collection) throw new Error(`Unknown Root ${root}`)
      const droppedFeed = await collection._gc_visit(now, memo)
      if (droppedFeed) {
        evicted.push(droppedFeed)
        mutated.add(root)
      }
    }
    const arrMutated = Array.from(mutated)
    D('Stores mutated', arrMutated, 'segments evicted', evicted.length)
    return { mutated: arrMutated, evicted }
  }
}

let _ctr = 0
/**
 * Creates a binary LevelDB key indexes tasks by
 * Timestamp.
 * @param {number} date Timestamp
 * @return {Uint8Array} a 9-byte binary key
 */
function mkKey (date, counter) {
  // Manually writeBigUInt64BE
  const b = new Uint8Array(10)
  for (let i = 0; i < 8; i++) {
    b[i] = Number((BigInt(date) >> BigInt((7 - i) * 8)) & BigInt(255))
  }
  if (!Number.isFinite(counter)) counter = _ctr = ++_ctr % 65536
  b[8] = _ctr & 0xff
  b[9] = (_ctr >> 8) & 0xff
  return b
}
