import debug from 'debug'
import { toHex } from 'picofeed'
import { encode, decode } from 'cborg'
const D = debug('pico:gc')
const REG_TIMER = 'GCt'

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
   * setTimemout over LevelDB
   * Schedules an event to be triggered on next sweep.
   * Used by 'mark()'-function to mark objects that should be removed.
   *
   * @param payload {Object} A state reference to check.
   * @param date {Number} When to run the check next time default to next run.
   */
  async schedule (root, blockRoot, id, date = Date.now()) {
    const memo = encode({ root, blockRoot, id, date })
    const key = mkKey(date)
    D('schedule:', toHex(key), root, id, date)
    await this.db.put(key, memo)
  }

  async tickQuery (now) {
    const query = {
      gt: Buffer.alloc(9), // All zeroes
      lt: mkKey(now, 0xff)
    }
    D('sweep range:', query.gt.toString('hex'), '...', query.lt.toString('hex'))
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
    let mutated = new Set()
    for (const memo of pending) {
      const { root } = memo
      const collection = picoStore.roots[root]
      if (!collection) throw new Error(`Unknown Root ${root}`)
      const droppedFeed = await collection._gc_visit(now, memo)
      if (droppedFeed) {
        evicted.push(droppedFeed)
        mutated.add(root)
      }
    }
    mutated = Array.from(mutated)
    D('Stores mutated', mutated, 'segments evicted', evicted.length)
    return { mutated, evicted }
  }
}

let _ctr = 0
let _lastDate = 0
/**
 * Creates a binary LevelDB key indexes tasks by
 * Timestamp.
 * @param {number} date Timestamp
 * @return {Buffer<9>} a 9-byte binary key
 */
function mkKey (date, counter) {
  // Manually writeBigUInt64BE
  const b = new Uint8Array(9)
  for (let i = 0; i < 8; i++) {
    b[i] = Number((BigInt(date) >> BigInt((7 - i) * 8)) & BigInt(255))
  }
  if (counter ?? false) b[8] = counter
  else {
    // Automatic counter '_ctr' prevents task-overwrites
    // when mkKey invoked within same millisecond.
    if (date === _lastDate) _ctr++
    else _ctr = 0
    _lastDate = date
    if (_ctr > 255) console.warn('Warning: >255 GC-tasks enqueued, systemfault?')
    b[8] = _ctr % 256
  }
  return b
}
