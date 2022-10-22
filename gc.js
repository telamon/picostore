const D = require('debug')('pico:gc')
// const { pack, unpack } = require('msgpackr')
const REG_TIMER = 'GCt' // magic // can be replaced with sublvl

class GarbageCollector { // What's left is the scheduler
  intervalId = null
  constructor (repo, packr) {
    this.packr = packr // || { pack, unpack }
    this.repo = repo
    // this.db = repo._db
    this.db = repo._db.sublevel(REG_TIMER, {
      keyEncoding: 'buffer',
      valueEncoding: 'buffer'
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
  schedule (slice, tags, block, payload, date = Date.now()) {
    if (!payload) throw new Error('PayloadExpected')
    const sig = block.sig
    const memo = this.packr.pack({
      slice,
      sig,
      payload,
      tags,
      date
    })
    const key = mkKey(date)
    D('mark', new Date(date), 'key:', key.toString('hex'), 'payload:', payload)
    this.db.put(key, memo)
      .catch(error => console.error('Failed queing GC: ', error))
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
      result.push(this.packr.unpack(value))
      batch.push({ type: 'del', key })
    }
    await iter.close()
    await this.db.batch(batch)
    return result
  }

  async collectGarbage (now = Date.now(), picoStore) {
    D('Starting collecting garbage...')
    const pending = await this.tickQuery(now)

    D('Fetched pending from store:', pending.length)
    let mutated = new Set()
    const dropOps = []
    for (const p of pending) {
      const {
        slice,
        sig,
        tags,
        payload,
        date: sweepAt
      } = p
      const store = picoStore._stores.find(s => s.name === slice)
      const block = await this.repo.readBlock(sig)
      const parentBlock = await (block && this.repo.readBlock(block.parentSig))
      if (typeof store.sweep !== 'function') throw new Error('Slice does not implement Sweep()')
      const val = await store.sweep({
        block,
        parentBlock,
        now,
        sig,
        sweepAt,
        ...tags,
        payload,
        rootState: picoStore.state,
        state: store.value,
        mark: (p, time) => this.schedule(slice, tags, block, p || payload, time),
        didMutate: n => mutated.add(n), // Not sure about this.
        drop: stopBlock => dropOps.push([tags.CHAIN, stopBlock])
      })

      if (typeof val !== 'undefined') {
        await picoStore._commitHead(store, sig, val)
        mutated.add(slice)
      }
    }
    const evicted = []
    for (const [ptr, stop] of dropOps) {
      try {
        evicted.push(await this.repo.rollback(ptr, stop))
      } catch (err) {
        if (err.message === 'FeedNotFound') {
          D('RollbackFailed, Feed already gone', err)
        } else console.error('Rollback failed', err)
      }
    }
    // notify all affected stores
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
  // SpaceEfficient + FastLookups not supported by all Buffer shims
  // b.writeBigUInt64BE(BigInt(date), 0) // Writes 8-bytes

  // Manually writeBigUInt64BE
  const b = Buffer.alloc(9)
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

module.exports = GarbageCollector
