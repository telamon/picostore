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
   * @param payload {Object} A state reference to check.
   * @param date {Number} When to run the check next time default to next run.
   */
  schedule (slice, tags, block, payload, date = Date.now()) {
    if (!payload) throw new Error('PayloadExpected')
    date = date | 0 // Floor floats
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

  // TODO: move to index.js
  start (interval = 3 * 1000) {
    if (this.intervalId) return
    this.intervalId = setInterval(this._collectGarbage.bind(this), interval)
  }

  // TODO move to index.js
  stop () {
    if (!this.intervalId) return
    clearInterval(this.intervalId)
    this.intervalId = null
  }

  async tickQuery (now) {
    const query = {
      gt: mkKey(0),
      lt: mkKey(now)
    }
    D('sweep', query.lt.toString('hex'))
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
    now = now | 0 // Floor floats
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

let __taskCounter = 0
/**
 * Creates a binary LevelDB key indexes tasks by
 * Timestamp.
 * @param {Number} date Timestamp
 */
function mkKey (date) {
  __taskCounter = ++__taskCounter % 1000
  // 20bytes+ workaround (who cares.. :( )
  const str = `${date}`.padStart(20, 0) + __taskCounter
  return Buffer.from(str.split(''))
  // SpaceEfficient + Lookups but not supported by all Buffer shims
  // b.writeBigUInt64BE(BigInt(date), 0) // Writes 8-bytes
}

module.exports = GarbageCollector
