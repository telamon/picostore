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
    const key = mkKey(date, slice, sig)
    // console.info('Scheduing removal', type, new Date(date))
    this.db.put(key, memo)
      .catch(error => console.error('Failed queing GC: ', error))
  }

  start (interval = 3 * 1000) {
    if (this.intervalId) return
    this.intervalId = setInterval(this._collectGarbage.bind(this), interval)
  }

  stop () {
    if (!this.intervalId) return
    clearInterval(this.intervalId)
    this.intervalId = null
  }

  async tickQuery (now) {
    const query = {
      gt: mkKey(0),
      lt: mkKey(now, '', Buffer.allocUnsafe(5).fill(0xff))
    }
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

function mkKey (date, slice = '', sig = '') {
  // return `${date}${slice}${sig.slice(0, 6).toString('hex')}`
  if (sig?.length) sig = sig.slice(0, 6)
  const d = 20 // date size
  const b = Buffer.allocUnsafe(d + slice.length + sig.length)
  date = (date + '').padStart(d, 0) // Workaround
  for (let i = 0; i < d; i++) b[i] = date[i]
  // SpaceEfficient + Lookups but not supported by all Buffer shims
  // b.writeBigUInt64BE(BigInt(date), 0) // Writes 8-bytes
  for (let i = 0; i < slice.length; i++) b[d + i] = slice.charCodeAt(i)
  for (let i = 0; i < sig.length; i++) b[d + slice.length + i] = sig[i]
  return b
}

module.exports = GarbageCollector
