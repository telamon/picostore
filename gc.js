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
  const b = Buffer.allocUnsafe(8 + slice.length + sig.length)
  b.writeBigUInt64BE(BigInt(date), 0)
  for (let i = 0; i < slice.length; i++) b[8 + i] = slice.charCodeAt(i)
  for (let i = 0; i < sig.length; i++) b[8 + slice.length + i] = sig[i]
  return b
}

module.exports = GarbageCollector

function GarbageCollectModule (store) {

  // TODO: if this hack works, move the time-based GarbageCollector functionality to PicoStore
  // and allow store.register() to include a sweep function, `expiresAt` prop becomes a first citizen.
  function sweep ({ now, rootState, drop, gcType, id, didMutate }) {
    if (gcType === 'chat') {
      const match = rootState.vibes.matches[id.toString('hex')]
      const chat = rootState.chats.chats[id.toString('hex')]
      const head = chat?.head || match?.response || match?.chatId
      let del = false
      if (!head) return // nothing to do
      const expiresAt = (chat?.expiresAt || match?.expiresAt || 0)
      const hasExpired = expiresAt < now
      D('Chat[%h] expired: %s, timeLeft: %d', id, hasExpired, expiresAt - now)
      if (!hasExpired) return // not yet

      // Clear chat registry
      //!!! Remove element in chats[] and heads[]
      if (chat) {
        const { chats, heads, own } = rootState.chats
        delete chats[id.toString('hex')]
        delete heads[head.toString('hex')]
        const idx = own.findIndex(b => b.equals(id))
        if (~idx) own.splice(idx, 1)
        didMutate('chats')
        del = true
      }

      // Clear vibes registry
      //!!! Remove Element in vibes[] and seen[]
      if (match) {
        // Restore state
        const { seen, matches, own } = rootState.vibes
        if (match.a && seen[match.a.toString('hex')]?.equals(id)) delete seen[match.a.toString('hex')]
        if (match.b && seen[match.b.toString('hex')]?.equals(id)) delete seen[match.b.toString('hex')] // redundant?
        delete matches[id.toString('hex')]
        const idx = own.findIndex(b => b.equals(id))
        if (~idx) own.splice(idx, 1)
        didMutate('vibes')
        del = true
      }
      //!!! Rollback to chat.id; inclusive?
      if (del) drop(head, id) // id: chatId, head: lastBlock on chat
    } else if (gcType === 'peer') {
      const { peers } = rootState
      const isSelf = rootState.peer.pk?.equals(id)
      if (isSelf && rootState.peer.expiresAt < now) {
        rootState.peer.state = EXPIRED // Not sure if should delete.
        didMutate('peer')
      }
      //!!! Remove element peers[]
      const peer = peers[id.toString('hex')]
      if (peer && peer.expiresAt < now) {
        peer.state = EXPIRED
        didMutate('peers')
        if (!isSelf) {
          delete peers[id.toString('hex')]
          //!!! Full feed rollback
          drop(peer.sig)
        }
      }
    } else throw new Error(`Garbage collection for "${gcType}" not supported!`)
  }
}
