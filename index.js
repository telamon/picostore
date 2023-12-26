import { Repo } from 'picorepo'
import { Feed, getPublicKey, toHex, toU8, s2b } from 'picofeed'
import { init, get } from 'piconuro'
import Cache from './cache.js'
import GarbageCollector from './gc.js'
import { encode, decode } from 'cborg'

/** @typedef {string} hexstring */
/** @typedef {hexstring} Author */
/** @typedef {hexstring} Chain */
/** @typedef {Author|Chain} RootID */
/** @typedef {import('picofeed').SecretKey} SecretKey */
/** @typedef {
  initialValue?: any,
  id: () => hexstring|number,
  validate: () => string?,
  expiresAt: () => number
} CollectionSpec */

class Collection {
  #keyspace = 0 // 0: Author, 1: Chain ID, -1: Singularity/Monochain
  #refs = {}
  /**
   * @param {Store} store
   * @param {string} name
   * @param {CollectionSpec} config
   */
  constructor (store, name, config = {}) {
    this.store = store
    this.name = name
    this.config = config
    this.state = {}
  }

  /**
   * Mutates the decentralized state of this collection.
   * @param {RootID?} rootId Item identified by chain or author
   * @param {(value, MutationContext) => value} mutFn
   * @param {SecretKey} secret Signing secret
   */
  async mutate (rootId, mutFn, secret) {
    // const author = getPublicKey(secret)
    const feed = rootId ? await this.readBranch(rootId) : new Feed()
    const oValue = rootId ? await this.readState(rootId) : this.config.initialValue
    const nValue = mutFn(Object.freeze(oValue), {})
    const diff = formatPatch(oValue, nValue)
    feed.append(encode({ root: this.name, diff, date: Date.now() }), secret)
    return await this.store.dispatch(feed)
  }

  async readState (id) {
    return this.state[id] || this.config.initialValue
  }

  async writeState (id, o) {
    this.state[id] = o
  }

  async sweepState (id) {
    delete this.state[id]
  }

  /** @param {RootID} id */
  async readBranch (id) {
    switch (this.#keyspace) {
      case -1: throw new Error('monochain mode not implemented')
      case 0: return this.store.repo.loadHead(id)
      case 1: return this.store.repo.loadFeed(id)
    }
  }

  async _validateAndMutate (ctx) {
    const {
      data,
      AUTHOR,
      CHAIN
    } = ctx
    const blockRoot = this.store.repo.allowDetached ? CHAIN : AUTHOR
    const id = blockRoot // TODO: config.id(ctx)
    const value = await this.readState(id)
    const mValue = applyPatch(value, data.diff)
    const ectx = { id, blockRoot, previous: value, ...ctx }
    const err = this.config.validate(mValue, ectx)
    if (err) return err
    await this.writeState(id, mValue)
    // Reschedule GC & Ref
    const expiresAt = this.config.expiresAt(mValue, ectx) || Date.now() + 60000
    await this.store.refIncrement(blockRoot, this.name, id)
    await this.store._gc.schedule(this.name, blockRoot, id, expiresAt)
  }

  async _gc_visit (gcDate, memo) {
    /* Order of Operations
     * 1. Visit state-id and determine if it should still be deleted
     * 2a). Reschedule visit to another time
     * 2b). Yes; state dropped, evict chain (no partial rollbacks not yet supported)
     * 3. notify all listening parties
     */
    const { root, id, date, blockRoot } = memo
    if (root !== this.name) throw new Error(`WrongCollection: ${root}`)
    const expired = () => date < gcDate // TODO: config.isExp
    if (expired()) {
      await this.sweepState(id) // TODO: notify neurons?
      const evicted = await this.store.refDecrement(blockRoot, this.name, id)
      return evicted
    }
  }
}

/**
 * @param {hexstring|Uint8Array} blockRoot
 * @param {string} stateRoot
 * @param {hexstring|Uint8Array} objId
 */
function mkRefKey (blockRoot, stateRoot, objId) {
  const b = toU8(blockRoot)
  const s = typeof stateRoot === 'string' && s2b(stateRoot)
  const k = new Uint8Array(b.length + 12 + 32)
  k.set(b, 0)
  if (stateRoot === Infinity) for (let i = 0; i < 12; i++) k[b.length + i] = 0xff
  else if (s) k.set(s, b.length)
  if (objId) k.set(toU8(objId), b.length + 12)
  return k
}

export default class Store {
  roots = {} // StateRoots
  _loaded = false
  _tap = null // global signal trap
  mutexTimeout = 5000
  stats = {
    blocksIn: 0,
    blocksOut: 0,
    bytesIn: 0,
    bytesOut: 0,
    date: Date.now()
  }

  constructor (db, mergeStrategy) {
    this.repo = Repo.isRepo(db) ? db : new Repo(db)
    this.cache = new Cache(this.repo._db)
    this._gc = new GarbageCollector(this.repo)
    this._strategy = mergeStrategy || (() => {})
    this._refReg = this.repo._db.sublevel('REFs', {
      keyEncoding: 'view',
      valueEncoding: 'view'
    })

    // this._stores = []
  }

  /**
   * Initializes a collection decentralized state
   * @param {string} name The name of the collection
   * @param {CollectionSpec} config collection configuration
   */
  spec (name, config) {
    if (config && this._loaded) throw new Error('Hotconf not supported')
    console.log('spec()', name, config)
    const c = new Collection(this, name, config)
    this.roots[name] = c
    return c
  }

  async refIncrement (blockRoot, stateRoot, objId) {
    const key = mkRefKey(blockRoot, stateRoot, objId)
    // console.log('INC', toHex(key))
    await this._refReg.put(key, Date.now())
  }

  async refDecrement (blockRoot, stateRoot, objId) {
    const key = mkRefKey(blockRoot, stateRoot, objId)
    // console.log('DEC', toHex(key))
    await this._refReg.del(key)
    // TODO: safe but not efficient place to ref-count and block-sweep
    const nRefs = await this.referencesOf(blockRoot)
    if (!nRefs) {
      debugger // TODO: bug???
      console.log('Purging blockRoot', blockRoot)
      const e = await this.repo.rollback(blockRoot) // passing objId as stop could enable partial rollback
      return e
    }
  }

  async referencesOf (blockRoot) {
    const iter = this._refReg.iterator({
      gt: mkRefKey(blockRoot, 0),
      lt: mkRefKey(blockRoot, Infinity)
    })
    // console.log('>', toHex(mkRefKey(blockRoot, 0)))
    // console.log('<', toHex(mkRefKey(blockRoot, Infinity)))
    let c = 0
    for await (const _ of iter) c++
    return c
  }

  /*
  register (name, initialValue, validator, reducer, trap, sweep) {
    if (this._loaded) throw new Error('register() must be invoked before load()')
    if (typeof name !== 'string') {
      return this.register(name.name, name.initialValue, name.filter, name.reducer, name.trap, name.sweep)
    } else if (typeof initialValue === 'function') {
      return this.register(name, undefined, initialValue, validator)
    }

    this._stores.push({
      name,
      validator,
      reducer,
      trap,
      sweep,
      version: 0,
      head: undefined,
      value: initialValue,
      initialValue,
      observers: new Set()
    })
  }
  */

  async _lockRun (asyncCallback) {
    // WebLocks API trades-off the result of locked context runs.
    // this is a workaround for that.
    const [p, resolve, reject] = unpromise()
    // Generate error at callee stack
    let timeoutError = null
    try { throw new Error('MutexTimeout') } catch (err) { timeoutError = err }

    await locks.request('default', async () => {
      const timerId = setTimeout(() => {
        console.error('MutexTimeout', timeoutError.stack)
        reject(timeoutError)
      }, this.mutexTimeout)

      try {
        const res = await asyncCallback()
        resolve(res)
      } catch (err) {
        reject(err)
      } finally { clearTimeout(timerId) }
    }).catch(reject)
    return p
  }

  async load () {
    await this._lockRun(async () => {
      if (this._loaded) throw new Error('Store already loaded')
      /* TODO: Rewrite to roots
      for (const store of this._stores) {
        const head = await this.repo.readReg(`HEADS/${store.name}`)
        if (!head) continue // Empty
        store.head = head
        store.version = this._decodeValue(await this.repo.readReg(`VER/${store.name}`))
        store.value = this._decodeValue(await this.repo.readReg(`STATES/${store.name}`))
        for (const listener of store.observers) listener(store.value)
      }
      */
      this._loaded = true
    })
  }

  /**
   * Mutates the state, if a reload is in progress the dispatch will wait
   * for it to complete.
   */
  async dispatch (patch, loud) {
    return this._lockRun(() => this._dispatch(patch, loud))
  }

  async _dispatch (patch, loud = false) {
    if (!this._loaded) throw Error('Store not ready, call load()')
    /// align + grow blockroots
    patch = Feed.from(patch)
    let local = null // Target branch to merge to
    try {
      const sig = patch.first.genesis
        ? patch.first.sig
        : patch.first.psig
      local = await this.repo.resolveFeed(sig)
    } catch (err) {
      if (err.message !== 'FeedNotFound') throw err
      if (patch.first.genesis) local = new Feed() // Happy birthday!
      else {
        await this.cache.push(patch) // Stash for later
        // throw new Error('UnknownTargetBranch')
        return []
      }
    }

    // Local reference point exists,
    // attempt to extend patch from blocks in cache
    for (const block of patch.blocks) {
      const [bwd, fwd] = await this.cache.pop(block.psig, block.sig)
      if (bwd) assert(patch.merge(bwd), 'Backward merge failed')
      if (fwd) assert(patch.merge(fwd), 'Forwrad merge failed')
    }

    // canMerge?
    if (!local.clone().merge(patch)) {
      if (loud) throw new Error('UnmergablePatch')
      else return []
    }

    // Extract tags (used by application to keep track of objects)
    const tags = {}
    if (local.first?.genesis) {
      tags.AUTHOR = toHex(local.first.key)
      tags.CHAIN = toHex(local.first.sig)
    } else if (!local.length && patch.first.genesis) {
      tags.AUTHOR = toHex(patch.first.key)
      tags.CHAIN = toHex(patch.first.sig)
    }

    const diff = !local.length ? patch.length : local.diff(patch) // WARNING untested internal api.
    if (diff < 1) return [] // Patch contains equal or less blocks than local

    let parentBlock = null
    const _first = patch.blocks[patch.blocks.length - diff]
    if (!_first.genesis && !local.length) {
      if (loud) throw new Error('NoCommonParent')
      return []
    } else if (!_first.genesis && local.length) {
      const it = local.blocks[Symbol.iterator]()
      let res = it.next()
      while (!parentBlock && !res.done) { // TODO: avoid loop using local.get(something)
        if (res.value.sig.equals(_first.psig)) parentBlock = res.value
        res = it.next()
      }
      if (!parentBlock) {
        throw new Error('ParentNotFound') // real logical error
      }
    }

    /// Merge DSTATE-roots
    const modified = new Set()
    // TODO: The slice(-diff) here is all wrong;
    // identify patch block sequence containing mergable/new blocks
    // then return those blocks that were merged
    const merged = []
    for (const block of patch.blocks.slice(-diff)) {
      const data = decode(block.body) // TODO: Support multi-root blocks?
      const collection = this.roots[data.root]
      if (!collection) throw new Error('UnknownCollection' + data.root)
      const validationError = await collection._validateAndMutate({
        data,
        block,
        parentBlock,
        ...tags
      })
      // Abort merging // TODO: What was `loud`-flag again?
      if (typeof validationError === 'string') throw new Error(`InvalidBlock: ${validationError}`)
      else if (validationError === true) return Feed.from(merged)
      parentBlock = block
      await this.repo.merge(block) // TODO: batch merge?
      this.stats.blocksIn++ // TODO: move to collection
      merged.push(block)
    }
    this._notifyObservers(modified)
    return Feed.from(merged)
  }

  async _mutateState (block, parentBlock, tags, dryMerge = false, loud = false) {
    const modified = []

    // Generate list of stores that want to reduce this block
    const stores = []
    for (const store of this._stores) {
      if (typeof store.validator !== 'function') continue
      if (typeof store.reducer !== 'function') continue
      const root = this.state
      const rejected = store.validator({ block, parentBlock, state: store.value, root, ...tags })
      if (rejected) continue
      stores.push(store)
    }

    // Attempt repo.merge if at least one store accepts block
    if (stores.length) {
      const merged = await this.repo.merge(block, this._strategy)
      if (!dryMerge && !merged) {
        if (loud) console.warn('RejectedByBucket: MergeStrategy failed')
        return modified // Rejected by bucket
      }
    }

    // Interrupts are buffered until after the mutations have run
    const interrupts = []
    const signal = (i, p) => interrupts.push([i, p])

    // Run all state reducers
    for (const store of stores) {
      // If repo accepted the change, apply it
      const mark = (payload, date) => this._gc.schedule(store.name, tags, block, payload, date)
      const root = this.state
      const context = {
        // blocks
        block,
        parentBlock,
        // AUTHOR & CHAIN
        ...tags,
        // state
        state: store.value,
        root,
        // helpers
        signal,
        mark
      }
      const val = store.reducer(context)
      if (typeof val === 'undefined') console.warn('Reducer returned `undefined` state.')
      await this._commitHead(store, block.sig, val)
      modified.push(store.name)
    }

    // Run all traps in signal order
    // NOTE: want to avoid reusing term 'signal' as it's a function
    // TODO: rewrite to while(interrupts.length) to let traps re-signal
    // TODO: max-interrupts counter of 255 to throw and avoid overuse
    for (const [code, payload] of interrupts) {
      for (const store of this._stores) {
        if (typeof store.trap !== 'function') continue
        const root = this.state
        const val = store.trap({ code, payload, block, parentBlock, state: store.value, root, ...tags })
        if (typeof val === 'undefined') continue // undefined equals no change
        await this._commitHead(store, block.sig, val)
        modified.push(store.name)
      }
    }
    if (typeof this._tap === 'function') {
      for (const [code, payload] of interrupts) this._tap(code, payload)
    }
    return modified
  }

  _notifyObservers (modified) {
    /*for (const name of modified) {
      const store = this._stores.find(s => s.name === name)
      for (const listener of store.observers) listener(store.value)
    }*/
  }

  async _commitHead (store, head, val) {
    await this.repo.writeReg(`STATES/${store.name}`, this._encodeValue(val))
    await this.repo.writeReg(`HEADS/${store.name}`, head)
    await this.repo.writeReg(`VER/${store.name}`, this._encodeValue(store.version++))

    // who needs a seatbelt anyway? let's save some memory.
    store.value = val // Object.freeze(val)
    store.head = head
  }

  get state () {
    return this._stores.reduce((state, store) => {
      state[store.name] = store.value
      return state
    }, {})
  }

  /**
   * Restores all registers to initial values and re-applies all mutations from database.
   */
  async reload () {
    return this._lockRun(async () => {
      const modified = []
      const feeds = await this.repo.listFeeds()

      for (const store of this._stores) {
        store.value = store.initialValue
        store.version = 0
        store.head = undefined
        for (const listener of store.observers) listener(store.value)
      }

      for (const { key: ptr, value: CHAIN } of feeds) {
        const part = await this.repo.loadFeed(ptr)
        const tags = {
          AUTHOR: part.first.key,
          CHAIN
        }
        let parentBlock = null
        for (const block of part.blocks()) {
          const mods = await this._mutateState(block, parentBlock, tags, true)
          // TODO: bug, reload does not persist state?
          for (const s of mods) {
            if (!~modified.indexOf(s)) modified.push(s)
          }
          parentBlock = block
        }
      }
      this._notifyObservers(modified)
      return modified
    })
  }

  // Nuro Shortcut
  $ (name) { return sub => this.on(name, sub) }

  on (name, observer) {
    const store = this._stores.find(s => s.name === name)
    if (!store) throw new Error(`No such store: "${name}"`)
    if (typeof observer !== 'function') throw new Error('observer must be a function')
    store.observers.add(observer)
    observer(store.value)
    // unsub
    return () => store.observers.delete(observer)
  }


  _decodeValue (buf) {
    if (!buf) return buf
    return decode(buf)
  }

  _encodeValue (obj) {
    return encode(obj)
  }

  async gc (now) {
    const { mutated, evicted } = await this._gc.collectGarbage(now, this)
    this._notifyObservers(mutated)
    return evicted
  }

  gcStart (interval = 3 * 1000) {
    if (this.intervalId) return
    this.intervalId = setInterval(this._gc.collectGarbage.bind(this), interval)
  }

  gcStop () {
    if (!this.intervalId) return
    clearInterval(this.intervalId)
    this.intervalId = null
  }
}

function assert (c, m) { if (!c) throw new Error(m || 'AssertionError') }

// Weblocks shim
const locks = (
  typeof window !== 'undefined' &&
  window.navigator?.locks
) || {
  resources: {},
  async request (resource, options, handler) {
    const resources = locks.resources
    if (!resources[resource]) resources[resource] = Promise.resolve()
    const prev = resources[resource]
    resources[resource] = prev
      .catch(err => console.warn('Previous lock unlocked with error', err))
      .then(() => (handler || options)())
    await prev
  }
}

function unpromise () {
  let solve, eject
  const p = new Promise((resolve, reject) => { solve = resolve; eject = reject })
  return [p, solve, eject]
}

// Experimental
function formatPatch (a, b) {
  if (typeof a === 'undefined') return b
  if (typeof a !== typeof b) throw new Error('Dynamic typeswitching not supported')
  const type = typeof a
  switch (type) {
    case 'number': return b - a
    case 'object': {
      const out = { ...a }
      for (const k in b) out[k] = formatPatch(a[k], b[k])
      return out
    }
    default:
      console.warn('formatPatch: Unhandled type', type, a, b)
      return b
  }
}

function applyPatch (a, b) {
  const type = typeof b
  switch (type) {
    case 'number': return a + b
    case 'object': {
      const out = { ...a }
      for (const k in b) out[k] = applyPatch(a[k], b[k])
      return out
    }
    default:
      console.warn('applyPatch: Unhandled type', type, a, b)
      return b
  }
}
