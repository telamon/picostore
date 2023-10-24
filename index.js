import { Repo } from 'picorepo'
import { Feed, getPublicKey, b2h as toHex } from 'picofeed'
import { init, get } from 'piconuro'
import Cache from './cache.js'
import GarbageCollector from './gc.js'
import { Packr, pack, unpack } from 'msgpackr'
const STRUCTS = new Uint8Array([0xFE,0xED])

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
    const blockRoot = rootId ? await this.readBranch(rootId) : new Feed()
    const oValue = rootId ? await this.readState(rootId) : this.config.initialValue
    const nValue = mutFn(Object.freeze(oValue), {})
    const diff = formatPatch(oValue, nValue)
    blockRoot.append(pack({ root: this.name, diff, date: Date.now() }), secret)
    return await this.store.dispatch(blockRoot)
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
    if (this.#keyspace) return this.store.repo.loadFeed(id)
    else return this.store.repo.loadHead(id)
  }

  async _validateAndMutate (ctx) {
    const {
      data,
      block,
      AUTHOR,
      CHAIN
    } = ctx
    const id = this.store.repo.allowDetached ? CHAIN : AUTHOR
    const value = await this.readState(id)
    const mValue = applyPatch(value, data.diff)
    const ectx = { id, previous: value, ...ctx }
    const err = this.config.validate(mValue, ectx)
    if (err) return err
    await this.writeState(id, mValue)
    // Reschedule GC & Ref
    const expiresAt = this.config.expiresAt(mValue, ectx)
    debugger
    await this.store.gc.schedule(expiresAt, id)
    this.refs[id] ||= []
    this.refs[id].push(block.sig)
  }
}

export default class Store {
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
    this._strategy = mergeStrategy || (() => {})
    // this._stores = []
    this.roots = {}
    this._loaded = false
    this._tap = null // global sigint trap
    this._packr = null
    this.mutexTimeout = 5000
    this._collections = {}
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

      let structures = await this.repo.readReg(STRUCTS)
      if (structures) structures = unpack(structures)
      else structures = []

      this._packr = new Packr({
        structures,
        saveStructures: () => this.repo.writeReg(STRUCTS, pack(structures))
        // .then(console.info.bind(null, 'msgpackr:structs saved'))
          .catch(console.error.bind(null, 'Failed saving msgpackr:structs'))
      })

      this._gc = new GarbageCollector(this.repo, this._packr)
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

    const diff = !local.length ? patch.length : local._compare(patch) // WARNING untested internal api.
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
    let nMerged = 0
    const modified = new Set()
    for (const block of patch.blocks.slice(-diff)) {
      const data = unpack(block.body) // Support multi-root blocks?
      const collection = this.roots[data.root]
      if (!collection) throw new Error('UnknownCollection' + data.root)
      const validationError = await collection._validateAndMutate({
        data,
        block,
        parentBlock,
        ...tags
      })

      // Abort merging // TODO: What was loud again?
      if (typeof validationError === 'string') throw new Error(`InvalidBlock: ${validationError}`)
      else if (validationError === true) return patch.slice(-diff, -diff + nMerged)

      // Mutate state
      // await collection._mutateState(block, parentBlock, tags, false, loud)
      parentBlock = block
      nMerged++
      this.stats.blocksIn++ // TODO: move to collection
    }
    this._notifyObservers(modified)
    return patch.slice(-diff, -diff + nMerged)
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
    for (const name of modified) {
      const store = this._stores.find(s => s.name === name)
      for (const listener of store.observers) listener(store.value)
    }
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

  /**
   * TODO: remove
   * Hotswaps data-storage immediately reloading store from given bucket
   * and destroying previous database deleting all values in the background
   * to free up memory in storage.
   * returns an array containing two promises:
   *   Reload op: Store ready for use again when resolves.
   *   Destroy op: Old database was succesfully cleared when resolved.
   */
  hotswap (db) {
    const prev = this.repo
    // Swap database and begin reload
    this.repo = db instanceof PicoRepo ? db : new PicoRepo(db)
    const reloaded = this.reload()
    // TODO: move this to PicoRepo#destroy() => Promise
    const destroyed = prev._db.clear()
    return [reloaded, destroyed]
  }

  _decodeValue (buf) {
    if (!buf) return buf
    return this._packr.unpack(buf)
  }

  _encodeValue (obj) {
    return this._packr.pack(obj)
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
