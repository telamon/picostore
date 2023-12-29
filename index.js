import { Repo } from 'picorepo'
import { Feed, getPublicKey, toHex, toU8, s2b, cmp } from 'picofeed'
// import { init, get } from 'piconuro'
import Cache from './cache.js'
import GarbageCollector from './gc.js' // TODO: rename to scheduler?
import { encode, decode } from 'cborg'

/** @typedef {string} hexstring */
/** @typedef {hexstring} Author */
/** @typedef {hexstring} Signature */
/** @typedef {Signature} Chain */
/** @typedef {Author|Chain|Signature} RootID */
/** @typedef {import('picofeed').SecretKey} SecretKey */
/** @typedef {{
  initialValue?: any,
  id?: () => hexstring|number,
  validate: () => string?,
  expiresAt?: () => number
}} CollectionSpec */

/**
 * A reference-counting decentralized state manager
 */
export class Memory {
  initialValue = undefined
  store = null
  #cache = {} // in-memory cached state
  #version = 0
  #head = undefined

  /**
   * @param {Store} store
   * @param {string} name
   * @param {CollectionSpec} config
   */
  constructor (store, name) {
    this.store = store
    this.name = name
    // SanityCheck required methods
    for (const method of [
      'reduce'
    ]) {
      if (typeof this[method] !== 'function') throw new Error(`${this.constructor.name} must implement async ${method}`)
    }
  }

  get state () { return decode(encode(this.#cache)) } // clone
  async idOf ({ AUTHOR, CHAIN }) { return this.store.repo.allowDetached ? CHAIN : AUTHOR }
  async validate () { return false } // Accept everything
  async expiresAt () { return Infinity } // Never

  /**
   * Mutates the decentralized state of this collection.
   * @param {Chain|BlockId|Feed} branch
   * @param {(value, MutationContext) => value} mutFn
   * @param {SecretKey} secret Signing secret
   * @param {Feed} feed The branch to append to
   */
  async mutate (branch, mutFn, secret) {
    const AUTHOR = getPublicKey(secret)
    branch = await this.fetchBranch(branch)
    // TODO: get chain id from repo incase partial feed was passed
    const CHAIN = branch.first?.genesis ? toHex(branch.first.id) : undefined
    // TODO: stateId/reduce/sweep should be interchangeable. (support different mem/transition models)
    const stateRoot = await this.idOf({ AUTHOR, CHAIN })
    const oValue = await this.readState(stateRoot)
    const nValue = mutFn(Object.freeze(oValue), {})
    const diff = formatPatch(oValue, nValue)
    branch.append(encode({ root: this.name, diff, date: Date.now() }), secret)
    return await this.store.dispatch(branch, true)
  }

  async readState (id) {
    return id in this.#cache
      ? this.#cache[id]
      : this.initialValue
  }

  async _writeState (id, o) {
    this.#cache[id] = o
    this.store._notify('change', { root: this.name, id, value: o })
  }

  async _sweepState (id) {
    delete this.#cache[id]
    this.store._notify('change', { root: this.name, id, value: undefined })
  }

  /** Fetches a writable feed
   *  @param {undefined|Signature|PublicKey} branch
    * @returns {Promise<Feed>} */
  async fetchBranch (branch) {
    if (!branch) return new Feed()
    if (Feed.isFeed(branch)) return branch
    // TODO: This signature/pk assumption does not sit well.
    if (this.store.repo._allowDetached) return await this.store.repo.loadFeed(branch)
    return await this.store.repo.loadHead(branch) // Fallback to assuming 'branch' is a pubkey
  }

  async _validateAndMutate (ctx) {
    const {
      data,
      AUTHOR,
      CHAIN,
      block
    } = ctx
    const blockRoot = this.store.repo.allowDetached ? CHAIN : AUTHOR // TODO: this is utterly wrong
    const id = await this.idOf(ctx)
    const value = await this.readState(id) // TODO: Copy?
    const mValue = await this.reduce(value, ctx)

    const ectx = { id, previous: value, ...ctx }
    const err = await this.validate(mValue, ectx) // TODO: postpone()
    if (err) return err
    await this._writeState(id, mValue)
    ectx.value = mValue
    // Schedule GC & Ref
    const expiresAt = await this.expiresAt(data.date, ectx)
    console.log('ExpiresAt', expiresAt)
    if (expiresAt !== Infinity) await this.store._gc.schedule(this.name, blockRoot, id, expiresAt)
    await this.store._refIncrement(blockRoot, this.name, id)
    await this._incrState(block.id)
  }

  async _gc_visit (gcDate, memo) {
    const { root, id, date, blockRoot } = memo
    if (root !== this.name) throw new Error(`WrongCollection: ${root}`)
    const expiresAt = this.config.expiresAt(date, memo)
    if (expiresAt <= gcDate) {
      await this._sweepState(id)
      this.#version++
      return await this.store._refDecrement(blockRoot, this.name, id)
    } else if (expiresAt !== Infinity) { // Reschedule
      await this.store._gc.schedule(this.name, blockRoot, id, expiresAt)
    }
  }

  async _incrState (head) {
    this.#head = head
    this.#version++
    await this.store.repo.writeReg(s2b(`STATES/${this.name}`), encode({
      version: this.#version,
      head: this.#head,
      state: this.#cache
    }))
  }

  async _loadState () {
    const dump = await this.store.repo.readReg(s2b(`STATES/${this.name}`))
    if (!dump) return
    const { version, head, state } = decode(dump)
    this.#cache = state
    this.version = version
    this.head = head
    this.store._notify('change', { root: this.name })
  }
}

export class CRDTMemory extends Memory {
  reduce (value, { data }) {
    return applyPatch(value, data.diff)
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
  _onUnlock = []
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
  }

  /**
   * Initializes a collection decentralized state
   * @param {string} name The name of the collection
   * @param {CollectionSpec} config collection configuration
   */
  register (name, MemorySubClass) {
    if (this._loaded) throw new Error('Hotconf not supported')
    if (!(MemorySubClass.prototype instanceof Memory)) throw new Error('Expected a subclass of Memory')
    const c = new MemorySubClass(this, name)
    this.roots[name] = c
    return c
  }

  _notify (event, payload) {
    console.info('event:', event, payload.root)
  }

  async _refIncrement (blockRoot, stateRoot, objId) {
    const key = mkRefKey(blockRoot, stateRoot, objId)
    await this._refReg.put(key, Date.now())
  }

  async _refDecrement (blockRoot, stateRoot, objId) {
    const key = mkRefKey(blockRoot, stateRoot, objId)
    await this._refReg.del(key)
    // TODO: safe but not efficient place to ref-count and block-sweep
    const nRefs = await this.referencesOf(blockRoot)
    if (!nRefs) {
      // TODO: passing objId as stop could potentially allow partial rollback
      const evicted = await this.repo.rollback(blockRoot)
      this._notify('rollback', { evicted, root: stateRoot })
      return evicted
    }
  }

  async referencesOf (blockRoot) {
    const iter = this._refReg.iterator({
      gt: mkRefKey(blockRoot, 0),
      lt: mkRefKey(blockRoot, Infinity)
    })
    let c = 0
    for await (const _ of iter) c++ // eslint-disable-line no-unused-vars
    return c
  }

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
      } finally {
        clearTimeout(timerId)
        let action = null
        while ((action = this._onUnlock.shift())) action()
      }
    }).catch(reject)
    return p
  }

  async load () {
    await this._lockRun(async () => {
      if (this._loaded) throw new Error('Store already loaded')
      for (const root in this.roots) {
        await this.roots[root]._loadState()
      }
      this._loaded = true
    })
  }

  /**
   * Mutates the state, if a reload is in progress the dispatch will wait
   * for it to complete.
   * @returns {Promise<Feed|undefined>} merged blocks
   */
  async dispatch (patch, loud) {
    return this._lockRun(() => this._dispatch(patch, loud))
  }

  /** @returns {Promise<Feed|undefined>} merged blocks */
  async _dispatch (patch, loud = false) {
    if (!this._loaded) throw Error('Store not ready, call load()')
    // align + grow blockroots
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
        return
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
      else return
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
    if (diff < 1) return // Patch contains equal or less blocks than local

    let parentBlock = null
    const _first = patch.blocks[patch.blocks.length - diff]
    if (!_first.genesis && !local.length) {
      if (loud) throw new Error('NoCommonParent')
      return []
    } else if (!_first.genesis && local.length) {
      const it = local.blocks[Symbol.iterator]()
      let res = it.next()
      while (!parentBlock && !res.done) { // TODO: avoid loop using local.get(something)
        if (cmp(res.value.sig, _first.psig)) parentBlock = res.value
        res = it.next()
      }
      if (!parentBlock) {
        throw new Error('ParentNotFound') // real logical error
      }
    }

    /// Merge DSTATE-roots
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
      else if (validationError === true) return merged.length ? Feed.from(merged) : undefined
      parentBlock = block
      const m = await this.repo.merge(block, this._strategy) // TODO: batch merge?
      if (!m && loud) console.warn('RejectedByBucket: MergeStrategy failed')
      this._notify('merged', { block, root: data.root })
      merged.push(block)
    }
    return Feed.from(merged)
  }

  /**
   * Restores all registers to initial values and re-applies all mutations from database.
   */
  async reload () {
    return this._lockRun(async () => {
      const feeds = await this.repo.listFeeds()

      for (const root in this.roots) {
        const collection = this.roots[root]
        collection.state = {}
        collection.version = 0
        collection.head = undefined
      }

      const modified = new Set()
      for (const { key: ptr, value: CHAIN } of feeds) {
        const part = await this.repo.loadFeed(ptr)
        const tags = {
          AUTHOR: part.first.key,
          CHAIN
        }
        let parentBlock = null
        for (const block of part.blocks) {
          const data = decode(block.body) // TODO: Support multi-root blocks?
          const collection = this.roots[data.root]
          if (!collection) throw new Error('UnknownCollection' + data.root)
          const validationError = await collection._validateAndMutate({
            data,
            block,
            parentBlock,
            ...tags
          })
          if (validationError) throw new Error(`InvalidBlock during reload: ${validationError}`)
          this._notify('reload', { block, root: data.root })
          parentBlock = block
          modified.add(data.root)
        }
      }
      return Array.from(modified)
    })
  }

  // Nuro Shortcut
  $ (name) { return sub => this.on(name, sub) }

  on (name, observer) {
    const collection = this.roots[name]
    if (!collection) throw new Error(`No collection named "${name}"`)
    if (typeof observer !== 'function') throw new Error('observer must be a function')
    return collection.sub(observer)
  }

  async gc (now) {
    const { evicted } = await this._gc.collectGarbage(now, this)
    return evicted
  }

  gcStart (interval = 3 * 1000) {
    if (this.intervalId) return
    this.intervalId = setInterval(
      this._gc.collectGarbage.bind(this),
      interval
    )
  }

  gcStop () {
    if (!this.intervalId) return
    clearInterval(this.intervalId)
    this.intervalId = null
  }

  /*
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
    // DISABLED 4 NOW
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
  } */
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
/**
 * Compares two states and returns the incremental diff
 * between the two.
 */
export function formatPatch (a, b) {
  if (typeof a === 'undefined') return b
  if (typeOf(a) !== typeOf(b)) throw new Error('Dynamic typeswitching not supported')
  const type = typeOf(a)
  switch (type) {
    case 'number': return b - a
    case 'object': {
      const out = { ...a }
      for (const k in b) out[k] = formatPatch(a[k], b[k])
      return out
    }
    case 'array': {
      const out = []
      const m = Math.max(a.length, b.length)
      for (let i = 0; i < m; i++) out[i] = formatPatch(a[i], b[i])
      return out
    }
    default: // eslint-disable-line default-case-last
      console.warn('formatPatch: Unhandled type', type, a, b)
    case 'u8': // eslint-disable-line no-fallthrough
      return b
  }
}
/**
 * @returns {Object} a new instance with the patch applied */
export function applyPatch (a, b) {
  const type = typeOf(b)
  switch (type) {
    case 'number': return a + b
    case 'object': {
      if (typeOf(a) === 'undefined') a = {}
      const out = { ...a }
      for (const k in b) out[k] = applyPatch(a[k], b[k])
      return out
    }
    case 'array': {
      if (typeOf(a) === 'undefined') a = []
      const out = []
      const m = Math.max(a.length, b.length)
      for (let i = 0; i < m; i++) out[i] = applyPatch(a[i], b[i])
      return out
    }
    default: // eslint-disable-line default-case-last
      console.warn('applyPatch: Unhandled type', type, a, b)
    case 'u8': // eslint-disable-line no-fallthrough
      return b
  }
}

export function typeOf (o) {
  if (o === null) return 'null'
  if (Array.isArray(o)) return 'array'
  if (o instanceof Uint8Array) return 'u8'
  return typeof o
}
