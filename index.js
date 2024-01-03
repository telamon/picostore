import { Repo } from 'picorepo'
import { Feed, getPublicKey, toHex, toU8, s2b, cmp } from 'picofeed'
// import { init, get } from 'piconuro'
import Cache from './cache.js'
import GarbageCollector from './gc.js' // TODO: rename to scheduler?
import { encode, decode } from 'cborg'
const SymPostpone = Symbol.for('pDVM::postpone')
const SymReject = Symbol.for('pDVM::reject')
/** @typedef {Uint8Array} u8 */
/** @typedef {string} hexstring */
/** @typedef {hexstring} Author */
/** @typedef {import('picofeed').Block} Block */
/** @typedef {u8} BlockID */
/** @typedef {u8|hexstring} Signature */
/** @typedef {import('picofeed').SecretKey} SecretKey */
/** @typedef {import('picofeed').PublicKey} PublicKey */
/**
    @typedef {{ [SymReject]: true, message?: string, silent: boolean }} Rejection
    @typedef {{ [SymPostpone]: true, message?: string, time: number, retries: number }} Reschedule
    @typedef {{
      block: Block,
      parentBlock: Block,
      AUTHOR: hexstring,
      CHAIN: hexstring,
      data: any
    }} DispatchContext

    // Root of the problem
    @typedef { DispatchContext & {
      reject: (message) => Rejection,
      postpone: (timeMillis: number, errorMsg: string, nTries: 3) => Reschedule,
      signal: (name: string, payload: any) => void,
      lookup: (key: u8|string) => Promise<BlockID>
    }} ComputeContext

    @typedef { DispatchContext & {
      index: (key: u8|string) => Promise<Void>,
      signal: (name: string, payload: any) => void
    }} PostapplyContext
 */

/**
 * A reference-counting decentralized state manager
 */
export class Memory {
  initialValue = undefined
  #store = null
  #cache = {} // in-memory cached state
  #version = 0
  #head = undefined

  /**
   * @param {Store} store
   * @param {string} name
   */
  constructor (store, name) {
    this.#store = store
    this.name = name

    // SanityCheck required methods
    for (const method of [
      'compute'
    ]) { if (typeof this.compute !== 'function') throw new Error(`${this.constructor.name} must implement async ${method}`) }
  }

  /** @returns {Store} */
  get store () { return this.#store }
  get state () { return decode(encode(this.#cache)) } // clone

  /** @param {DispatchContext} ctx */
  async idOf (ctx) { return this.store.repo.allowDetached ? ctx.CHAIN : ctx.AUTHOR }
  /** @param {DispatchContext} ctx */
  async expiresAt (ctx) { return Infinity } // eslint-disable-line no-unused-vars
  /** @param {any} value Current state
    * @param {ComputeContext} ctx
    * @returns {Promise<Rejection|Reschedule|any>} The new value */
  async compute (value, ctx) { throw new Error('Memory.compute(ctx: ComputeContext) => draft; must be implemented by subclass') }
  async postapply (value, ctx) { }

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

  /**
   * Index a block using a secondary-key
   */
  async #index (secondaryKey, blockId) {
    const k = ccat(toU8('sIdx'), toU8(this.name), toU8(secondaryKey))
    await this.store.repo.writeReg(k, toU8(blockId))
  }

  async #lookup (secondaryKey) {
    const k = ccat(toU8('sIdx'), toU8(this.name), toU8(secondaryKey))
    return this.store.repo.readReg(k)
  }

  /** @param {DispatchContext} ctx */
  async _validateAndMutate (ctx) {
    const { data, block, AUTHOR, CHAIN } = ctx
    // TODO: this is not wrong but CHAIN and AUTHOR is mutually exclusive in picorepo ATM.
    const blockRoot = this.store.repo.allowDetached ? CHAIN : AUTHOR
    const id = await this.idOf(ctx) // ObjId
    ctx.id = id // Safe to pollute I guess.
    const value = tripWire(await this.readState(id)) // TODO: Copy?
    // Phase0: Set up mutually exclusive rejection/postpone flow-control.
    let abortMerge = false
    const reject = message => {
      if (abortMerge) throw new Error(`Merge already rejected: ${abortMerge.message}`)
      abortMerge = { [SymReject]: true, message, silent: !message }
      return abortMerge
    }
    const postpone = (time, message, retries = 3) => {
      if (abortMerge) throw new Error(`Merge already rejected: ${abortMerge.message}`)
      abortMerge = { [SymPostpone]: true, message, time, retries }
    }
    // Phase1: Compute State change
    const computeContext = /** @type {ComputeContext} */ {
      ...ctx,
      reject,
      postpone,
      lookup: key => this.#lookup(key)
    }
    const draft = await this.compute(value, computeContext)

    if (abortMerge) return abortMerge

    // Phase2: Apply the draft and run the post-apply hook
    await this._writeState(id, draft)
    await this.postapply(draft, {
      ...ctx,
      index: key => this.#index(key, block.id),
      // Signals have to be queued post-block exectution for all
      // memorystores but before processing next block
      signal: () => { throw new Error('TODO') }
    })

    // Phase3: Schedule deinitialization & increment reference counts
    const expiresAt = await this.expiresAt(data.date, { ...ctx, value: draft })
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

/**
 * This memory model comes with a built-in
 * block creation function: 'mutate()'
 * making it easy to sync changes across peers
 */
export class CRDTMemory extends Memory {
  async compute (value, computeContext) {
    const { data } = computeContext
    const draft = applyPatch(value, data.diff)
    const res = await this.validate(draft, { previous: value, ...computeContext })

    // This should prevent validate from doing
    // anything else but validating.
    if (res && (res[SymPostpone] || res[SymReject])) return res
    else return draft
  }

  /**
   * Mutates the decentralized state of this collection.
   * @param {BlockID|Feed} branch
   * @param {(value, MutationContext) => value} mutFn
   * @param {SecretKey} secret Signing secret
   */
  async mutate (branch, mutFn, secret) {
    const AUTHOR = getPublicKey(secret)
    branch = await this.store.readBranch(branch)
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
}

/**
 * @param {hexstring|Uint8Array} blockRoot
 * @param {string} stateRoot
 * @param {hexstring|Uint8Array} objId
 */
function mkRefKey (blockRoot, stateRoot, objId = null) {
  const b = toU8(blockRoot)
  const s = typeof stateRoot === 'string' && s2b(stateRoot)
  const k = new Uint8Array(b.length + 12 + 32)
  k.set(b, 0)
  if (stateRoot === Infinity) for (let i = 0; i < 12; i++) k[b.length + i] = 0xff
  else if (s) k.set(s, b.length)
  if (objId) k.set(toU8(objId).subarray(0, 32), b.length + 12)
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
   * Initializes a managed decentralized memory area
   * @param {string} name - The name of the collection
   * @param {() => Memory} MemoryClass - Constructor of Memory or subclass
   * @returns {Memory} - An instance of the MemorySubClass
   */
  register (name, MemoryClass) {
    if (this._loaded) throw new Error('Hotconf not supported')
    if (!(MemoryClass.prototype instanceof Memory)) throw new Error('Expected a subclass of Memory')
    const c = new MemoryClass(this, name)
    this.roots[name] = c
    return c
  }

  _notify (event, payload) {
    console.info('event:', event, payload.root)
  }

  async _refIncrement (blockRoot, stateRoot, objId) {
    const key = mkRefKey(blockRoot, stateRoot, objId)
    await this._refReg.put(key, encode({ stateRoot, objId }))
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

  /** Fetches a writable feed
   *  @param {undefined|BlockID|PublicKey} branch
    * @returns {Promise<Feed>} */
  async readBranch (branch) {
    if (!branch) return new Feed()
    if (Feed.isFeed(branch)) return branch
    // TODO: This signature/pk assumption does not sit well.
    branch = toU8(branch)
    return branch.length === 32
      ? await this.repo.loadHead(branch) // Assume pubkey
      : await this.repo.resolveFeed(branch) // Assume blockid
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
      const abort = await collection._validateAndMutate({
        data,
        block,
        parentBlock,
        ...tags
      })
      if (abort) {
        if (abort[SymReject]) {
          // Loud should be set when locally produced blocks are dispatched.
          if (loud && !abort.silent) throw new Error(`InvalidBlock: ${abort.message}`)
        } else if (abort[SymPostpone]) {
          console.error('TODO: Implement retries of postponed blocks')
          // TODO: schedule block-id with gc/scheduler to reattempt
          // merge in validationError.time -seconds.
          // usecase: Missing cross-chain weak-reference. (action depends on secondary profile or something)
        }
        break // Abort merge loop and release locks
      }
      parentBlock = block
      const nMerged = await this.repo.merge(block, this._strategy) // TODO: batch merge?
      // nMerged is 0 when repo rejects a block, at this stage it must not be rejected.
      if (!nMerged) throw new Error('[CRITICAL] MergeStrategy failed! state and blockstore is desynced. Fix bug and store.reload()')
      this._notify('merged', { block, root: data.root })
      merged.push(block)
    }
    if (merged.length) return Feed.from(merged)
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

/** @returns {[Promise, (v: any) => void, (e: Error) => void]} */
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

export function ccat (...buffers) { // TODO: Move to Picofeed ?
  if (buffers.length === 1 && Array.isArray(buffers[0])) buffers = buffers[0]
  const out = new Uint8Array(buffers.reduce((sum, buf) => sum + buf.length, 0))
  let o = 0
  for (const buf of buffers) { out.set(buf, o); o += buf.length }
  return out
}

export function tripWire (o, path = []) {
  if (!o || typeof o !== 'object') return o
  return new Proxy(o, {
    get (target, key) {
      switch (typeOf(o)) {
        case 'array': // TODO?
        case 'object': return tripWire(target[key], [...path, key])
        default: return target[key]
      }
    },
    set (_, key) {
      throw new Error(`Attempted to modifiy: ${path.join('.')}[${key}]`)
    }
  })
}
