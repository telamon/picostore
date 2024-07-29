import { Repo } from 'picorepo'
import { Feed, getPublicKey, toHex, toU8, s2b, cmp } from 'picofeed'
// import { init, get } from 'piconuro'
import { Mempool } from './mempool.js'
import { Scheduler } from './scheduler.js'
import { encode, decode } from 'cborg'

const SymPostpone = Symbol.for('PiC0VM::postpone')
const SymReject = Symbol.for('PiC0VM::reject')
const SymSignal = Symbol.for('PiC0VM::signal')

/** @typedef {import('abstract-level').AbstractLevel} AbstractLevel */
/** @typedef {Uint8Array} u8 */
/** @typedef {string} hexstring */
/** @typedef {hexstring} Author */
/** @typedef {import('picofeed').Block} Block */
/** @typedef {u8} BlockID */
/** @typedef {u8|hexstring} Signature */
/** @typedef {import('picofeed').SecretKey} SecretKey */
/** @typedef {import('picofeed').PublicKey} PublicKey */
/** @typedef {Signature|PublicKey|number} ObjectId */
/**
    @typedef {{ [SymReject]: true, message?: string, silent: boolean }} Rejection
    @typedef {{ [SymPostpone]: true, message?: string, time: number, retries: number }} Reschedule
    @typedef {{
      block: Block,
      parentBlock: Block,
      AUTHOR: hexstring,
      CHAIN: hexstring,
      root: string,
      payload: any,
      date: number
    }} DispatchContext

    // Root of the problem
    @typedef { DispatchContext & {
      id: ObjectId,
      reject: (message) => Rejection,
      postpone: (timeMillis: number, errorMsg: string, nTries: 3) => Reschedule,
      lookup: (key: u8|string) => Promise<BlockID|void>,
      index: (key: u8|string) => void,
      signal: (name: string, payload: any) => void
    }} ComputeContext
    @typedef {(value: any, context: DispatchContext) => Promise<Rejection|Reschedule|any>} ComputeFunction

    @typedef {{ [SymSignal]: true, type: string, payload: any, objId: u8, rootName: string }} SignalInterrupt
 */

/**
 * A memory slice of the decentralized state.
 * Create a subclass of Memory to compute your own
 * distributed collection and define the rules to how it should
 * behave.
 */
export class Memory {
  initialValue = undefined
  #store = null
  #cache = {} // in-memory cached state
  #version = 0
  #head = undefined
  /** @type {function[]} */
  #observers = []

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

  _reset () {
    this.#cache = {}
    this.#version = 0
    this.#head = undefined
  }

  /** @return {Store} */
  get store () { return this.#store }
  get state () { return tripWire(this.#cache) }

  /// Begin hooks

  /** @param {{ CHAIN: Signature, AUTHOR: PublicKey}} ctx
    * @return {Promise<ObjectId>} */
  async idOf ({ CHAIN, AUTHOR }) { return this.store.repo.allowDetached ? CHAIN : AUTHOR }

  /** @param {any} value The current state
   *  @param {(root:string, id: ObjectId) => number} latch Tell garbage collector to latch this object's expiry onto another object.
   *  @return {Promise<number|Infinity>} */
  async expiresAt (value, latch) { return Infinity }

  /** @type {ComputeFunction} */
  async compute (value, ctx) { throw new Error('Memory.compute(ctx: ComputeContext) => draft; must be implemented by subclass') }

  /// End of hooks

  async readState (id) {
    if (id instanceof Uint8Array) id = toHex(id)
    return id in this.#cache
      ? this.#cache[id]
      : clone(this.initialValue)
  }

  async hasState (id) {
    if (id instanceof Uint8Array) id = toHex(id)
    return id in this.#cache
  }

  async _writeState (id, o) {
    if (id instanceof Uint8Array) id = toHex(id)
    this.#cache[id] = o
    this._notify('change', { root: this.name, id, value: o })
  }

  async _sweepState (id) {
    if (id instanceof Uint8Array) id = toHex(id)
    delete this.#cache[id]
    this._notify('change', { root: this.name, id, value: undefined })
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

  /** @param {DispatchContext} ctx Context containing identified chain tags
    * @return {Promise<Rejection|Reschedule|Array<SignalInterrupt>>} Store alteration result */
  async _validateAndMutate (ctx) {
    const { block, AUTHOR, CHAIN } = ctx
    // TODO: this is not wrong but CHAIN and AUTHOR is mutually exclusive in picorepo ATM.
    const blockRoot = this.store.repo.allowDetached ? CHAIN : AUTHOR
    const id = await this.idOf(ctx) // ObjId
    // ctx.id = id // Safe to pollute I guess. Maybe not necessary?
    const value = tripWire(await this.readState(id)) // TODO: Copy|ICE?
    // Phase0: Set up mutually exclusive rejection/postpone flow-control.
    /** @type {Rejection|Reschedule|undefined} */
    let abortMerge

    const reject = message => {
      if (abortMerge) throw new Error(`Merge already rejected: ${abortMerge.message}`)
      abortMerge = { [SymReject]: true, message, silent: !message }
      return abortMerge
    }
    const postpone = (time, message, retries = 3) => {
      if (abortMerge) throw new Error(`Merge already rejected: ${abortMerge.message}`)
      abortMerge = { [SymPostpone]: true, message, time, retries }
      return abortMerge
    }

    const signals = []
    const indices = []
    // Phase1: Compute State change
    const computeContext = /** @type {ComputeContext} */ {
      ...ctx,
      id,
      reject,
      postpone,
      index: key => indices.push(key), // Index op is post-apply
      lookup: async key => this.#lookup(key), // Lookups are immediate
      // Signals are always exected after a block
      signal: (type, payload) => signals.push({ [SymSignal]: true, type, payload, objId: id, rootName: this.name })
    }

    const draft = await this.compute(value, computeContext)

    if (abortMerge) return abortMerge

    // Phase2: Apply the draft and run indice-ops
    await this._writeState(id, draft)

    for (const key of indices) {
      await this.#index(key, block.id)
    }

    // Phase3: Schedule deinitialization & increment reference counts
    const latch = this.store._latchExpireAt.bind(this.store, 0)
    const exp = await this.expiresAt(draft, latch) // latchpatch
    if (!Number.isFinite(exp) && exp !== Infinity) throw new Error(`Expected ${this.name}.expiresAt() to return number|Infinity, got ${exp}`)
    if (exp !== Infinity) await this.store._gc.schedule(this.name, blockRoot, id, exp)
    await this.store._refIncrement(blockRoot, this.name, id)
    await this._incrState(block.id) // TODO: IncrState once on unlock
    // Complete
    return signals
  }

  async _gc_visit (gcDate, memo) {
    const { root, id, blockRoot } = memo
    if (root !== this.name) throw new Error(`WrongCollection: ${root}`)
    if (!(await this.hasState(id))) return // console.info('_gc_visit', this.name, toHex(toU8(id)), 'already swept')
    const value = await this.readState(id)
    const latch = this.store._latchExpireAt.bind(this.store, 0)
    const exp = await this.expiresAt(value, latch) // latchpatch
    if (!Number.isFinite(exp) && exp !== Infinity) throw new Error(`Expected ${this.name}.expiresAt() to return number|Infinity, got ${exp}`)
    // console.info('_gc_visit', this.name, exp - gcDate, value)
    if (exp <= gcDate) {
      await this._sweepState(id)
      this.#version++
      return await this.store._refDecrement(blockRoot, this.name, id)
    } else if (exp !== Infinity) { // Reschedule
      // console.log('Rescheduling', this.name, toHex(blockRoot), id, exp)
      await this.store._gc.schedule(this.name, blockRoot, id, exp)
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
    this._notify('change', { root: this.name })
  }

  /**
   * @param {SignalInterrupt} signal an interrupt/signal
   * @param {DispatchContext} ctx Dispatch context
   */
  async _trap (signal, ctx) {
    // @ts-ignore
    if (typeof this.trap !== 'function') return
    const { type, payload } = signal
    let didMutate = false
    const mutate = async (id, callback) => {
      const value = await this.readState(id)
      const draft = await callback(value)
      if (typeof draft === 'undefined') throw new Error('Expected mutate#callback() to return new state')
      await this._writeState(id, draft)
      didMutate = true
    }
    // TODO: signals/interrupt cannot increase refcounts as we do not provide any anti-signals
    // @ts-ignore
    await this.trap(type, payload, mutate)
    if (didMutate) await this._incrState(ctx.block.id) // TODO: IncrState once on unlock
  }

  /**
   * Creates a new block that targets this collection.
   * Use this function in your actions to apply changes
   * to local state.
   *
   * @param {BlockID|Feed} branch // TODO formalize actual input
   * @param {any} payload Your state-changes/instructions
   * @param {SecretKey} secret Signing secret
   * @return {Promise<Feed>} Feed containing merged blocks
   */
  async createBlock (branch, payload, secret) {
    const patch = await this.formatPatch(branch, payload, secret)
    return await this.store.dispatch(patch, true)
  }

  /**
   * Same as createBlock except does not dispatch.
   * used for simulating scenarios in tests.
   * use createBlock in production.
   * @param {BlockID|Feed} branch
   * @param {any} payload Your state-changes/instructions
   * @param {SecretKey} secret Signing secret
   * @return {Promise<Feed>} Feed containing merged blocks
  */
  async formatPatch (branch, payload, secret) {
    const feed = await this.store.readBranch(branch)
    // TODO: move Date and Collection name to BlockHeaders
    feed.append(encode([this.name, payload, Date.now()]), secret)
    return feed
  }

  // TODO: we are able to do filtered subscriptions now with the _notify function.
  sub (observer) {
    if (typeof observer !== 'function') throw new Error('observer must be a function')
    this.#observers.push(observer)
    observer(this.state)
    return () => this.#observers.splice(this.#observers.indexOf(observer), 1)
  }

  _notify (event, payload) {
    const state = this.state
    for (const cb of this.#observers) cb(state)
    this.store._notify(event, payload) // forward
  }
}

/**
 * This memory model comes with a built-in
 * block creation function: 'mutate()'
 * making it easy to sync changes across peers
 * The initialValue must be an Object because
 * because this memory automatically maintains `_created` and `_updated`,
 * which must be treated as read-only
 */
export class DiffMemory extends Memory {
  initialValue = {}

  /** @override */
  async compute (value, computeContext) {
    const { payload, date } = computeContext
    const draft = applyPatch(value, payload)
    draft._updated = date
    draft._created ||= draft._updated
    // console.info('CRDTMemory.compute() draft:', draft, 'data:', payload)
    const res = await this.validate(draft, { previous: value, ...computeContext })

    // if (res === true) return computeContext.reject() // I don't like this, does anyone like this?

    // This should prevent validate from doing
    // anything else but validating.
    if (res && (res[SymPostpone] || res[SymReject])) return res
    else return draft
  }

  /**
   * Mutates the decentralized state of this collection.
   * @param {BlockID|Feed} branch
   * @param {(value, MutationContext) => Promise<value>} mutFn
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

    // TODO: MutationContext as second param to mutFn
    const nValue = await mutFn(Object.freeze(oValue))
    if (typeof nValue === 'undefined') throw new Error('Expected mutate to return new state, received undefined!')
    const diff = formatPatch(oValue, nValue) // TODO: rename to payload
    return this.createBlock(branch, diff, secret)
  }

  /**
   * @param {any} value
   * @param {ComputeContext&{ previous: any }} context
   * @return {Promise<Rejection|boolean>}
   */
  async validate (value, context) {
    // accept all override for diff behaviour
    return false
  }
}

/**
 * @param {hexstring|Uint8Array} blockRoot
 * @param {string|0|Infinity} stateRoot
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

export class Store {
  /** @type {Record<string, Memory>} */
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

  /**
   * @typedef {import('picorepo').BinaryLevel} BinaryLevel
   * @typedef {import('picorepo').MergeStrategy} MergeStrategy
   * @typedef {{
   *  allowDetached?: boolean,
   *  strategy?: MergeStrategy,
   *  mempoolDB?: BinaryLevel
   * }} StoreOptions
   *
   * @param {BinaryLevel} db Database to use for persistance
   * @param {StoreOptions} options
   */
  constructor (db, options = {}) {
    this.repo = Repo.isRepo(db) ? db : new Repo(db)
    this.cache = new Mempool(options?.mempoolDB || this.repo._db)
    this._gc = new Scheduler(this.repo)
    this._strategy = /** @type {MergeStrategy} */ options?.strategy || (() => false)
    this.repo.allowDetached = options?.allowDetached || false
    /** @type {import('abstract-level').AbstractSublevel} */
    this._refReg = this.repo._db.sublevel('REFs', {
      keyEncoding: 'view',
      valueEncoding: 'view'
    })
  }

  /**
   * Initializes a managed decentralized memory area
   * @param {string} name - The name of the collection
   * @param {new (...args: any[]) => Memory} MemoryClass - Constructor of Memory or subclass
   * @return {Memory} - An instance of the MemorySubClass
   */
  register (name, MemoryClass) {
    if (this._loaded) throw new Error('Hotconf not supported')
    if (!(MemoryClass.prototype instanceof Memory)) throw new Error('Expected a subclass of Memory')
    this.roots[name] = new MemoryClass(this, name)
    return this.roots[name]
  }

  #tap = null
  _notify (event, payload) {
    // console.info('event:', event, payload.root)
    if (typeof this.#tap === 'function') this.#tap(event, payload)
  }

  /**
   * Tap into internal eventlog, useful for debugging.
   * @type {(cb: (event:string, payload: any) => void) => void}
   */
  tap (cb) {
    if (typeof cb !== 'function') throw new Error('Expected callback to be function')
    this.#tap = cb
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
      const evicted = await this.repo.rollback(blockRoot, null)
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

  /** Debug utility; dumps reference registry to console */
  async _dumpReferences (log = console.info) {
    const iter = this._refReg.iterator()
    for await (const [key, value] of iter) {
      log(toHex(key.subarray(0, 32)), '=>', decode(value))
    }
  }

  /** Fetches a writable feed
   *  @param {undefined|Feed|BlockID|PublicKey} branch
    * @return {Promise<Feed>} */
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

    await locks.request('default', null, async () => {
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
   * @param {Feed|Block} patch Incoming blocks with data
   * @param {boolean} loud Throws errors on invalid blocks
   * @return {Promise<Feed|undefined>} merged blocks
   */
  async dispatch (patch, loud = false) {
    return this._lockRun(() => this._dispatch(patch, loud))
  }

  /** @return {Promise<Feed|undefined>} merged blocks */
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
      // TODO: minor bug, this error gets thrown when patch === local
      // correct way is to only throw if local.diff(patch) throws
      // instead of returning a number.
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

    const diff = !local.length ? patch.length : local.diff(patch)
    if (diff < 1) return // Patch contains equal or less blocks than local

    let parentBlock = null
    const _first = patch.blocks[patch.blocks.length - diff]
    if (!_first.genesis && !local.length) {
      if (loud) throw new Error('NoCommonParent')
      return
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

    // Merge DSTATE-roots
    const merged = []
    for (const block of patch.blocks.slice(-diff)) {
      const data = decode(block.body) // TODO: [collection, payload, date] // date is last because it's gonna be moved to BLOCK:POP8-header
      if (!Array.isArray(data)) { console.error('Block.body:', data); throw new Error('UnrecognizedBlockFormat') }
      const [root, payload, date] = data
      const collection = this.roots[root]

      if (!collection) throw new Error('UnknownCollection: ' + root)
      /** @type {DispatchContext} */
      const dispatchContext = { root, payload, date, block, parentBlock, ...tags }
      const success = await this._processBlock(collection, dispatchContext, loud)
      if (!success) break // Abort merge on first failed block
      parentBlock = block
      merged.push(block)
    }
    if (merged.length) return Feed.from(merged)
  }

  /**
   * Logic used by dispatch & reload
   * @param {Memory} collection
   * @param {DispatchContext} dispatchContext
   * @return {Promise<boolean>}
   */
  async _processBlock (collection, dispatchContext, loud = false, skipMerge = false) {
    const { block, root } = dispatchContext
    const res = await collection._validateAndMutate(dispatchContext)
    // console.log('_processBlock() res', res)
    if (!Array.isArray(res)) { // If not Array<Signal> it's an error
      if (res[SymReject]) {
        // Loud is set when locally produced blocks are dispatched / Do not allow failing blocks to be created
        // res.silent is a by-product of old scheme where all MemorySlices always processed all blocks.
        // this has been changed in 3.x | TODO: remove rejection.silent (no such thing)
        // if (loud && !res.silent) throw new Error(`InvalidBlock: ${res.message}`)
        if (loud) throw new Error(`InvalidBlock: ${res.message}`)
      } else if (res[SymPostpone]) {
        console.error('TODO: Implement retries of postponed blocks')
        // TODO: schedule block-id with gc/scheduler to reattempt
        // merge in validationError.time -seconds.
        // usecase: Missing cross-chain weak-reference. (action depends on secondary profile or something)
      }
      return false
    }

    if (!skipMerge) {
      const nMerged = await this.repo.merge(block, this._strategy) // TODO: batch merge?
      // nMerged is 0 when repo rejects a block, at this stage it must not be rejected.
      if (!nMerged) throw new Error('[CRITICAL] MergeStrategy failed! state and blockstore is desynced. Fix bug and store.reload()')
      this._notify('merged', { block, root })
    }

    // Last phase run signals
    for (const sig of res) {
      // Signals are broadcast across all collections
      for (const name in this.roots) this.roots[name]._trap(sig, dispatchContext)
    }
    return true
  }

  /**
   * Restores all registers to initial values and re-applies all mutations from database.
   */
  async reload () {
    return this._lockRun(async () => {
      const feeds = await this.repo.listFeeds()
      // Reset all states
      for (const root in this.roots) this.roots[root]._reset()
      const modified = new Set()
      for (const { key: ptr, value: CHAIN } of feeds) {
        const feed = await this.repo.loadFeed(ptr)
        const tags = { AUTHOR: toHex(feed.first.key), CHAIN: toHex(CHAIN) }
        let parentBlock = null

        for (const block of feed.blocks) {
          const [root, payload, date] = decode(block.body)
          const collection = this.roots[root]
          console.info('Replaying: ', root, payload, date)
          /** @type{DispatchContext} */
          const dispatchContext = { root, payload, date, block, parentBlock, ...tags }
          const success = await this._processBlock(collection, dispatchContext, true, true) // Force loud & skip repo-remerge
          if (!success) throw new Error('InvalidBlock during reload')
          parentBlock = block
          this._notify('reload', { block, root })
          modified.add(root)
        }
      }
      return Array.from(modified) // old api
    })
  }

  // Nuro Shortcut
  $ (name) { return sub => this.on(name, sub) }

  on (name, ...args) {
    const collection = this.roots[name]
    if (!collection) throw new Error(`No collection named "${name}"`)
    return collection.sub(...args)
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

  async _latchExpireAt (depth, rootName, id) {
    if (depth > 5) return Infinity
    // console.log('Latch, called', rootName, id)
    if (id instanceof Uint8Array) id = toHex(id)
    const root = /** @type {Memory} */ this.roots[rootName]
    if (!root) throw new Error(`Cannot latch, memory ${rootName} does not exist`)
    if (!(await root.hasState(id))) return 0 // Object is gone
    const value = await root.readState(id)
    const latch = this._latchExpireAt.bind(this, ++depth)
    const exp = await root.expiresAt(value, latch) // latchpatch
    if (!Number.isFinite(exp) && exp !== Infinity) throw new Error(`Expected ${rootName}.expiresAt() to return number|Infinity, got ${exp}`)
    return exp
  }
}

function assert (c, m) { if (!c) throw new Error(m || 'AssertionError') }

// Weblocks shim when navigator||Lockmanager is missing
const locks = (
  globalThis.navigator?.locks
) || {
  resources: {},
  async request (resource, options, handler) {
    const callback = /** @type {function} */ handler || options
    // @ts-ignore
    const resources = locks.resources
    if (!resources[resource]) resources[resource] = Promise.resolve()
    const prev = resources[resource]
    resources[resource] = prev
      .catch(err => console.warn('Previous lock unlocked with error', err))
      .then(callback)
    await prev
  }
}

/** @return {[Promise, (v: any) => void, (e: Error) => void]} */
export function unpromise () {
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
  if (typeOf(a) !== typeOf(b)) throw new Error(`Dynamic typeswitching not supported, ${typeOf(a)} ~> ${typeOf(b)}`)
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
    case 'string': // eslint-disable-line no-fallthrough
    case 'u8': // eslint-disable-line no-fallthrough
      return b
  }
}
/**
 * @return {Object} a new instance with the patch applied */
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
    case 'string': // eslint-disable-line no-fallthrough
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

// Merge with ice.js
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
      throw new Error(`Attempted to modifiy: ${path.join('.')}[${String(key)}]`)
    }
  })
}

export function clone (o) { return decode(encode(o)) }
