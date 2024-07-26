import { Repo } from 'picorepo'
import { Feed, getPublicKey, toHex, toU8, s2b, cmp } from 'picofeed'
// import { init, get } from 'piconuro'
import Cache from './cache.js'
import GarbageCollector from './gc.js' // TODO: rename to scheduler
import { encode, decode } from 'cborg' // @ts-ignore
import { ice, thaw } from './ice.js'

/** @typedef {string} hexstring */
/** @typedef {hexstring} Author */
/** @typedef {hexstring} Signature */
/** @typedef {Signature} Chain */
/** @typedef {Author|Chain|Signature} RootID */
/** @typedef {import('picofeed').SecretKey} SecretKey */
/** @typedef {import('picofeed').Block} Block */


/**
  @typedef {{
    id: RootID|number,
    blockRoot: RootID,
    payload: any,
    date: number,
    block: Block,
    parentBlock: Block,
    AUTHOR: Author,
    CHAIN: Signature
  }} ReducerContext

  @typedef {{
    initialValue?: any,
    id: () => hexstring|number,
    validate: (ValidationContext) => Promise<string?>,
    expiresAt: () => number
    reduce?: (state: any, payload: any, ctx: ReducerContext) => Promise<any>
  }} CollectionSpec */

/**
 * A reference-counting dstate manager
 */
class Collection {
  version = 0
  head = undefined
  state = {}
  /**
   * @param {Store} store
   * @param {string} name
   * @param {CollectionSpec} config
   */
  constructor (store, name, config = {}) {
    this.store = store
    this.name = name
    this.config = {
      id: ({ AUTHOR, CHAIN }) => store.repo.allowDetached ? CHAIN : AUTHOR,
      validate: () => false,
      expiresAt: () => -1, // non-perishable
      ...config
    }
    if (config.reduce && typeof config.reduce !== 'function') throw Error('Expected reduce to be a function')
  }

  get hasCustomReducer () { return !!this.config.reduce }

  /**
   * Mutates the decentralized state of this collection.
   * @param {Chain|BlockId|Feed|undefined} branch TODO: replace with Chain or null == AUTHOR
   * @param {(value, MutationContext) => value} mutFn
   * @param {SecretKey} secret Signing secret
   * @return {Promise<Feed>} the new branch
   */
  async mutate (branch, mutFn, secret) {
    const AUTHOR = getPublicKey(secret)
    branch = !branch
      ? new Feed() // create new
      : Feed.isFeed(branch)
        ? branch
        : await this.readBranch(branch)
    // TODO: get chain id from repo incase partial feed was passed
    const CHAIN = branch.first?.genesis ? toHex(branch.first.id) : undefined
    // TODO: stateId/reduce/sweep should be interchangeable. (support different mem/transition models)
    const stateRoot = this.config.id({ AUTHOR, CHAIN })

    let payload = null // payload
    if (!this.hasCustomReducer) {
      const oValue = await this.readState(stateRoot)
      if (typeof mutFn !== 'function') throw new Error('Expected parameter mutFn to be a function')
      const nValue = await mutFn(ice(oValue), {})
      payload = formatPatch(oValue, thaw(nValue))
    } else {
      payload = mutFn
      if (typeof payload === 'function') throw new Error('Expected payload to be an object or primitive, got function')
    }

    branch.append(encode([this.name, Date.now(), payload]), secret)
    return await this.store.dispatch(branch, true)
  }

  async readState (id) {
    return id in this.state
      ? this.state[id]
      : this.config.initialValue
  }

  async writeState (id, o) {
    this.state[id] = o
    this.store._notify('change', { root: this.name, id, value: o })
  }

  async sweepState (id) {
    delete this.state[id]
    this.store._notify('change', { root: this.name, id, value: undefined })
  }

  /** @param {RootID} id */
  async readBranch (id) {
    return this.store.repo.allowDetached
      ? this.store.repo.loadFeed(id)
      : this.store.repo.loadHead(id) // TODO: deprecate loadHead?
  }

  async _validateAndMutate (ctx) {
    const {
      payload,
      date,
      AUTHOR,
      CHAIN,
      block
    } = ctx
    const blockRoot = this.store.repo.allowDetached ? CHAIN : AUTHOR
    const id = this.config.id({ blockRoot, ...ctx })
    const ectx = { id, blockRoot, ...ctx }

    const value = await this.readState(id)
    let mValue = null
    if (!this.hasCustomReducer) {
      mValue = applyPatch(value, payload)
    } else {
      // throw new Error('Not implemented')
      mValue = thaw(await this.config.reduce(ice(value, true), payload, ectx))
      if (typeof mValue === 'undefined') mValue = thaw(value)
      if (typeof mValue === 'undefined' || mValue === null) throw new Error('No Changes Detected')
    }

    ectx.previous = value

    const err = await this.config.validate(mValue, ectx)
    if (err) return err
    await this.writeState(id, mValue)
    ectx.value = mValue
    // Schedule GC & Ref
    const expiresAt = this.config.expiresAt(date, ectx)
    await this.store._refIncrement(blockRoot, this.name, id)
    if (expiresAt !== -1) await this.store._gc.schedule(this.name, blockRoot, id, expiresAt)
    await this._incrState(block.id)
  }

  async _gc_visit (gcDate, memo) {
    const { root, id, date, blockRoot } = memo
    if (root !== this.name) throw new Error(`WrongCollection: ${root}`)
    const expiresAt = this.config.expiresAt(date, memo)
    if (expiresAt <= gcDate) {
      await this.sweepState(id)
      this.version++
      return await this.store._refDecrement(blockRoot, this.name, id)
    } else if (expiresAt !== -1) { // Reschedule
      await this.store._gc.schedule(this.name, blockRoot, id, expiresAt)
    }
  }

  async _incrState (head) {
    this.head = head
    this.version++
    await this.store.repo.writeReg(s2b(`STATES/${this.name}`), encode({
      version: this.version,
      head: this.head,
      state: this.state
    }))
  }

  async _loadState () {
    const dump = await this.store.repo.readReg(s2b(`STATES/${this.name}`))
    if (!dump) return
    const { version, head, state } = decode(dump)
    this.state = state
    this.version = version
    this.head = head
    this.store._notify('change', { root: this.name })
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
  static encode = encode
  static decode = decode
  roots = {} // StateRoots
  _loaded = false
  _tap = null // global signal trap
  mutexTimeout = 10000
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
   * @return {Collection}
   */
  spec (name, config) {
    if (config && this._loaded) throw new Error('Hotconf not supported')
    const c = new Collection(this, name, config)
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
      const [root, date, payload] = decode(block.body) // TODO: Support multi-root blocks?
      const collection = this.roots[root]
      if (!collection) throw new Error('UnknownCollection: ' + root)
      const validationError = await collection._validateAndMutate({
        root,
        payload,
        date,
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
      this._notify('merged', { block, root })
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
          const [root, date, payload] = decode(block.body) // TODO: Support multi-root blocks?
          const collection = this.roots[root]
          if (!collection) throw new Error('UnknownCollection' + root)
          const validationError = await collection._validateAndMutate({
            root,
            date,
            payload,
            block,
            parentBlock,
            ...tags
          })
          if (validationError) throw new Error(`InvalidBlock during reload: ${validationError}`)
          this._notify('reload', { block, root })
          parentBlock = block
          modified.add(root)
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
