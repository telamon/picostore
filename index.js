const PicoRepo = require('picorepo')
const Feed = require('picofeed')
const Cache = require('./cache.js')
const { Packr, pack, unpack } = require('msgpackr')
const STRUCTS = 'msgpackr::structs'

class PicoStore {
  constructor (db, mergeStrategy) {
    this.repo = PicoRepo.isRepo(db) ? db : new PicoRepo(db)
    this.cache = new Cache(this.repo._db)
    this._strategy = mergeStrategy || (() => {})
    this._stores = []
    this._loaded = false
    this._tap = null // global sigint trap
    this._packr = null
    this.mutexTimeout = 5000
  }

  register (name, initialValue, validator, reducer, trap) {
    if (this._loaded) throw new Error('register() must be invoked before load()')
    if (typeof name !== 'string') {
      return this.register(name.name, name.initialValue, name.filter, name.reducer, name.trap)
    } else if (typeof initialValue === 'function') {
      return this.register(name, undefined, initialValue, validator)
    }

    this._stores.push({
      name,
      validator,
      reducer,
      trap,
      version: 0,
      head: undefined,
      value: initialValue,
      initialValue,
      observers: new Set()
    })
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

      for (const store of this._stores) {
        const head = await this.repo.readReg(`HEADS/${store.name}`)
        if (!head) continue // Empty
        store.head = head
        store.version = this._decodeValue(await this.repo.readReg(`VER/${store.name}`))
        store.value = this._decodeValue(await this.repo.readReg(`STATES/${store.name}`))
        for (const listener of store.observers) listener(store.value)
      }
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
    patch = Feed.from(patch)

    let local = null // Target branch to merge to
    try {
      const sig = patch.first.isGenesis
        ? patch.first.sig
        : patch.first.parentSig
      local = await this.repo.resolveFeed(sig)
    } catch (err) {
      if (err.message !== 'FeedNotFound') throw err
      if (patch.first.isGenesis) local = new Feed() // Happy birthday!
      else {
        await this.cache.push(patch) // Stash for later
        // throw new Error('UnknownTargetBranch')
        return []
      }
    }

    // Local reference point exists,
    // attempt to extend patch from blocks in cache
    for (const block of patch.blocks()) {
      const [bwd, fwd] = await this.cache.pop(block.parentSig, block.sig)
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
    if (local.first?.isGenesis) {
      tags.HEAD = local.first.key
      tags.CHAIN = local.first.sig
    } else if (!local.length && patch.first.isGenesis) {
      tags.HEAD = patch.first.key
      tags.CHAIN = patch.first.sig
    }

    const diff = !local.length ? patch.length : local._compare(patch) // WARNING untested internal api.
    if (diff < 1) return [] // Patch contains equal or less blocks than local

    const modified = new Set()
    const root = this.state

    let parentBlock = null
    const _first = patch.get(-diff)
    if (!_first.isGenesis && !local.length) {
      if (loud) throw new Error('NoCommonParent')
      return []
    } else if (!_first.isGenesis && local.length) {
      const it = local.blocks()
      let res = it.next()
      while (!parentBlock && !res.done) { // TODO: avoid loop using local.get(something)
        if (res.value.sig.equals(_first.parentSig)) parentBlock = res.value
        res = it.next()
      }
      if (!parentBlock) {
        throw new Error('ParentNotFound') // real logical error
      }
    }
    let nMerged = 0
    for (const block of patch.blocks(-diff)) {
      const accepted = []
      for (const store of this._stores) {
        if (typeof store.validator !== 'function') continue
        let validationError = store.validator({
          block,
          parentBlock,
          state: store.value,
          root,
          ...tags
        })

        if (!validationError) accepted.push(store) // no error, proceed.
        else if (validationError === true) {
          // NO-OP
          // Returning 'true' from a validator explicitly means silent ignore.
        } else if (loud) { // throw validation errors
          if (typeof validationError === 'string') validationError = new Error(`InvalidBlock: ${validationError}`)
          throw validationError
        }
      }
      if (!accepted.length) break // reject rest of chain if block not accepted
      const mod = await this._mutateState(block, parentBlock, tags, false, loud)

      for (const s of mod) modified.add(s)
      parentBlock = block
      nMerged++
    }
    this._notifyObservers(modified)

    // TODO: Clean this up, patch needs
    // to be exported to RPC for transmission
    const m = Array.from(modified)
    Object.defineProperty(m, 'patch', {
      enumerable: false,
      get () { return patch.slice(-diff, -diff + nMerged) }
    })

    return m
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
      const root = this.state
      const val = store.reducer({ block, parentBlock, state: store.value, root, signal, ...tags })
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
          HEAD: part.first.key,
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
}

module.exports = PicoStore

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
