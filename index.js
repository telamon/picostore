const PicoRepo = require('picorepo')
const Feed = require('picofeed')
const { Packr, pack, unpack } = require('msgpackr')
const STRUCTS = 'msgpackr::structs'
class PicoStore {
  constructor (db, mergeStrategy) {
    this.repo = PicoRepo.isRepo(db) ? db : new PicoRepo(db)
    this._strategy = mergeStrategy || (() => {})
    this._stores = []
    this._loaded = false
    this._mutex = Promise.resolve(0)
    this._tap = null // global sigint trap
    this._packr = null
    this.mutexTimeout = 5000
  }

  async _waitLock () {
    const current = this._mutex
    let release, fail
    const next = new Promise((resolve, reject) => { release = resolve; fail = reject })
    this._mutex = next
    let timeoutError = null
    try { throw new Error('MutexTimeout') } catch (err) { timeoutError = err }
    const timerId = setTimeout(() => {
      console.error('MutexTimeout', timeoutError.stack)
      fail(timeoutError)
    }, this.mutexTimeout)
    await current
    return () => {
      clearTimeout(timerId)
      release()
    }
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

  async load () {
    const unlock = await this._waitLock()
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
    unlock()
  }

  /**
   * Mutates the state, if a reload is in progress the dispatch will wait
   * for it to complete.
   */
  async dispatch (patch, loud) {
    const unlock = await this._waitLock()
    try { // Ensure mutex release on error
      const res = await this._dispatch(patch, loud)
      unlock()
      return res
    } catch (err) {
      unlock()
      throw err
    }
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
      if (err.message !== 'Unknown feed') throw err
      if (!patch.first.isGenesis) throw new Error('Unknown target branch')
      local = new Feed() // Happy birthday!
    }

    // TODO: Local can be undef, happens when blocks are sent
    // sparsely for feeds that already been GC'd or yet unknown.
    // Should be fixed somehow using a temporary scratch bucket
    // where unmergable blocks are held until chains are resolved.

    // canMerge?
    if (!local.clone().merge(patch)) {
      if (loud) throw new Error('UnmergablePatch')
      else return []
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

    for (const block of patch.blocks(-diff)) {
      const accepted = []
      for (const store of this._stores) {
        if (typeof store.validator !== 'function') continue
        let validationError = store.validator({ block, parentBlock, state: store.value, root })

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
      const mod = await this._mutateState(block, parentBlock, false, loud)

      for (const s of mod) modified.add(s)
      parentBlock = block
    }
    this._notifyObservers(modified)
    return Array.from(modified)
  }

  async _mutateState (block, parentBlock, dryMerge = false, loud = false) {
    const modified = []

    // Generate list of stores that want to reduce this block
    const stores = []
    for (const store of this._stores) {
      if (typeof store.validator !== 'function') continue
      if (typeof store.reducer !== 'function') continue
      const root = this.state
      const rejected = store.validator({ block, parentBlock, state: store.value, root })
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
      // TODO: maybe push block to stupid cache at this point
      // to avoid discarding an out of order block
    }
    // Interrupts are buffered until after the mutations have run
    const interrupts = []
    const signal = (i, p) => interrupts.push([i, p])

    // Run all state reducers
    for (const store of stores) {
      // If repo accepted the change, apply it
      const root = this.state
      const val = store.reducer({ block, parentBlock, state: store.value, root, signal })
      if (typeof val === 'undefined') console.warn('Reducer returned `undefined` state.')
      await this._commitHead(store, block.sig, val)
      modified.push(store.name)
    }

    // Run all traps in signal order
    // NOTE: want to avoid reusing term 'signal' as it's a function
    // in reducer context, candidates: code/type/event ('type' already overused)
    for (const [code, payload] of interrupts) {
      for (const store of this._stores) {
        if (typeof store.trap !== 'function') continue
        const root = this.state
        const val = store.trap({ code, payload, block, parentBlock, state: store.value, root })
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
    const unlock = await this._waitLock()
    try {
      const modified = []
      const peers = await this.repo.listHeads()

      for (const store of this._stores) {
        store.value = store.initialValue
        store.version = 0
        store.head = undefined
        for (const listener of store.observers) listener(store.value)
      }

      // for (const { key, value: ptr } of peers) {
      for (const { value: ptr } of peers) {
        let done = false
        while (!done) {
          const part = await this.repo.loadFeed(ptr)
          // TODO: Multiparent resolve chains and prioritize
          // paths that lead to `key` (peer id) genesis
          let parentBlock = null
          for (const block of part.blocks()) {
            const mods = await this._mutateState(block, parentBlock, true)

            for (const s of mods) {
              if (!~modified.indexOf(s)) modified.push(s)
            }
            // if (block.isGenesis) done = true
            // else ptr = some other reference
            parentBlock = block
          }
          done = true
        }
      }

      // TODO: considered to release mutex with error if occurs within reload this scope,
      // but there is no engine-independent way to handle/rethrow an error
      // without clobbering stack-traces.
      // Some set stack on `throw` others on `new Error()` *shrug*
      unlock()
      this._notifyObservers(modified)
      return modified
    } catch (err) {
      unlock()
      throw err
    }
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
