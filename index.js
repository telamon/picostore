const PicoRepo = require('picorepo')
const Feed = require('picofeed')

class PicoStore {
  constructor (db, mergeStrategy) {
    this.repo = db instanceof PicoRepo ? db : new PicoRepo(db)
    this._strategy = mergeStrategy || (() => {})
    this._stores = []
    this._loaded = false
    this._acceptMutations = mutex()
  }

  register (name, initialValue, validator, reducer) {
    if (this._loaded) throw new Error('register() must be invoked before load()')
    if (typeof name !== 'string') {
      return this.register(name.name, name.initialValue, name.filter, name.reducer)
    } else if (typeof initialValue === 'function') {
      return this.register(name, undefined, initialValue, validator)
    }

    this._stores.push({
      name,
      validator,
      reducer,
      version: 0,
      head: undefined,
      value: initialValue,
      initialValue,
      observers: []
    })
  }

  async load () {
    if (this._loaded) throw new Error('Store already loaded')
    for (const store of this._stores) {
      const head = await this.repo.readReg(`HEADS/${store.name}`)
      if (!head) continue // Empty
      store.head = head
      store.version = decodeValue(await this.repo.readReg(`VER/${store.name}`))
      store.value = decodeValue(await this.repo.readReg(`STATES/${store.name}`))
      for (const listener of store.observers) listener(store.value)
    }
    this._loaded = true
    this._acceptMutations.release()
  }

  /**
   * Mutates the state, if a reload is in progress the dispatch will wait
   * for it to complete.
   */
  async dispatch (patch, loud = false) {
    if (!this._loaded) throw Error('Store not ready, call load()')
    await this._acceptMutations.lock
    const modified = []
    patch = Feed.from(patch)
    // Check if head can be fast-forwarded
    const local = (await this.repo.loadFeed(patch.last.parentSig, 1)) ||
      (await this.repo.loadHead(patch.last.key, 1)) ||
      new Feed()
    const root = this.state
    let n = 0
    let p = -1
    const canMerge = local.merge(patch, (block, abort) => {
      if (p === -1) while ((p + 1) < local.length && !local.get(++p)?.sig.equals(block.parentSig)) { (() => 'NOOP')() } // fuck
      const parentBlock = block.isGenesis ? null : local.get(p++)
      const accepted = []
      for (const store of this._stores) {
        if (typeof store.validator !== 'function') continue
        let validationError = store.validator({ block, parentBlock, state: store.value, root })

        if (!validationError) accepted.push(store) // no error, proceed.
        else { // handle validation errors
          if (typeof validationError === 'string') validationError = new Error(`InvalidBlock: ${validationError}`)
          if (loud && validationError !== true) { // Returning 'true' from a validator implicity means silent ignore.
            abort()
            throw validationError
          }
        }
      }
      if (accepted.length) n++
      else abort()
    })
    if (!canMerge) return modified

    const mutations = local.slice(-n)
    n++
    for (const block of mutations.blocks()) {
      const parentBlock = local.get(-n--)
      const mod = await this._mutateState(block, parentBlock)
      for (const s of mod) {
        if (!~modified.indexOf(s)) modified.push(s)
      }
    }
    return modified
  }

  async _mutateState (block, parentBlock, dryMerge = false) {
    const modified = []
    const root = this.state

    // Generate list of stores that want to reduce this block
    const stores = []
    for (const store of this._stores) {
      if (typeof store.validator !== 'function') continue
      if (typeof store.reducer !== 'function') continue
      const rejected = store.validator({ block, parentBlock, state: store.value, root })
      if (rejected) continue
      stores.push(store)
    }

    // Attempt repo.merge if at least one store accepts block
    if (stores.length) {
      const merged = await this.repo.merge(block, this._strategy)
      if (!dryMerge && !merged) return modified // Rejected by bucket
      // TODO: maybe push block to stupid cache at this point
      // to avoid discarding an out of order block
    }

    // Run all state reducers
    for (const store of stores) {
      // If repo accepted the change, apply it
      const val = store.reducer({ block, parentBlock, state: store.value, root })
      if (typeof val === 'undefined') console.warn('Reducer returned `undefined` state.')
      await this.repo.writeReg(`STATES/${store.name}`, encodeValue(val))
      await this.repo.writeReg(`HEADS/${store.name}`, block.sig)
      await this.repo.writeReg(`VER/${store.name}`, encodeValue(store.version++))

      // who needs a seatbelt anyway? let's save some memory.
      store.value = val // Object.freeze(val)
      store.head = block.sig
      for (const listener of store.observers) listener(store.value)
      modified.push(store.name)
    }
    return modified
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
    this._acceptMutations = mutex() // block all incoming mutations
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
    this._acceptMutations.release()
    return modified
  }

  on (name, observer) {
    const store = this._stores.find(s => s.name === name)
    if (!store) throw new Error(`No such store: "${name}"`)
    if (typeof observer !== 'function') throw new Error('observer must be a function')
    store.observers.push(observer)
    observer(store.value)
    return () => { // unsub
      store.observers.splice(store.observers.indexOf(observer), 1)
    }
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
}

function encodeValue (val) {
  return JSON.stringify(val)
}

function decodeValue (val) {
  return JSON.parse(val, (k, o) =>
    (typeof o === 'object' && o.type === 'Buffer') ? Buffer.from(o.data) : o
  )
}

function mutex () {
  let release = null
  const lock = new Promise((resolve, reject) => {
    release = err => err ? reject(err) : resolve()
  })
  if (!release) throw new Error('Mental error')
  return { lock, release }
}

module.exports = PicoStore
