const PicoRepo = require('picorepo')
const Feed = require('picofeed')

class PicoStore {
  constructor (db, mergeStrategy) {
    this.repo = db instanceof PicoRepo ? db : new PicoRepo(db)
    this._strategy = mergeStrategy || (() => {})
    this._stores = []
    this._loaded = false
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
    if (this._loaded) return
    for (const store of this._stores) {
      const head = await this.repo.readReg(`HEADS/${store.name}`)
      if (!head) continue // Empty
      store.head = head
      store.version = decodeValue(await this.repo.readReg(`VER/${store.name}`))
      store.value = decodeValue(await this.repo.readReg(`STATES/${store.name}`))
      for (const listener of store.observers) listener(store.value)
    }
    this._loaded = true
  }

  async dispatch (patch) {
    const modified = []
    patch = Feed.from(patch)
    // Check if head can be fast-forwarded
    const local = (await this.repo.loadHead(patch.last.key)) || new Feed()
    let n = 0
    const canMerge = local.merge(patch, (block, abort) => {
      let valid = true
      for (const store of this._stores) {
        if (typeof store.validator === 'function') {
          valid = valid && !store.validator({ block, state: store.value })
          if (!valid) abort()
        }
      }
      if (valid) n++
    })
    if (!canMerge) return modified

    const mutations = local.slice(-n)

    for (const block of mutations.blocks()) {
      const mod = await this._mutateState(block)
      for (const s of mod) {
        if (!~modified.indexOf(s)) modified.push(s)
      }
    }
    return modified
  }

  async _mutateState (block) {
    const modified = []
    for (const store of this._stores) {
      if (typeof store.validator === 'function') {
        if (store.validator({ block, state: store.value })) return modified
      }

      if (typeof store.reducer !== 'function') continue
      const val = store.reducer({ block, state: store.value })
      if (typeof val === 'undefined') continue
      await this.repo.merge(block, this._strategy)
      await this.repo.writeReg(`STATES/${store.name}`, encodeValue(val))
      await this.repo.writeReg(`HEADS/${store.name}`, block.sig)
      await this.repo.writeReg(`VER/${store.name}`, encodeValue(store.version++))
      // who needs a seatbelt anyway? let's save some memory.
      store.value = val // Object.freeze(val)
      store.head = block.sig
      for (const listener of store.observers) listener(store.value)
      if (!~modified.indexOf(store.name)) modified.push(store.name)
    }
    return modified
  }

  get state () {
    return this._stores.reduce((state, store) => {
      state[store.name] = store.value
      return state
    }, {})
  }

  async reload () {
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
        for (const block of part.blocks()) {
          const mods = await this._mutateState(block)
          for (const s of mods) {
            if (!~modified.indexOf(s)) modified.push(s)
          }
          // if (block.isGenesis) done = true
          // else ptr = some other reference
        }
        done = true
      }
    }
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
}

function encodeValue (val) {
  return JSON.stringify(val)
}

function decodeValue (val) {
  return JSON.parse(val)
}

module.exports = PicoStore
