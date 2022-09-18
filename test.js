const test = require('tape')
const Feed = require('picofeed')
const { MemoryLevel } = require('memory-level')
const PicoStore = require('.')
const DB = () => new MemoryLevel({
  valueEncoding: 'buffer',
  keyEncoding: 'buffer'
})

test('PicoStore', async t => {
  const db = DB()
  const store = new PicoStore(db)
  const validator = ({ state, block }) => {
    const n = JSON.parse(block.body)
    if (n <= state) return true
  }
  const reducer = ({ state, block }) => {
    return JSON.parse(block.body)
  }
  store.register('counter', 5, validator, reducer)
  await store.load()

  t.equal(store.state.counter, 5)

  // Create mutation
  const { sk } = Feed.signPair()
  const mutations = new Feed()
  mutations.append(JSON.stringify(7), sk)
  let changed = await store.dispatch(mutations)
  t.ok(changed.find(s => s === 'counter'))
  t.equal(store.state.counter, 7)

  // Try to restore previously persisted state
  const store2 = new PicoStore(db)
  store2.register('counter', 5, validator, reducer)
  await store2.load()
  t.equal(store2.state.counter, 7)

  // Disqualified mutation does not affect state
  mutations.append(JSON.stringify(2), sk)
  mutations.append(JSON.stringify(10), sk)
  changed = await store2.dispatch(mutations)
  t.equal(changed.length, 0)
  t.equal(store2.state.counter, 7)
  mutations.truncate(1) // remove bad blocks
  mutations.append(JSON.stringify(12), sk)

  changed = await store2.dispatch(mutations)
  t.equal(changed.length, 1)
  t.equal(store2.state.counter, 12)

  // Purge and rebuild state from scratch
  changed = await store.reload()
  t.equal(changed.length, 1)
  t.equal(store.state.counter, 12)

  t.end()
})

test('Hotswap repo/bucket', async t => {
  try {
    const { sk } = Feed.signPair()
    const db = DB()
    const store = new PicoStore(db)
    store.register('x', 0, () => false, ({ block }) => JSON.parse(block.body))
    await store.load()
    t.equal(store.state.x, 0)

    const mutations = new Feed()
    mutations.append(JSON.stringify(1), sk)
    mutations.append(JSON.stringify(2), sk)
    mutations.append(JSON.stringify(3), sk)
    await store.dispatch(mutations)
    t.equal(store.state.x, 3)

    const mutB = new Feed()
    mutB.append(JSON.stringify(7), sk)

    // Flush should immediatly restore initial values and
    // and swap in the new bucket while freeing
    // storage up memory in the background
    const newBucket = DB()
    const [reloaded, destroyed] = store.hotswap(newBucket)
    // Que mutations while swap/reload in progress
    const mutated = store.dispatch(mutB)
    await reloaded // Await reload, value should be 0 but mutation que will race to 7
    await mutated // Await new mutations to have been applied
    t.equal(store.state.x, 7)
    await destroyed
  } catch (e) { t.error(e) }
  t.end()
})

test('Buffers should not be lost during state reload', async t => {
  const { pk, sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)
  store.register('pk', {}, () => false, ({ state, block }) => {
    state.nested = { buf: block.key }
    state.arr = [block.key]
    state.prop = block.key
    return state
  })
  await store.load()
  const mutations = new Feed()
  mutations.append(JSON.stringify(1), sk)
  await store.dispatch(mutations)
  t.ok(pk.equals(store.state.pk.prop))
  t.ok(pk.equals(store.state.pk.nested.buf))
  t.ok(pk.equals(store.state.pk.arr[0]))

  // Open second store forcing it to load cached state

  const s2 = new PicoStore(db)
  s2.register('pk', {}, () => false, ({ state, block }) => {
    state.nested = { buf: block.key }
    state.arr = [block.key]
    state.prop = block.key
    return state
  })
  await s2.load()
  t.ok(pk.equals(s2.state.pk.prop))
  t.ok(pk.equals(s2.state.pk.nested.buf))
  t.ok(pk.equals(s2.state.pk.arr[0]))
  t.end()
})

test('Throw validation errors on dispatch(feed, loudFail = true)', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)
  // returning "true" from a validator now is a forced silent ignore
  store.register('y', 0, () => true, ({ block }) => JSON.parse(block.body))
  store.register('x', 0, () => 'do not want', ({ block }) => JSON.parse(block.body))
  await store.load()
  const mutations = new Feed()
  mutations.append(JSON.stringify(1), sk)
  try {
    await store.dispatch(mutations, true)
    t.fail('error not thrown')
  } catch (err) {
    t.equal(err.message, 'InvalidBlock: do not want')
  }
  t.end()
})

test('Parent block provided to validator', async t => {
  t.plan(35)
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)

  store.register('x', 0, () => true, () => 0) // dummy store
  store.register('y', 0,
    ({ block, parentBlock }) => {
      if (block.isGenesis) t.notOk(parentBlock, 'Genesis has no parent')
      else {
        t.ok(parentBlock, 'Parent available')
        t.ok(block.parentSig.equals(parentBlock.sig))
      }
    },
    ({ block, parentBlock }) => {
      if (block.isGenesis) t.notOk(parentBlock, 'Genesis has no parent')
      else {
        t.ok(parentBlock, 'Parent available')
        t.ok(block.parentSig.equals(parentBlock.sig))
      }
      return 0
    }
  )
  await store.load()

  const f = new Feed()
  f.append('0', sk)
  f.append('1', sk)
  await store.dispatch(f)
  f.append('2', sk)
  await store.dispatch(f.slice(-1))
  f.append('3', sk)
  await store.dispatch(f)
  await store.reload()
})

test('Root state available in slices', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)

  store.register('x', 5, () => true, () => 0) // dummy store
  store.register('y', 7,
    ({ root }) => {
      t.ok(root)
      t.equal(root.x, 5)
      t.equal(root.y, 7)
    },
    ({ root }) => {
      t.ok(root)
      t.equal(root.x, 5)
      t.equal(root.y, 7)
      return 8
    }
  )
  await store.load()
  const f = new Feed()
  f.append('0', sk)
  await store.dispatch(f, true)
  t.end()
})

test('Same block not reduced twice', async t => {
  const { pk, sk } = Feed.signPair()

  const db = DB()
  const store = new PicoStore(db)
  store.register('x', 0, () => false, ({ block, state }) => {
    const n = parseInt(block.body.toString())
    t.ok(state < n, 'unseen block')
    return n
  }) // dummy store

  await store.load()
  const f = new Feed()
  f.append('1', sk)
  f.append('2', sk)
  f.append('3', sk)
  await store.dispatch(f)
  f.append('4', sk)
  f.append('5', sk)
  await store.dispatch(f)
  await store.dispatch(f)

  const stored = await store.repo.loadHead(pk)
  t.equal(store.state.x, 5, 'Store state is correct')
  t.equal(f.length, stored.length, 'All blocks persisted')
})

test('Same block not reduced twice, given multiple identities', async t => {
  const a = Feed.signPair().sk
  const b = Feed.signPair().sk

  const db = DB()
  const store = new PicoStore(db)
  store.register('x', 0, () => false, ({ block, state }) => {
    const n = parseInt(block.body.toString())
    t.ok(state < n, 'unseen block')
    return n
  }) // dummy store

  await store.load()
  const f = new Feed()
  f.append('1', a)
  f.append('2', b)
  f.append('3', a)
  await store.dispatch(f)
  f.append('4', b)
  f.append('5', a)
  await store.dispatch(f)
  await store.dispatch(f)

  const stored = await store.repo.loadHead(f.last.key)
  t.equal(store.state.x, 5, 'Store state is correct')
  t.equal(f.length, stored.length, 'All blocks persisted')
})

test('State modifications are mutex locked', async t => {
  const { pk, sk } = Feed.signPair()

  const db = DB()
  const store = new PicoStore(db)
  store.register('x', 0, () => false, ({ block, state }) => {
    const n = parseInt(block.body.toString())
    t.ok(state < n, 'unseen block')
    return n
  }) // dummy store

  await store.load()
  const f = new Feed()
  f.append('1', sk)
  f.append('2', sk)
  f.append('3', sk)
  store.dispatch(f)
  f.append('4', sk)
  f.append('5', sk)
  const f2 = f.clone()
  f2.append('6', sk)
  f2.append('7', sk)
  const tasks = [
    store.dispatch(f2),
    store.dispatch(f),
    store.dispatch(f2)
  ]
  await Promise.all(tasks)
  const stored = await store.repo.loadHead(pk)
  t.equal(store.state.x, 7, 'Store state is correct')
  t.equal(f2.length, stored.length, 'All blocks persisted')
})

test('Filter does not run on unmutated state', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)
  store.register('x', 0,
    ({ block, state }) => {
      const n = parseInt(block.body.toString())
      if (state !== n - 1) return 'InvalidSequence'
    },
    ({ block, state }) => parseInt(block.body.toString())
  )

  await store.load()
  const f = new Feed()
  f.append('1', sk)
  await store.dispatch(f, true)
  f.append('2', sk)
  f.append('3', sk)
  f.append('4', sk)
  f.append('5', sk)
  await store.dispatch(f, true)
  t.equal(store.state.x, 5)
})

test('reducerContext.signal(int, payload)', async t => {
  /**
   * Each slices provides their own registers, a memory region
   * that they own and maintain.
   * When a high-level change happens in one slice that needs to be
   * reflected in another. I want to change the dispatch lifecycle:
   * - Phase 0: canMerge?(patch)
   * - Phase 1: slice.filter(block)
   * - Phase 2: slice.reduce(block) =>  lv0 state, interrupts
   * + Phase 3: slice.handleInterrupts(interrupts) => lv1 state
   * - Phase 4: notify slice.observers
   *
   *   Introduces internal high-priority higher-level-events that run before
   *   register observers are notified.
   */
  // The Problem:
  const db = DB()
  const store = new PicoStore(db)
  const INT_RST = 0 // 'reset'-interrupt
  let resetFired = 0
  // X.slice is a incremental counter
  store.register({
    name: 'x',
    initialValue: 0,
    filter: ({ block, state }) => {
      return parseInt(block.body.toString()) > state
        ? false // accept
        : 'XValueTooLow' // reject
    },
    reducer: ({ block, state }) => parseInt(block.body.toString()),
    trap: ({ code, payload, state, root }) => {
      if (code !== INT_RST) return // undefined return means trap not triggered.
      t.equal(code, INT_RST)
      t.equal(payload, 'hit-the-brakes!', 'playload visible')
      t.ok(root)
      resetFired++
      return 0
    }
  })
  // Y.slice computes (x^2) and signals reset at a threshhold
  function compute (x) { return x * x }
  store.register({
    name: 'y',
    initialValue: 0,
    filter: ({ block, state }) => {
      return compute(parseInt(block.body.toString())) > state
        ? false // accept
        : 'YValueTooLow' // reject
    },
    reducer: ({ block, state, root, signal }) => {
      const y = compute(parseInt(block.body.toString()))
      if (y > root.x * 5) { // Too fast, reset throttle
        t.equal(typeof signal, 'function', 'signal method provided in context')
        signal(INT_RST, 'hit-the-brakes!')
        return 0
      } else return y // accellerate as usual
    }
  })
  await store.load()
  const { sk } = Feed.signPair()
  const f = new Feed()
  f.append('1', sk)
  await store.dispatch(f, true)
  t.equal(resetFired, 0, 'Signal not yet triggered')
  f.append('2', sk)
  f.append('3', sk)
  f.append('4', sk)
  f.append('5', sk)
  await store.dispatch(f, true)
  t.equal(resetFired, 0, 'not yet')
  f.append('6', sk)
  await store.dispatch(f, true)
  t.equal(resetFired, 1, 'Fired')
  t.equal(store.state.x, 0)
  t.equal(store.state.y, 0)
  f.append('3', sk)
  f.append('4', sk)
  await store.dispatch(f, true)
  t.equal(resetFired, 1, 'reset still only once')
  t.equal(store.state.x, 4, 'Counter works after reset')
  t.equal(store.state.y, 16)
})

test('Allow multiple feeds from same author', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)
  store.repo.allowDetached = true

  store.register('x', [],
    ({ block, state }) => false,
    ({ block, state }) => [...state, parseInt(block.body.toString())]
  )

  await store.load()
  const a = new Feed()
  a.append('1', sk)
  a.append('7', sk)
  await store.dispatch(a, true)

  const b = new Feed()
  b.append('2', sk)
  b.append('9', sk)

  await store.dispatch(b, true)

  // await require('picorepo/dot').dump(store.repo)
  t.deepEqual(store.state.x, [1, 7, 2, 9])
})

test('Dispatching blocks one at a time', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)
  store.mutexTimeout = 60000000
  store.register('x', 0,
    ({ block, state }) => {
      const n = parseInt(block.body.toString())
      if (state !== n - 1) return 'InvalidSequence'
    },
    ({ block, state }) => parseInt(block.body.toString())
  )
  const loud = true
  await store.load()
  const f = new Feed()
  f.append('1', sk)
  await store.dispatch(f, loud)
  f.append('2', sk)
  await store.dispatch(f.slice(-1), loud)
  f.append('3', sk)
  await store.dispatch(f.slice(-1), loud)
  f.append('4', sk)
  await store.dispatch(f.slice(-1), loud)
  t.equal(store.state.x, 4)
  const l = await store.repo.resolveFeed(f.first.sig)
  t.ok(l.last.sig.equals(f.last.sig), 'repo and store is in sync')
})

test('Block cache solves out of order blocks', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new PicoStore(db)
  store.mutexTimeout = 60000000
  store.register('x', -1,
    ({ block, state }) => {
      const n = parseInt(block.body.toString())
      if (state !== n - 1) return 'InvalidSequence'
    },
    ({ block, state }) => parseInt(block.body.toString())
  )
  await store.load()
  const f = new Feed()
  for (let i = 0; i < 10; i++) {
    f.append(`${i}`, sk)
  }

  const slice0 = f.slice(0, 1)
  const slice1 = f.slice(1, 2)
  const slice2 = f.slice(2, 3)
  const slice3 = f.slice(3, 4)
  const slice456 = f.slice(4, 7)
  const slice7 = f.slice(7, 8)
  const slice89 = f.slice(8, 10)

  let m = await store.dispatch(slice0)
  t.ok(m.length)
  m = await store.dispatch(slice2)
  t.notOk(m.length)
  m = await store.dispatch(slice1) // Block 1
  t.ok(m.length, 'Blocks recovered and merged')
  t.equal(store.state.x, 2)
  t.ok(m.patch, 'patch exported')
  t.equal(m.patch.length, 2)

  // Next, create a gap [4,5,6, empty, 8,9]
  m = await store.dispatch(slice456) // Blocks 4,5,6
  t.notOk(m.length, 'blocks cached')

  m = await store.dispatch(slice89) // Blocks 8,9
  t.notOk(m.length, 'blocks cached')

  m = await store.dispatch(slice7) // missing Block 7
  t.notOk(m.length, 'blocks cached')

  // Check if cache merged the gap
  const [cached] = await store.cache.pop(slice3.first.parentSig, slice3.first.sig)
  t.equals(cached.first.body.toString(), slice456.first.body.toString(), 'poped starts at 4')
  t.equals(cached.last.body.toString(), slice89.last.body.toString(), 'poped ends at 9')

  // Last test
  m = await store.dispatch(cached)
  t.notOk(m.length, 'blocks re-cached')
  t.equal(store.state.x, 2)

  m = await store.dispatch(slice3)
  t.ok(m.length, 'blocks merged')
  t.equal(store.state.x, 9)
})

test.skip('Simple stupid slice with manual garbage collection', async t => {
  const store = new PicoStore(DB())
  store.repo.allowDetached = true
  store.register(CulledClock())
  await store.load()
  // Identities
  const { sk: A } = Feed.signPair()
  const { sk: B } = Feed.signPair()
  const { sk: C } = Feed.signPair()
  // Feeds/Chains
  const a = new Feed()
  const b = new Feed()
  const c = new Feed()

  // Consensus; expunge ticks where tick < avg(ticks) - 5
  a.append('' + 0, A)
  a.append('' + 1, A)
  a.append('' + 2, A)

  await store.dispatch(a, true)

  t.equal(Object.keys(store.state.clock.peers).length, 1, 'PeerA ticks')
  t.equal(store.state.clock.t, 2, 'time = PeerA')
  b.append('' + 3, B)
  c.append('' + 5, C)

  await store.dispatch(b, true)
  await store.dispatch(c, true)
  t.equal(Object.keys(store.state.clock.peers).length, 3, 'Peer A,B,C ticks')
  t.equal(store.state.clock.t, (2 + 3 + 5) / 3, 'Time avg(a,b,c)')

  b.append('' + 9, B)
  await store.dispatch(b, true)
  await store.gc(store.state.clock.t)

  t.equal(Object.keys(store.state.clock.peers).length, 2, 'Peer A removed/timeout')
  t.equal(store.state.clock.t, (9 + 5) / 2, 'Time avg(b,c)')

  function CulledClock () {
    return {
      name: 'clock',
      initialValue: { t: 0, peers: {} },
      filter ({ CHAIN, state, block }) {
        const n = parseInt(block.body.toString())
        const key = CHAIN.hexSlice(0, 6)
        if (state.peers[key] && !(state.peers[key] < n)) return 'InvalidTime'
        // TODO: only check this on last-block of a chain to allow
        // a long lived chain to be merged on new peers.
        if (!(state.t <= n)) return 'ThePast'

        return false
      },
      reducer ({ CHAIN, state, block, trash }) {
        const n = parseInt(block.body.toString())
        const key = CHAIN.hexSlice(0, 6)
        state.peers[key] = n
        const vector = Object.values(state.peers)
        // trash(key, n + 5) // Schedule removal
        state.t = vector.reduce((sum, i) => sum + i, 0) / vector.length
        return state
      },

      sweep ({ now, drop, payload }) {
        debugger
      }
    }
  }
})
