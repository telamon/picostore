import { test, skip, solo } from 'brittle'
import { Feed, cmp, toU8 } from 'picofeed'
import { MemoryLevel } from 'memory-level'
import { get } from 'piconuro'
// import { inspect } from 'picorepo/dot.js'
// import { writeFileSync } from 'node:fs'
import Store, { Memory, CRDTMemory } from './index.js'

const DB = () => new MemoryLevel({
  valueEncoding: 'view',
  keyEncoding: 'view'
})

/**
 * Block-roots are either tracked by Author or Chain
 * A blockroot has many referenes to Objects in Dstate
 * Each object in Dstate has a kill-condition (timer|gameOverFunc)
 * when block-root refs reach 0 then the feed is expunged/collected.
 * -- state complexities:
 *  - All Objects have defined Lifetimes
 *  - Does custom-block-types require custom index-fn?
 *  - Locks/Unlocks requires cross Memory events or signals
 */
test('PicoStore 3.x', async t => {
  const { pk, sk } = Feed.signPair()
  const db = DB()
  const store = new Store(db)
  // API Change
  const collection = store.spec('counter', {
    initialValue: 0,
    // malloc() => id/ptr
    id: () => 0, // PK<Author>|ChainID<Task,Note>|BlockID<Chat,Battle>
    validate: () => false, // ErrorMSG|void
    expiresAt: date => date + 10000
  })
  await store.load()

  let v = await collection.readState(0)
  t.is(v, 0) // Singular states are the exception

  // CRDt like patches
  const blocks = await collection.mutate(null, () => 4, sk)
  t.ok(Feed.isFeed(blocks))

  v = await collection.readState(0)
  t.is(v, 4)

  let nRefs = await store.referencesOf(pk) // Author is blockRoot
  t.is(nRefs, 1, 'Refcount by Author = 1')

  const expunged = await store.gc(Date.now() + 30000)
  // writeFileSync('bug.dot', await inspect(store.repo))
  t.ok(Array.isArray(expunged))
  t.ok(Feed.isFeed(expunged[0]))
  v = await collection.readState(0)
  t.is(v, 0)

  nRefs = await store.referencesOf(pk)
  t.is(nRefs, 0, 'Refcount zero')
})

solo('DVM3.x bidirectional memory refs', async t => {
  const db = DB()
  const store = new Store(db)

  // CRDt Managed Memory
  const profiles = store.register('profile', class Profiles extends CRDTMemory {
    initialValue = { name: '', hp: 3, sent: 0, accepted: 0, rejected: 0 }

    idOf ({ AUTHOR }) { // Only one profile per Public key
      return AUTHOR // return blockRoot
    }

    validate (value, { previous }) {
      if (previous.name === '' && typeof value.name === 'undefined') return 'NameMustBeSet'
      if (previous.name !== '' && previous.name !== value.name) return 'NameChangeNotPermitted'
    }

    expiresAt (date, { value }) {
      return date + value.hp * (20*60000) // All creatures die without hugs
    }

    // CRDtMemory implements reduce and sweep

    async trap (signal, payload, mutate) {
      if (signal !== 'hug-settled') return
      const { status, from, to } = payload
      // This is where it becomes problematic / breaks CRDT behaviour
      if (status === 'accepted') {
        await mutate(from, s => ({ ...s, hp: s.hp + 1.5, sent: s.sent + 1 }))
        await mutate(to, s => ({ ...s, hp: s.hp + 1.5, accepted: s.accepted + 1 }))
      } else {
        await mutate(from, s => ({ ...s, hp: s.hp - 0.3, sent: s.sent + 1 }))
        await mutate(to, s => ({ ...s, hp: s.hp + 2, rejected: s.rejected + 1 }))
      }
    }
  })

  // Custom Instruction Memory
  const hugs = store.register('hugs', class Hugs extends Memory {
    idOf ({ data, block, parent }) {
      if (data.type === 'hug') return block.id
      if (data.type === 'response') return parent.id
    }

    async validate (data, { id, AUTHOR, block, postpone, lookup }) {
      if (data.type === 'hug') {
        if (block.genesis) return 'NoAnonymousHugs'
        if (cmp(data.target, AUTHOR)) return 'NoSelfhugs'
        // Secondary Index lookups
        const lastHug = await this.readState(await lookup(AUTHOR))
        if (lastHug?.status === 'pending') return 'CannotInitiateWhilePending'
        // Cross memory lookups
        const peer = await profiles.readState(data.target) // reffing "profiles" collection
        if (!peer) return postpone(30000, 'NoGhostHugs') // Postpone upto 3 Times then RejectionMessage
      } else if (data.type === 'response') {
        const pData = this.readState(id)
        if (pData.target !== AUTHOR) return 'NotYourHug'
        if (![true, false].includes(data.ok)) return 'InvalidResponse'
      } else return 'InvalidType'
    }

    async reduce (currentValue, { data, id, AUTHOR, signal, index }) {
      const out = { ...currentValue }
      if (data.type === 'hug') {
        out.from = AUTHOR
        out.to = data.target
        out.status = 'pending'
        await index(AUTHOR, id)
      } else {
        out.status = data.ok ? 'accepted' : 'rejected'
        signal('hug-settled', { to: out.to, from: out.from, status: out.status })
      }
    }

    async sweep ({ id, AUTHOR, deIndex }) {
      await deIndex(AUTHOR) // Release Hug-cap
    }

  })

  await store.load()
  const { pk, sk } = Feed.signPair()
  const generatedFeed = await profiles.mutate(null, p => ({ ...p, name: 'telamohn' }), sk)
  const objId = await profiles.idOf(pk) // We know the blockRoot === ObjectId
  t.is(objId, pk)
  const sigHEAD = await profiles.blockRootOf(objId)
  const feed = await profiles.readBranch(objId)
  t.ok(cmp(sigHEAD, feed.last.id))
})

test('PicoStore 2.x scenario', async t => {
  const db = DB()
  const store = new Store(db)
  const spec = {
    id: () => 0,
    initialValue: 5,
    validate: (v, { previous }) => v < previous ? `Must Increment ${v} > ${previous}` : false
  }
  const colCounter = store.spec('counter', spec)
  await store.load()

  t.is(await colCounter.readState(0), 5)
  // Create mutation
  const { sk } = Feed.signPair()
  let branch = await colCounter.mutate(null, v => {
    t.is(v, 5, 'Previous value')
    return 7
  }, sk)
  t.is(await colCounter.readState(0), 7)

  // Try to restore previously persisted state
  const store2 = new Store(db)

  const counter2 = store2.spec('counter', spec)
  await store2.load()
  t.is(await counter2.readState(0), 7)

  // Rejected changes do not affect state
  await t.exception(async () =>
    await counter2.mutate(branch, () => 2, sk)
  )
  branch.truncate(branch.length - 1) // Lop off the invalid block
  t.is(await counter2.readState(0), 7)

  // Test valid changes
  branch = await counter2.mutate(branch, () => 10, sk)
  branch = await counter2.mutate(branch, () => 12, sk)

  // let changed = await store.dispatch(branch)
  // t.equal(changed.length, 1)
  t.is(await counter2.readState(0), 12)

  // Purge and rebuild state from scratch
  await store.reload()
  t.is(await colCounter.readState(0), 12)
})

test('Buffers should not be lost during state reload', async t => {
  const { pk, sk } = Feed.signPair()
  const db = DB()
  const store = new Store(db)
  const profiles1 = store.spec('pk', { initialValue: {} })
  await store.load()

  await profiles1.mutate(null, (_) => ({
    nested: { buf: toU8(pk) },
    arr: [toU8(pk)],
    prop: toU8(pk)
  }), sk)
  let o = await profiles1.readState(pk)
  t.ok(cmp(toU8(pk), o.nested.buf))
  t.ok(cmp(toU8(pk), o.arr[0]))
  t.ok(cmp(toU8(pk), o.prop))

  // Open second store forcing it to load cached state
  const s2 = new Store(db)
  const profiles2 = s2.spec('pk', { initialValue: {} })
  await s2.load()
  o = await profiles2.readState(pk)
  t.ok(cmp(toU8(pk), o.nested.buf))
  t.ok(cmp(toU8(pk), o.arr[0]))
  t.ok(cmp(toU8(pk), o.prop))
})

test('Throw validation errors on dispatch(feed, loudFail = true)', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new Store(db)
  // returning "true" from a validator now is a forced silent ignore
  const x = store.spec('x', { initialValue: 0, validate: () => 'do not want' })
  const y = store.spec('y', { initialValue: 0, validate: () => true })
  await store.load()
  // validate: 'errstring' = lout fail
  await t.exception(async () =>
    await x.mutate(null, () => 1, sk)
  )
  // validate: true = silent fail
  const m = await y.mutate(null, () => 1, sk)
  t.is(typeof m, 'undefined')
})

skip('Parent block provided to validator', async t => {
  t.plan(35)
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new Store(db)

  const x = store.spec('x', { initialValue: 0, validate: () => true })
  const y = store.spec('y', {
    initialValue: 0,
    validate (value, { block, parentBlock }) {
      if (block.isGenesis) t.ok(!parentBlock, 'Genesis has no parent')
      else {
        t.ok(parentBlock, 'Parent available')
        t.ok(cmp(block.parentSig, parentBlock.sig))
      }
    },
    reduce ({ block, parentBlock }) {
      if (block.isGenesis) t.notOk(parentBlock, 'Genesis has no parent')
      else {
        t.ok(parentBlock, 'Parent available')
        t.ok(block.parentSig.equals(parentBlock.sig))
      }
      return 0
    }
  })
  await store.load()
  // -- Let's pause rewriting tests,
  // we need to experiment to see if this new
  // pattern actualy is better than previous
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

skip('Root state available in slices', async t => {
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

// Important test
skip('Same block not reduced twice', async t => {
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

skip('Same block not reduced twice, given multiple identities', async t => {
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

skip('State modifications are mutex locked', async t => {
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

skip('Filter does not run on unmutated state', async t => {
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

skip('reducerContext.signal(int, payload)', async t => {
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

skip('Allow multiple feeds from same author', async t => {
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

skip('Dispatching blocks one at a time', async t => {
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

skip('Block cache solves out of order blocks', async t => {
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

skip('Simple stupid slice with crude manual boring garbage collection', async t => {
  const store = new PicoStore(DB())
  store.repo.allowDetached = true
  store.register(CulledClock())
  await store.load()
  // store.$('clock')(state => store.gc(state.t)) // Automatic GC on mutate

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
  const dropped = await store.gc(store.state.clock.t)
  t.equal(dropped.length, 1, '1 chain removed')

  t.equal(Object.keys(store.state.clock.peers).length, 2, 'Peer A removed/timeout')
  t.equal(store.state.clock.t, (9 + 5) / 2, 'Time avg(b,c)')

  /**
   * A vector clock that does not care
   */
  function CulledClock () {
    const TTL = 5
    return {
      name: 'clock',
      initialValue: { t: 0, peers: {} },
      filter ({ CHAIN, state, block }) {
        const n = parseInt(block.body.toString())
        const key = CHAIN.hexSlice(0, 6)
        if (state.peers[key] && !(state.peers[key] < n)) return 'InvalidTime'
        // TODO: only check this on last-block of a chain to allow
        // a long lived chain to be merged on new peers.
        // (Extend filter-ctx with isLast and invalidatePrevious/abort())
        if (!(state.t <= n)) return 'ThePast'
        return false
      },
      reducer ({ CHAIN, state, block, mark }) {
        const n = parseInt(block.body.toString())
        const key = CHAIN.hexSlice(0, 6)
        state.peers[key] = n
        const vector = Object.values(state.peers)
        mark(key, n + TTL) // Schedule removal
        state.t = vector.reduce((sum, i) => sum + i, 0) / vector.length
        return state
      },
      sweep ({ now, drop, payload: peer, state, mark }) {
        if (state.peers[peer] >= state.t) {
          // Peer still lives; Re-queue
          mark(peer, state.peers[peer] + TTL)
          return // state not changed
        }
        drop() // Drop entire chain (no stop-block provided)
        delete state.peers[peer] // clear out state
        // recalculate time
        const vector = Object.values(state.peers)
        state.t = vector.reduce((sum, i) => sum + i, 0) / vector.length
        return state // Notify subscribers that state changed.
      }
    }
  }
})
