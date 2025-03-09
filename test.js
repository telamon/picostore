import { test, skip } from 'brittle'
import { Feed, toU8, cmp, toHex, isBlock } from 'picofeed'
import { MemoryLevel } from 'memory-level'
import { createGame } from './example_cgoh.js'
import { decode } from 'cborg'
// import { get } from 'piconuro'
import { inspect } from 'picorepo/dot.js'
import { writeFileSync } from 'node:fs'
import { Store, DiffMemory, Memory } from './index.js'
import { ice, thaw } from './ice.js'
Feed.__vctr = 0
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
  // API Change twice
  const collection = store.register('counter', class CRDTCounter extends DiffMemory {
    initialValue = { x: 0 }
    idOf () { return 0 } // one global counter
    validate () { return false } // Accept all,
    expiresAt () { return 0 }
  })
  await store.load()

  let v = await collection.readState(0)
  t.is(v.x, 0) // Singular states are the exception

  // CRDt like patches
  const blocks = await collection.mutate(null, () => ({ x: 4 }), sk)
  t.ok(Feed.isFeed(blocks))

  v = await collection.readState(0)
  t.is(v.x, 4)

  let nRefs = await store.referencesOf(pk) // Author is blockRoot
  t.is(nRefs, 1, 'Refcount by Author = 1')

  const expunged = await store.gc(Date.now() + 30000)
  // writeFileSync('bug.dot', await inspect(store.repo))
  t.ok(Array.isArray(expunged))
  t.ok(Feed.isFeed(expunged[0]))
  v = await collection.readState(0)
  t.is(v.x, 0)

  nRefs = await store.referencesOf(pk)
  t.is(nRefs, 0, 'Refcount zero')
  await store.reload()
})

test('DVM3.x Conways Game Of Bumps', async t => {
  const db = DB()
  const store = new Store(db)
  const [profiles, bumps] = createGame(store)
  await store.load()
  const A = Feed.signPair()
  const B = Feed.signPair()
  const feedA = await profiles.mutate(null, p => ({ ...p, name: 'alice' }), A.sk)
  // const objId = A.pk // Bad example: We know that blockRoot === ObjectId
  // const sigHEAD = await profiles.blockRootOf(objId) // TODO: undefined
  const feed = await store.readBranch(A.pk)
  t.is(feed.diff(feedA), 0)
  // t.ok(cmp(sigHEAD, feed.last.id))

  await profiles.mutate(null, p => ({ ...p, name: 'bob' }), B.sk)
  const f2 = await bumps.createBump(B.pk, A.sk)
  await bumps.respondBump(f2.last.id, B.sk)

  const bumpAB = Object.values(bumps.state)[0]
  t.is(bumpAB.status, 'accepted')
  for (const profile of Object.values(profiles.state)) {
    t.is(profile.hp, 4.5, `${profile.name} hp increased`)
  }
  writeFileSync('repo.dot', await inspect(store.repo))
  // This is tiring but now the most important part, run GC witness deinitalization
  await store.gc(Date.now() + 9000000)
  t.is(Object.values(profiles.state).length, 0, 'Profiles Cleared')
  t.is(Object.values(bumps.state).length, 0, 'Bumps Cleared')
})

test('PicoStore 2.x/ counters scenario', async t => {
  const store = new Store(DB())

  class Counter extends Memory {
    initialValue = 5
    idOf () { return 0 }
    compute (v, { payload, reject }) {
      if (payload <= v) return reject(`Must Increment ${payload} > ${v}`)
      return payload
    }
  }

  const colCounter = store.register('counter', Counter)
  await store.load()

  t.is(await colCounter.readState(0), 5)
  // Create mutation
  const { sk } = Feed.signPair()

  let branch = await colCounter.createBlock(null, 7, sk)
  t.is(await colCounter.readState(0), 7)

  // Simulate network connection between two stores
  const store2 = new Store(DB())
  const counter2 = store2.register('counter', Counter)
  await store2.load()
  t.is(await counter2.readState(0), 5)
  await store2.dispatch(branch)
  t.is(await counter2.readState(0), 7)

  // Rejected changes do not affect state
  await t.exception(counter2.createBlock(branch, 2, sk))
  branch.truncate(-1) // Lop off the invalid block
  t.is(await counter2.readState(0), 7, 'state not modified')

  // Test valid changes
  branch = await counter2.createBlock(branch, 10, sk)
  branch = await counter2.createBlock(branch, 12, sk)

  // let changed = await store.dispatch(branch)
  // t.equal(changed.length, 1)
  t.is(await counter2.readState(0), 12, 'is 12')

  // Purge and rebuild state from scratch
  await store2.reload()
  t.is(await counter2.readState(0), 12)
})

test('Buffers should not be lost during state reload', async t => {
  const { pk, sk } = Feed.signPair()
  const db = DB()
  const store = new Store(db)
  const profiles1 = store.register('pk', DiffMemory)
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
  const profiles2 = s2.register('pk', DiffMemory)
  await s2.load()
  o = await profiles2.readState(pk)
  t.ok(cmp(toU8(pk), o.nested.buf))
  t.ok(cmp(toU8(pk), o.arr[0]))
  t.ok(cmp(toU8(pk), o.prop))
})

test('Validation errors cause createBlock() to fail', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new Store(db)
  class DummyCounter extends DiffMemory {
    initialValue = { n: 0 }
    idOf () { return 0 }
  }
  const x = store.register('x', class X extends DummyCounter { validate (_, { reject }) { return reject('do not want') } })
  const y = store.register('y', class Y extends DummyCounter { validate (_, { reject }) { return reject() } }) // usually silent ignore
  await store.load()

  await t.exception(
    x.mutate(null, () => ({ n: 1 }), sk),
    'Validation error became mutation error'
  )

  await t.exception(
    y.mutate(null, () => ({ n: 1 }), sk),
    'Silent validation error became mutation error'
  )
  t.is((await y.readState(0)).n, 0, 'state not mutated')
})

const makePlainCounter = t => class extends Memory {
  initialValue = 0
  async idOf () { return 0 }
  async compute (state, { payload, reject }) {
    const isHigher = state < payload
    if (!isHigher) return reject(`Expected '${state}' < '${payload}'`)
    t.ok(isHigher, `unseen block ${payload}`)
    return payload
  }
}

// Important test
test('Same block not reduced twice', async t => {
  const { pk, sk } = Feed.signPair()
  const store = new Store(DB())

  const unit = store.register('x', makePlainCounter(t))
  await store.load()
  let f = new Feed()
  f = await unit.formatPatch(f, 1, sk)
  f = await unit.formatPatch(f, 2, sk)
  f = await unit.formatPatch(f, 3, sk)
  await store.dispatch(f)
  f = await unit.formatPatch(f, 4, sk)
  f = await unit.formatPatch(f, 5, sk)
  await store.dispatch(f)
  await store.dispatch(f)
  const stored = await store.repo.loadHead(pk)
  const x = await unit.readState(0)
  t.is(x, 5, 'Store state is correct')
  t.is(f.length, stored.length, 'All blocks persisted')
})

test('Same block not reduced twice, given multiple identities', async t => {
  const a = Feed.signPair().sk
  const b = Feed.signPair().sk

  const db = DB()
  const store = new Store(db)
  const unit = store.register('x', makePlainCounter(t))

  await store.load()
  let f = new Feed()
  f = await unit.formatPatch(f, 1, a)
  f = await unit.formatPatch(f, 2, b)
  f = await unit.formatPatch(f, 3, a)
  await store.dispatch(f)
  f = await unit.formatPatch(f, 4, b)
  f = await unit.formatPatch(f, 5, a)
  await store.dispatch(f)
  await store.dispatch(f)

  const stored = await store.repo.loadHead(f.last.key)
  const x = await unit.readState(0)
  t.is(x, 5, 'Store state is correct')
  t.is(f.length, stored.length, 'All blocks persisted')
})

test('State modifications are mutex locked', async t => {
  const { pk, sk } = Feed.signPair()

  const db = DB()
  const store = new Store(db)

  const unit = store.register('x', makePlainCounter(t))

  await store.load()
  const f = new Feed()
  await unit.formatPatch(f, 1, sk)
  await unit.formatPatch(f, 2, sk)
  await unit.formatPatch(f, 3, sk)
  // const p = store.dispatch(f)
  await unit.formatPatch(f, 4, sk)
  await unit.formatPatch(f, 5, sk)
  const f2 = f.clone()
  await unit.formatPatch(f2, 6, sk)
  await unit.formatPatch(f2, 7, sk)
  const tasks = [
    // p,
    store.dispatch(f2),
    store.dispatch(f),
    store.dispatch(f2)
  ]
  await Promise.all(tasks)
  const stored = await store.repo.loadHead(pk)
  const x = await unit.readState(0)
  t.is(x, 7, 'Store state is correct')
  t.is(f2.length, stored.length, 'All blocks persisted')
})

test('Filter/Compute does not run on unmutated state', async t => {
  const { sk } = Feed.signPair()
  const db = DB()
  const store = new Store(db)
  const unit = store.register('x', class extends Memory {
    initialValue = 0
    async idOf () { return 0 }
    async compute (state, { payload, reject }) {
      if (state !== payload - 1) return reject(`Expected '${state}' < '${payload}'`)
      return payload
    }
  })
  await store.load()
  const f = new Feed()
  await unit.formatPatch(f, 1, sk)
  await store.dispatch(f, true)
  await unit.formatPatch(f, 2, sk)
  await unit.formatPatch(f, 3, sk)
  await unit.formatPatch(f, 4, sk)
  await unit.formatPatch(f, 5, sk)
  await store.dispatch(f, true)
  t.is(await unit.readState(0), 5)
})

test('Allow multiple feeds from same author (1K)', async t => {
  const { sk } = Feed.signPair()
  const store = new Store(DB())
  store.repo.allowDetached = true // TODO: make default behaviour
  const unit = store.register('x', class extends Memory {
    idOf () { return 0 } // Single global register
    initialValue = []
    compute (state, { payload }) {
      return [...state, payload] // Just concat in new values
    }
  })

  await store.load()
  const a = new Feed()
  await unit.formatPatch(a, 1, sk)
  await unit.formatPatch(a, 7, sk)
  await store.dispatch(a, true)

  const b = new Feed()
  await unit.formatPatch(b, 2, sk)
  await unit.formatPatch(b, 9, sk)
  await store.dispatch(b, true)

  // await require('picorepo/dot').dump(store.repo)
  t.alike(await unit.readState(0), [1, 7, 2, 9])
})

test('Block cache solves out of order blocks', async t => {
  const { sk } = Feed.signPair()
  const store = new Store(DB())
  store.mutexTimeout = 60000000
  const unit = store.register('x', class extends Memory {
    initialValue = -1
    idOf () { return 0 } // make global
    compute (state, { payload, reject }) {
      // console.log('inc mutation', state, payload)
      if (payload !== state + 1) return reject('InvalidSequence')
      return payload
    }
  })
  await store.load()
  const f = new Feed()

  for (let i = 0; i < 10; i++) {
    await unit.formatPatch(f, i, sk)
  }

  const slice0 = f.slice(0, 1)
  const slice1 = f.slice(1, 2)
  const slice2 = f.slice(2, 3)
  const slice3 = f.slice(3, 4)
  const slice456 = f.slice(4, 7)
  const slice7 = f.slice(7, 8)
  const slice89 = f.slice(8, 10)

  let m = await store.dispatch(slice0, true)
  t.ok(m.length)
  m = await store.dispatch(slice2, true) // Does not throw, because slice 2 ends up in cache
  t.not(m, 'returns undefined')
  m = await store.dispatch(slice1, true) // Block 1
  t.ok(m.length, 'Blocks seem recovered and merged')
  t.is(await unit.readState(0), 2, 'Value is correct')
  t.is(m.length, 2, 'patch exported')

  // Next, create a gap [4,5,6, empty, 8,9]
  m = await store.dispatch(slice456, true) // Blocks 4,5,6
  t.not(m, 'blocks cached')

  m = await store.dispatch(slice89, true) // Blocks 8,9
  t.not(m, 'blocks cached')

  m = await store.dispatch(slice7, true) // missing Block 7
  t.not(m, 'blocks cached')

  // Check if cache merged the gap
  const [cached] = await store.cache.pop(slice3.first.psig, slice3.first.sig)
  t.alike(decode(cached.first.body), decode(slice456.first.body), 'poped starts at 4')
  t.alike(decode(cached.last.body), decode(slice89.last.body), 'poped ends at 9')

  // Last test
  m = await store.dispatch(cached)
  t.not(m, 'blocks re-cached')
  t.is(await unit.readState(0), 2)

  m = await store.dispatch(slice3)
  t.ok(m.length, 'blocks merged')
  t.is(await unit.readState(0), 9, 'cache flushed all blocks merged')
})

// TODO Rewrite this test to validate DispatchContext, ComputeContext, PostapplyContext
// Dispatch < Compute < Postapply
test('The ComputeContext and callback API', async t => {
  const { sk } = Feed.signPair()
  const engine = new Store(DB())
  let _block = null
  const unit = engine.register('mySlice', class extends Memory {
    initialValue = { name: 'unnamed player', hp: 10 }

    idOf ({ AUTHOR, CHAIN }) {
      t.is(AUTHOR, toHex(_block.key), 'AUTHOR is a hexstring of block.key')
      t.is(CHAIN, toHex(_block.sig), 'CHAIN is a hexstring of block.sig')
      return 0
    }

    /** @type {(value: any, ctx: ComputeContext) => any} */
    async compute (value, ctx) {
      t.ok(value !== this.initialValue)
      t.alike(value, this.initialValue, 'current value = deep clone of initial value')
      // DispatchContext
      t.ok(isBlock(ctx.block), 'ctx.block = PicoBlock')
      t.is(ctx.parentBlock, null, 'ctx.parentBlock = PicoBlock|null')
      t.is(typeof ctx.AUTHOR, 'string', 'ctx.AUTHOR = hexstring')
      t.is(typeof ctx.CHAIN, 'string', 'ctx.CHAIN = hexstring')
      t.is(ctx.root, 'mySlice', 'ctx.root = Name of memory unit')
      t.alike(ctx.payload, { type: 'spawn', name: 'bob' }, 'ctx.payload = user input')
      t.is(typeof ctx.date, 'number', 'ctx.date = Block date')
      // ComputeContext
      //
      t.is(ctx.id, 0, 'ctx.id = Object id')
      t.is(typeof ctx.reject, 'function', 'ctx.reject = function')
      t.is(typeof ctx.postpone, 'function', 'ctx.postpone = function')
      t.is(typeof ctx.lookup, 'function', 'ctx.lookup = function')

      t.is(typeof ctx.index, 'function', 'ctx.index = function')
      t.is(typeof ctx.signal, 'function', 'ctx.signal = function')

      await ctx.index('bob') // Index player-name => chain

      ctx.signal('new-player', 'bob')
      return { ...value, name: ctx.payload.name }
    }
  })

  await engine.load()
  const feed = await unit.formatPatch(null, { type: 'spawn', name: 'bob' }, sk)
  _block = feed.last
  await engine.dispatch(feed, true)
})

// TODO: port to V3, game-of-bumps informally tests signals, but clean signal API test is good
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
  const store = new Store(DB())
  let resetFired = 0
  // X.slice is a incremental counter
  const unitX = store.register('x', class extends Memory {
    initialValue = 0
    idOf () { return 0 }
    /** @override */
    compute (value, { payload, reject, signal }) {
      if (payload <= value) return reject('value too low')
      if (!(payload % 2)) signal('even-value', payload)
      return payload
    }
  })

  const unitY = store.register('y', class extends Memory {
    initialValue = 0
    idOf () { return 0 }
    compute (_, { reject }) { return reject('Y listens only to signals') }

    async trap (signalName, signalPayload, mutate) {
      t.is(signalName, 'even-value')
      resetFired++
      /** mutate(objId, currentValue => (currentValue + 1)) */
      await mutate(0, () => signalPayload)
    }
  })

  await store.load()
  const { sk } = Feed.signPair()
  const f = new Feed()
  await unitX.createBlock(f, 1, sk)
  // await store.dispatch(f, true) trigger to minor bug
  t.is(resetFired, 0, 'Signal not yet triggered')
  await unitX.createBlock(f, 2, sk)
  t.is(resetFired, 1, 'trapped once')
  await unitX.createBlock(f, 3, sk)
  await unitX.createBlock(f, 4, sk)
  t.is(resetFired, 2, 'trapped twice')

  t.is(await unitX.readState(0), 4, 'X = 4')
  t.is(await unitY.readState(0), 4, 'Y = 4')
})

// @deprecated I believe
skip('Simple stupid slice with crude manual boring garbage collection', async t => {
  const store = new Store(DB())
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

skip('util.ICE: A tripwire object proxy', async t => {
  const sample = {
    name: 'Sven',
    items: [
      { name: 'torch', attack: 1 },
      { name: 'rope', def: 0, attr: { magic: 0, fire: -1 } }
    ],
    hp: 50,
    stats: { str: 1, dex: 2, int: 0 },
    status: 'poisoned'
  }
  const tripwired = ice(sample)
  t.exception(() => { tripwired.status = 'healthy' }, 'Modifying object throws error')
  try {
    tripwired.items[0].radius = 999
    t.fail()
  } catch (e) {
    t.pass(e.message)
  }
  const sieved = ice(sample, true)
  sieved.name = 'bjorn'
  sieved.items.push({ name: 'holy fire sword', attack: 9000 })
  sieved.status = 'healthy'
  sieved.items[0].name = 'sling'
  sieved.key = new Uint8Array(32)
  sieved.desc = 'a beta tester'
  console.log(thaw(sieved))
})

/*
 * I think there is vast room for improvement here,
 * atm we support old picostore2.x api
 */
test('MemorySlices can be subscribed', async t => {
  const eng = new Store(DB())
  /** @type {DiffMemory} */
  const unit = eng.register('peers', class extends DiffMemory {
    initialValue = { name: 'bob', ping: 0 }
  })
  await eng.load()
  let observations = 0
  const unsub1 = eng.on('peers', value => {
    observations++
    // console.log('Changed', value)
  })
  const A = Feed.signPair()
  const feedA = await unit.mutate(null, value => ({ ...value, name: 'Alice', ping: 1 + value.ping }), A.sk)
  await unit.mutate(feedA, value => ({ ...value, ping: 1 + value.ping }), A.sk)

  const B = Feed.signPair()
  const feedB = await unit.mutate(null, value => ({ ...value, ping: 5 + value.ping }), B.sk)
  await unit.mutate(feedB, value => ({ ...value, ping: 5 + value.ping }), B.sk)
  await unit.mutate(feedA, value => ({ ...value, ping: 1 + value.ping }), A.sk)
  unsub1()
  await unit.mutate(feedB, value => ({ ...value, ping: 5 + value.ping }), B.sk)
  t.is(observations, 6)
})

test('test suite verifcation counts', async () => {
  console.log('block.verify() was invoked n-times:', Feed.__vctr)
})
