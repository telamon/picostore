const test = require('tape')
const Feed = require('picofeed')
const levelup = require('levelup')
const memdown = require('memdown')
const PicoStore = require('.')
const DB = () => levelup(memdown())

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
  t.plan(1)
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
  } catch (err) {
    t.equal(err.message, 'InvalidBlock: do not want')
  }
  t.end()
})

/* TODO: instead of complicating PicoStore to alow multiple storages
 * i want to make an experiment using multiple PicoStores.
 */
/*
test.skip('Multi-bucket trust levels', async t => {
  try {
    // Set up kernel
    const buckets = {
      master: DB(),
      cache: DB()
    }
    const store = new PicoStore(true, buckets)
    store.register('peers', {}, profileValidator, profileReducer)
    await store.load()

    // Set up peers
    const Alice = Feed.signPair() // Alice is a friend
    const Daphne = Feed.signPair() // Daphne is a hot wife in your area looking for action.
    // I get ya Bob, don't despair, finding a good peer was never supposed to be easy.

    // Set up a list of trusted peers
    const friends = [Alice.pk]

    // Dispatch mutation/blocks
    await store.dispatch(mkProfile('Daphne', 35, 'Visit me at tinyscam.com/Phishy', Daphne.sk))

    debugger
  } catch (e) { t.error(e) }

  function mkProfile (name, age, tagline, sk) {
    const f = new Feed()
    f.append(JSON.stringify({ name, age, tagline }), sk)
    return f
  }

  // Decides validity of block and destination bucket
  function profileValidator ({ state, block }) {
    const profile = JSON.parse(block.body)
    debugger
    if (n <= state) return true
  }

  function profileReducer ({ state, block }) {
    const key = block.key.toString('hex')
    const profile = JSON.parse(block.body)
    state[key] = profile
    return state
  }
})
*/
