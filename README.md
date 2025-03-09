[`pure | 📦`](https://github.com/telamon/create-pure)
[`code style | standard`](https://standardjs.com/)
# picostore (block-engine)

This engine takes consensus rules in JavaScript.
Then it consumes blocks, and maintains a local view
of the decentralized state.

It wouldn't be wrong to call it an indexer,
but it would only be half of it.

Because there's also a garbarge collector.

> __warn: instructions below are outdated__

## Use

```bash
$ npm i @telamon/picostore browser-level memory-level
```

Starter example using bundled DiffMemory:

```js
import { Feed } from 'picofeed'
import { MemoryLevel } from 'memory-level'
import { Store, DiffMemory } from 'picoengine'

// First an identity and a feed/chain
const { pk: pubkey, sk: secret } = Feed.signPair()
const feed = new Feed()

// Create a database and pass it to the engine
const db = new MemoryLevel('app', { keyEncoding: 'view', valueEncoding: 'view' })
const store = new Store(db)

// Register a collection and it's controller
const collection = engine.register('peers', DiffMemory)

// Restore state from previous boot/pageload
await store.load()


// Eventually you'll want to create your own subclass of `Memory`
// but the bundled `DiffMemory`-class is a good starting point
// because it provides the function `mutate()`

await collection.mutate(feed, currentValue => {
  return {
    name: 'Bob',
    location: 'bahamas',
    occupation: 'fishwarrior'
  }
}, secret)

await collection.readState(pubkey) // => { name: 'Bob', ...}

// TODO: transmit feed.buffer to a buddy
// and your local state will be replicated
// on their end
debugger
```

Please keep an eye on the [picostack](https://github.com/telamon/picostack) repo,
there will be more complete examples and starting points.

## API

TODO:

## Ad

```ad
|  __ \   Help Wanted!     | | | |         | |
| |  | | ___  ___ ___ _ __ | |_| |     __ _| |__  ___   ___  ___
| |  | |/ _ \/ __/ _ \ '_ \| __| |    / _` | '_ \/ __| / __|/ _ \
| |__| |  __/ (_|  __/ | | | |_| |___| (_| | |_) \__ \_\__ \  __/
|_____/ \___|\___\___|_| |_|\__|______\__,_|_.__/|___(_)___/\___|

If you're reading this it means that the docs are missing or in a bad state.

Writing and maintaining friendly and useful documentation takes
effort and time.


  __How_to_Help____________________________________.
 |                                                 |
 |  - Open an issue if you have questions!         |
 |  - Star this repo if you found it interesting   |
 |  - Fork off & help document <3                  |
 |  - Say Hi! :) https://discord.gg/K5XjmZx        |
 |.________________________________________________|
```

## Changelog

### [3.0.0] - 2024-07-29
- Holding off package rename Engine ("pico-engine" is taken, pengine?)
- Migrated to Picofeed 8.x
- Completeley reworked slices into 'Memory'
- combined `filter()`, `rreduce()` callbacks into `compute()`
- Garbage collector now uses Reference counting.
- Dropped required `sweep()` callback, possible to override default if needed.
- `signal()` and `trap()` is mostly unchanged
- New api's missing docs: `index()`, `lookup()`, `postpone()`
- Jumped from `msgpackr` to `cborg`, not sure why.

### [2.0.0] - 2022-10-19
- WeblocksAPI
- internal serializer changed to [msgpackr]()
- added OOOB cache
- Exposed tags AUTHOR & CHAIN on public api's
- memdown devDep replaced with MemoryLevel
- signal() & trap() api
- craploads of bugfixes


### [1.3.0] - 2021-10-29
- added `parentBlock` to validator & reducer interface
- added `dispatch(feed, loudFail = false)` setting to true will throw on validation error
- changed returning `true` within a validator silently ignores block even with loudFail active
- fixed craploads of bugs

### [1.0.0] first release

## Contributing

By making a pull request, you agree to release your modifications under
the license stated in the next section.

Only changesets by human contributors will be accepted.

## License

[AGPL-3.0-or-later](./LICENSE)

2021 &#x1f12f; Tony Ivanov
