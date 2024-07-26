[`pure | 📦`](https://github.com/telamon/create-pure)
[`code style | standard`](https://standardjs.com/)
# picostore (block-engine)

This engine takes consensus rules in JavaScript.
Then it consumes blocks, and maintains a local view
of the decentralized state.

It wouldn't be wrong to call it an indexer,
but it would be half of it.

Because there's also a garbarge collector.

> __warn: instructions below are outdated__

## Use

```bash
$ npm i @telamon/picostore browser-level
```

```js
// Initialize a new store using
const db = ... // level-dbs-like-interface
const store = new PicoStore(db)

// Define a block-validator function,
// that checks if a block can be applied given content, author / something.
const canMutate = ({ state, block }) => {
  // Extract value from block
  const n = JSON.parse(block.body)

  // New value is valid if higher than previous value
  if (n <= state) return true
}

// Define a state reducer
const reducer = ({ state, block }) => {
  // returns new state of counter
  return JSON.parse(block.body)
}

// Registers the counter store with 5 being initial state.
store.register('counter', 5, canMutate, reducer)
await store.load()

console.log(store.state.counter) // => 5

// Mutate the state by creating a new feed and dispatching it.
const { sk } = Feed.signPair()
const mutations = new Feed()

// append new value as a transaction
mutations.append(JSON.stringify(7), sk)

// dispatch the transactions
let changed = await store.dispatch(mutations)

// dispatch returns a list of registers that were modified by the feed
console.log(changed) // => ['counter']

// state is mutated.
console.log(store.state.counter) // => 7
```

Using with Svelte:
```js
import { readable } from 'svelte/stores'
import PicoStore from 'picostore'

// Initialize a store maintaining "posts"
const store = new PicoStore(...)
store.register('posts', ...)

// Voilá a Svelte readable store
const posts = readable(store.state.posts, set => store.on('posts', set))

```

Using with React:
```js
import PicoStore from 'picostore'
import { useState, useEffect } from 'react'

// PicoHook
function usePico (store, name) {
  const [value, set] = useState(store.state[name])
  useEffect(() => store.on(name, set), [store, name, set]) // ensure unsub on unmount
  return value
}

// Initialize a store maintaining "posts"
const store = new PicoStore(...)
store.register('posts', ...)

function ListPostsComponent() {
  const posts = usePico(store, 'posts')
  return (<ul>
    {posts.map(post => (<li>{post.title}</li>))}
  </ul>)
}
```
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
