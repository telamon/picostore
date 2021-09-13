[`pure | 📦`](https://github.com/telamon/create-pure)
[`code style | standard`](https://standardjs.com/)
# picostore

> Magical redux-like state handler using picofeed blockchains

PicoStore - picofeed powered blockchain state reducer (blockend)
Compatible with what ever frontend framework you wish to use.

The reduced state is always persisted and consistent across reloads and
application restarts. (Automagically restores in-memory state from cold-storage)

Works in browser and nodejs using leveldb and IndexedDB. (levelup)
Use `memdown` in unit-tests.

## Use

```bash
$ npm install @telamon/picostore levelup
```


```
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


## Donations

```ad
|  __ \   Help Wanted!     | | | |         | |
| |  | | ___  ___ ___ _ __ | |_| |     __ _| |__  ___   ___  ___
| |  | |/ _ \/ __/ _ \ '_ \| __| |    / _` | '_ \/ __| / __|/ _ \
| |__| |  __/ (_|  __/ | | | |_| |___| (_| | |_) \__ \_\__ \  __/
|_____/ \___|\___\___|_| |_|\__|______\__,_|_.__/|___(_)___/\___|
Discord: https://discord.gg/K5XjmZx

If you're reading this it means that the docs are missing or in a bad state.

Writing and maintaining friendly and useful documentation takes
effort and time. In order to do faster releases
I will from now on provide documentation relational to project activity.

  __How_to_Help____________________________________.
 |                                                 |
 |  - Open an issue if you have questions! :)      |
 |  - Star this repo if you found it interesting   |
 |  - Fork off & help document <3                  |
 |.________________________________________________|
```


## Changelog

### 1.0.0 first release

## Contributing

By making a pull request, you agree to release your modifications under
the license stated in the next section.

Only changesets by human contributors will be accepted.

## License

[AGPL-3.0-or-later](./LICENSE)

2021 &#x1f12f; Tony Ivanov
