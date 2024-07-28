/**
 _                                     _
/  _ ._      _.   _  _  _.._ _  _   __|_ |_    ._ _ ._  _
\_(_)| |\/\/(_|\/_> (_|(_|| | |(/_ (_)|  |_)|_|| | ||_)_>
               /     _|                             |
To test the garbage collector we build a game where
a peer profile goes stale after some amount of time.

In order to stay active they have 2 choices.

1. Choose a peer to bump
2. Accept/Reject a bump from other peer
  - accepting grants both parties 1.5 * 30min time
  - rejecting grants receiver 40min and initiator 6min time.
 */
import { toHex, toU8, getPublicKey } from 'picofeed'
import { Memory, DiffMemory } from './index.js'

export function createGame (store) {
  // CRDt Managed Memory
  const profiles = store.register('profile', Profiles)
  // Custom Instruction Memory
  const bumps = store.register('bumps', Bumps)
  return [profiles, bumps]
}

export class Profiles extends DiffMemory {
  initialValue = { name: '', hp: 3, sent: 0, accepted: 0, rejected: 0 }

  idOf ({ AUTHOR }) { // Only one profile per Public key
    return AUTHOR // return blockRoot
  }

  validate (value, { previous, reject }) {
    if (previous.name === '' && typeof value.name === 'undefined') return reject('NameMustBeSet')
    if (previous.name !== '' && previous.name !== value.name) return reject('NameChangeNotPermitted')
  }

  expiresAt (value) {
    const { _created, hp } = value
    debugger
    return _created + hp * (20 * 60000) // All creatures die without bumps
  }

  async trap (signal, payload, mutate) {
    if (signal !== 'bump-settled') return
    const { status, from, to } = payload
    // This is where it becomes problematic / breaks CRDT behaviour
    // edit: which is the reason why we export Memory class to design custom operations engines
    // but CRDTMemory is a good start before you end up with signal hell
    if (status === 'accepted') {
      await mutate(from, s => ({ ...s, hp: s.hp + 1.5, sent: s.sent + 1 }))
      await mutate(to, s => ({ ...s, hp: s.hp + 1.5, accepted: s.accepted + 1 }))
    } else {
      await mutate(from, s => ({ ...s, hp: s.hp - 0.3, sent: s.sent + 1 }))
      await mutate(to, s => ({ ...s, hp: s.hp + 2, rejected: s.rejected + 1 }))
    }
  }
}

export class Bumps extends Memory {
  /* @override */
  idOf ({ payload, block, parentBlock }) {
    if (payload.type === 'bump') return block.id
    if (payload.type === 'response') return parentBlock.id
    // Else await lookup(HeadOf) || await PRNG.uuid()
  }

  /* @override */
  async compute (currentValue, { payload, date, AUTHOR, block, postpone, reject, lookup, index, signal }) {
    if (payload.type === 'bump') {
      const to = toHex(payload.target)
      // Validate block
      if (block.genesis) return reject('NoAnonymousBumps')
      if (to === AUTHOR) return reject('NoSelfBumps')

      // Secondary Index lookups: bumpsByAuthor[author] => block-id
      const lastBump = await this.readState(await lookup(AUTHOR))
      if (lastBump?.status === 'pending') return reject('CannotInitiateWhilePending')

      // Cross memory lookups
      const peer = await this.store.roots.profile.readState(payload.target) // referencing "profiles" collection
      if (!peer) return postpone(10000, 'NoGhostBumps', 3) // Postpone upto 3 Times then RejectionMessage

      // Return new state
      await index(AUTHOR)
      return { ...currentValue, from: AUTHOR, to, status: 'pending', date }
    } else if (payload.type === 'response') {
      // Validate block
      const blockAuthor = toHex(block.key) // AUTHOR is a tag referecing blockRoot, not author of block
      if (currentValue.to !== blockAuthor) return reject('NotYourBump')
      if (![true, false].includes(payload.ok)) return reject('InvalidResponse')
      // Return new state
      const out =  { ...currentValue, status: payload.ok ? 'accepted' : 'rejected', date }
      signal('bump-settled', { to: out.to, from: out.from, status: out.status })
      return out
    } else {
      return reject('InvalidType')
    }
  }

  /* @override */
  async expiresAt (value, latch) {
    const { status, to, from, date } = value
    if (status === 'pending') return date + 5 * 60000 // 5-minutes timeout
    const lastPeerOut = Math.max(
      await latch('profile', to),
      await latch('profile', from)
    )
    return Math.min(
      date + 3 * 60 * 60000, // 3-hours / prevent re-bumps
      lastPeerOut // Or expire when last peer is collected
    )
  }

  /* @override */
  async sweep ({ AUTHOR, deIndex }) {
    await deIndex(AUTHOR) // Release Bump-cap
  }

  // -- END OF Overrides, begin action definitions.

  /**
   * Create a new Bump request
   * @param {PublicKey} target The key of bump-target
   * @param {SecretKey} secret Secret of initator
   * @returns {Feed} The new block ready to be transmitted to network.
   */
  async createBump (target, secret) {
    return this.createBlock(getPublicKey(secret), { type: 'bump', target: toU8(target) }, secret)
  }

  /**
   * Respond to bump request
   * @param {BlockID} bumpId the block id of the bump-request
   * @param {SecretKey} secret Secret of responder
   * @param {boolean} ok Response, default accept, but can be rejected.
   * @returns {Feed} The new block ready to be transmitted to network.
   */
  async respondBump (bumpId, secret, ok = true) {
    return this.createBlock(bumpId, { type: 'response', ok }, secret)
  }
}
