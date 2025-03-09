import { Block, feedFrom, Feed } from 'picofeed'
// TODO:
// - choose eviction algo (avoid ddos)
export class Mempool {
  /** @typedef {import('abstract-level').AbstractLevel<any,Uint8Array,Uint8Array>} BinaryLevel */
  /** @typedef {import('picofeed').SignatureBin} SignatureBin */
  /** @type {BinaryLevel} */
  blocks = null
  /** @type {BinaryLevel} */
  refs = null

  /** @constructor
    * @param {BinaryLevel} db */
  constructor (db) {
    this.blocks = db.sublevel('b', {
      keyEncoding: 'view',
      valueEncoding: 'view'
    })

    this.refs = db.sublevel('r', {
      keyEncoding: 'view',
      valueEncoding: 'view'
    })
  }

  /** @param {Feed} feed */
  async push (feed) {
    if (feed.first.genesis) throw new Error('GenesisRefused')
    for (const block of feed.blocks) {
      // Store forward ref
      await this.refs.put(block.psig, block.sig)
      await this._writeBlock(block)
    }
  }

  /**
   * @param {SignatureBin} backward
   * @param {SignatureBin} forward
   * @return {Promise<Feed[]>}
   */
  async pop (backward, forward) {
    const delRefs = []
    const delBlocks = []
    const segments = []
    let next = backward
    let blocks = []
    // Load backwards
    while (1) {
      const block = await this._readBlock(next)
      if (!block) break
      delRefs.push(block.psig)
      delBlocks.push(block.psig)
      blocks.push(block)
      next = block.psig
    }
    if (blocks.length) segments.push(feedFrom(blocks, true))

    // Load forward
    next = forward
    blocks = []
    while (1) {
      const ptr = await this.refs.get(next).catch(ignore404)
      if (!ptr) break
      const block = await this._readBlock(ptr)
      delRefs.push(ptr)
      delBlocks.push(block.sig)
      blocks.push(block)
      next = block.sig
    }
    if (blocks.length) segments.push(feedFrom(blocks, true))

    await Promise.all([
      this.blocks.batch(delBlocks.map(key => ({ type: 'del', key }))),
      this.refs.batch(delRefs.map(key => ({ type: 'del', key })))
    ])
    return segments
  }

  /** @type {(block: Block) => Promise<void>} */
  async _writeBlock (block) {
    const key = block.sig
    await this.blocks.put(key, block.buffer)
  }

  /** @type {(id: SignatureBin) => Promise<Block>} */
  async _readBlock (id) {
    const buffer = await this.blocks.get(id).catch(ignore404)
    if (buffer) return new Block(buffer)
  }

  /** @type {(id: SignatureBin) => Promise<boolean>} */
  async _hasBlock (id) {
    return !!(await this._readBlock(id))
  }
}

function ignore404 (err) { if (!err.notFound) throw err }
