import { Block, feedFrom } from 'picofeed'
// TODO:
// - extract into separate package (sibling to repo)
// - choose eviction algo (avoid ddos)
export default class MemPool {
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

  async push (feed) {
    if (feed.first.isGenesis) throw new Error('GenesisRefused')
    for (const block of feed.blocks) {
      // Store forward ref
      await this.refs.put(block.psig, block.sig)
      await this._writeBlock(block)
    }
  }

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
      delRefs.push(block.parentSig)
      delBlocks.push(block.parentSig)
      blocks.push(block)
      next = block.parentSig
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

  async _writeBlock (block) {
    const key = block.sig
    await this.blocks.put(key, block.buffer)
  }

  async _readBlock (id) {
    const buffer = await this.blocks.get(id).catch(ignore404)
    if (buffer) return new Block(buffer)
  }

  async _hasBlock (id) {
    return !!(await this._readBlock(id))
  }
}

function ignore404 (err) { if (!err.notFound) throw err }
