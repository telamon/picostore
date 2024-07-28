/**
 * Creates an object proxy that prevent writes
 * to target `o`, think Object.freeze() v300
 * @param {any} o object you wish to encase
 * @parma {'throw'|'catch'|?} write Write behaviour
 * @returns {Proxy}
 */
const V = 1
const ICED = Symbol.for('ICE')
const ICEB = Symbol.for('ICE_SIEVE')
export function ice (o, _sieve) {
  if (!o || typeof o !== 'object') return o // noop
  if (_sieve === true) _sieve = Array.isArray(o) ? [] : {}
  const handler = {
    get (target, property) {
      if (_sieve && property in _sieve) return _sieve[property]
      const value = target[property]
      if (value && typeof value === 'object') {
        _sieve[property] = Array.isArray(value) ? [] : {}
        return ice(value, _sieve[property])
      }
      return value
    },

    set (target, property, value) {
      if (!_sieve) throw new Error(`Attempted to write ${value} to property "${property}" on target ${target}`)
      _sieve[property] = value
      return true
    },
  }
  const p = new Proxy(o, handler)
  if (p[ICEB]) debugger
  Object.defineProperty(p, ICED, { value: V, enumerable: false, writable: false, configurable: false })
  Object.defineProperty(p, ICEB, { value: _sieve, enumerable: false, writable: false, configurable: false })
  return p
}

export function thaw (target) {
  if (!target[ICED]) return target
  // if (target[ICED] !== V) throw new Error('!')
  const _sieve = target[ICEB]
  const out = Array.isArray(target) ? [] : {}
  for (const prop in target) {
    if (_sieve && prop in _sieve) out[prop] = _sieve[prop]
    else if (target[prop]?.[ICED]) out[prop] = thaw(target)
    else out[prop] = target[prop]
  }
  return out
}
/**
export function tripWire (o, path = []) {
  if (!o || typeof o !== 'object') return o
  return new Proxy(o, {
    get (target, key) {
      switch (typeOf(o)) {
        case 'array': // TODO?
        case 'object': return tripWire(target[key], [...path, key])
        default: return target[key]
      }
    },
    set (_, key) {
      throw new Error(`Attempted to modifiy: ${path.join('.')}[${key}]`)
    }
  })
} */
