/**
 * HOLDUP!!!
 * This is not what i need...
 * What i need is an object -> mutation transactions log.
 * or 'needed', this is a contender for the diff() and applyPatch() functions in index.js
 * I am unsure at the moment how useful the CRDTMemory is so i will leave this code here for later.
 *
 * Creates an object proxy that prevent writes
 * to target `o`, think Object.freeze() v300
 * @param {any} o object you wish to encase
 * @parma {'throw'|'catch'|?} write Write behaviour
 * @returns {Proxy}
 */
const V = 1
export const SYM_ICE = Symbol.for('ICE')
export const SYM_SIEVE = Symbol.for('ICE_SIEVE')
export const SYM_PATH = Symbol.for('ICE_PATH')

export function ice (o, _sieve, path = '') {
  console.info('new proxy', path, _sieve)
  if (!o || typeof o !== 'object') return o // noop
  if (_sieve === true) _sieve = Array.isArray(o) ? [] : {}

  const handler = {
    get (target, property) {
      if (property === SYM_ICE) return V
      if (property === SYM_SIEVE) return _sieve
      if (property === SYM_PATH) return path

      if (_sieve && property in _sieve) return _sieve[property] // Value is in sieve

      const value = target[property]
      if (!(value && typeof value === 'object')) return value // Value is primitive

      if (_sieve) _sieve[property] = Array.isArray(value) ? [...value] : { ...value } // Configure new sieve
      return ice(value, _sieve && _sieve[property], path + '/' + property) // Return new proxy
    },

    set (_, property, value) {
      console.info(`set "${path}/${property}" = ${value}`)
      if (!_sieve) throw new Error(`Prevented write to property "${path}/${property}" = "${value}"`)
      _sieve[property] = value
      return true
    }
  }
  const p = new Proxy(o, handler)
  return p
}

export function thaw (target) {
  if (!target[SYM_ICE]) return target
  const path = target[SYM_PATH]
  const _sieve = target[SYM_SIEVE]
  console.info(`thaw(${path})`, _sieve)
  const out = Array.isArray(target) ? [] : {}

  for (const prop in target) {
    const value = _sieve && prop in _sieve
      ? _sieve[prop]
      : target[prop]
    console.info(`thaw(${path}/${prop})`, value[SYM_SIEVE],  value)
    out[prop] = thaw(value)
  }
  return out
}
