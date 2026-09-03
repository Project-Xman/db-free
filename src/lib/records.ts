// SOT: typed-record-helpers, keys-of

// WHAT:  `Object.keys` that keeps the key union instead of widening to string.
// WHY:   Registries are objects keyed by a bindings enum; iterating them must
//        stay exhaustive without an `as` cast.
export function keysOf<K extends string>(record: Record<K, object>): K[] {
  return Object.keys(record).filter((key): key is K => key in record);
}
