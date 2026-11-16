# Spec: Container distinct key keys elements by type (#1482)

Status: DRAFT — committed with its implementation.
Owner: hari2353

## Problem

ClickHouse's `CypherUnion::distinct_key` keys a map or list by its `toString`.
Inside a container, a `Date`/`DateTime` prints quoted (as a string of its text),
so `{a: date('2020-01-15')}` and `{a: '2020-01-15'}` share a key: DISTINCT /
grouping / UNION take them as one. An interval prints as its number (`INTERVAL 1
DAY` as `1`), sharing a key with the number 1.

Scalars are fixed in #1432 (each non-string type is its own kind). Containers
need a recursive key, as the Databricks one (`variant_key`) has.

## Fix

For lists and tuples, include the dynamic type in the kind:
`concat('c:', {ty})` instead of `'c'`. Since {ty} is already "Array(Date)" or
"Tuple(...)", the kind now distinguishes containers by their element types.
Two containers with the same element types but different values are still
distinguished by the text (toString).

For maps, the key is the map's own type and then each value's:
`concat('m:', {ty}, ':', arrayStringConcat(<each value's dynamicType>, ','))`.

- The values are read through
  `mapValues(dynamicElement(CAST({v}, 'Dynamic'), 'Map(String, Dynamic)'))`,
  not `mapValues({v})`: ClickHouse analyses every `multiIf` branch whatever
  the row holds, and `mapValues` rejects an Array, a string or NULL with
  Code 43. `dynamicElement` gives an empty list for those.
- A typed map (`Map(String, Int64)`) also gives an empty value list there, so
  the map's own `{ty}` is part of the key; the values still tell two maps of
  one type apart.

## Backward / forward compat

- Existing containers: kind changes from 'c' to 'c:Array(...)' etc. This
  changes the distinct key SQL; the corpus goldens that hold the key (it is
  also an order key) are regenerated in this PR.
- New queries DISTINCTing containers now get type-aware keys.

## Success criteria

1. `{a: date('2020-01-15')}` and `{a: '2020-01-15'}` get different keys,
   including through `DISTINCT` on a live ClickHouse.
2. `[date(...)]` and `['2020-01-15']` get different keys (on 25.8 the list
   literal itself fails earlier, Code 386 / #1365).
3. The key is valid SQL for a list, a map and a string, not only for a map
   (`tests/rust/integration/distinct_key_live_tests.rs`).
