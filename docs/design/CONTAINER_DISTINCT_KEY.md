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

For containers (Array/Map/Tuple), include the dynamic type in the kind:
`concat('c:', {ty})` instead of `'c'`. Since {ty} is already "Array(Date)" or
"Map(String, Date)" etc., the kind now distinguishes containers by their
element types. Two containers with the same element types but different
values are still distinguished by the text (toString). Two containers with
different element types are distinguished by the kind.

## Backward / forward compat

- Existing containers: kind changes from 'c' to 'c:Array(...)' etc. This
  changes the distinct key SQL, so corpus goldens would change if any corpus
  query DISTINCTs a container. None do today (ratchet green).
- New queries DISTINCTing containers now get type-aware keys.

## Success criteria

1. `{a: date('2020-01-15')}` and `{a: '2020-01-15'}` get different keys.
2. `[date(...)]` and `['2020-01-15']` get different keys.
3. Corpus goldens byte-identical (ratchet).
