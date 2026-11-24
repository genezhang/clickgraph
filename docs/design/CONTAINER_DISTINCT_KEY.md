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

### The type is classed, not spelled

The `{ty}` in a container's key is classed as the scalar branch classes a
value (`container_type` in `function_mapper/clickhouse.rs`): numbers of any
width are one class (`Number`), the string-like types (`FixedString`, `Enum`,
`UUID`, `IPv4/6`) are `String`, `Date32` is `Date`, and a `Nullable(...)` /
`LowCardinality(...)` wrapper is removed with its parenthesis. A `UNION` of two
sources holding the same value in different storage types (`Array(Int32)` and
`Array(Int64)`, `Array(String)` and `Array(LowCardinality(String))`,
`Array(Int64)` and `Array(Nullable(Int64))`, `Map(String, Int32)` and
`Map(String, Int64)`) must give one row, as it does for scalars ("integers of
any width are one number") and on `main`; only a type that is a different kind
of value (a `Date` against a `String`) splits. The classing is applied to the
container's type and to each map value's `dynamicType`.

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
4. The same list / map value in two storage types is one group
   (`a_union_of_one_value_in_two_storage_types_is_one_row_on_live_clickhouse`).
5. A container inside a container is keyed by what is in it, to eight levels
   (`nested_containers_are_keyed_by_what_is_in_them_on_live_clickhouse`), and
   one instant is one value in a `DateTime` and a `DateTime64`, in a list or a
   map (`one_instant_in_two_datetime_types_is_one_row_in_a_container_on_live_clickhouse`).

## Nested containers (#1482, second part)

The first part keyed a container by its type and its text. That is exact where
the type tells what the elements are (`Array(Date)`), but a map literal's values
are `Dynamic` (the type is `Map(String, Dynamic)` whatever they hold), so
`{a: {b: date(...)}}` and `{a: {b: '...'}}`, `[{a: date(...)}]` and
`[{a: '...'}]` were one group. And `DateTime` / `DateTime64` print a fraction or
none (`[00:00:00]`, `[00:00:00.000]`): one instant, two texts.

`distinct_key` is now a gate outside every lambda:

```
multiIf(<keyed by elements>, <deep key>, <plain key>)
```

- **Plain key** (every scalar; a container whose elements are what its type
  says, such as `Array(Int64)`): exactly the key above. A long typed list is
  one `toString`.
- **Keyed by elements** (`keyed_by_elements`): a map, or a list of one
  dimension, whose type hides its elements (`Dynamic`, `Variant`) or prints
  them by storage type (`DateTime`, `Time`).
- **Deep key** (`deep_key`): the container as a tree, level by level
  (`arrayFold`), one token per node, the node's kind, its text and its number of
  elements (a container looked into has neither kind nor text of its own: its
  elements' are its). The elements of one level are the next level's nodes, so
  one lambda serves every level, and ClickHouse's planning time does not grow
  with `KEY_LEVELS` (nested lambdas did: 0.1 s at four levels, 0.25 s on 25.8).
  A level order with the number of elements of each node is one tree only, and
  each token is `toString` of a tuple, so no text can pass for another's.
  Because every token is a scalar's own, an instant is one value in any
  `DateTime` type, a date is not a string, and numbers of any width are one.

The elements of a `Dynamic` are read through a cast to `Array(Dynamic)` or to
`Array(Tuple(String, Dynamic))` (a map as its pairs), taken only where the value
is one: a cast of a scalar is an error and an `if` is evaluated for every row,
so the others hold a dummy container, and the cast is of an `identity(...)`
because ClickHouse moves a cast into the two arms of an `if`.

A `UNION` column that every arm lowers to a scalar (`Kind::Scalar`, `Boolean`
or `Null`: a literal, a function of numbers, a property of a declared scalar
type) is keyed by the plain key (`CypherUnion::scalar_key`) outright: ClickHouse
plans the whole expression of a key, the deep arm too, whatever the gate does at
run time (a 10-column `UNION` of integers took 1.7 s to plan on 25.8 with it,
79 ms without). A column of a type the lowering does not know (an undeclared
property, offline `cg sql`), a list or a map keeps the gate. The gate reads the
type's name after `(` or a space, so an `Enum` label such as `'Timeout'` is not
a type.

### Limits

- `KEY_LEVELS` (8) levels below the first container; deeper, a container is its
  type and text.
- A list in a list whose elements hide their types (`Array(Array(Dynamic))`) is
  its type and its text: ClickHouse casts a `Dynamic` to a list of one dimension
  only.
- The same list held as `Array(Date)` in one source and `Array(Dynamic)` in
  another is two groups (the plain key and the deep key are different keys).
- An `Interval` in a list through a `UNION` fails inside ClickHouse (Code 53).
