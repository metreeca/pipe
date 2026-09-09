# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unpublished](https://github.com/metreeca/flow/compare/v0.10.0...HEAD)

## [0.10.0](https://github.com/metreeca/flow/compare/v0.9.21...v0.10.0) - 2026-09-09

### Added

- `flat` and `join` tasks collapsing a feed of feeds into a single one: `flat` splices the nested feeds, draining one
  at a time and keeping the items of each together and in source order, while `join` opens them together and
  interleaves their items as they are reported; both optionally wrap a task opening the feeds to collapse, so
  `flat(map(mapper))` expands each item into the items of its own feed

- `tee` task handing every item to several branches and interleaving the items they report: the branches draw in
  lockstep, so nothing is held beyond the item on offer and the source advances at the pace of the slowest one, while
  a branch closing early drops out without holding back the others; unlike the runs of a `fork`, which split the items
  among themselves, every branch is applied to the whole feed and draws every item

- `drain` task carrying on with the items a sink computes over the whole feed: a step that cannot decide before the feed
  runs dry, reconciling it against a stored snapshot or clearing it against a quota, is written as an ordinary
  asynchronous function of the feed rather than as a generator, and a sink already available is lifted back into the
  pipe the same way, as `drain(toSet())` is to carry on with the distinct items alone

- `seek` sink retrieving the first matching item of a feed, failing where none does instead of resolving to
  `undefined` as `find` does, so the item handed back is usable as is, with no check to tell a missing item from an
  `undefined` item the feed legitimately carries

### Changed

- `feed` and `items` merged into a single `items` feed, opened over a data source of any shape: a batch contributes
  the items it carries, while any other value, a promise once awaited, is contributed whole, so `feed(source)` becomes
  `items(source)`, the variadic `items(a, b, c)` becomes `items([a, b, c])` and a lone value no longer needs wrapping
  in an array

- `items` hands back a source that is already a feed, rather than wrapping it in a new one: opening a feed over a feed
  is safe to repeat and preserves identity, so a step adapting the source it draws from costs nothing

- `Feed` contract extends `AsyncIterable`: a feed is iterated directly, replacing the call with no arguments that
  retrieved the underlying async iterable

- `Task` and `Sink` contracts are handed a `Feed` rather than a bare `AsyncIterable`, and a task reports one in turn,
  mirroring the feed it is applied to: a step may delegate the whole job, or part of it, to steps already available,
  applying them to the feed it draws from, while a step reporting an async generator of its own adapts it with
  `items()`, as custom feeds already do

- `Sink` result type defaults to `unknown` rather than to the item type: `Sink<V>` names a sink over `V` whatever it
  resolves to, so a sink handed as an argument or stored under a common type no longer has to state a result nobody
  reads

- `pipe()` collapses its two overloads into a single signature accepting a `Feed` or a `Promise` and reporting it back
  as handed in: a pipe left open now brackets to the feed it ends with, rather than to the async iterable underlying
  it, leaving consumers typing the result as `AsyncIterable` unaffected, as a feed is one

- `Data` no longer names `Feed` among its shapes, subsumed by `AsyncIterable` now that a feed is one: the shapes a
  feed can be opened from are unchanged

- feeds carry items exactly as contributed, dropping the filtering that withdrew `undefined` from the item domain:
  data shapes, `items` sources and `map` results are no longer widened to accept it, and pipes relying on the
  filtering discard those items explicitly, with `filter` or within the task reporting them

- `filter` predicates report a definite verdict, no longer widened to accept `undefined` as a stand-in for `false`:
  predicates leaning on it settle the undecided case themselves, typically with `?? false`

- `iterate` renamed to `inlet` and no longer bounded by an `undefined` result: a source ends its feed by reporting
  the new `done` marker, a unique symbol no produced value is mistaken for, so `undefined` and every other falsy value
  stay legal items, while a source never reporting it opens an endless feed, bounded downstream

- `inlet` accepts an optional `AbortSignal` bounding the feed from the outside, tested before each source call and
  ending the feed as an exhausted source does, leaving the items already contributed to the pipe consuming them

- `forEach` sink renamed to `each`, dropping the prefix the surrounding pipe already implies: `forEach(consumer)`
  becomes `each(consumer)`

- `concurrent` task renamed to `fork`, opening its runs on demand rather than all at once and reading `0` as an
  uncapped fork, a run per item drawn, in place of the sequential fallback it stood for: `concurrent(n, task)` becomes
  `fork(n, task)`

- `find` predicate is optional, defaulting to a test matching every item, so `find()` retrieves the first item of the
  feed

### Removed

- `chain` and `merge` feeds, expressed as a composition of the steps already available: `chain(a, b)` becomes
  `items([a, b])(flat())` and `merge(a, b)` becomes `items([a, b])(join())`, with the feeds to combine carried by a
  feed of their own

- `flatMap` task, expressed as `flat()` wrapping a `map()` reporting a feed for each item: `flatMap(mapper)` becomes
  `flat(map(item => items(mapper(item))))`, with the expansion opened as a feed before being spliced

- `Data` type, no longer part of the surface now that `flatMap` is gone: the shapes a feed is opened from are declared
  by the `items` signature

### Fixed

- steps take the item type from the feed they are applied to, no longer from the function handed to them: a consumer,
  predicate, selector, extractor, reducer or comparator accepting any item, `console.log` and `Boolean` among them,
  left the item type inferred as `any` or `unknown`, silently erasing the feed type for every step downstream of a
  `peek`, `filter`, `distinct` or `group` and widening the results of `find`, `seek`, `toMap` and `toObject`; a step
  composed on its own states the item type in the `Task` or `Sink` type it is declared under, or as an explicit type
  argument, rather than through an annotated function

## [0.9.21](https://github.com/metreeca/flow/releases/tag/v0.9.21) - 2026-08-28

### Added

- `concurrent` task interleaving several runs of another task over the same feed, under an explicit bound on the
  items in flight

- `group` task collecting feed items into groups sharing the same primitive key

- `toObject` sink collecting feed items into an object keyed by extracted property keys

- `sum` and `avg` sinks aggregating `number` and `bigint` feeds, with `bigint` averages rounded to the nearest
  integer, halves away from zero

- `min` and `max` sinks selecting the least and greatest feed item according to an optional comparator, defaulting
  to the `ascending` order from `@metreeca/core/order`

- `feed` opening a new feed over a data source, normalised according to its shape

### Changed

- Package renamed from `@metreeca/pipe` to `@metreeca/flow`, restarting version numbering at `0.1.0`: imports and the
  dependency entry must be updated, while the `/feeds`, `/tasks` and `/sinks` subpath exports are unchanged

- `Pipe` contract renamed to `Feed`: a feed carries the items open for composition, while a pipe is the composition
  of a feed, its tasks and an optional sink, as assembled by `pipe()`

- `items` feed accepts any number of values, each contributed to the feed as a single item whatever its shape;
  opening a feed over a data source expanded according to its shape is now the responsibility of `feed`

- `chain` and `merge` feeds accept only feeds, rather than data sources of any shape: sources of a different shape are
  opened with `feed` before being combined

- `map` and `flatMap` tasks no longer accept a `parallel` option: concurrency is now controlled by wrapping any task
  with `concurrent`, which replaces both the CPU-core default and the `parallel: 0` unbounded mode with an explicit
  concurrency limit

- `toMap` sink reports duplicate keys as an error rather than silently overwriting previously collected entries

- `toArray`, `toSet`, `toMap`, and `toObject` sinks return deeply immutable results, freezing both the returned
  container and the items, keys, and values collected into it

- `range` feed and `take`, `skip`, `batch`, and `concurrent` tasks reject non-integer bounds with a `TypeError` rather
  than accepting them

- `feed` and `iterate` feeds, `filter`, `distinct`, `map`, and `flatMap` tasks, and `some`, `every`, `find`, `reduce`,
  and `toMap` sinks accept any thenable wherever a native `Promise` was previously required, as declared by the
  `Awaitable` type from `@metreeca/core/async`

### Fixed

- data shapes no longer accept bare values, so a batch of arrays is no longer mistaken for the array carrying it:
  feeds of arrays are expressible and sources shadowing the batch that carries them are rejected at compile time

- feeds never carry `undefined`: optional entries are accepted in every data shape and in task results alike, and
  dropped as they enter the feed, so no task or sink observes one
