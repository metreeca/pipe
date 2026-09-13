# @metreeca/flow

[![npm](https://img.shields.io/npm/v/@metreeca/flow)](https://www.npmjs.com/package/@metreeca/flow)

Composable primitives for asynchronous data flows.

**@metreeca/flow** replaces the boilerplate of async data processing with declarative data flows, read as one unit
from source to result:

- **Focused API**: small operator set covering the common async iterable operations and composing with a natural syntax
- **Type Safety**: item types carried from the data source through every stage to the final result, never dictated by
  the functions handed to the steps
- **Concurrency Control**: any task run concurrently over the feed, under an explicit bound on the items in flight
- **Extensible Design**: custom steps written against the same contracts as the built-in ones, chaining in any order

# Installation

```shell
npm install @metreeca/flow
```

> [!WARNING]
>
> TypeScript consumers must use `"moduleResolution": "nodenext"/"node16"/"bundler"` in `tsconfig.json`.
> The legacy `"node"` resolver is not supported.

# Core Usage

> [!NOTE]
>
> This section introduces essential concepts and common patterns: see the
> [API reference](https://metreeca.github.io/flow/) for complete coverage.

**@metreeca/flow** builds on three composable [abstractions](https://metreeca.github.io/flow/modules/index.html):

- **[Feeds](https://metreeca.github.io/flow/modules/feeds.html)**: carry the items and accept the steps advancing the
  pipe, opened from values and data sources
- **[Tasks](https://metreeca.github.io/flow/modules/tasks.html)**: intermediate operations transforming, filtering or
  reshaping the items
- **[Sinks](https://metreeca.github.io/flow/modules/sinks.html)**: terminal operations consuming the items and computing
  the final result

A **[pipe](https://metreeca.github.io/flow/functions/pipe.html)** composes a feed, any number of tasks and an optional
sink. Closed by a sink, it resolves to the final result; left open, it hands back its final feed, iterated with
`for await` to draw the items it carries.

A feed ends when the source it draws from stops yielding items: an array or a generator ends it on its own, under the
[iteration protocols](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols), while a
source without a natural end is bounded by the feed it is opened with, or downstream: see
[Bounding Feeds](#bounding-feeds).

The three combine into a single pipe. This example draws a set of URLs, retrieves them concurrently, parses each
response as JSON and collects the documents:

```typescript
import { pipe } from '@metreeca/flow';
import { items } from '@metreeca/flow/feeds';
import { toArray } from '@metreeca/flow/sinks';
import { fork, map } from '@metreeca/flow/tasks';

const urls = [1, 2, 3, 4, 5, 6].map(page => `https://example.com/docs?page=${page}`);

const docs = await pipe(
	(items(urls))
	(fork(4, map(url => fetch(url))))  // never more than 4 requests running at a time
	(map(response => response.json())) // JSON responses
	(toArray())                        // the parsed documents
);
```

The contracts are the whole of the API: the built-in tasks and sinks are ordinary functions honouring them, with no
privileged core underneath. Custom steps honour the same contracts, so they chain with the built-in ones in any
order, `items()` turning any value, source or generator into a conforming feed. Every step is handed a feed in turn,
so a custom one either draws the items itself or passes the feed on to steps already available.

## Creating Feeds

Start a pipe with a [feed](https://metreeca.github.io/flow/modules/feeds.html), adapting values and data sources into a
feed ready for composition.

### Generators

Open a feed over values, ranges or an external source drawn one item at a time.

```typescript
import { done, inlet, items, range } from '@metreeca/flow/feeds';

items([1, 2, 3, 4, 5]);                         // from arrays
items(new Set([1, 2, 3]));                      // from iterables
items(asyncGenerator());                        // from async iterables, an existing feed among them
items(42);                                      // from single values
items(fetch(url).then(r => r.json()));          // from promises, awaited on consumption; the payload enters whole

range(10, 0);                                   // from numeric ranges

inlet(() => Math.random());                     // from an external source, endless unless the source ends it
inlet(() => queue.size ? queue.poll() : done);  // ended by the source reporting the done marker
```

`items()` contributes a source according to its shape: iterables and async iterables contribute the items they yield,
while any other value, a promise once awaited, is contributed whole as a single item. The source is drawn exactly as
handed over, so a feed opened from a generator object runs dry after the first pass, while one opened from a repeatable
source, an array or a set among them, is consumed afresh at each. A feed reported by a task is drained by a single pass
whatever it draws from: consume a composition twice by building it afresh from the feed it opens with.

Several feeds are combined into one by carrying them in a feed of their own and splicing it with `flat()` or `join()`:
see [Splicers](#splicers).

## Transforming Data

Chain [tasks](https://metreeca.github.io/flow/modules/tasks.html) to reshape the feed and map its items.

### Operators

Select, reorder and inspect items, leaving their type unchanged.

> [!TIP]
>
> The [order](https://metreeca.github.io/core/modules/order.html) module of `@metreeca/core` provides helper functions
> for assembling complex sorting criteria.

```typescript
import { distinct, filter, peek, skip, sort, take } from '@metreeca/flow/tasks';

await pipe(
	(items([1, 2, 3, 4, 5]))
	(filter(n => n%2 === 0))
	(toArray())
);  // [2, 4]

await pipe(
	(items([1, 2, 2, 3, 1]))
	(distinct())
	(toArray())
);  // [1, 2, 3]

await pipe(
	(items([3, 1, 4, 1, 5]))
	(sort())
	(toArray())
);  // [1, 1, 3, 4, 5]

await pipe(
	(items([{ name: "Alice", age: 30 }, { name: "Bob", age: 25 }]))
	(sort(by(x => x.age)))
	(toArray())
);  // [{ name: "Bob", age: 25 }, { name: "Alice", age: 30 }]

await pipe(
	(items([1, 2, 3, 4, 5]))
	(skip(2))
	(toArray())
);  // [3, 4, 5]

await pipe(
	(items([1, 2, 3, 4, 5]))
	(take(3))
	(toArray())
);  // [1, 2, 3]

await pipe(
	(items([1, 2, 3]))
	(peek(n => console.log(n)))
	(toArray())
);  // logs 1, 2, 3; [1, 2, 3]
```

### Transformers

Map items to values of a different type, either one by one, in groups, or over the feed as a whole.

```typescript
import { batch, group, map, recast } from '@metreeca/flow/tasks';

await pipe(
	(items([1, 2, 3]))
	(map(n => n*2))
	(toArray())
);  // [2, 4, 6]

await pipe(
	(items([1, 2, 3, 4, 5]))
	(batch(2))
	(toArray())
);  // [[1, 2], [3, 4], [5]]

await pipe(
	(items([1, 2, 3, 4, 5]))
	(group(n => n%2))
	(toArray())
);  // [[1, [1, 3, 5]], [0, [2, 4]]]

await pipe(
	(items([1, 2, 3, 4]))
	(recast(async feed => (await feed(toArray())).slice(-2)))
	(toArray())
);  // [3, 4], as the last items are known only once the feed runs dry
```

`recast()` hands the feed to a mapper and carries on with the items it computes, so a step that cannot decide before the
feed runs dry (reconciling it against a stored snapshot, ranking it, clearing it against a quota) is written as an
ordinary asynchronous function of the feed rather than as a generator. Sinks already available are lifted back into the
pipe the same way, as `recast(toSet())` is to carry on with the distinct items alone. The items are supplied either as
a batch listing them or as a feed yielding them, handed back as they are or awaited, so a step composing the feed it
draws from with tasks already available is spared the generator as well. Whatever the mapper computes is emitted item
by item, so the items carried on need be neither the ones drawn, nor as many, nor of the same type.

### Splicers

Splice several feeds into the pipe: the nested feeds a source carries or a task opens, in source order or interleaved
as their items become available, the concurrent runs of a task, or the branches every item is handed to.

```typescript
import { filter, flat, fork, join, map, take, tee } from '@metreeca/flow/tasks';

await pipe(
	(items([items([1, 2]), items([3, 4])]))
	(flat())
	(toArray())
);  // [1, 2, 3, 4]

await pipe(
	(items([1, 2, 3]))
	(flat(map(n => items([n, n*10]))))
	(toArray())
);  // [1, 10, 2, 20, 3, 30], as each item is expanded into the items of its own feed

await pipe(
	(items([items([1, 2, 3]), items([4, 5, 6])]))
	(flat(map(feed => feed(take(2)))))
	(toArray())
);  // [1, 2, 4, 5], as the quota is scoped to each nested feed

await pipe(
	(items([slow, fast]))  // slow yields 1, 2; fast yields 3, 4
	(join())
	(toArray())
);  // [3, 4, 1, 2], as the faster feed reports first

await pipe(
	(items(ids))
	(fork(4, retrieve()))
	(toArray())
);  // at most 4 items in flight, results in completion order

await pipe(
	(items([1, 2, 3]))
	(tee(map(n => n*2), filter(n => n > 2)))
	(toArray())
);  // 2, 4, 6 from the doubling branch and 3 from the filtering one, interleaved in no defined order
```

`flat()` splices one level only: a feed carried by a nested feed is reported as an item, ready for a further splice. Its
optional task opens the feeds to splice, drawing from the whole feed, so an item mapped to a feed of its own is expanded
in place; scope a task to each nested feed by applying it within `map()`, where the source already carries feeds.
`join()` splices the same way, but opens every nested feed as soon as it is reported and emits items as they become
available, so output order is not preserved and nothing bounds the number of feeds open at once.

`tee()` fans out instead of splitting: every branch is applied to the whole feed and handed every item, so a stateful
branch decides on every item, unlike a forked run; the items the branches report are interleaved as `join()` interleaves
nested feeds. Branches draw in lockstep, so nothing is held beyond the item on offer and the source advances at the pace
of the slowest branch: pacing and long-running work belong downstream of the fan-out, while a branch closing early, as
`take()` does, drops out and stops holding back the others.

See [Concurrent Processing](#concurrent-processing) for the bounds `fork()` sets and the state it tolerates.

## Consuming Data

Close a pipe with a [sink](https://metreeca.github.io/flow/modules/sinks.html), consuming the items and computing the
final result.

### Predicates

Test the feed against a condition, stopping as soon as the outcome is decided.

```typescript
import { every, some } from '@metreeca/flow/sinks';

await pipe(
	(items([1, 2, 3]))
	(some(n => n > 2))
);  // true

await pipe(
	(items([2, 4, 6]))
	(every(n => n%2 === 0))
);  // true
```

### Aggregators

Reduce the feed to a single value, resolving to `undefined` on an empty feed where no result is defined. `sum()`
and `avg()` handle `number` and `bigint` feeds alike, rounding `bigint` means to the nearest integer, halves away from
zero. `min()` and `max()` rank items with the same comparators as `sort()`, resolving to the first of equally ranking
ones.

> [!TIP]
>
> The [order](https://metreeca.github.io/core/modules/order.html) module of `@metreeca/core` provides helper functions
> for assembling complex ranking criteria.

```typescript
import { avg, count, max, min, sum } from '@metreeca/flow/sinks';

await pipe(
	(items([1, 2, 3, 4, 5]))
	(count())
);  // 5

await pipe(
	(items([1, 2, 3, 4, 5]))
	(sum())
);  // 15

await pipe(
	(items([1, 2, 3, 4]))
	(avg())
);  // 2.5

await pipe(
	(items([1n, 2n, 4n]))
	(avg())
);  // 2n

await pipe(
	(items([3, 1, 4, 1, 5]))
	(min())
);  // 1

await pipe(
	(items([{ name: "Alice", age: 30 }, { name: "Bob", age: 25 }]))
	(max(by(x => x.age)))
);  // { name: "Alice", age: 30 }
```

### Scanners

Retrieve a single item from the feed or fold it into a value of an arbitrary type. Where no item matches, `find()`
resolves to `undefined`, leaving the choice of a fallback to the caller, while `seek()` fails, so the item it hands
back is usable as is.

```typescript
import { find, reduce, seek } from '@metreeca/flow/sinks';

await pipe(
	(items([1, 2, 3, 4]))
	(find(n => n > 2))
);  // 3

await pipe(
	(items([1, 2, 3, 4]))
	(seek(n => n > 10))
);  // fails, as no item matches

await pipe(
	(items([1, 2, 3, 4]))
	(reduce((total, n) => total+n, 0))
);  // 10
```

### Collectors

Collect items into a container. The result is deeply immutable, freezing both the container and the items, keys and
values collected into it.

```typescript
import { toArray, toMap, toObject, toSet, toString } from '@metreeca/flow/sinks';

await pipe(
	(items([1, 2, 3]))
	(toArray())
);  // [1, 2, 3]

await pipe(
	(items([1, 2, 2, 3]))
	(toSet())
);  // Set(3) { 1, 2, 3 }

await pipe(
	(items([{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]))
	(toMap(x => x.id, x => x.name))
);  // Map(2) { 1 => "Alice", 2 => "Bob" }

await pipe(
	(items([{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]))
	(toObject(x => x.id, x => x.name))
);  // { 1: "Alice", 2: "Bob" }

await pipe(
	(items([1, 2, 3]))
	(toString(" - "))
);  // "1 - 2 - 3"
```

### Effects

Consume the feed for its side effects, resolving to the number of items processed.

```typescript
import { each } from '@metreeca/flow/sinks';

await pipe(
	(items([1, 2, 3]))
	(each(n => console.log(n)))
);  // logs 1, 2, 3; 3
```

# Advanced Usage

## Manual Iteration

Feeds are async iterables, so a pipe left open without a sink is iterated directly, pulling items as the surrounding
code is ready for them. Bracketing the composition in `pipe()` hands back the feed it ends with, reading as one unit
exactly as a closed pipe does.

```typescript
import { pipe } from '@metreeca/flow';
import { items } from '@metreeca/flow/feeds';
import { filter } from '@metreeca/flow/tasks';

const evens = pipe(
	(items([1, 2, 3, 4]))
	(filter(n => n%2 === 0))
);  // Feed<number>

for await (const value of evens) {
	console.log(value);  // 2, 4
}
```

## Bounding Feeds

Use `inlet()` to open infinite feeds over an external source. Items are pulled lazily, so an infinite feed is consumed
only as far as the pipe demands: bound it with a task like `take()`, or with a sink deciding its outcome early, such as
`some()`, `every()`, `find()` or `seek()`.

A source knowing when it is exhausted ends the feed itself, either reporting the `done` marker in place of a value or
wrapped in a generator of its own; an `AbortSignal` bounds it from the outside instead, ending it as an exhausted source
does and leaving the items already contributed to the pipe that consumes them.

```typescript
inlet(() => queue.size ? queue.poll() : done);           // every value queued, until the queue runs dry

inlet(() => cursor.next(), AbortSignal.timeout(1_000));  // every value reported within the deadline
```

Two hazards follow, the first on infinite feeds alone, the second on any feed large enough:

- **never completing**: `sort()`, `group()`, an unbounded `batch()` and a `recast()` whose mapper draws the feed entire
  emit nothing before the whole feed is drawn, and every sink but `some()`, `every()`, `find()` and `seek()` needs
  every item
- **exhausting memory**: the first three of those tasks materialise the feed whole and a `recast()` holds whatever its
  mapper retains and computes, `distinct()` retains every key seen, `join()` holds a pending item per open nested feed
  and an uncapped `fork()` a run per item drawn, and the collectors build the whole container before resolving

Batch by a positive size, cap the runs of a fork, or bound the feed upstream.

The [API reference](https://metreeca.github.io/flow/) classifies every feed, task and sink along four axes:

- **bounded** or **infinite**, for how far a feed goes
- **incremental** or **exhaustive**, for how much of a feed a task or sink draws before emitting or resolving
- **streaming** or **materialising**, for what is held in memory
- **stateless** or **stateful**, for whether the outcome depends on the items drawn before it

```typescript
import { inlet } from '@metreeca/flow/feeds';
import { filter, take } from '@metreeca/flow/tasks';
import { each } from '@metreeca/flow/sinks';
import { pipe } from '@metreeca/flow';

await pipe(
	(inlet(() => Math.random()))
	(filter(n => n > 0.5))
	(take(3))
	(each(n => console.info(n)))
);
```

## Concurrent Processing

Wrap any task with `fork()` to interleave several runs of it over the same feed: the runs draw from the same source,
each item going to exactly one of them, and results are emitted as soon as they are ready rather than in source order.

Runs are interleaved on the event loop rather than executed in parallel: the bound covers the asynchronous operations
overlapping at any time, not the computation carried out simultaneously, so forking a task that blocks the event loop
buys nothing.

The number of runs bounds how far the task reads ahead of the downstream consumer, so it controls memory usage and
backpressure, not the rate at which work is submitted. A capped fork starts every run when the feed is opened, so the
task is invoked exactly as many times as the bound, whether or not the source is fast enough to keep every run busy.

Pass `0` to leave the fork uncapped, opening a run for each item the source delivers: throughput is limited only by the
source, at the cost of an unbounded number of items in flight. The number of runs must be an integer, otherwise a
`TypeError` is thrown; negative values are treated as 1, that is, as sequential processing.

A forked task never sees the whole feed. The task is a single function invoked once per run: state it initialises on
invocation, as `distinct()`, `sort()`, `take()`, `skip()`, `batch()`, `group()` and `recast()` do, is tracked per run
rather than across the feed as a whole, while state captured in its enclosing closure is shared by every run and
accessed concurrently. Fork a stateful task only where its outcome is sound on the items one run happens to draw.

Where the state belongs to one item rather than to the feed, open a pipe per item and collapse the pipes with
`join(map(…))`, which keeps it scoped to that item while still drawing from every pipe at once. `fork()` composes
within each nested feed in turn, as `join(map(feed => feed(fork(…))))`, bounding the work carried out inside it.

Control the rate at which work is submitted with utilities from the
[async](https://metreeca.github.io/core/modules/async.html) module of `@metreeca/core`, such as throttling:

```typescript
import { createThrottle } from '@metreeca/core/async';
import { pipe } from '@metreeca/flow';
import { items } from '@metreeca/flow/feeds';
import { each } from '@metreeca/flow/sinks';
import { fork } from '@metreeca/flow/tasks';

const throttle = createThrottle({ minimum: 1000 });  // at most 1 request per second

await pipe(
	(items(ids))
	(fork(4, retrieve(throttle)))  // inject delays to enforce the rate limit
	(each(x => console.log(x)))
);
```

## Creating Custom Feeds

A custom feed opens a pipe over a source of your own.

```typescript
import { pipe } from '@metreeca/flow';
import { items } from '@metreeca/flow/feeds';
import { toArray } from '@metreeca/flow/sinks';
import type { Feed } from '@metreeca/flow';

function repeat<V>(value: V, count: number): Feed<V> {
	return items((async function* () {
		for (let i = 0; i < count; i++) { yield value; }
	})());
}

await pipe(
	(repeat(42, 3))
	(toArray())
);  // [42, 42, 42]
```

> [!NOTE]
>
> A custom feed is only required to honour the `Feed` contract, however it is assembled: handing the source to
> [`items()`](https://metreeca.github.io/flow/functions/items.html) is the shortest route there, as the adapter takes
> the contract on, while a source that is already a feed honours it as it is. Whichever route is taken, a feed replays
> only as far as its source does: see [Creating Feeds](#creating-feeds).

## Creating Custom Tasks

A custom task extends a pipe, consuming the items of a feed and reporting a new one. The transformation is most easily
written as an async generator handed to `items()`, with items to be dropped left unyielded.

```typescript
import { pipe } from '@metreeca/flow';
import { items } from '@metreeca/flow/feeds';
import { toArray } from '@metreeca/flow/sinks';
import type { Task } from '@metreeca/flow';

function double<V extends number>(): Task<V, V> {
	return source => items((async function* () {
		for await (const item of source) { yield item*2 as V; }
	})());
}

await pipe(
	(items([1, 2, 3]))
	(double())
	(toArray())
);  // [2, 4, 6]
```

> [!NOTE]
>
> A custom task is only required to report a feed honouring the `Feed` contract, however it is obtained: handing the
> generator object to [`items()`](https://metreeca.github.io/flow/functions/items.html) is the shortest route there,
> while a transformation delegating to tasks already available composes the feed it draws from with them and reports
> what they report, a feed already. A transformation deciding on the feed as a whole is spared the generator altogether
> by `recast()`, which carries on with the items a mapper computes over it. The reported feed is drained by a single
> pass, as every built-in one is, and so is the feed the task draws from.

## Creating Custom Sinks

A custom sink closes a pipe, consuming the items and returning a promise for the final result. A computation delegating
to operations already available applies them to the feed it draws from, which is drained by a single pass, however
repeatable the source behind it.

```typescript
import { pipe } from '@metreeca/flow';
import { items } from '@metreeca/flow/feeds';
import type { Sink } from '@metreeca/flow';

function histogram<V>(): Sink<V, Map<V, number>> {
	return async source => {

		const counts = new Map<V, number>();

		for await (const item of source) { counts.set(item, (counts.get(item) ?? 0)+1); }

		return counts;
	};
}

await pipe(
	(items(["a", "b", "a"]))
	(histogram())
);  // Map(2) { "a" => 2, "b" => 1 }
```

# Support

- open an [issue](https://github.com/metreeca/flow/issues) to report a problem or to suggest a new feature
- start a [discussion](https://github.com/metreeca/flow/discussions) to ask a how-to question or to share an idea

# License

This project is licensed under the Apache 2.0 License –
see [LICENSE](https://github.com/metreeca/flow?tab=Apache-2.0-1-ov-file) file for details.
