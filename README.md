# @metreeca/pipe

[![npm](https://img.shields.io/npm/v/@metreeca/pipe)](https://www.npmjs.com/package/@metreeca/pipe)

A lightweight TypeScript library for composable async iterable processing.

**@metreeca/pipe** provides an idiomatic, easy-to-use functional API for working with async iterables through pipes,
tasks, and sinks. The composable design enables building complex data processing pipelines with full type safety and
minimal boilerplate. Key features include:

- **Focused API** › Small set of operators covering common async iterable use cases
- **Natural syntax** › Readable pipeline composition: `pipe(items(data)(filter())(map())(toArray()))`
- **Minimal boilerplate** › Automatic `undefined` filtering and seamless type inference across pipeline stages
- **Task/Sink pattern** › Clear separation between transformations and terminal operations
- **Parallel processing** › Built-in `{ parallel: true }` option for concurrent execution
- **Extensible design** › Easy creation of custom feeds, tasks, and sinks

# Installation

```shell
npm install @metreeca/pipe
```

> [!WARNING]
> TypeScript consumers must use `"moduleResolution": "bundler"` (or `"node16"`/`"nodenext"`) in `tsconfig.json`.
> The legacy `"node"` resolver is not supported.

# Usage

## Core Concepts

**@metreeca/pipe** provides four main abstractions:

- **Pipes**: Async iterables with a composable API
- **Feeds**: Create pipes from arrays, ranges, generators, and other sources
- **Tasks**: Transform items with map, filter, and other operations
- **Sinks**: Collect results into arrays, reduce values, find items, etc.

## Creating Feeds

Create [feeds](https://metreeca.github.io/pipe/modules.html#Feeds) from various data sources.

```typescript
import { range, items, chain, merge, iterate } from '@metreeca/pipe';

items(42);                    // from single values
items(1, 2, 3, 4, 5);         // from multiple scalar values
items([1, 2, 3, 4, 5]);       // from arrays
items(new Set([1, 2, 3]));    // from iterables
items(asyncGenerator());      // from async iterables
items(pipe);                  // from pipes
range(10, 0);                 // from numeric ranges

iterate(() => Math.random()); // from repeated generator calls

chain(                        // sequential consumption
	items([1, 2, 3]),
	items([4, 5, 6])
);

merge(                        // concurrent consumption
	items([1, 2, 3]),
	items([4, 5, 6])
);
```

## Transforming Data

Chain [tasks](https://metreeca.github.io/pipe/modules.html#Tasks) to transform, filter, and process items.

```typescript
import { pipe, items, map, filter, take, distinct, batch, toArray } from '@metreeca/pipe';

await pipe(
	(items([1, 2, 3, 4, 5]))
	(filter(x => x%2 === 0))
	(map(x => x*2))
	(take(2))
	(toArray())
);  // [4, 8]

await pipe(
	(items([1, 2, 2, 3, 1]))
	(distinct())
	(toArray())
);  // [1, 2, 3]

await pipe(
	(items([1, 2, 3, 4, 5]))
	(batch(2))
	(toArray())
);  // [[1, 2], [3, 4], [5]]
```

## Parallel Processing

Process items concurrently with the `parallel` option in `map()` and `flatMap()` tasks.

```typescript
import { flatMap, items, map, pipe, toArray } from "@metreeca/pipe";


await pipe( // mapping with auto-detected concurrency (CPU cores)
	(items([1, 2, 3]))
	(map(async x => x*2, { parallel: true }))
	(toArray())
);

await pipe( // mapping with unbounded concurrency (I/O-heavy tasks)
	(items(urls))
	(map(async url => fetch(url), { parallel: 0 }))
	(toArray())
);

await pipe( // flat-mapping with explicit limit
	(items([1, 2, 3]))
	(flatMap(async x => [x, x*2], { parallel: 2 }))
	(toArray())
);
```

## Consuming Data

Apply [sinks](https://metreeca.github.io/pipe/modules.html#Sinks) as terminal operations that consume pipes and return
promises with final results.

```typescript
import { pipe, items, some, every, find, reduce, toArray, toSet, toMap, forEach } from '@metreeca/pipe';

await pipe(
	(items([1, 2, 3]))
	(some(x => x > 2))
);  // Promise<true>

await pipe(
	(items([1, 2, 3, 4]))
	(find(x => x > 2))
);  // Promise<3>

await pipe(
	(items([1, 2, 3, 4]))
	(reduce((a, x) => a+x, 0))
);  // Promise<10>

await pipe(
	(items([1, 2, 3]))
	(toArray())
);  // Promise<[1, 2, 3]>

await pipe(
	(items([1, 2, 3]))
	(forEach(x => console.log(x)))
);  // Promise<void>
```

Alternatively, call `pipe()` without a sink to get the underlying async iterable for manual iteration.

```typescript
import { filter, items, pipe } from "@metreeca/pipe";

const iterable = pipe(
	(items([1, 2, 3]))
	(filter(x => x > 1))
);  // AsyncIterable<number>

for await (const value of iterable) {
	console.log(value);  // 2, 3
}
```

## Working with Infinite Feeds

Use `iterate()` to create infinite feeds from generator functions. Tasks and sinks handle infinite feeds gracefully,
processing values lazily until a limiting operator (like `take()`) or terminal sink stops consumption.

```typescript
import { pipe, iterate, filter, take, forEach } from '@metreeca/pipe';

pipe(
	(iterate(() => Math.random()))
	(filter(v => v > 0.5))
	(take(3))
	(forEach(console.info))
);
```

## Creating Custom Tasks

Tasks are functions that transform async iterables. Create custom tasks by returning an async generator function.

```typescript
import { items, toArray, type Task } from '@metreeca/pipe';

function double<V extends number>(): Task<V, V> {
	return async function* (source) {
		for await (const item of source) { yield item*2 as V; }
	};
}

await items([1, 2, 3])(double())(toArray());  // [2, 4, 6]
```

## Creating Custom Feeds

Feeds are functions that create new pipes.

```typescript
import { items, toArray, type Pipe } from '@metreeca/pipe';

function repeat<V>(value: V, count: number): Pipe<V> {
	return items(async function* () {
		for (let i = 0; i < count; i++) { yield value; }
	}());
}

await repeat(42, 3)(toArray());  // [42, 42, 42]
```

> [!CAUTION]
> When creating custom feeds, always wrap async generators, async generator functions, or `AsyncIterable<T>` objects
> with [`items()`](https://metreeca.github.io/pipe/functions/items.html) to ensure `undefined` filtering and proper
> pipe interface integration.

# Support

- open an [issue](https://github.com/metreeca/pipe/issues) to report a problem or to suggest a new feature
- start a [discussion](https://github.com/metreeca/pipe/discussions) to ask a how-to question or to share an idea

# License

This project is licensed under the Apache 2.0 License –
see [LICENSE](https://github.com/metreeca/pipe?tab=Apache-2.0-1-ov-file) file for details.
