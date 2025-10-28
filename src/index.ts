/*
 * Copyright © 2025 Metreeca srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Asynchronous stream processing utilities.
 *
 * Provides a composable API for working with async iterables through pipes, tasks, and sinks.
 **
 * > [!WARNING]
 * > **All streams automatically filter out `undefined` values.** This filtering occurs at stream creation
 * > and when tasks are chained together, ensuring that `undefined` never flows through your pipeline.
 * >
 * > - `undefined` values are **removed** from all streams
 * > - Other falsy values (`null`, `0`, `false`, `""`) are **preserved**
 * > - Custom tasks yielding `undefined` have those values automatically filtered
 * > - This behavior is centralized in the {@link items} function, which is used internally for all stream creation
 *
 *
 * @remarks
 *
 * The library is designed to be extensible. You can create custom tasks and feeds to suit your specific needs.
 *
 * **Custom Tasks** transform async iterables by returning async generator functions:
 *
 * ```typescript
 * function double<V extends number>(): Task<V, V> {
 *   return async function* (source) {
 *     for await (const item of source) { yield item * 2 as V; }
 *   };
 * }
 *
 * await items([1, 2, 3])(double())(toArray());  // [2, 4, 6]
 * ```
 *
 * **Custom Feeds** create new pipes:
 *
 * ```typescript
 * function repeat<V>(value: V, count: number): Pipe<V> {
 *   return items(async function* () {
 *     for (let i = 0; i < count; i++) { yield value; }
 *   }());
 * }
 *
 * await repeat(42, 3)(toArray());  // [42, 42, 42]
 * ```
 *
 * > [!CAUTION]
 * > When creating custom feeds, always wrap the returned async iterable with {@link items} to ensure `undefined`
 * > filtering and proper pipe interface integration.
 * >
 * > Directly returning raw async iterables (async generators, async generator
 * > functions, or objects implementing `AsyncIterable<T>`) bypasses the automatic `undefined` filtering mechanism.
 *
 * @groupDescription Pipes
 * Core utilities for processing pipes.
 *
 * @groupDescription Feeds
 * Functions that create new pipes from various input sources.
 *
 * @groupDescription Tasks
 * Intermediate operations that transform, filter, or process stream items while maintaining the stream flow.
 *
 * @groupDescription Sinks
 * Terminal operations that consume streams and produce final results.
 *
 * @module
 */

import { isAsyncIterable, isFunction, isIterable, isNumber, isPromise, isString } from "@metreeca/core";
import { cpus } from "os";


/**
 * Number of CPU cores available for parallelize processing.
 *
 * Defaults to 4 if CPU detection fails (cpus() returns empty array).
 *
 * This conservative default maintains meaningful parallelism while avoiding
 * severe over-subscription on systems where CPU count cannot be determined.
 */
const cores = cpus().length || 4;


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Flexible data source for stream processing.
 *
 * Represents various data formats that can be converted into async streams:
 *
 * - Single value: `V` - yields one item
 * - Array: `readonly V[]` - yields items from the array
 * - Synchronous iterable: `Iterable<V>` - yields items from the iterable
 * - Asynchronous iterable: `AsyncIterable<V>` - yields items from the async iterable
 * - Pipe: {@link Pipe}`<V>` - yields items from the pipe's underlying async iterable
 *
 * @typeParam V The type of values in the data source
 *
 * @remarks
 *
 * Enables functions to accept data in the most convenient format for the caller,
 * internally normalizing it to async iterables for uniform processing.
 * Used by {@link items} to create pipes and by {@link flatMap} to flatten nested data sources.
 *
 * @example
 *
 * ```typescript
 * // All of these are valid Data<number> values:
 * const scalarData: Data<number> = 42;
 * const arrayData: Data<number> = [1, 2, 3];
 * const iterableData: Data<number> = new Set([1, 2, 3]);
 * const asyncData: Data<number> = (async function*() { yield 1; yield 2; })();
 * const pipeData: Data<number> = items([1, 2, 3]);
 * ```
 */
export type Data<V> = V | readonly V[] | Iterable<V> | AsyncIterable<V> | Pipe<V>

/**
 * Fluent interface for composing async stream operations.
 *
 * @typeParam V The type of values in the stream
 */
export interface Pipe<V> {

	/**
	 * Retrieves the underlying async iterable.
	 *
	 * @returns The async iterable for manual iteration
	 */
	(): AsyncIterable<V>;

	/**
	 * Applies a transformation task to the stream.
	 *
	 * @typeParam R The type of transformed values
	 *
	 * @param task The task to apply
	 *
	 * @returns A new pipe with transformed values
	 */<R>(task: Task<V, R>): Pipe<R>;

	/**
	 * Applies a terminal sink operation to consume the stream.
	 *
	 * @typeParam R The type of result
	 *
	 * @param sink The sink to apply
	 *
	 * @returns A promise resolving to the sink's result
	 */<R>(sink: Sink<V, R>): Promise<R>;

}

/**
 * Transformation that processes stream items and yields results.
 *
 * Tasks are intermediate operations that can be chained together.
 * They transform, filter, or otherwise process items while maintaining the stream.
 *
 * @typeParam V The type of input values
 * @typeParam R The type of output values (defaults to V)
 */
export interface Task<V, R = V> {

	(value: AsyncIterable<V>): AsyncIterable<R>;

}

/**
 * Terminal operation that consumes a stream and produces a result.
 *
 * Sinks trigger stream execution and return a promise that resolves
 * when all items have been processed.
 *
 * @typeParam V The type of input values
 * @typeParam R The type of result (defaults to V)
 */
export interface Sink<V, R = V> {

	(value: AsyncIterable<V>): Promise<R>;

}


//// Pipes /////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Processes a promise, returning it as-is.
 *
 * @group Pipes
 *
 * @param source The promise to process
 *
 * @returns The same promise
 */
export function pipe<V>(source: Promise<V>): Promise<V>;

/**
 * Processes a pipe, retrieving its underlying async iterable.
 *
 * @group Pipes
 *
 * @param source The pipe to process
 *
 * @returns The underlying async iterable
 */
export function pipe<V>(source: Pipe<V>): AsyncIterable<V>;

export function pipe(source: unknown): unknown {

	return isFunction(source) ? source() : source;

}


//// Feeds /////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a pipe from an item feed.
 *
 * This is the central point where `undefined` filtering occurs (see module documentation).
 * All `undefined` values are automatically removed from the stream, while other falsy values
 * (`null`, `0`, `false`, `""`) are preserved.
 *
 * @group Feeds
 *
 * @typeParam V The type of items in the stream
 *
 * @param feed The source to create a pipe from
 *
 * @returns A pipe for fluent composition
 *
 * @remarks
 *
 * **Data Source Handling**:
 *
 * - **Primitives** (strings, numbers, booleans, null): Treated as atomic values and yielded as single items
 * - **Arrays/Iterables** (excluding strings): Items are yielded individually
 * - **Async Iterables/Pipes**: Items are yielded as they become available
 * - **Other values** (objects, etc.): Yielded as single items
 *
 * **When creating custom feeds**, always wrap async iterables with `items()` to ensure
 * `undefined` filtering and proper pipe interface integration. Directly returning raw
 * async iterables bypasses the filtering mechanism.
 */
export function items<V>(feed: Data<V>): Pipe<V> {

	const generator = async function* () {
		for await (const item of flatten(feed)) {
			if ( item !== undefined ) {
				yield item;
			}
		}
	};

	function pipe(): AsyncIterable<V>;
	function pipe<R>(task: Task<V, R>): Pipe<R>;
	function pipe<R>(sink: Sink<V, R>): Promise<R>;
	function pipe<R>(step?: Task<V, R> | Sink<V, R>): unknown {

		if ( step ) {

			const result = step(generator());

			return isPromise(result) ? result : items(result);

		} else {

			return generator();

		}
	}

	return pipe;

}

/**
 * Creates a pipe that yields a sequence of numbers within a range.
 *
 * Generates numbers in ascending order if `start` < `end`, or descending order if `start` > `end`.
 *
 * @group Feeds
 *
 * @param start The starting value (inclusive)
 * @param end The ending value (exclusive)
 *
 * @returns A pipe yielding numbers from start to end
 */
export function range(start: number, end: number): Pipe<number> {

	async function* generator() {
		if ( start < end ) {

			for (let i = start; i < end; i++) {
				yield i;
			}

		} else {

			for (let i = start; i > end; i--) {
				yield i;
			}

		}
	}

	return items(generator());
}


/**
 * Chains multiple pipes into a single stream, preserving source order.
 *
 * Items are emitted in source order: all items from the first pipe,
 * then all items from the second pipe, and so on.
 * Each source is fully consumed before moving to the next.
 *
 * @group Feeds
 *
 * @typeParam V The type of items in the streams
 *
 * @param sources The pipes to chain
 *
 * @returns A pipe containing all items from all sources in order
 */
export function chain<V>(...sources: readonly Pipe<V>[]): Pipe<V> {

	const generator = async function* () {

		for (const source of sources) {
			yield* source();
		}

	};

	return items(generator());
}

/**
 * Merges multiple pipes into a single stream, yielding items as they become available.
 *
 * Items are emitted in the order they resolve, not in source order.
 * All sources are consumed concurrently.
 *
 * @group Feeds
 *
 * @typeParam V The type of items in the streams
 *
 * @param sources The pipes to merge
 *
 * @returns A pipe containing all items from all sources
 */
export function merge<V>(...sources: readonly Pipe<V>[]): Pipe<V> {

	const generator = async function* () {

		const iterators = sources.map(source => source()[Symbol.asyncIterator]());

		const pending = new Map(
			iterators.map(iterator => {
				return [iterator, iterator.next().then(result => ({ iterator, result }))] as const;
			})
		);

		try {

			while ( pending.size > 0 ) {

				const { iterator, result } = await Promise.race(pending.values());

				if ( result.done ) {

					pending.delete(iterator);

				} else { // schedule next iteration before yielding to prevent race conditions

					pending.set(iterator, iterator.next().then((result: IteratorResult<V>) => ({
						iterator,
						result
					})));

					yield result.value;

				}
			}

		} finally { // clean up any remaining iterators on error or early termination

			await Promise.allSettled(
				Array.from(pending.keys()).map(iterator => iterator.return?.())
			);

		}
	};

	return items(generator());
}


//// Tasks /////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a task skipping the first n items from the stream.
 *
 * Items are processed sequentially and output order is preserved.
 *
 * @group Tasks
 *
 * @typeParam V The type of items in the stream
 *
 * @param n The number of items to skip (negative values treated as zero)
 *
 * @returns A task that skips the first n items
 */
export function skip<V>(n: number): Task<V> {
	return async function* (source: AsyncIterable<V>) {

		let count = 0;

		for await (const item of source) {
			if ( count >= n ) {
				yield item;
			} else {
				count++;
			}
		}
	};
}

/**
 * Creates a task taking only the first n items from the stream.
 *
 * Items are processed sequentially and output order is preserved.
 *
 * @group Tasks
 *
 * @typeParam V The type of items in the stream
 *
 * @param n The maximum number of items to take (negative values treated as zero)
 *
 * @returns A task that takes the first n items
 */
export function take<V>(n: number): Task<V> {
	return async function* (source: AsyncIterable<V>) {

		let count = 0;

		for await (const item of source) {
			if ( count < n ) {
				yield item;
				count++;
			}
		}
	};
}

/**
 * Creates a task executing a side effect for each item without modifying the stream.
 *
 * Items are processed sequentially and output order is preserved.
 * Useful for debugging or monitoring items as they flow through the pipeline.
 *
 * @group Tasks
 *
 * @typeParam V The type of items in the stream
 *
 * @param consumer The function to execute for each item (return value is ignored)
 *
 * @returns A task that executes the consumer for each item
 */
export function peek<V>(consumer: (item: V) => unknown): Task<V> {
	return async function* (source: AsyncIterable<V>) {
		for await (const item of source) {
			await consumer(item);
			yield item;
		}
	};
}

/**
 * Creates a task filtering stream items based on a predicate.
 *
 * Items are processed sequentially and output order is preserved.
 *
 * @group Tasks
 *
 * @typeParam V The type of items in the stream
 *
 * @param predicate The function to test each item
 *
 * @returns A task that filters items based on the predicate
 */
export function filter<V>(predicate: (item: V) => boolean | Promise<boolean>): Task<V> {
	return async function* (source: AsyncIterable<V>) {
		for await (const item of source) {
			if ( await predicate(item) ) {
				yield item;
			}
		}
	};
}

/**
 * Creates a task filtering out duplicate items.
 *
 * Items are processed sequentially and output order is preserved.
 * Only the first occurrence of each unique item is yielded.
 *
 * @group Tasks
 *
 * @typeParam V The type of items in the stream
 * @typeParam K The type of comparison key
 *
 * @param selector Optional function to extract comparison key from items
 *
 * @returns A task that filters out duplicate items
 *
 * @remarks
 *
 * Maintains a `Set` of all seen items in memory. For large or infinite streams
 * with many unique items, this may cause memory issues.
 */
export function distinct<V, K>(selector?: (item: V) => K | Promise<K>): Task<V> {
	return async function* (source: AsyncIterable<V>) {

		const seen = new Set();

		for await (const item of source) {

			const key = selector ? await selector(item) : item;

			if ( !seen.has(key) ) {
				seen.add(key);
				yield item;
			}
		}

	};
}

/**
 * Creates a task that maps each input item to an output value using a mapper function.
 *
 * Items are processed sequentially by default, preserving output order. In parallel mode,
 * items are processed concurrently and emitted as they complete without preserving order.
 *
 * @group Tasks
 *
 * @typeParam V The type of input values
 * @typeParam R The type of mapped result values
 *
 * @param mapper The function to transform each item (can be sync or async)
 * @param parallel Concurrency control: `false`/`undefined`/number ≤ 1 for sequential (default),
 *   `true` for parallel with auto-detected concurrency, or a number > 1 for explicit concurrency limit
 *
 * @returns A task that transforms items using the mapper function
 *
 * @remarks
 *
 * In parallel mode, when an error occurs all pending operations are awaited
 * (but not failed) before the error is thrown to prevent resource leaks.
 *
 * @example
 *
 * ```typescript
 * map((n: number) => n * 2)                                             // sequential
 * map(async (id: string) => fetch(`/users/${id}`), { parallel: true })  // parallel
 * map(heavyOperation, { parallel: 4 })                                  // parallel with limit
 * ```
 */
export function map<V, R>(
	mapper: (item: V) => R | Promise<R>,
	{ parallel }: { parallel?: boolean | number } = {}
): Task<V, R> {

	if ( parallel === true || isNumber(parallel) && parallel > 1 ) {

		return source => parallelize(
			source,
			item => Promise.resolve(mapper(item)),
			isNumber(parallel) ? parallel : cores,
			async function* (result) { yield result; }
		);

	} else {

		return async function* (source: AsyncIterable<V>) {
			for await (const item of source) {
				yield await mapper(item);
			}
		};

	}

}

/**
 * Creates a task that transforms each item into a data source and flattens the results.
 *
 * Items are processed sequentially by default, preserving output order. In parallel mode,
 * items are processed concurrently and flattened results are emitted as they complete without preserving order.
 *
 * @group Tasks
 *
 * @typeParam V The type of input items
 * @typeParam R The type of output items after flattening
 *
 * @param mapper The function to transform each item into a data source (can be sync or async)
 * @param parallel Concurrency control: `false`/`undefined`/number ≤ 1 for sequential (default),
 *   `true` for parallel with auto-detected concurrency, or a number > 1 for explicit concurrency limit
 *
 * @returns A task that transforms and flattens items
 *
 * @remarks
 *
 * **Flattening Behavior**:
 *
 * - **Primitives** (strings, numbers, booleans, null): Yielded as single atomic items
 * - **Arrays/Iterables** (excluding strings): Items are yielded individually from each mapper result
 * - **Async Iterables/Pipes**: Items are yielded as they become available
 * - **Other values** (objects, etc.): Yielded as single items
 *
 * In parallel mode, when an error occurs all pending operations are awaited
 * (but not failed) before the error is thrown to prevent resource leaks.
 */
export function flatMap<V, R>(
	mapper: (item: V) => Data<R> | Promise<Data<R>>,
	{ parallel }: { parallel?: boolean | number } = {}
): Task<V, R> {

	if ( parallel === true || isNumber(parallel) && parallel > 1 ) {

		return source => parallelize(
			source,
			item => Promise.resolve(mapper(item)),
			isNumber(parallel) ? parallel : cores,
			flatten
		);

	} else {

		return async function* (source: AsyncIterable<V>) {
			for await (const item of source) {
				yield* flatten(await mapper(item));
			}
		};

	}

}

/**
 * Creates a task grouping items into batches of a specified size.
 *
 * Items are processed sequentially and order is preserved both across batches and within each batch.
 *
 * If size is 0 or negative, collects all items into a single batch.
 * The final batch may contain fewer items than the specified size.
 *
 * @group Tasks
 *
 * @typeParam V The type of items in the stream
 *
 * @param size The maximum number of items per batch (0 or negative for unbounded)
 *
 * @returns A task that groups items into batches
 *
 * @remarks
 *
 * When size is 0 or negative, all stream items are accumulated in memory before
 * yielding a single batch. For large or infinite streams, this may cause
 * memory issues. Use a positive size for bounded memory consumption.
 */
export function batch<V>(size: number = 0): Task<V, readonly V[]> {
	return async function* (source: AsyncIterable<V>) {

		const batch: V[] = [];

		for await (const item of source) {

			batch.push(item);

			if ( size > 0 && batch.length >= size ) {
				yield batch.splice(0);
			}
		}

		if ( batch.length > 0 ) {
			yield batch;
		}
	};
}


//// Sinks /////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a sink counting the total number of items in the stream.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @returns A sink that counts all items in the stream
 */
export function count<V>(): Sink<V, number> {
	return async source => {

		let count = 0;

		for await (const _ of source) {
			count++;
		}

		return count;

	};
}

/**
 * Creates a sink executing a side effect for each item and consuming the stream.
 *
 * Terminal operation that triggers stream execution.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @param consumer The function to execute for each item (return value is ignored)
 *
 * @returns A sink that executes the consumer for each item
 */
export function forEach<V>(consumer: (item: V) => unknown): Sink<V, void> {
	return async source => {

		for await (const item of source) {
			await consumer(item);
		}

	};
}

/**
 * Creates a sink checking if any item satisfies the predicate.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @param predicate The function to test each item
 *
 * @returns A sink that checks if any item satisfies the predicate
 */
export function some<V>(predicate: (item: V) => boolean | Promise<boolean>): Sink<V, boolean> {
	return async source => {

		for await (const item of source) {
			if ( await predicate(item) ) {
				return true;
			}
		}

		return false;
	};
}

/**
 * Creates a sink checking if all items satisfy the predicate.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @param predicate The function to test each item
 *
 * @returns A sink that checks if all items satisfy the predicate
 */
export function every<V>(predicate: (item: V) => boolean | Promise<boolean>): Sink<V, boolean> {
	return async source => {

		for await (const item of source) {
			if ( !await predicate(item) ) {
				return false;
			}
		}

		return true;
	};
}

/**
 * Creates a sink retrieving the first item that satisfies the predicate.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @param predicate The function to test each item
 *
 * @returns A sink that retrieves the first matching item or undefined
 */
export function find<V>(predicate: (item: V) => boolean | Promise<boolean>): Sink<V, undefined | V> {
	return async source => {

		for await (const item of source) {
			if ( await predicate(item) ) {
				return item;
			}
		}

		return undefined;
	};
}

/**
 * Creates a sink reducing the stream to a single value without an initial value.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @param reducer The function to combine the accumulator with each item
 *
 * @returns A sink that reduces the stream to a single value, the first item for singleton streams,
 * or `undefined` for empty streams
 */
export function reduce<V>(reducer: (accumulator: V, item: V) => V | Promise<V>): Sink<V, undefined | V>;

/**
 * Creates a sink reducing the stream to a single value with an initial value.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 * @typeParam R The type of the accumulated result
 *
 * @param reducer The function to combine the accumulator with each item
 * @param initial The initial value for the accumulator
 *
 * @returns A sink that reduces the stream to a single value
 */
export function reduce<V, R>(reducer: (accumulator: R, item: V) => R | Promise<R>, initial: R): Sink<V, R>;

export function reduce<V, R>(reducer: Function, initial?: R): Sink<V, undefined | V | R> {
	return async source => {

		let started = arguments.length > 1;
		let accumulator: V | R | undefined = started ? initial : undefined;

		for await (const item of source) {
			if ( started ) {
				accumulator = await reducer(accumulator, item);
			} else {
				accumulator = item;
				started = true;
			}
		}

		return accumulator;

	};
}

/**
 * Creates a sink collecting all items into an array.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @returns A sink that collects all items into an array
 */
export function toArray<V>(): Sink<V, readonly V[]> {
	return async source => {

		const array: V[] = [];

		for await (const item of source) {
			array.push(item);
		}

		return array;
	};
}

/**
 * Creates a sink collecting all unique items into a set.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 *
 * @returns A sink that collects all unique items into a set
 */
export function toSet<V>(): Sink<V, ReadonlySet<V>> {
	return async source => {

		const set = new Set<V>();

		for await (const item of source) {
			set.add(item);
		}

		return set;
	};
}

/**
 * Creates a sink collecting items into a map using item values.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 * @typeParam K The type of map keys
 *
 * @param key The function to extract the key from each item
 *
 * @returns A sink that collects items into a map with keys from the key selector and items as values
 */
export function toMap<V, K>(
	key: (item: V) => K | Promise<K>
): Sink<V, ReadonlyMap<K, V>>;

/**
 * Creates a sink collecting items into a map using custom keys and values.
 *
 * @group Sinks
 *
 * @typeParam V The type of items in the stream
 * @typeParam K The type of map keys
 * @typeParam R The type of map values
 *
 * @param key The function to extract the key from each item
 * @param value The function to transform each item into a map value
 *
 * @returns A sink that collects items into a map with keys and values from the selectors
 */
export function toMap<V, K, R>(
	key: (item: V) => K | Promise<K>,
	value: (item: V) => R | Promise<R>
): Sink<V, ReadonlyMap<K, R>>;

export function toMap<V, K, R>(
	key: (item: V) => K | Promise<K>,
	value?: (item: V) => R | Promise<R>
): Sink<V, ReadonlyMap<K, V | R>> {
	return async source => {

		const map = new Map<K, V | R>();

		for await (const item of source) {

			map.set(
				await key(item),
				value ? await value(item) : item
			);

		}

		return map;
	};
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Helper to flatten Data<R> into individual items.
 *
 * @remarks
 *
 * Converts various data sources into an async generator:
 *
 * - Functions (Pipe instances): Invokes and yields from the returned async iterable
 * - Async iterables: Yields items directly from the async iterable
 * - Sync iterables (excluding strings): Yields items from the iterable
 * - All other values (primitives, objects, etc.): Yields the value as a single item
 *
 * **String Handling**: Strings are treated as atomic values and yielded whole, not
 * iterated character by character, ensuring consistent behavior where they represent
 * single data items rather than character sequences.
 */
async function* flatten<R>(data: Data<R>): AsyncGenerator<R, void, unknown> {

	if ( isString(data) ) {

		yield data as R;

	} else if ( isFunction(data) ) {

		yield* data();

	} else if ( isAsyncIterable<R>(data) ) {

		yield* data;

	} else if ( isIterable<R>(data) ) {

		yield* data;

	} else {

		yield data;

	}

}

/**
 * Helper for parallel processing with configurable result handling.
 */
async function* parallelize<V, R, T>(
	source: AsyncIterable<V>,
	mapper: (item: V) => Promise<R>,
	threads: number,
	handler: (result: R) => AsyncIterable<T>
): AsyncGenerator<T, void, unknown> {

	type Task<U> = { id: number; promise: Promise<U> };

	const pending = new Map<number, Task<R>>();
	const iterator = source[Symbol.asyncIterator]();

	let nextId = 0;
	let done = false;

	try {

		while ( pending.size > 0 || !done ) {

			// Fill up to threads limit

			while ( pending.size < threads && !done ) {

				const next = await iterator.next();

				if ( next.done ) {
					done = true;
				} else {
					const id = nextId++;
					const promise = mapper(next.value);
					pending.set(id, { id, promise });
				}
			}

			// Wait for next completion

			if ( pending.size > 0 ) {

				const completed = await Promise.race(
					Array.from(pending.values()).map(({ id, promise }) =>
						promise.then(result => ({ id, result }), error => ({ id, error }))
					)
				);

				pending.delete(completed.id);

				if ( "error" in completed ) {
					await Promise.allSettled(Array.from(pending.values()).map(t => t.promise));
					throw completed.error;
				}

				yield* handler(completed.result);
			}
		}

	} finally {

		try {
			await iterator.return?.();
		} catch {
			// Suppress cleanup errors
		}

	}
}
