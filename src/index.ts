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
 *
 * @remarks
 *
 * **All streams automatically filter out `undefined` values.** This filtering occurs at stream creation
 * and when tasks are chained together, ensuring that `undefined` never flows through your pipeline.
 *
 * - `undefined` values are **removed** from all streams
 * - Other falsy values (`null`, `0`, `false`, `""`) are **preserved**
 * - Custom tasks yielding `undefined` have those values automatically filtered
 * - This behavior is centralized in the {@link items} function, which is used internally for all stream creation
 *
 * @example
 *
 * ```typescript
 * await items([1, undefined, 2])(toArray())                // [1, 2]
 * await items([0, false, "", null, undefined])(toArray()) // [0, false, "", null]
 * await items(["1", "bad", "2"])(parseNumbers)(toArray()) // [1, 2] - undefined from task filtered
 * ```
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

import { isAsyncIterable, isFunction, isIterable, isPromise } from "@metreeca/core";

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
 * Enables functions to accept data in the most convenient format for the caller,
 * internally normalizing it to async iterables for uniform processing.
 * Used by {@link items} to create pipes and by {@link flatMap} to flatten nested data sources.
 *
 * @example
 * ```typescript
 * // All of these are valid Data<number> values:
 * const scalarData: Data<number> = 42;
 * const arrayData: Data<number> = [1, 2, 3];
 * const iterableData: Data<number> = new Set([1, 2, 3]);
 * const asyncData: Data<number> = (async function*() { yield 1; yield 2; })();
 * const pipeData: Data<number> = items([1, 2, 3]);
 * ```
 */
export type Data<V>=V | readonly V[] | Iterable<V> | AsyncIterable<V> | Pipe<V>

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
export interface Task<V, R=V> {

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
export interface Sink<V, R=V> {

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
 */
export function pipe<T>(source: Pipe<T>): AsyncIterable<T>;

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
 * **When creating custom feeds**, always wrap async iterables with `items()` to ensure
 * `undefined` filtering and proper pipe interface integration. Directly returning raw
 * async iterables bypasses the filtering mechanism.
 *
 * @example
 * ```typescript
 * function customFeed(): Pipe<number> {
 *   return items(async function*() {
 *     yield 1;
 *     yield undefined; // Will be filtered out
 *     yield 2;
 *   }());
 * }
 * ```
 */
export function items<V>(feed: Data<V>): Pipe<V> {

	const generator=isFunction(feed) ?

		async function* () {
			for await (const item of feed()) {
				if ( item !== undefined ) {
					yield item;
				}
			}
		}

		: isAsyncIterable<V>(feed) ?

			async function* () {
				for await (const item of feed) {
					if ( item !== undefined ) {
						yield item;
					}
				}
			}

			: isIterable<V>(feed) ?

				async function* () {
					for (const item of feed) {
						if ( item !== undefined ) {
							yield item;
						}
					}
				}

				: async function* () {
					if ( feed !== undefined ) {
						yield feed;
					}
				};

	function pipe(): AsyncIterable<V>;
	function pipe<R>(task: Task<V, R>): Pipe<R>;
	function pipe<R>(sink: Sink<V, R>): Promise<R>;
	function pipe<R>(step?: Task<V, R> | Sink<V, R>): unknown {

		if ( step ) {

			const result=step(generator());

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
 * @typeParam T The type of items in the streams
 *
 * @param sources The pipes to chain
 *
 * @returns A pipe containing all items from all sources in order
 */
export function chain<V>(...sources: readonly Pipe<V>[]): Pipe<V> {

	const generator=async function* () {

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
 * @typeParam T The type of items in the streams
 *
 * @param sources The pipes to merge
 *
 * @returns A pipe containing all items from all sources
 */
export function merge<V>(...sources: readonly Pipe<V>[]): Pipe<V> {

	const generator=async function* () {

		const iterators=sources.map(source => source()[Symbol.asyncIterator]());

		const pending=new Map(
			iterators.map(iterator => {
				return [iterator, iterator.next().then(result => ({ iterator, result }))] as const;
			})
		);

		try {

			while ( pending.size > 0 ) {

				const { iterator, result }=await Promise.race(pending.values());

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

		let count=0;

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

		let count=0;

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
 * @param consumer The function to execute for each item
 *
 * @returns A task that executes the consumer for each item
 */
export function peek<V>(consumer: (item: V) => void | Promise<void>): Task<V> {
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
 * Maintains a `Set` of all seen items in memory. For large or infinite streams
 * with many unique items, this may cause memory issues.
 */
export function distinct<V, K>(selector?: (item: V) => K | Promise<K>): Task<V> {
	return async function* (source: AsyncIterable<V>) {

		const seen=new Set();

		for await (const item of source) {

			const key=selector ? await selector(item) : item;

			if ( !seen.has(key) ) {
				seen.add(key);
				yield item;
			}
		}

	};
}

/**
 * Creates a task transforming each item using a mapper function.
 *
 * Items are processed sequentially and output order is preserved.
 *
 * @group Tasks
 *
 * @typeParam V The type of input items
 * @typeParam R The type of output items
 *
 * @param mapper The function to transform each item
 *
 * @returns A task that transforms items using the mapper
 */
export function map<V, R>(mapper: (item: V) => R | Promise<R>): Task<V, R> {
	return async function* (source: AsyncIterable<V>) {
		for await (const item of source) {
			yield await mapper(item);
		}
	};
}

/**
 * Creates a task transforming and flattening stream items.
 *
 * Items are processed sequentially and output order is preserved.
 *
 * @group Tasks
 *
 * @typeParam V The type of input items
 * @typeParam R The type of output items
 *
 * @param mapper The function to transform each item into an item feed
 *
 * @returns A task that transforms and flattens items
 */
export function flatMap<V, R>(mapper: (item: V) => Data<R> | Promise<Data<R>>): Task<V, R> {
	return async function* (source: AsyncIterable<V>) {
		for await (const item of source) {

			const result=await mapper(item);

			if ( isFunction(result) ) {

				yield* result();

			} else if ( isAsyncIterable<R>(result) ) {

				yield* result;

			} else if ( isIterable<R>(result) ) {

				for (const value of result) {
					yield value;
				}

			} else {

				yield result;

			}

		}
	};
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
 * When size is 0 or negative, all stream items are accumulated in memory before
 * yielding a single batch. For large or infinite streams, this may cause
 * memory issues. Use a positive size for bounded memory consumption.
 */
export function batch<V>(size: number=0): Task<V, readonly V[]> {
	return async function* (source: AsyncIterable<V>) {

		const batch: V[]=[];

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

		for await (const item of source) {
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
 * @param consumer The function to execute for each item
 *
 * @returns A sink that executes the consumer for each item
 */
export function forEach<V>(consumer: (item: V) => void | Promise<void>): Sink<V, void> {
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

		let started=arguments.length > 1;
		let accumulator: V | R | undefined=started ? initial : undefined;

		for await (const item of source) {
			if ( started ) {
				accumulator= await reducer(accumulator, item);
			} else {
				accumulator=item;
				started=true;
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

		const array: V[]=[];

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

		const set=new Set<V>();

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

		const map=new Map<K, V | R>();

		for await (const item of source) {

			map.set(
				await key(item),
				value ? await value(item) : item
			);

		}

		return map;
	};
}
