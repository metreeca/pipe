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
 * Factory functions that create new pipes from various input sources.
 *
 * @remarks
 *
 * **Custom Feeds** are functions that create new pipes:
 *
 * ```typescript
 * import { items, toArray, type Pipe } from '@metreeca/flow';
 *
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
 * > When creating custom feeds, always wrap async generators, async generator functions,
 * > or `AsyncIterable<T>` objects with {@link items} to ensure `undefined` filtering and proper pipe
 * > interface integration.
 *
 * @module
 */

import { isPromise } from "@metreeca/core";
import { Data, Pipe, Sink, Task } from ".";
import { flatten } from "./utils";

/**
 * Creates a pipe from a data source.
 *
 * Data sources are handled as follows:
 *
 * - `undefined` - filtered out and not yielded
 * - **Primitives** (strings, numbers, booleans, null): Treated as atomic values and yielded as single items
 * - **Arrays/Iterables** (excluding strings): Items are yielded individually (with `undefined` items filtered out)
 * - **Async Iterables/Pipes**: Items are yielded as they become available (with `undefined` items filtered out)
 * - **Promises**: Awaited and then processed according to their resolved value
 * - **Other values** (objects, etc.): Yielded as single items
 *
 * The feed parameter can be synchronous or asynchronous (returning a Promise).
 * Async feeds are useful for fetching data from APIs, databases, or any async source.
 *
 * @typeParam V The type of items in the stream
 *
 * @param feed The source to create a pipe from
 *
 * @returns A pipe for fluent composition
 */
export function items<V>(feed: Data<V> | Promise<Data<V>>): Pipe<V>;

/**
 * Creates a pipe from multiple scalar values.
 *
 * @typeParam V The type of items in the stream
 *
 * @param values The scalar values to create a pipe from
 *
 * @returns A pipe for fluent composition
 */
export function items<V>(...values: readonly [V, V, ...V[]]): Pipe<V>;

export function items<V>(feed: Data<V> | Promise<Data<V>>, ...values: V[]): Pipe<V> {

	async function* generator() {
		for await (const item of flatten(values.length > 0 ? [await feed, ...values] as V[] : await feed)) {
			if ( item !== undefined ) {
				yield item;
			}
		}
	}

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
 * Returns an empty sequence if `start` === `end`.
 *
 * @param start The starting value (inclusive)
 * @param end The ending value (exclusive)
 *
 * @returns A pipe yielding numbers from start to end
 *
 * @example
 *
 * ```typescript
 * await range(1, 5)(toArray());   // [1, 2, 3, 4]
 * await range(5, 1)(toArray());   // [5, 4, 3, 2]
 * await range(3, 3)(toArray());   // []
 * ```
 */
export function range(start: number, end: number): Pipe<number> {

	return items((function* () {

		if ( start < end ) {

			for (let i = start; i < end; i++) {
				yield i;
			}

		} else if ( start > end ) {

			for (let i = start; i > end; i--) {
				yield i;
			}

		}

	})());

}

/**
 * Creates a pipe by repeatedly calling a generator function until exhausted.
 *
 * The generator is called on demand and returned data is flattened into the stream.
 * Iteration stops when the generator returns `undefined`, an empty array, or an empty iterator.
 *
 * The generator can be either synchronous or asynchronous (returning a Promise).
 * Async generators are useful for pagination APIs, database cursors, or any data source
 * where fetching the next batch requires an async operation.
 *
 * @typeParam V The type of items in the stream
 *
 * @param generator The function to call repeatedly to generate data, returning `undefined` to terminate
 *
 * @returns A pipe yielding items from successive generator calls
 *
 * @example
 *
 * ```typescript
 * // Infinite random numbers
 * await iterate(() => Math.random())(take(3))(toArray());  // [0.123, 0.456, 0.789]
 *
 * // Counter that stops at 3
 * let count = 0;
 * await iterate(() => count++ < 3 ? count : undefined)(toArray());  // [1, 2, 3]
 * ```
 */
export function iterate<V>(generator: () => undefined | Data<V> | Promise<undefined | Data<V>>): Pipe<V> {

	return items((async function* () {

		for (let data = await generator(); data !== undefined; data = await generator()) {

			const iterable = flatten(data);
			const iterator = iterable[Symbol.asyncIterator]();
			const first = await iterator.next();

			if ( first.done ) {

				return;

			} else {

				yield first.value;
				yield* iterator;

			}
		}

	})());

}


/**
 * Chains multiple data sources into a single stream, preserving source order.
 *
 * Items are emitted in source order: all items from the first source,
 * then all items from the second source, and so on.
 * Each source is fully consumed before moving to the next.
 *
 * @typeParam V The type of items in the streams
 *
 * @param sources The data sources to chain (can be synchronous or asynchronous)
 *
 * @returns A pipe containing all items from all sources in order
 *
 * @example
 *
 * ```typescript
 * await chain(items([1, 2]), items([3, 4]))(toArray());  // [1, 2, 3, 4]
 * ```
 */
export function chain<V>(...sources: readonly (Data<V> | Promise<Data<V>>)[]): Pipe<V> {

	return items((async function* () {

		for (const source of sources) {
			yield* flatten(await source);
		}

	})());

}

/**
 * Merges multiple data sources into a single stream, yielding items as they become available.
 *
 * Items are emitted in the order they resolve, not in source order.
 * All sources are consumed concurrently.
 *
 * @typeParam V The type of items in the streams
 *
 * @param sources The data sources to merge (can be synchronous or asynchronous)
 *
 * @returns A pipe containing all items from all sources
 *
 * @example
 *
 * ```typescript
 * // Order depends on async timing
 * await merge(items([1, 2]), items([3, 4]))(toArray());  // e.g., [1, 3, 2, 4]
 * ```
 */
export function merge<V>(...sources: readonly (Data<V> | Promise<Data<V>>)[]): Pipe<V> {

	return items((async function* () {

		const iterators = await Promise.all(
			sources.map(async source => flatten(await source)[Symbol.asyncIterator]())
		);

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

	})());

}
