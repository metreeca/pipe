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
 * Intermediate operations that transform, filter, or process stream items.
 *
 * @remarks
 *
 * **Custom Tasks** are functions that transform async iterables by returning async generator functions:
 *
 * ```typescript
 * import { items, toArray, type Task } from '@metreeca/flow';
 *
 * function double<V extends number>(): Task<V, V> {
 *   return async function* (source) {
 *     for await (const item of source) { yield item * 2 as V; }
 *   };
 * }
 *
 * await items([1, 2, 3])(double())(toArray());  // [2, 4, 6]
 * ```
 *
 * @module
 */

import { isNumber } from "@metreeca/core";
import { cpus } from "os";
import { Data, Task } from "./index";
import { flatten } from "./utils";


/**
 * Number of CPU cores available for parallel processing.
 *
 * Used as the default concurrency level when `parallel: true` is specified.
 * Optimal for CPU-bound tasks. For I/O-heavy tasks, consider using `parallel: 0`
 * for unbounded concurrency instead.
 *
 * Defaults to 4 if CPU detection fails (cpus() returns empty array).
 *
 * This conservative default maintains meaningful parallelism while avoiding
 * severe over-subscription on systems where CPU count cannot be determined.
 */
const cores = cpus().length || 4;


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a task skipping the first n items from the stream.
 *
 * Items are processed sequentially and output order is preserved.
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
			} else {
				return;
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
 * @typeParam V The type of items in the stream
 *
 * @param predicate The function to test each item. When the predicate returns `undefined`,
 *   it is treated as `false` and the item is filtered out.
 *
 * @returns A task that filters items based on the predicate
 */
export function filter<V>(predicate: (item: V) => undefined | boolean | Promise<undefined | boolean>): Task<V> {
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
 * @typeParam V The type of input values
 * @typeParam R The type of mapped result values
 *
 * @param mapper The function to transform each item (can be sync or async). When the mapper returns `undefined`,
 *   that value is filtered out and not included in the output stream.
 * @param parallel Concurrency control: `false`/`undefined`/`1` for sequential (default),
 *   `true` for parallel with auto-detected concurrency (CPU cores), `0` for unbounded concurrency (I/O-heavy tasks),
 *   or a number > 1 for explicit concurrency limit
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
 * map((n: number) => n * 2)                                            // sequential
 * map(async (id: string) => fetch(`/users/${id}`), { parallel: true }) // parallel (CPU cores)
 * map(async (url: string) => fetch(url), { parallel: 0 })              // unbounded (I/O-heavy)
 * map(heavyOperation, { parallel: 4 })                                 // parallel with limit
 * ```
 */
export function map<V, R>(
	mapper: (item: V) => undefined | R | Promise<undefined | R>,
	{ parallel }: { parallel?: boolean | number } = {}
): Task<V, R> {

	if ( parallel === true || isNumber(parallel) && parallel !== 1 ) {

		const concurrency = parallel === true ? cores
			: parallel === 0 ? Infinity
				: parallel;

		return source => parallelize(
			source,
			item => Promise.resolve(mapper(item)),
			concurrency,
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
 * Data sources returned by the mapper are handled as follows:
 *
 * - `undefined` - filtered out and not included in the output stream
 * - **Primitives** (strings, numbers, booleans, null): Yielded as single atomic items
 * - **Arrays/Iterables** (excluding strings): Items are yielded individually from each mapper result
 * - **Async Iterables/Pipes**: Items are yielded as they become available
 * - **Promises**: Awaited and then processed according to their resolved value
 * - **Other values** (objects, etc.): Yielded as single items
 *
 * The mapper function can be synchronous or asynchronous (returning a Promise).
 * Async mappers are useful for fetching data from APIs, databases, or any async source.
 *
 * @typeParam V The type of input items
 * @typeParam R The type of output items after flattening
 *
 * @param mapper The function to transform each item into a data source. When the mapper returns `undefined`,
 *   that value is filtered out and not included in the output stream.
 * @param parallel Concurrency control: `false`/`undefined`/`1` for sequential (default),
 *   `true` for parallel with auto-detected concurrency (CPU cores), `0` for unbounded concurrency (I/O-heavy tasks),
 *   or a number > 1 for explicit concurrency limit
 *
 * @returns A task that transforms and flattens items
 *
 * @remarks
 *
 * In parallel mode, when an error occurs all pending operations are awaited
 * (but not failed) before the error is thrown to prevent resource leaks.
 *
 * @example
 *
 * ```typescript
 * // Synchronous mapper
 * await items([1, 2, 3])(flatMap(x => [x, x * 2]))(toArray());
 * // [1, 2, 2, 4, 3, 6]
 *
 * // Async mapper for API calls
 * await items([1, 2, 3])(flatMap(async id => {
 *   const response = await fetch(`/api/items/${id}`);
 *   return response.json();
 * }))(toArray());
 *
 * // Parallel processing
 * await items([1, 2, 3])(flatMap(async id => {
 *   const data = await fetchData(id);
 *   return data.items;
 * }, { parallel: true }))(toArray());
 * ```
 */
export function flatMap<V, R>(
	mapper: (item: V) => undefined | Data<R> | Promise<undefined | Data<R>>,
	{ parallel }: { parallel?: boolean | number } = {}
): Task<V, R> {

	if ( parallel === true || isNumber(parallel) && parallel !== 1 ) {

		const concurrency = parallel === true ? cores
			: parallel === 0 ? Infinity
				: parallel;

		return source => parallelize(
			source,
			item => Promise.resolve(mapper(item)),
			concurrency,
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
 * @typeParam V The type of items in the stream
 *
 * @param size The maximum number of items per batch (0 or negative for unbounded)
 *
 * @defaultValue 0
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


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Helper for parallel processing with configurable result handling.
 */
async function* parallelize<V, R, T>(
	source: AsyncIterable<V>,
	mapper: (item: V) => Promise<R>,
	threads: number,
	handler: (result: R) => AsyncIterable<T>
): AsyncGenerator<T, void, unknown> {

	/**
	 * Represents a worker in the mapping phase.
	 *
	 * A mapper applies the mapper function to a source value and waits for the result.
	 * Once the promise resolves, the mapper transitions to a consumer.
	 */
	interface Mapper {
		promise: Promise<R>;
	}

	/**
	 * Represents a worker in the consuming phase.
	 *
	 * A consumer iterates through the async iterable produced by the handler function,
	 * yielding values one at a time. The iterator is retained to request subsequent values,
	 * while the promise tracks the pending next() operation.
	 */
	interface Consumer {
		iterator: AsyncIterator<T>;
		promise: Promise<IteratorResult<T>>;
	}

	const iterator = source[Symbol.asyncIterator]();
	const mappers = new Map<number, Mapper>();
	const consumers = new Map<number, Consumer>();

	// Track settled promises to prevent them from being included in subsequent Promise.race() calls
	const settledMappers = new Set<number>();
	const settledConsumers = new Set<number>();

	let nextId = 0;
	let done = false;


	/**
	 * Initiates the mapping phase for a source value.
	 *
	 * Creates a new mapper that applies the mapper function to the value
	 * and registers it in the mappers collection with a unique ID.
	 *
	 * If the mapper function throws or rejects, the error propagates to
	 * the generator, causing it to throw and trigger cleanup in the finally block.
	 */
	function startMapping(value: V): void {
		mappers.set(nextId++, { promise: mapper(value) });
	}

	/**
	 * Transitions a completed mapper to the consuming phase.
	 *
	 * Removes the mapper from the mappers collection and creates a new consumer
	 * that will iterate through the async iterable produced by the handler function.
	 * The consumer is registered with the same ID to maintain tracking.
	 */
	function completeMapping(id: number, result: R): void {
		mappers.delete(id);

		const iter = handler(result)[Symbol.asyncIterator]();

		consumers.set(id, {
			iterator: iter,
			promise: iter.next()
		});
	}

	/**
	 * Processes a consumer iteration result.
	 *
	 * If the iterator is done, removes the consumer from the collection and marks it as settled.
	 * Otherwise, advances the iterator by requesting the next value and yields
	 * the current value to the output stream.
	 *
	 * Uses atomic get-and-check to prevent race conditions where multiple
	 * completions might try to process the same consumer.
	 */
	function* completeConsuming(id: number, result: IteratorResult<T>): Generator<T> {
		// Atomically retrieve consumer to prevent racing completions
		const consumer = consumers.get(id);

		if ( !consumer ) {
			// Already processed by racing completion
			return;
		}

		if ( result.done ) {
			consumers.delete(id);
			settledConsumers.add(id);
		} else {
			// Re-register with new promise for next iteration
			// Note: Remove from settled set since we're still active
			settledConsumers.delete(id);
			consumers.set(id, {
				iterator: consumer.iterator,
				promise: consumer.iterator.next()
			});

			yield result.value;
		}
	}

	try {

		while ( mappers.size > 0 || consumers.size > 0 || !done ) {

			// Stage 1: Feed source values into mapping phase until thread limit reached

			// Thread management: The `threads` parameter defines the total concurrency limit
			// across both mapping and consuming phases. This ensures we never exceed the
			// specified number of concurrent operations, regardless of which phase they're in.
			// Only active (unsettled) workers count against the thread limit.

			while ( mappers.size+consumers.size < threads && !done ) {

				const next = await iterator.next();

				if ( next.done ) {
					done = true;
				} else {
					startMapping(next.value);
				}
			}

			// Stage 2: Process completed mappings and consumptions

			type MappedEvent = { type: "mapped"; id: number; mapped: R };
			type ConsumedEvent = { type: "consumed"; id: number; consumed: IteratorResult<T> };

			// Only include unsettled promises in the race to avoid processing the same
			// promise multiple times when it wins consecutive races

			const events = [
				...Array.from(mappers.entries())
					.filter(([id]) => !settledMappers.has(id))
					.map(([id, m]) =>
						m.promise.then(mapped => ({ type: "mapped" as const, id, mapped }))
					),
				...Array.from(consumers.entries())
					.filter(([id]) => !settledConsumers.has(id))
					.map(([id, c]) =>
						c.promise.then(consumed => ({ type: "consumed" as const, id, consumed }))
					)
			];

			if ( events.length !== 0 ) {

				// If any mapper or consumer promise rejects, the error propagates here
				// and triggers cleanup in the finally block

				const event = await Promise.race(events);

				if ( event.type === "mapped" ) {
					settledMappers.add(event.id);
					if ( mappers.has(event.id) ) {
						completeMapping(event.id, event.mapped);
						// Clear settled marker after transition to consumer phase
						settledMappers.delete(event.id);
					}
				} else {
					if ( consumers.has(event.id) ) {
						yield* completeConsuming(event.id, event.consumed);
						// Clear settled marker after consumer completes
						if ( !consumers.has(event.id) ) {
							settledConsumers.delete(event.id);
						}
					}
				}
			}

			// All active promises have settled but haven't been processed yet
			// Loop will process them on next iteration or exit if all work is done

		}

	} finally {

		// Cleanup: Close source iterator and wait for all in-flight work to complete

		try {
			await iterator.return?.();
		} catch ( error ) {
			// Suppress iterator cleanup errors to ensure all cleanup completes
		}

		// Wait for all mapper promises to settle (resolve or reject)
		// This ensures no pending async work is abandoned

		await Promise.allSettled(
			Array.from(mappers.values()).map(m => m.promise)
		);

		// Close all consumer iterators
		// Errors during consumer cleanup are suppressed to ensure all iterators are closed

		await Promise.allSettled(
			Array.from(consumers.values()).map(c => c.iterator.return?.())
		);

	}
}
