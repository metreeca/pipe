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
 * Terminal operations that consume streams and produce final results.
 *
 * @remarks
 *
 * Sinks trigger stream execution and return promises that resolve when processing completes.
 * They represent the final stage in a pipeline, collecting, aggregating, or validating stream data.
 *
 * @module
 */

import { Sink } from "./index";

/**
 * Creates a sink checking if any item satisfies the predicate.
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
 * Creates a sink counting the total number of items in the stream.
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
 * Creates a sink retrieving the first item that satisfies the predicate.
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
 * Creates a sink executing a side effect for each item and consuming the stream.
 *
 * Terminal operation that triggers stream execution.
 *
 * @typeParam V The type of items in the stream
 *
 * @param consumer The function to execute for each item (return value is ignored)
 *
 * @returns A sink that executes the consumer for each item and returns the number of processed items
 */
export function forEach<V>(consumer: (item: V) => unknown): Sink<V, number> {
	return async source => {

		let count = 0;

		for await (const item of source) {
			await consumer(item);
			count++;
		}

		return count;

	};
}


/**
 * Creates a sink reducing the stream to a single value without an initial value.
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


/**
 * Creates a sink joining all items into a string using a separator.
 *
 * @typeParam V The type of items in the stream
 *
 * @param separator The string to insert between items
 *
 * @returns A sink that joins all items into a single string
 *
 * @remarks
 *
 * Behaves like `Array.prototype.join()`, converting each item to a string and joining them
 * with the specified separator. Items are converted using their default string representation.
 * `null` values are converted to empty strings.
 *
 * > [!WARNING]
 * >
 * > Unlike `Array.prototype.join()`, `undefined` values are automatically filtered out
 * > by the stream pipeline before reaching this sink, so they will not appear in the output.
 */
export function toString<V>(separator: string = ","): Sink<V, string> {
	return async source => {

		const items: string[] = [];

		for await (const item of source) {
			items.push(item == null ? "" : String(item));
		}

		return items.join(separator);
	};
}
