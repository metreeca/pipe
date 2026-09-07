/*
 * Copyright © 2025-2026 Metreeca srl
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

import type { Awaitable } from "@metreeca/core/async";
import { immutable } from "@metreeca/core/structures";
import { Sink } from "../index.js";


/**
 * Creates a sink collecting the items of the feed into an object under extracted keys.
 *
 * Keys must be unique: an item whose key was already collected fails the sink rather than silently overwriting the
 * entry standing under it. Keys are limited to `PropertyKey` values and compared after property key coercion, so the
 * number `1` and the string `"1"` denote the same entry.
 *
 * Entries are collected as own properties, so a `__proto__` key is stored as data rather than altering the prototype
 * chain of the returned object; they are enumerated in standard property order, that is integer-like keys in
 * ascending numeric order, followed by other string keys and symbol keys in collection order. The object is made
 * {@link immutable} once the feed is drained, freezing it together with the values collected into it.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is collected before the sink resolves, so an infinite feed never completes.
 * > - **Materialising**: the whole feed is held in memory, so a large feed may exhaust it.
 * > - **Stateful**: the object covers the items drawn, so a sink closing a nested or truncated feed sees those alone,
 * >   and a key repeated across two of them fails neither.
 *
 * > [!WARNING]
 * >
 * > Freezing clones structured items, giving them a fresh identity: entries are not reachable through the original
 * > item reference. Supply structured items as {@link immutable} values to keep their identity stable.
 *
 * @typeParam V The type of items in the feed
 * @typeParam K The type of object keys
 *
 * @param key The function extracting the key from each item
 *
 * @returns A sink resolving to the deeply {@link immutable} object pairing each extracted key with the item it was
 *   extracted from
 *
 * @throws {@link !Error Error} If two items yield the same key
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]))
 *   (toObject(x => x.id))
 * );  // { 1: { id: 1, name: "Alice" }, 2: { id: 2, name: "Bob" } }
 * ```
 */
export function toObject<V, K extends PropertyKey>(
	key: (item: NoInfer<V>) => Awaitable<K>
): Sink<V, Readonly<Record<K, V>>>;

/**
 * Creates a sink collecting extracted values into an object under extracted keys.
 *
 * Collects like {@link toObject} without a `value` argument, subject to the same key, freezing and axis rules, pairing
 * each key with an extracted value rather than with the item itself.
 *
 * @typeParam V The type of items in the feed
 * @typeParam K The type of object keys
 * @typeParam T The type of object values
 *
 * @param key The function extracting the key from each item
 * @param value The function transforming each item into an object value
 *
 * @returns A sink resolving to the deeply {@link immutable} object pairing each extracted key with the value
 *   extracted alongside it
 *
 * @throws {@link !Error Error} If two items yield the same key
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]))
 *   (toObject(x => x.id, x => x.name))
 * );  // { 1: "Alice", 2: "Bob" }
 * ```
 */
export function toObject<V, K extends PropertyKey, T>(
	key: (item: NoInfer<V>) => Awaitable<K>,
	value: (item: NoInfer<V>) => Awaitable<T>
): Sink<V, Readonly<Record<K, T>>>;

/**
 * Creates a sink collecting items or extracted values into an object under extracted keys.
 */
export function toObject<V, K extends PropertyKey, R>(
	key: (item: V) => Awaitable<K>,
	value?: (item: V) => Awaitable<R>
): Sink<V, Readonly<Record<PropertyKey, V | R>>> {

	return async source => {

		const object = {};

		for await (const item of source) {

			const entry = await key(item);

			if ( Object.hasOwn(object, entry) ) {
				throw new Error(`duplicate key <${String(entry)}>`);
			}

			Object.defineProperty(object, entry, {

				value: value ? await value(item) : item,

				enumerable: true

			});

		}

		return immutable(object);

	};

}
