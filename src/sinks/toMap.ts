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


const readonly = () => { throw new TypeError("unsupported mutation of immutable map"); };


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a sink collecting the items of the feed into a map under extracted keys.
 *
 * Keys and values are made {@link immutable} as they are collected, in source order. Keys must be unique: an item
 * whose key was already collected fails the sink rather than silently overwriting the entry standing under it. Keys
 * are compared with `SameValueZero` semantics, so `NaN` matches itself and `-0` matches `0`.
 *
 * The returned map is frozen, with `set`, `delete` and `clear` shadowed by own properties that throw: entries cannot
 * be altered through the map, although mutating methods invoked directly on `Map.prototype` still reach the internal
 * slots backing them.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is collected before the sink resolves, so an infinite feed never completes.
 * > - **Materialising**: the whole feed is held in memory, so a large feed may exhaust it.
 * > - **Stateful**: the map covers the items drawn, so a sink closing a nested or truncated feed sees those alone,
 * >   and a key repeated across two of them fails neither.
 *
 * > [!WARNING]
 * >
 * > Freezing clones structured keys, giving them a fresh identity: entries are not reachable through the original key
 * > reference, and the same mutable key extracted twice yields two distinct entries rather than a duplicate key
 * > error. Extract structured keys as {@link immutable} values to keep their identity stable.
 *
 * @typeParam V The type of items in the feed
 * @typeParam K The type of map keys
 *
 * @param key The function extracting the key from each item
 *
 * @returns A sink resolving to the deeply {@link immutable} read-only map pairing each extracted key with the item it
 *   was extracted from
 *
 * @throws {@link !Error Error} If two items yield the same key
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]))
 *   (toMap(x => x.id))
 * );  // Map(2) { 1 => { id: 1, name: "Alice" }, 2 => { id: 2, name: "Bob" } }
 * ```
 */
export function toMap<V, K>(
	key: (item: NoInfer<V>) => Awaitable<K>
): Sink<V, ReadonlyMap<K, V>>;

/**
 * Creates a sink collecting extracted values into a map under extracted keys.
 *
 * Collects like {@link toMap} without a `value` argument, subject to the same key, freezing and axis rules, pairing
 * each key with an extracted value rather than with the item itself.
 *
 * @typeParam V The type of items in the feed
 * @typeParam K The type of map keys
 * @typeParam T The type of map values
 *
 * @param key The function extracting the key from each item
 * @param value The function transforming each item into a map value
 *
 * @returns A sink resolving to the deeply {@link immutable} read-only map pairing each extracted key with the value
 *   extracted alongside it
 *
 * @throws {@link !Error Error} If two items yield the same key
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]))
 *   (toMap(x => x.id, x => x.name))
 * );  // Map(2) { 1 => "Alice", 2 => "Bob" }
 * ```
 */
export function toMap<V, K, T>(
	key: (item: NoInfer<V>) => Awaitable<K>,
	value: (item: NoInfer<V>) => Awaitable<T>
): Sink<V, ReadonlyMap<K, T>>;

/**
 * Creates a sink collecting items or extracted values into a map under extracted keys.
 */
export function toMap<V, K, R>(
	key: (item: V) => Awaitable<K>,
	value?: (item: V) => Awaitable<R>
): Sink<V, ReadonlyMap<K, V | R>> {

	return async source => {

		const map = new Map<K, V | R>();

		for await (const item of source) {

			const entry = immutable(await key(item));

			if ( map.has(entry) ) {
				throw new Error(`duplicate key <${String(entry)}>`);
			}

			map.set(entry, immutable(value ? await value(item) : item));

		}

		// shadow mutators with own properties: freezing doesn't reach the internal slots backing map entries

		return Object.freeze(Object.defineProperties(map, {

			set: { value: readonly },
			delete: { value: readonly },
			clear: { value: readonly }

		}));

	};

}
