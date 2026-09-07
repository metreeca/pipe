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

import type { Primitive } from "@metreeca/core";
import type { Awaitable } from "@metreeca/core/async";
import type { Task } from "../index.js";
import { items } from "../feeds/items.js";


/**
 * Creates a task collecting items sharing the same key.
 *
 * Groups are emitted in first-appearance order of their key, with items inside each group in source order. Keys are
 * limited to {@link Primitive} values and compared with `SameValueZero` semantics, so `NaN` matches itself and `-0`
 * matches `0`.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: the whole feed is drained before the first group is emitted, so an infinite feed never
 * >   completes.
 * > - **Materialising**: the whole feed is held in memory before grouping, so a large feed may exhaust it.
 * > - **Stateful**: groups cover the items drawn, so a task invoked per nested feed or per run groups each
 * >   independently, yielding the same key once per invocation rather than once for the feed as a whole.
 *
 * @typeParam V The type of items in the feed
 * @typeParam K The type of the grouping key
 *
 * @param key The function extracting the grouping key from each item
 *
 * @returns A task yielding each distinct key paired with the read-only list of the items sharing it
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4]))
 *   (group(n => n%2))
 *   (toArray())
 * );  // [[1, [1, 3]], [0, [2, 4]]]
 *
 * await pipe(
 *   (items([{ id: 1 }, { id: 2 }, { id: 1 }]))
 *   (group(x => x.id))
 *   (toArray())
 * );  // [[1, [{ id: 1 }, { id: 1 }]], [2, [{ id: 2 }]]]
 * ```
 */
export function group<V, K extends Primitive>(
	key: (item: NoInfer<V>) => Awaitable<K>
): Task<V, readonly [K, readonly V[]]> {

	return source => items((async function* () {

		const groups = new Map<K, V[]>();

		for await (const item of source) {

			const entry = await key(item);

			const values = groups.get(entry) ?? [];

			groups.set(entry, values);

			values.push(item);

		}

		yield* groups;

	})());

}
