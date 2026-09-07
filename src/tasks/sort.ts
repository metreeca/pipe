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

import { ascending } from "@metreeca/core/order";
import { Task } from "../index.js";
import { items } from "../feeds/items.js";


/**
 * Creates a task reordering the feed.
 *
 * Items are emitted in the order the comparator establishes; equal items keep their relative source order.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: the whole feed is drained before the first item is emitted, so an infinite feed never
 * >   completes.
 * > - **Materialising**: the whole feed is held in memory before sorting, so a large feed may exhaust it.
 * > - **Stateful**: the ordering covers the items drawn, so a task invoked per nested feed or per run orders each
 * >   independently rather than the feed as a whole.
 *
 * > [!TIP]
 * >
 * > The {@link https://metreeca.github.io/core/modules/order.html order} module of `@metreeca/core` provides helper
 * > functions for assembling complex sorting criteria.
 *
 * @typeParam V The type of items in the feed
 *
 * @param comparator The function establishing the relative order of two items, defaulting to {@link ascending},
 *   which ranks values in natural order, placing `null` first
 *
 * @returns A task yielding the items of the feed in comparator order
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([3, 1, 2]))
 *   (sort())
 *   (toArray())
 * );  // [1, 2, 3]
 *
 * await pipe(
 *   (items([3, 1, 2]))
 *   (sort(descending))
 *   (toArray())
 * );  // [3, 2, 1]
 *
 * await pipe(
 *   (items([{ age: 30 }, { age: 20 }]))
 *   (sort(by(x => x.age)))
 *   (toArray())
 * );  // [{ age: 20 }, { age: 30 }]
 *
 * await pipe(
 *   (items(["Émile", "Alice"]))
 *   (sort((a, b) => a.localeCompare(b)))
 *   (toArray())
 * );  // ["Alice", "Émile"]
 * ```
 */
export function sort<V>(comparator: (a: NoInfer<V>, b: NoInfer<V>) => number = ascending): Task<V> {

	return source => items((async function* () {

		const items: V[] = [];

		for await (const item of source) {
			items.push(item);
		}

		yield* items.sort(comparator);

	})());

}
