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

import { add } from "@metreeca/core/numbers";
import { Sink } from "../index.js";


/**
 * Creates a sink summing the items of the feed.
 *
 * Items are added in source order, seeding the total with the first one. `number` items are added as IEEE 754 doubles
 * and `bigint` items as exact integers: a feed is expected to carry one numeric type throughout, and mixing the two
 * is reported rather than silently coerced.
 *
 * An empty feed resolves to `undefined`; callers wanting the additive identity supply it with `?? 0` or `?? 0n`.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is drawn before the sink resolves, so an infinite feed never completes.
 * > - **Streaming**: no more than the running total is held in memory, whatever the size of the feed.
 * > - **Stateful**: the total covers the items drawn, so a sink closing a nested or truncated feed sees those alone.
 *
 * @typeParam V The type of items in the feed
 *
 * @returns A sink resolving to the sum of the items of the feed, or to `undefined` if the feed carried no items
 *
 * @throws {@link !TypeError TypeError} If the feed mixes `number` and `bigint` items
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (sum())
 * );  // 15
 *
 * await pipe(
 *   (items([1n, 2n, 3n]))
 *   (sum())
 * );  // 6n
 *
 * await pipe(
 *   (items<number>([]))
 *   (sum())
 * ) ?? 0;  // 0
 * ```
 */
export function sum<V extends number | bigint>(): Sink<V, undefined | V> {

	return async source => {

		let total: undefined | V = undefined;

		for await (const value of source) {
			total = total === undefined ? value : add(total, value);
		}

		return total;

	};

}
