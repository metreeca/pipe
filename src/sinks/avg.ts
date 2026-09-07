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

import { add, div } from "@metreeca/core/numbers";
import { Sink } from "../index.js";


/**
 * Creates a sink averaging the items of the feed.
 *
 * The mean is computed from the running total and the item count. `number` items are averaged as IEEE 754 doubles and
 * `bigint` items as exact integers: a feed is expected to carry one numeric type throughout, and mixing the two is
 * reported rather than silently coerced.
 *
 * `bigint` means are rounded to the nearest integer, halves away from zero, as no fractional `bigint` can carry the
 * remainder; callers needing another rounding, or a fractional mean, combine {@link sum} with {@link count}
 * themselves.
 *
 * An empty feed resolves to `undefined`, as a mean over no items is not defined; callers wanting a default supply
 * it with `?? 0` or `?? 0n`.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is drawn before the sink resolves, so an infinite feed never completes.
 * > - **Streaming**: no more than the running total and count are held in memory, whatever the size of the feed.
 * > - **Stateful**: the mean covers the items drawn, so a sink closing a nested or truncated feed sees those alone.
 *
 * @typeParam V The type of items in the feed
 *
 * @returns A sink resolving to the mean of the items of the feed, or to `undefined` if the feed carried no items
 *
 * @throws {@link !TypeError TypeError} If the feed mixes `number` and `bigint` items
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4]))
 *   (avg())
 * );  // 2.5
 *
 * await pipe(
 *   (items([1n, 2n, 4n]))
 *   (avg())
 * );  // 2n
 *
 * await pipe(
 *   (items<number>([]))
 *   (avg())
 * );  // undefined
 * ```
 */
export function avg<V extends number | bigint>(): Sink<V, undefined | V> {

	return async source => {

		let total: undefined | V = undefined;
		let count = 0;

		for await (const value of source) {
			total = total === undefined ? value : add(total, value);
			count++;
		}

		return total === undefined ? undefined : div(total, count);

	};

}
