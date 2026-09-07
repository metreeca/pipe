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

import { assert } from "@metreeca/core";
import { Task } from "../index.js";
import { items } from "../feeds/items.js";


/**
 * Creates a task truncating the feed to a prefix.
 *
 * Items are emitted in source order and the feed ends as soon as the quota is met, leaving the rest of the source
 * unconsumed.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: items are emitted as they are drawn and the reported feed ends at `n` items, whatever the feed
 * >   drawn from, which is the standard way to bound an infinite feed.
 * > - **Streaming**: items are emitted one at a time, none retained.
 * > - **Stateful**: the quota is counted over the items drawn, so a task invoked per nested feed or per run grants
 * >   its quota to each rather than to the feed as a whole.
 *
 * @typeParam V The type of items in the feed
 *
 * @param n The maximum number of leading items to emit; values less than 1 empty the feed
 *
 * @returns A task yielding at most the first `n` items
 *
 * @throws {@link !TypeError TypeError} If `n` is not an integer
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (take(3))
 *   (toArray())
 * );  // [1, 2, 3]
 * ```
 */
export function take<V>(n: number): Task<V> {

	const limit = assert(n, Number.isInteger, value => `expected integer count <${value}>`);

	return source => items((async function* () {

		let count = 0;

		for await (const item of source) {
			if ( count < limit ) {
				yield item;
				count++;
			} else {
				return;
			}
		}
	})());

}
