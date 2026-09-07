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
 * Creates a task discarding a prefix of the feed.
 *
 * The leading items are drawn and dropped, then the rest of the feed is emitted in source order; a shorter feed is
 * consumed entirely and nothing is emitted.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: items are emitted as they are drawn, so the reported feed runs dry as the feed drawn from
 * >   does.
 * > - **Streaming**: items are emitted one at a time, none retained.
 * > - **Stateful**: the items already discarded decide the ones that follow, so a task invoked per nested feed or per
 * >   run discards a prefix of each rather than of the feed as a whole.
 *
 * @typeParam V The type of items in the feed
 *
 * @param n The number of leading items to discard; values less than 1 leave the feed untouched
 *
 * @returns A task yielding the items following the first `n`
 *
 * @throws {@link !TypeError TypeError} If `n` is not an integer
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (skip(2))
 *   (toArray())
 * );  // [3, 4, 5]
 * ```
 */
export function skip<V>(n: number): Task<V> {

	const limit = assert(n, Number.isInteger, value => `expected integer count <${value}>`);

	return source => items((async function* () {

		let count = 0;

		for await (const item of source) {
			if ( count >= limit ) {
				yield item;
			} else {
				count++;
			}
		}
	})());

}
