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
import { Feed } from "../index.js";
import { items } from "./items.js";


/**
 * Creates a feed from a range of consecutive integers.
 *
 * Numbers are generated in ascending order if `start` is less than `end`, in descending order if it is greater; equal
 * bounds yield an empty feed.
 *
 * > [!NOTE]
 * >
 * > **Bounded**: the feed runs dry at `end`, whatever draws from it.
 *
 * @param start The first value of the range, included
 * @param end The value the range stops at, excluded
 *
 * @returns A feed yielding the numbers from `start` towards `end`
 *
 * @throws {@link !TypeError TypeError} If either `start` or `end` is not an integer
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (range(1, 5))
 *   (toArray())
 * );  // [1, 2, 3, 4]
 *
 * await pipe(
 *   (range(5, 1))
 *   (toArray())
 * );  // [5, 4, 3, 2]
 *
 * await pipe(
 *   (range(3, 3))
 *   (toArray())
 * );  // []
 * ```
 */
export function range(start: number, end: number): Feed<number> {

	const from = assert(start, Number.isInteger, value => `expected integer bound <${value}>`);
	const to = assert(end, Number.isInteger, value => `expected integer bound <${value}>`);

	return items((function* () {

		if ( from < to ) {

			for (let i = from; i < to; i++) {
				yield i;
			}

		} else if ( from > to ) {

			for (let i = from; i > to; i--) {
				yield i;
			}

		}

	})());

}
