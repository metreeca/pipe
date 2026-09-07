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
import { Sink } from "../index.js";


/**
 * Creates a sink reporting whether some item matches a predicate.
 *
 * Items are tested in source order and consumption stops at the first match, leaving the rest of the feed
 * unconsumed; an empty feed reports no match.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: items are drawn only until one matches, so an infinite feed completes unless none does.
 * > - **Streaming**: items are tested one at a time, none retained.
 * > - **Stateless**: every item is tested on its own.
 *
 * @typeParam V The type of items in the feed
 *
 * @param predicate The function testing each item
 *
 * @returns A sink resolving to `true` if at least one item matches `predicate`; `false` otherwise
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (some(n => n > 3))
 * );  // true
 * ```
 */
export function some<V>(predicate: (item: NoInfer<V>) => Awaitable<boolean>): Sink<V, boolean> {

	return async source => {

		for await (const item of source) {
			if ( await predicate(item) ) {
				return true;
			}
		}

		return false;
	};

}
