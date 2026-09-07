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
 * Creates a sink retrieving the first matching item of the feed, failing if none does.
 *
 * Items are tested in source order and consumption stops at the first match, leaving the rest of the feed
 * unconsumed.
 *
 * A feed carrying no matching item fails the sink instead of resolving to `undefined` as {@link find} does, so the
 * item handed back is usable as is, with no check to tell a missing item from an `undefined` item the feed
 * legitimately carries.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: items are drawn only until one matches, so an infinite feed completes unless none does.
 * > - **Streaming**: items are tested one at a time, none retained.
 * > - **Stateless**: every item is tested on its own.
 *
 * @typeParam V The type of items in the feed
 *
 * @param predicate The function testing each item, defaulting to a test matching every item, which retrieves the
 *   first item of the feed
 *
 * @returns A sink resolving to the first item matching `predicate`
 *
 * @throws {@link !Error Error} If no item matches `predicate`
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (seek(n => n > 3))
 * );  // 4
 *
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (seek())
 * );  // 1
 * ```
 */
export function seek<V>(predicate: (item: NoInfer<V>) => Awaitable<boolean> = () => true): Sink<V, V> {

	return async source => {

		for await (const item of source) {
			if ( await predicate(item) ) {
				return item;
			}
		}

		throw new Error("missing matching item");
	};

}
