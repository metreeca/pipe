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

import { Sink } from "../index.js";


/**
 * Creates a sink handing every item to a consumer.
 *
 * Items are handed over in source order. This is the sink of choice for pipes run for their side effects rather than
 * for a collected result.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is handed over before the sink resolves, so an infinite feed never completes.
 * > - **Streaming**: items are handed over one at a time, none retained.
 * > - **Stateful**: the resolved count covers the items drawn, so a sink closing a nested or truncated feed counts
 * >   those alone.
 *
 * @typeParam V The type of items in the feed
 *
 * @param consumer The function processing each item; a returned promise is awaited before the next item is pulled,
 *   so items are processed one at a time
 *
 * @returns A sink resolving to the number of items handed to `consumer`
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3]))
 *   (each(n => console.log(n)))
 * );  // logs 1, 2, 3; 3
 * ```
 */
export function each<V>(consumer: (item: NoInfer<V>) => unknown): Sink<V, number> {

	return async source => {

		let count = 0;

		for await (const item of source) {
			await consumer(item);
			count++;
		}

		return count;

	};

}
