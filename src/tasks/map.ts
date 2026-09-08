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
import { Task } from "../index.js";
import { items } from "../feeds/items.js";


/**
 * Creates a task converting each item into a new value.
 *
 * Items are emitted in source order.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: items are emitted as they are drawn, so the reported feed runs dry as the feed drawn from
 * >   does.
 * > - **Streaming**: items are converted one at a time, none retained.
 * > - **Stateless**: every item is converted on its own, so the outcome is unaffected by how the feed is split across
 * >   nested feeds or runs.
 *
 * @typeParam V The type of input items
 * @typeParam R The type of output items
 *
 * @param mapper The function converting each item
 *
 * @returns A task yielding the values `mapper` converts the items into
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3]))
 *   (map(n => n*2))
 *   (toArray())
 * );  // [2, 4, 6]
 * ```
 */
export function map<V, R>(mapper: (item: NoInfer<V>) => Awaitable<R>): Task<V, R> {

	return source => items((async function* () {
		for await (const item of source) {
			yield await mapper(item);
		}
	})());

}
