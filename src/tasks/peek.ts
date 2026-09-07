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

import { Task } from "../index.js";
import { items } from "../feeds/items.js";


/**
 * Creates a task observing the feed without altering it.
 *
 * Every item is handed to the consumer before being emitted unchanged, making the feed observable at any point of a
 * pipe for tracing, logging or metering purposes.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: items are emitted as they are drawn, so the reported feed runs dry as the feed drawn from
 * >   does.
 * > - **Streaming**: items are observed one at a time, none retained.
 * > - **Stateless**: every item is observed on its own, so the outcome is unaffected by how the feed is split across
 * >   nested feeds or runs.
 *
 * @typeParam V The type of items in the feed
 *
 * @param consumer The function observing each item; a returned promise is awaited before the item moves on, so
 *   asynchronous side effects hold the feed back until they complete
 *
 * @returns A task yielding the items of the feed unchanged
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3]))
 *   (peek(n => console.log(n)))
 *   (toArray())
 * );  // logs 1, 2, 3; [1, 2, 3]
 * ```
 */
export function peek<V>(consumer: (item: NoInfer<V>) => unknown): Task<V> {

	return source => items((async function* () {
		for await (const item of source) {
			await consumer(item);
			yield item;
		}
	})());

}
