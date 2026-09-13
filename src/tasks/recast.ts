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

import type { Awaitable, Awaitables } from "@metreeca/core/async";
import { items } from "../feeds/items.js";
import type { Feed, Task } from "../index.js";


/**
 * Creates a task emitting the items a mapper computes over the feed.
 *
 * The mapper is handed the feed and computes the items to carry on with, so a step deciding on the feed as a whole,
 * reconciling it against a stored snapshot, ranking it or clearing it against a quota before letting anything through,
 * is written as an ordinary asynchronous function of the feed rather than as a generator of its own. Sinks already
 * available are lifted back into the pipe the same way: handing over {@link sinks.toSet toSet} carries on with the
 * distinct items alone.
 *
 * The items are supplied either as a batch listing them or as a feed yielding them, handed back as they are or
 * awaited, so a step composing the feed it draws from with tasks already available is spared the generator as well.
 *
 * Whatever the mapper computes is emitted item by item, so the items carried on with need be neither the ones drawn,
 * nor as many, nor of the same type: a mapper computing nothing empties the feed, and one computing items of its own
 * emits them even where the feed drew none.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: nothing is emitted before the mapper resolves, so a mapper drawing the feed entire, as most do,
 * >   never completes on an infinite feed; one composing the feed it draws from emits as its items are drawn instead.
 * > - **Materialising**: a batch is held whole before its first item is emitted, on top of whatever the mapper retains
 * >   while computing it, while a feed is drawn item by item.
 * > - **Stateful**: the outcome covers the items drawn, so a task invoked per nested feed or per run decides on each
 * >   independently rather than on the feed as a whole.
 *
 * > [!NOTE]
 * >
 * > A mapper computing a string emits its characters, as any other iterable does its items: carry a string on whole
 * > by wrapping it in an array.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of items the mapper computes
 *
 * @param mapper The function drawing the feed and computing the items to carry on with
 *
 * @returns A task yielding the items `mapper` computes over the feed
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4]))
 *   (recast(async feed => (await feed(toArray())).slice(-2)))
 *   (toArray())
 * );  // [3, 4], as the last items are known only once the feed runs dry
 *
 * await pipe(
 *   (items([1, 2, 2, 3]))
 *   (recast(toSet()))
 *   (toArray())
 * );  // [1, 2, 3], as a sink already available is lifted back into the pipe
 * ```
 */
export function recast<V, R>(mapper: (feed: Feed<NoInfer<V>>) => Awaitable<Awaitables<R>>): Task<V, R> {

	return source => items((async function* () {

		yield* await mapper(source);

	})());

}
