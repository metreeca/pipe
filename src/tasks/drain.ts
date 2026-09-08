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

import type { Awaitables } from "@metreeca/core/async";
import { items } from "../feeds/items.js";
import type { Sink, Task } from "../index.js";


/**
 * Creates a task emitting the items a sink computes over the feed.
 *
 * The sink is handed the feed and computes the items to carry on with, so a step deciding on the feed as a whole,
 * reconciling it against a stored snapshot, ranking it or clearing it against a quota before letting anything through,
 * is written as an ordinary asynchronous function of the feed rather than as a generator of its own. Sinks already
 * available are lifted back into the pipe the same way: handing over {@link sinks.toSet toSet} carries on with the
 * distinct items alone.
 *
 * Whatever the sink resolves to is emitted item by item, so the items carried on with need be neither the ones drawn,
 * nor as many, nor of the same type: a sink resolving to nothing empties the feed, and one computing items of its own
 * emits them even where the feed drew none.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: nothing is emitted before the sink resolves, so a sink drawing the feed entire, as most do,
 * >   never completes on an infinite feed.
 * > - **Materialising**: a sink resolving to a batch has its items held whole before the first is emitted, on top of
 * >   whatever it retains while computing them, while one resolving to a feed of its own is drawn item by item.
 * > - **Stateful**: the outcome covers the items drawn, so a task invoked per nested feed or per run decides on each
 * >   independently rather than on the feed as a whole.
 *
 * > [!NOTE]
 * >
 * > A sink resolving to a string emits its characters, as any other iterable does its items: carry a string on whole
 * > by wrapping it in an array.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of items the sink computes
 *
 * @param sink The sink drawing the feed and resolving to the items to carry on with, supplied either as a batch or as
 *   a feed of their own
 *
 * @returns A task yielding the items `sink` computes over the feed
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4]))
 *   (drain(async feed => (await feed(toArray())).slice(-2)))
 *   (toArray())
 * );  // [3, 4], as the last items are known only once the feed runs dry
 *
 * await pipe(
 *   (items([1, 2, 2, 3]))
 *   (drain(toSet()))
 *   (toArray())
 * );  // [1, 2, 3], as a sink computing a batch carries on with its items
 * ```
 */
export function drain<V, R>(sink: Sink<V, Awaitables<R>>): Task<V, R> {

	return source => items((async function* () {

		yield* await source(sink);

	})());

}
