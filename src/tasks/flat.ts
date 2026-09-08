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

import type { Feed, Task } from "../index.js";
import { items } from "../feeds/items.js";


/**
 * Creates a task splicing a feed of feeds into a single feed.
 *
 * Nested feeds are consumed one at a time, each fully drained before the next is opened, so items are emitted in source
 * order, those of every nested feed kept together and in their own order; a nested feed contributing nothing simply
 * drops out. Nothing is drawn from a nested feed until its turn comes, so one deferring retrieval until consumption is
 * left untouched until then.
 *
 * > [!WARNING]
 * >
 * > - **Incremental**: items are emitted as the nested feeds are drained, so the reported feed runs dry as the source
 * >   and its nested feeds do; an infinite nested feed starves the ones behind it, which are never opened.
 * > - **Streaming**: nested feeds are drained one at a time, none held.
 * > - **Stateless**: nested feeds are spliced without state carried across them.
 *
 * @typeParam V The type of items carried by the nested feeds
 *
 * @returns A task yielding the items of every nested feed in source order
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([items([1, 2]), items([3, 4])]))
 *   (flat())
 *   (toArray())
 * );  // [1, 2, 3, 4]
 * ```
 */
export function flat<V>(): Task<Feed<V>, V>;

/**
 * Creates a task splicing the feeds another task reports into a single feed.
 *
 * Hands the feed drawn from to `task` and splices the feeds it reports as the overload taking no argument does, so
 * that an item mapped to a feed of its own is expanded in place into the items of that feed.
 *
 * > [!WARNING]
 * >
 * > - **Incremental**: items are emitted as the reported feeds are drained, so the reported feed runs dry as the
 * >   source, `task` and the feeds it reports do; an infinite one starves the feeds behind it, never opened.
 * > - **Streaming**: reported feeds are drained one at a time, none held, whatever `task` holds.
 * > - **Stateless**: the splice carries no state across the reported feeds, whatever `task` carries across the items
 * >   it draws.
 *
 * > [!NOTE]
 * >
 * > `task` draws from the whole feed, so state it initialises on invocation decides on every item, as it would
 * > anywhere else in the pipe. Where a source already carries feeds and a task is to be scoped to each of them,
 * > apply it within `map()`: `flat(map(feed => feed(sort())))` orders every nested feed on its own.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of items carried by the feeds `task` reports
 *
 * @param task The task opening the feeds to splice
 *
 * @returns A task yielding the items of every feed `task` reports, in source order
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3]))
 *   (flat(map(n => items([n, n*10]))))
 *   (toArray())
 * );  // [1, 10, 2, 20, 3, 30]
 * ```
 */
export function flat<V, R>(task: Task<V, Feed<R>>): Task<V, R>;

/**
 * Creates a task splicing nested feeds into a single feed, with or without a task opening the feeds to splice.
 */
export function flat<R>(task: Task<Feed<R>> = feed => feed): Task<Feed<R>, R> {

	return source => items((async function* () {

		for await (const feed of task(source)) {
			yield* feed;
		}

	})());

}
