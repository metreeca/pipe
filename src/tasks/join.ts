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

import { items } from "../feeds/items.js";
import type { Feed, Task } from "../index.js";


/**
 * Creates a task interleaving a feed of feeds into a single feed.
 *
 * Nested feeds are opened as the source yields them and consumed together, each kept one item ahead, so a slow nested
 * feed never holds back the others; the items of every nested feed keep their own order among themselves. A nested
 * feed contributing nothing simply drops out.
 *
 * > [!WARNING]
 * >
 * > - **Incremental**: items are emitted as the nested feeds report them, so the reported feed runs dry as the source
 * >   and its nested feeds do, an infinite nested feed keeping it open without holding back the items of the others.
 * > - **Materialising**: a pending item is held for every nested feed open at the same time, and nothing bounds their
 * >   number, so a source yielding feeds faster than they run dry may exhaust memory; splice with `flat()` instead
 * >   where the source carries an unbounded number of feeds.
 * > - **Stateless**: nested feeds are interleaved without state carried across them.
 *
 * > [!WARNING]
 * >
 * > Output order is not preserved: items interleave and overtake each other according to how quickly every nested
 * > feed produces them.
 *
 * > [!NOTE]
 * >
 * > Nested feeds failing while the consumer is idle report their error when the feed is next advanced, rather than
 * > escaping as unhandled rejections.
 *
 * > [!NOTE]
 * >
 * > The source and every nested feed still open are closed when the feed is exhausted, fails or is closed early,
 * > waiting for their pending items to settle first, so a feed idling between items delays it; failures reported
 * > while closing are suppressed.
 *
 * @typeParam V The type of items carried by the nested feeds
 *
 * @returns A task yielding the items of every nested feed as they become available
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([slow, fast]))  // slow yields 1, 2; fast yields 3, 4
 *   (join())
 *   (toArray())
 * );  // [3, 4, 1, 2], as the faster feed reports first
 * ```
 */
export function join<V>(): Task<Feed<V>, V>;

/**
 * Creates a task interleaving the feeds another task reports into a single feed.
 *
 * Hands the feed drawn from to `task` and interleaves the feeds it reports as the overload taking no argument does, so
 * that an item mapped to a feed of its own is expanded in place into the items of that feed.
 *
 * > [!WARNING]
 * >
 * > - **Incremental**: items are emitted as the reported feeds report them, so the reported feed runs dry as the
 * >   source, `task` and the feeds it reports do, an infinite one keeping it open without holding back the items of
 * >   the others.
 * > - **Materialising**: a pending item is held for every reported feed open at the same time, and nothing bounds
 * >   their number, so feeds opened faster than they run dry may exhaust memory; splice with `flat()` instead where
 * >   `task` reports an unbounded number of feeds.
 * > - **Stateless**: the interleaving carries no state across the reported feeds, whatever `task` carries across the
 * >   items it draws.
 *
 * > [!NOTE]
 * >
 * > `task` draws from the whole feed, so state it initialises on invocation decides on every item, as it would
 * > anywhere else in the pipe. Where a source already carries feeds and a task is to be scoped to each of them,
 * > apply it within `map()`: `join(map(feed => feed(take(2))))` yields its quota to every nested feed.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of items carried by the feeds `task` reports
 *
 * @param task The task opening the feeds to interleave
 *
 * @returns A task yielding the items of every feed `task` reports, as they become available
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([30, 10]))  // the delay of each retrieval
 *   (join(map(ms => retrieve(ms))))
 *   (toArray())
 * );  // the items of the faster retrieval first
 * ```
 */
export function join<V, R>(task: Task<V, Feed<R>>): Task<V, R>;

/**
 * Creates a task interleaving nested feeds into a single feed, with or without a task opening the feeds to interleave.
 */
export function join<R>(task: Task<Feed<R>> = feed => feed): Task<Feed<R>, R> {

	return source => items((async function* () {

		type Feeds = AsyncIterator<Feed<R>, void, undefined>; // the task, drawn for the feeds it reports
		type Items = AsyncIterator<R, void, undefined>; // one reported feed, drawn for the items it carries

		type Event =
			| { kind: "feed", iterator: Feeds, result: IteratorResult<Feed<R>, void> }
			| { kind: "item", iterator: Items, result: IteratorResult<R, void> };


		// the task is raced with the feeds it reports, so that a feed opened mid-race joins it at once

		const feeds = task(source)[Symbol.asyncIterator]();
		const polls = new Map<Feeds | Items, Promise<Event>>([[feeds, feed()]]);


		try {

			while ( polls.size > 0 ) {

				yield* process(await Promise.race(polls.values()));

			}

		} finally { // on failure or early termination as well

			await close();

		}


		async function* process(event: Event): AsyncGenerator<R, void, undefined> {

			if ( event.result.done ) { // the task reported its last feed, or a reported feed ran dry

				polls.delete(event.iterator);

			} else if ( event.kind === "feed" ) { // a new reported feed: open it and keep drawing from the task

				const iterator = event.result.value[Symbol.asyncIterator]();

				polls.set(iterator, item(iterator));
				polls.set(feeds, feed());

			} else if ( event.kind === "item" ) { // put its feed back to work before yielding, so it draws meanwhile

				polls.set(event.iterator, item(event.iterator));

				yield event.result.value;

			}

		}

		async function close(): Promise<void> {

			await Promise.allSettled([
				...Array.from(polls.keys(), async iterator => iterator.return?.()), // close the source and the feeds
				...polls.values() // suppress late failures from their abandoned polls
			]);

		}

		function feed(): Promise<Event> {

			return guard(feeds.next().then(result =>
				({ kind: "feed", iterator: feeds, result })
			));

		}

		function item(items: Items): Promise<Event> {

			return guard(items.next().then(result =>
				({ kind: "item", iterator: items, result })
			));

		}


		function guard(event: Promise<Event>): Promise<Event> {

			event.catch(() => {}); // suppress unhandled rejection reports while the event waits its turn in the race

			return event;

		}

	})());

}
