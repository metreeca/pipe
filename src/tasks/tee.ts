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
import type { Task } from "../index.js";
import { join } from "./join.js";


/**
 * Creates a task handing every item to several branches.
 *
 * Each task is applied to the feed as a branch of its own and handed every item, so one pass over the source is
 * reshaped, observed or routed several ways at once; the items the branches report are interleaved into a single
 * feed, emitted as soon as each is ready.
 *
 * Branches draw in lockstep: an item is drawn only once every branch still running has taken the one on offer, so
 * nothing is held beyond that item and the source advances at the pace of the slowest branch. A branch closing early
 * drops out and stops holding back the others; a branch reporting nothing simply contributes nothing.
 *
 * > [!WARNING]
 * >
 * > - **Incremental**: items are emitted as the branches report them, so the reported feed runs dry as the feed drawn
 * >   from and every branch do.
 * > - **Streaming**: one item is on offer at a time and none is retained, whatever the branches retain.
 * > - **Stateless**: the fan-out carries no state across items, whatever the branches carry.
 *
 * > [!WARNING]
 * >
 * > Output order is not preserved: the items of the branches interleave and overtake each other according to how long
 * > every branch takes, though the items of each branch keep their own order among themselves.
 *
 * > [!CAUTION]
 * >
 * > A branch idling while still running holds back every other one, as the item on offer is replaced only once all of
 * > them have taken it: pacing and long-running work belong downstream of the fan-out, where they no longer hold the
 * > branches together.
 *
 * > [!NOTE]
 * >
 * > Every branch draws the whole feed, so state a task initialises on invocation decides on every item, as it would
 * > anywhere else in the pipe: a quota, a deduplication or an ordering covers the feed entire.
 *
 * > [!NOTE]
 * >
 * > Branches failing while the consumer is idle report their error when the feed is next advanced, rather than
 * > escaping as unhandled rejections.
 *
 * > [!NOTE]
 * >
 * > Every branch and the source are closed when the feed is exhausted, fails or is closed early, waiting for the work
 * > already in flight to settle first, so a source idling between items delays it; failures reported while closing
 * > are suppressed.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of items reported by the branches
 *
 * @param tasks The tasks applied to the feed, each drawing every item; handing over none reports an empty feed,
 *   drawing nothing from the source
 *
 * @returns A task yielding the items every branch reports, as they become available
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3]))
 *   (tee(map(n => n*2), filter(n => n > 2)))
 *   (toArray())
 * );  // 2, 4, 6 from the doubling branch and 3 from the filtering one, interleaved in no defined order
 * ```
 */
export function tee<V, R>(...tasks: readonly Task<V, R>[]): Task<V, R> {

	return source => items((async function* () {

		type Round = {

			readonly item: IteratorResult<V, void>; // the item on offer, drawn once and handed to every branch
			readonly following: Promise<Round>; // the round after it, opened once every live branch has taken this one

		};

		type Branch = {

			readonly task: Task<V, R>;


			notify: () => void; // marks the branch as having taken the item on offer

		};


		// the single reader of the source: wrapping it in a generator serialises the draws issued by the rounds

		const reader = (async function* () { yield* source; })();

		// the branches still drawing: a round waits for these alone, so one closing early stops holding back the others

		const branches = tasks.map<Branch>(task => ({ task, notify: () => {} }));
		const live = new Set(branches);

		// the round the chain stops at: the source ran dry, or no branch is left to take another item

		const ended: Round = {
			item: { done: true, value: undefined },
			following: Promise.resolve().then(() => ended)
		};

		// opened before the branches are, so that they all draw from the same first round

		const first = guard(open());


		try {

			// every branch draws the same chain of rounds through a cursor of its own

			const feeds = branches.map(branch => branch.task(items(cursor(branch, first))));

			yield* items(feeds)(join());

		} finally { // on failure or early termination as well

			await Promise.allSettled([reader.return()]); // close the source, now that no branch is drawing from it

		}


		async function open(): Promise<Round> {

			const item = live.size > 0 ? await reader.next() : ended.item; // no branch left to take it, nothing drawn

			if ( item.done ) { // every branch takes the end marker and no further round opens

				return ended;

			} else {

				// the signal each live branch raises on taking the item on offer

				const arrivals = Array.from(live, branch => new Promise<void>(resolve => { branch.notify = resolve; }));

				return { item, following: guard(Promise.all(arrivals).then(open)) };

			}

		}

		function guard(round: Promise<Round>): Promise<Round> {

			round.catch(() => {}); // suppress unhandled rejection reports while the round waits to be drawn

			return round;

		}

		function cursor(branch: Branch, round: Promise<Round>): AsyncIterableIterator<V> { // one branch's view

			const position = { round };

			const iterator: AsyncIterableIterator<V> = {

				next: async () => {

					const current = await position.round; // waits for the round to open, that is, for the others

					position.round = current.following;

					branch.notify();

					return current.item;

				},

				return: async () => {

					live.delete(branch); // no round waits for it from here on, and

					branch.notify(); // the one on offer is released, whether or not it was taken

					return { done: true, value: undefined };

				},

				[Symbol.asyncIterator]: () => iterator

			};

			return iterator;

		}

	})());

}
