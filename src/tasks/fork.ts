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

import { assert } from "@metreeca/core";
import { items } from "../feeds/items.js";
import type { Task } from "../index.js";


/**
 * Creates a task interleaving several runs of another task over the same items.
 *
 * Runs draw from the same source, each item going to exactly one of them, processed through that run's own invocation
 * of `task` and reported as soon as it is ready, so a slow item never holds back the others. `runs` caps the items
 * being processed at the same time, which is also how far ahead of the consumer items are drawn from the source; an
 * uncapped fork opens a run for every item the source delivers instead, trading that ceiling for a throughput bounded
 * only by the source.
 *
 * This is the concurrency knob for the work a task performs: {@link flat} draws the feeds a task opens one at a time
 * and {@link join} opens every one of them at once, so neither caps the work in flight and the window belongs around
 * the task instead. Runs are interleaved on the event loop rather than executed in parallel, so a task blocking it
 * gains nothing from being forked.
 *
 * > [!WARNING]
 * >
 * > - **Incremental**: results are emitted as the runs settle, so the reported feed runs dry as the feed drawn from
 * >   and `task` do.
 * > - **Materialising**: an uncapped fork holds a run for every item drawn, so a source delivering faster than the
 * >   runs settle may exhaust memory; cap the items in flight with `runs`.
 * > - **Stateless**: the fork carries no state across runs, whatever `task` carries within each.
 *
 * > [!WARNING]
 * >
 * > Output order is not preserved: results interleave and overtake each other according to how long every item takes.
 *
 * > [!CAUTION]
 * >
 * > `runs` caps the items in flight, not the rate at which they are drawn: a source that never runs dry keeps every
 * > run permanently busy, so pacing belongs inside `task`.
 *
 * > [!CAUTION]
 * >
 * > A stateful task never sees the whole feed. `task` is invoked once per run, so state it initialises on invocation
 * > is scoped to that run and decides on the items that run happens to draw: a quota is granted to each run, a
 * > deduplication spans one alone, an ordering covers only what that run drew. State captured in the enclosing
 * > closure is shared across the runs instead, and reached concurrently.
 * >
 * > Fork a stateful task only where its outcome is sound on part of the items. Where the state belongs to one item
 * > rather than to the feed, open a pipe per item with {@link map} and collapse the pipes with {@link join}, as
 * > `join(map(…))`, which keeps the state scoped to its item while still drawing from every one of them at once.
 *
 * > [!NOTE]
 * >
 * > Runs failing while the consumer is idle report their error when the feed is next advanced, rather than escaping as
 * > unhandled rejections.
 *
 * > [!NOTE]
 * >
 * > Every run and the source are closed when the feed is exhausted, fails or is closed early, waiting for the work
 * > already in flight to settle first, so a source idling between items delays it; failures reported while closing are
 * > suppressed.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of items reported by `task`
 *
 * @param runs The number of concurrent runs; 0 opens a run on demand, capping none, while values below 0 are treated
 *   as 1, that is, as sequential processing
 * @param task The task each run applies to the items it draws from the feed
 *
 * @returns A task yielding the items `task` reports for every run, as they become available
 *
 * @throws {@link !TypeError TypeError} If `runs` is not an integer
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items(urls))
 *   (fork(4, retrieve()))
 *   (toArray())
 * );  // 4 retrievals in flight, results as they land
 * ```
 *
 * @see {@link join} to interleave the feeds carried by a feed, with `fork()` applied within {@link map} bounding the
 *   work carried out inside each
 * @see {@link flat} to splice those feeds in source order instead
 */
export function fork<V, R>(runs: number, task: Task<V, R>): Task<V, R> {

	const count = Math.max(1, assert(runs, Number.isInteger, value => // values below 1 are sequential
		`expected integer runs <${value}>`
	));

	const capped = runs !== 0; // 0 opens a run on demand rather than a fixed set

	return source => items((async function* () {

		type Items = AsyncIterator<R, void, undefined>; // one run, drawn for the items it reports

		type Event =
			| { kind: "hire" } // a run was opened while the loop was already racing, carrying nothing to report
			| { kind: "item", iterator: Items, result: IteratorResult<R, void> };


		// the single reader of the source: wrapping it in a generator serialises the draws issued by the runs

		const reader = (async function* () { yield* source; })();

		// one cursor, seen by every run as its own; without return() a run closing early leaves the source open

		const shared: AsyncIterableIterator<V> = {

			next: async () => {

				const result = await reader.next();

				// a capped fork opened all its runs upfront, and a closing one takes on no further ones

				if ( capped || result.done || !hiring.open ) {

					return result;

				} else { // an uncapped one hires another run for every item delivered

					const iterator = run();

					polls.set(iterator, item(iterator));

					hiring.notify();

					return result;

				}

			},

			[Symbol.asyncIterator]: () => shared

		};

		// wakes the loop, so that a run hired mid-race joins it at once rather than after one of the others reports;
		// closed before the runs are, so that a draw settling meanwhile doesn't hire one past the set being closed

		const hiring = {

			open: true,

			notify: () => {},

			wake(): Promise<Event> {
				return new Promise(resolve => { hiring.notify = () => resolve({ kind: "hire" }); });
			}

		};

		// every run is opened before any starts drawing, so a task failing to open one leaves nothing to clean up

		const polls = new Map<Items, Promise<Event>>(Array
			.from({ length: count }, run)
			.map<[Items, Promise<Event>]>(iterator => [iterator, item(iterator)])
		);


		try {

			while ( polls.size > 0 ) {

				yield* process(await Promise.race(capped ? [...polls.values()] : [...polls.values(), hiring.wake()]));

			}

		} finally { // on failure or early termination as well

			await close();

		}


		async function* process(event: Event): AsyncGenerator<R, void, undefined> {

			if ( event.kind === "hire" ) { // a run joined the race, with nothing to report until it draws

			} else if ( event.result.done ) { // a run ran dry: drop it from the race

				polls.delete(event.iterator);

			} else { // put the run back to work before yielding, so it keeps drawing meanwhile

				polls.set(event.iterator, item(event.iterator));

				yield event.result.value;

			}

		}

		async function close(): Promise<void> {

			hiring.open = false; // no run is hired past this point, so the set closed below is the whole of it

			await Promise.allSettled([
				...Array.from(polls.keys(), async iterator => iterator.return?.()), // close the runs and
				...polls.values() // suppress late failures from their abandoned polls
			]);

			await Promise.allSettled([reader.return()]); // close the source, now that no run is drawing from it

		}


		function run(): Items { // one run of the task, drawing from the shared cursor

			return task(items(shared))[Symbol.asyncIterator]();

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
