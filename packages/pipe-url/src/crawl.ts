/*
 * Copyright © 2026 Metreeca srl
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

import type { Optional, URLLike } from "@metreeca/core";
import type { Awaitable, Awaitables } from "@metreeca/core/async";
import type { Task } from "@metreeca/flow";
import { items } from "@metreeca/flow/feeds";


/**
 * Possibly asynchronous, possibly absent value.
 *
 * Absence and asynchrony are taken uniformly, so that a provider hands over whatever it already holds, a value, a
 * promise or nothing at all.
 *
 * @typeParam T The type of the supplied value
 */
export type Source<T> =
	Awaitable<Optional<T>>;


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * Creates a URL graph walker.
 *
 * The generated task converts a feed of seed URLs into a feed of the URLs reachable from them, so that a consumer
 * works on a whole graph of URLs while stating no more than the step from one URL to the next. URLs are emitted
 * breadth-first in level order, every seed first, then every URL one step away from a seed, and so on, so that the
 * first arrival at a URL is also its shallowest one.
 *
 * The walker navigates a graph of URLs without retrieving the resources they identify. Any retrieval needed to find
 * the links of a URL is up to `walker`, and deriving results from the crawled URLs is up to the tasks downstream; the
 * harvester form takes retrieval and derivation over when a crawl must read each resource anyway. Seeds and links are
 * stated as {@link URLLike} values, strings and {@link !URL URL} objects alike, but reach `walker` and the feed as
 * parsed objects. Each object is a fresh copy, safe to be altered.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: seeds are emitted as they are pulled, and reachable URLs level by level, so the feed produced
 * >   runs dry when the source and `walker` do. No URL reachable from a seed is emitted until the source runs dry,
 * >   so the feed never completes on an endless source.
 * > - **Materialising**: every crawled URL is retained for the whole lifetime of the feed, as are the seeds and the
 * >   level being crawled, so an unbounded or widely branching graph may exhaust memory.
 * > - **Stateful**: the URLs already crawled decide the ones that follow, so a task invoked per nested feed or per
 * >   run crawls each independently, reaching a URL once per invocation rather than once for the feed as a whole.
 *
 * > [!IMPORTANT]
 * >
 * > URLs are crawled at most once across the whole feed, whatever seed they are reached from, so cyclic and
 * > converging graphs are crawled without duplicates and without looping. URLs are matched in canonical form: an
 * > omitted path or an uppercase host doesn't make a URL distinct, while a trailing slash or a fragment does.
 *
 * @param walker The function stating the URLs linked from a URL, none if it is a leaf
 *
 * @returns A task converting a feed of seed URLs into a feed of the seeds and the URLs reachable from them, each as a
 *          parsed object
 *
 * @throws {@link !Error Error} While the feed is consumed, whatever the source reports while producing seeds, or
 *                              whatever `walker` reports while stating the URLs linked from a URL
 *
 * @throws {@link !TypeError TypeError} While the feed is consumed, if a seed or a link cannot be parsed on its
 *                                      own, a relative reference among them
 *
 * @example
 *
 * ```typescript
 * const pages: Record<string, string[]> = { "/a": ["/b", "/c"], "/b": ["/d"], "/c": ["/d"], "/d": [] };
 *
 * await pipe(
 *   (items(["https://example.com/a"]))
 *   (crawl(url => pages[url.pathname]?.map(path => new URL(path, url))))
 *   (toArray())
 * );  // the URLs of /a, /b, /c and /d, in that order
 * ```
 *
 * @group Factories
 */
export function crawl(
	walker: (url: URL) => Source<Awaitables<URLLike>>
): Task<URLLike, URL>;

/**
 * Creates a URL graph harvester.
 *
 * The generated task converts a feed of seed URLs into a feed of results derived from the resources the crawled URLs
 * identify, so that a consumer harvests a whole graph of URLs while stating the retrieval of a URL as a task of its
 * own. Each crawled URL is retrieved once, however many links converge on it. The value retrieved for it serves both
 * to find its links and to derive its results, so no resource is read twice. Results are emitted in level order: the
 * results of every seed first, then those of every URL one step away from a seed, and so on.
 *
 * Retrieval is stated as a task over a whole level rather than as a step per URL, so the consumer controls how many
 * URLs are retrieved at a time with the tasks already at hand: a forked `feeder` retrieves several at once, an
 * unforked one retrieves them in turn. A URL is left out of the harvest by emitting nothing for it. Seeds and links
 * are stated as {@link URLLike} values, strings and {@link !URL URL} objects alike, but reach `feeder` as parsed
 * objects. Each object is a fresh copy, safe to be altered.
 *
 * > [!NOTE]
 * >
 * > - **Incremental**: the results of the seeds are emitted as `feeder` draws them, and those of reachable URLs level
 * >   by level, so the feed produced runs dry when the source, `feeder`, `walker` and `mapper` do. No URL reachable
 * >   from a seed is fed until the source runs dry, so the feed never completes on an endless source.
 * > - **Materialising**: every crawled URL is retained for the whole lifetime of the feed, as for the walker form.
 * >   The value retrieved for a URL is released as soon as it is walked and mapped, so it is never retained across
 * >   levels.
 * > - **Stateful**: the URLs already crawled decide the ones that follow, as for the walker form.
 *
 * > [!IMPORTANT]
 * >
 * > URLs are crawled at most once across the whole feed, whatever seed they are reached from, and matched in
 * > canonical form, as for the walker form.
 *
 * > [!IMPORTANT]
 * >
 * > `feeder` is invoked once per level, so any state it initialises on invocation lasts for that level only. State
 * > spanning the whole crawl belongs to the enclosing closure. Levels never mix, but order within a level is up to
 * > `feeder`: a feeder retrieving several URLs at a time harvests a level in completion order.
 *
 * @typeParam V The type of the value retrieved for a crawled URL
 * @typeParam R The type of the results derived from a crawled URL
 *
 * @param feeder The task retrieving a value for each URL of a level, emitting nothing for a URL to be crawled no
 *               further and to contribute no result
 * @param walker The function stating the URLs linked from the value retrieved for a URL, none if it is a leaf
 * @param mapper The function stating the results derived from the value retrieved for a URL, either a single result
 *               or a sequence of them, none if it contributes no result
 *
 * @returns A task converting a feed of seed URLs into a feed of the results derived from every crawled URL
 *
 * @throws {@link !Error Error} While the feed is consumed, whatever the source reports while producing seeds, or
 *                              whatever `feeder`, `walker` and `mapper` report while retrieving, walking and mapping
 *                              a URL
 *
 * @throws {@link !TypeError TypeError} While the feed is consumed, if a seed or a link cannot be parsed on its
 *                                      own, a relative reference among them
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items(["https://example.com/products/"]))
 *   (crawl(
 *     fork(4, urls => urls(fetch())(html())(xpath())), // the index pages, four retrievals at a time
 *     page => page("//nav//a/@href").map(link), // the index pages it paginates to
 *     page => page("//article//a/@href").map(link) // the item links it lists
 *   ))
 *   (toArray())
 * );  // the item links of every index page
 * ```
 *
 * @group Factories
 */
export function crawl<V, R>(
	feeder: Task<URL, V>,
	walker: (data: V) => Source<Awaitables<URLLike>>,
	mapper: (data: V) => Source<R | Awaitables<R>>
): Task<URLLike, R>;

/**
 * Creates a URL graph walker or harvester.
 */
export function crawl<V, R>(...steps:
	| [
		walker: (url: URL) => Source<Awaitables<URLLike>>
	]
	| [
		feeder: Task<URL, V>,
		walker: (data: V) => Source<Awaitables<URLLike>>,
		mapper: (data: V) => Source<R | Awaitables<R>>
	]
): Task<URLLike, URL | R> {

	return steps.length === 1
		? roam(steps[0])
		: reap(steps[0], steps[1], steps[2]);


	function roam(walker: (url: URL) => Source<Awaitables<URLLike>>): Task<URLLike, URL> {

		return source => items((async function* () {

			const admitted = admitting();

			// the seed level, drained before descending so that the first arrival at a URL is its shallowest;
			// seeds are emitted as they are pulled, so a slow source doesn't withhold the ones already in

			const seeds: URL[] = [];

			for await (const url of admitted(source)) {

				seeds.push(url);

				yield url;

			}

			yield* descending(seeds, reach);


			async function* reach(frontier: readonly URL[], reached: URL[]): AsyncIterable<URL> {

				for (const url of frontier) {

					for await (const next of admitted(await walker(url))) {

						reached.push(next);

						yield next;

					}

				}

			}

		})());

	}

	function reap<V, R>(
		feeder: Task<URL, V>,
		walker: (data: V) => Source<Awaitables<URLLike>>,
		mapper: (data: V) => Source<R | Awaitables<R>>
	): Task<URLLike, R> {

		return source => items((async function* () {

			const admitted = admitting();

			// the URLs linked from the seed level, buffered until the source runs dry so that the first arrival at a
			// URL is its shallowest; seeds are fed as `feeder` draws them, so a slow source doesn't withhold the
			// ones already in

			const linked: URL[] = [];

			yield* reach(admitted(source), linked);

			yield* descending(linked, reach);


			async function* reach(frontier: Awaitables<URL>, reached: URL[]): AsyncIterable<R> {

				for await (const data of feeder(items(frontier))) {

					yield* items<R>(await mapper(data) ?? []);

					for await (const next of admitted(await walker(data))) {
						reached.push(next);
					}

				}

			}

		})());

	}


	function admitting(): (links: Optional<Awaitables<URLLike>>) => AsyncIterable<URL> {

		const crawled = new Set<string>();

		return async function* (links) {

			for await (const link of items(links ?? [])) {

				const url = new URL(link);

				if ( !crawled.has(url.href) ) {

					crawled.add(url.href);

					yield url;

				}

			}

		};

	}

	async function* descending<R>(
		seeds: readonly URL[],
		reach: (frontier: readonly URL[], reached: URL[]) => AsyncIterable<R>
	): AsyncIterable<R> {

		let frontier: readonly URL[] = seeds;

		while ( frontier.length > 0 ) {

			const reached: URL[] = [];

			yield* reach(frontier, reached);

			frontier = reached;

		}

	}

}
