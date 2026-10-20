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

import type { IRI } from "@metreeca/core/resource";
import { pipe, type Sink } from "@metreeca/flow";
import { items } from "@metreeca/flow/feeds";
import { reduce, toArray } from "@metreeca/flow/sinks";
import { flat } from "@metreeca/flow/tasks";
import { service } from "@metreeca/gear";
import { log, report, time } from "@metreeca/tape";
import type { Graph } from "@metreeca/trio";
import type { Repository, RepositoryClient } from "@metreeca/wire-sparql";
import { createRepository } from "./index.js";


const logger = log(import.meta.url);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a sink storing a feed of RDF graphs into a SPARQL repository.
 *
 * The sink adds the statements of every graph to the target graph, then resolves to the total number of statements it
 * stored. With `clear` set, the target graph is emptied first, so that after the upload it holds exactly the statements
 * in the feed; an empty feed then leaves the target graph empty rather than untouched.
 *
 * By default the upload is atomic: the sink collects the whole feed in memory, then stores it, clearing included, in a
 * single repository transaction. On repositories supporting transactions, a failure leaves the target graph as it was
 * before the upload; the transaction stays open only while statements are written, however slowly the feed is produced.
 *
 * With `batch` set, the sink stores each graph as soon as it arrives, in its own request and outside any transaction.
 * Memory use stays bounded on feeds of any length, but a failure leaves the graphs stored so far in place, and an
 * overwriting upload may leave the target graph cleared or only partially loaded.
 *
 * @param options The upload options
 * @param options.clear Whether to overwrite the target graph, removing its existing statements before the upload;
 *     defaults to `false`, adding the uploaded statements to the ones the target graph already holds
 * @param options.batch Whether to store each graph as it arrives, trading atomicity for bounded memory use;
 *     defaults to `false`, storing the whole feed in a single repository transaction
 * @param options.repository The target repository; defaults to the {@link createRepository} service
 * @param options.context The target named graph; defaults to the default graph
 *
 * @returns A sink storing each graph in the feed into the target graph and resolving to the number of stored statements
 *
 * @throws {@link !Error Error} If `options.repository` is omitted, and either no execution is running or the running
 *                              one binds no repository to {@link createRepository}, as {@link service} defines
 *
 * @throws {@link !Error Error} While the feed is consumed, whatever the source reports while producing graphs, or
 *                              whatever the repository reports while storing them
 *
 * @group Factories
 */
export function upload({

	clear = false,
	batch = false,

	repository,
	context

}: {

	readonly clear?: boolean;
	readonly batch?: boolean;

	readonly repository?: Repository;
	readonly context?: IRI;

} = {}): Sink<Graph, number> {

	const $repository = repository ?? service(createRepository);
	const $context = context ?? "default graph";

	return feed => time(async () => {

		if ( batch ) { // an undefined count means nothing was written yet, so an overwrite stores the first graph

			const count = await pipe(
				items(feed)
				(reduce<Graph, undefined | number>((count, statements) =>
					write($repository, statements, count), undefined
				))
			);

			return count ?? (clear ? store($repository, []) : 0);

		} else { // drain the feed outside the transaction, so a slow feed never holds it open

			const statements = await pipe(
				(feed)
				(flat())
				(toArray())
			);

			return $repository.execute(async repository => clear
				? store(repository, statements)
				: merge(repository, statements)
			);

		}

	}, (count, elapsed) => {

		logger.info`uploaded <${report(count)}> statements to <${$context}> in <${report(elapsed)}> ms`;

	});


	async function write(repository: RepositoryClient, statements: Graph, count: undefined | number): Promise<number> {

		return (count ?? 0)+await (clear && count === undefined ? store : merge)(repository, statements);

	}

	async function merge(repository: RepositoryClient, statements: Graph): Promise<number> {

		await repository.merge(statements, context);

		return statements.length;

	}

	async function store(repository: RepositoryClient, statements: Graph): Promise<number> {

		await repository.store(statements, context);

		return statements.length;

	}

}
