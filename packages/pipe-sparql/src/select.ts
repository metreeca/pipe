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

import { clip, tidy } from "@metreeca/core/strings";
import type { Task } from "@metreeca/flow";
import { flat, map } from "@metreeca/flow/tasks";
import { service } from "@metreeca/gear";
import { log, report, time } from "@metreeca/tape";
import type { Repository, SPARQL, Tuple } from "@metreeca/wire-sparql";
import { createRepository } from "./index.js";


const logger = log(import.meta.url);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a task running a feed of SPARQL `SELECT` queries against a SPARQL repository.
 *
 * The task emits the tuples matched by each query as a single feed, in query order. Every query runs on its own,
 * outside any transaction, so successive queries may observe different states of a repository that is updated by other
 * clients or by other steps of the same pipeline while the feed is being consumed.
 *
 * @param repository The target repository; defaults to the {@link createRepository} service
 *
 * @returns A task converting a feed of `SELECT` queries into the feed of tuples they match
 *
 * @throws {@link !Error Error} If `repository` is omitted, and either no execution is running or the running one binds
 *                              no repository to {@link createRepository}, as {@link service} defines
 *
 * @throws {@link !Error Error} While the feed is consumed, whatever the source reports while producing queries, or
 *                              whatever the repository reports while running them
 *
 * @group Factories
 */
export function select(repository?: Repository): Task<SPARQL, Tuple> {

	const $repository = repository ?? service(createRepository);

	return flat(map(query => {

		logger.info`select <${clip(tidy(query), 50)}>`;

		return time(() => $repository.select(query), (tuples, elapsed) => {
			logger.info`selected <${report(tuples.length)}> tuples in <${report(elapsed)}> ms`;
		});

	}));

}
