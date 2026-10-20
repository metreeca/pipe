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
import { pipe, type Sink } from "@metreeca/flow";
import { items } from "@metreeca/flow/feeds";
import { count } from "@metreeca/flow/sinks";
import { map } from "@metreeca/flow/tasks";
import { service } from "@metreeca/gear";
import { log, report, time } from "@metreeca/tape";
import type { Repository, SPARQL } from "@metreeca/wire-sparql";
import { createRepository } from "./index.js";


const logger = log(import.meta.url);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a sink applying a feed of SPARQL Update requests to a SPARQL repository.
 *
 * The sink applies the requests one at a time, in feed order, and resolves to their count. Every request runs on its
 * own, outside any transaction: a failing request rejects the sink, but the requests before it stay applied.
 *
 * @param repository The target repository; defaults to the {@link createRepository} service
 *
 * @returns A sink applying each request in the feed and resolving to the number of applied requests
 *
 * @throws {@link !Error Error} If `repository` is omitted, and either no execution is running or the running one binds
 *                              no repository to {@link createRepository}, as {@link service} defines
 *
 * @throws {@link !Error Error} While the feed is consumed, whatever the source reports while producing requests, or
 *                              whatever the repository reports while applying them
 *
 * @group Factories
 */
export function update(repository?: Repository): Sink<SPARQL, number> {

	const $repository = repository ?? service(createRepository);

	return feed => pipe(
		items(feed)
		(map(request => {

			logger.info`update <${clip(tidy(request), 50)}>`;

			return time(() => $repository.update(request), (_, elapsed) => {
				logger.info`updated in <${report(elapsed)}> ms`;
			});

		}))
		(count())
	);

}
