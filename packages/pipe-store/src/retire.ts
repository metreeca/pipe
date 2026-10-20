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

import { assert } from "@metreeca/core";
import { difference } from "@metreeca/core/arrays";
import type { IRI } from "@metreeca/core/resource";
import { type Feed, pipe, type Task } from "@metreeca/flow";
import { items } from "@metreeca/flow/feeds";
import { toSet } from "@metreeca/flow/sinks";
import { chain } from "@metreeca/flow/tasks";
import { log } from "@metreeca/tape";


const logger = log(import.meta.url);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a task that selects the resources to retire.
 *
 * The incoming feed is a scan of the source: it lists the identifiers of every resource the source still provides. The
 * `existing` feed lists the identifiers currently in the store. The task returns the existing identifiers that are
 * missing from the scan, each at most once. It returns nothing until the scan is complete, so a scan still in progress
 * never leads to the retirement of a resource that the rest of the scan would have listed.
 *
 * A scan that returns too few identifiers usually means a broken source rather than a source that was really emptied. A
 * scan is therefore refused if it lists fewer identifiers than the `survival` fraction of the existing ones. The check
 * counts every identifier in the scan, not only those already in the store. A source that renames all its resources at
 * once still passes the check, while a truncated or unreachable source is still caught.
 *
 * By default, a refused scan skips only the retirements and lets the other writes of the run go through. Once the
 * source works again, its next scan retires the same resources. With `atomic`, a refused scan fails the whole run
 * instead. This suits sources that cannot tolerate a partially applied run of the pipeline.
 *
 * A `survival` of `0` disables the check, so even an empty scan retires everything. A `survival` of `1` refuses any
 * scan that lists fewer identifiers than the store currently holds for the source.
 *
 * @param options The options governing what a single run may retire
 * @param options.atomic Whether a refused scan fails the whole run; defaults to `false`, skipping only the retirements
 * @param options.survival The minimum fraction of the existing identifiers a scan must list, in [0, 1], rounded up
 * @param options.existing A feed of the identifiers of the resources currently in the store
 *
 * @returns A task turning the identifiers listed by a scan into the existing identifiers to retire
 *
 * @throws {@link !TypeError TypeError} If `survival` is outside [0, 1]
 *
 * @throws {@link !RangeError RangeError} While the feed is consumed, with `atomic`, if a scan is refused
 */
export function retire({

	atomic = false,
	survival,

	existing

}: {

	readonly atomic?: boolean
	readonly survival: number

	readonly existing: Feed<IRI>

}): Task<IRI> {

	const $survival = assert(survival, value => value >= 0 && value <= 1,
		`expected survival rate in [0, 1]`
	);

	return chain(async incoming => {

		const [$existing, $incoming] = await Promise.all([
			existing(toSet()),
			pipe((items(incoming))(toSet()))
		]);

		const retired = difference([$existing, $incoming]);

		const current = $existing.size;
		const dropped = retired.length;

		const minimum = current*$survival;
		const scanned = $incoming.size; // every identifier the source still provides, renamed ones included

		if ( scanned < minimum ) { // a short scan more likely means a broken source than a really emptied one

			const action = atomic ? "prevented" : "skipping";
			const share = Math.round($survival*100);

			const message = `${action} mass retirement leaving less than <${share}%> of <${current}> current resources`;

			if ( atomic ) { // failing the whole run leaves the store as the last successful run left it

				throw new RangeError(message);

			} else { // skipping only the retirements lets the other writes go through

				logger.warn(message);

				return [];

			}

		} else {

			logger.info`retiring <${dropped}> of <${current}> current resources`;

			return retired;

		}

	});

}
