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

import type { ResourceShape } from "@metreeca/blue/resource";
import type { Draft } from "@metreeca/blue/value";
import type { Lazy } from "@metreeca/core";
import { immutable } from "@metreeca/core/values";
import type { Sink } from "@metreeca/flow";
import { reduce } from "@metreeca/flow/sinks";
import { service } from "@metreeca/gear";
import type { StoreClient } from "@metreeca/keep";
import type { Reference } from "@metreeca/qest/state";
import { log, report, time } from "@metreeca/tape";
import { createStore } from "./index.js";


const logger = log(import.meta.url);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Target state of a single resource.
 *
 * A pipeline produces one delta for each resource it handles: either the state to store, or `null` to remove the
 * resource. Each delta also names the shape of its resource, so a single feed can carry resources of different kinds.
 * Pipelines that add resources and pipelines that retire them can thus all end on the same {@link settle} sink.
 *
 * @typeParam S The shape of the resource, against which the state is checked
 */
export type Delta<S extends Lazy<ResourceShape> = Lazy<ResourceShape>> = readonly [

	/**
	 * The resource concerned.
	 */
	entry: Reference,

	/**
	 * The shape of the resource.
	 */
	shape: S,

	/**
	 * The state to store for the resource, or `null` to remove it.
	 */
	state: null | Draft<S>

]

/**
 * Counts of the deltas a {@link settle} run handled, by outcome.
 *
 * Each delta is counted exactly once, so the three counts add up to the number of deltas in the feed. A run completed
 * without any failed write if, and only if, its tally holds a `reject` count of zero.
 */
export interface Tally {

	/**
	 * The number of resources stored, whether new or replacing a previous state.
	 */
	readonly insert: number;

	/**
	 * The number of resources removed, including those the store did not hold.
	 */
	readonly remove: number;

	/**
	 * The number of resources left unchanged because their write failed.
	 */
	readonly reject: number;

}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a sink that writes the deltas of a pipeline to the store.
 *
 * By default, each delta is committed on its own. If a run is interrupted, the resources already written stay in the
 * store and the next run can carry on from there. A failed write is logged and counted as rejected: the resource keeps
 * its previous state and the feed keeps running. A single failure costs only its own resource.
 *
 * A state rejected as invalid is handled differently, as it signals a broken run rather than a bad resource. If the
 * states do not satisfy their shape, every write fails, and the run would still look clean while storing nothing. The
 * error is therefore propagated, so the run fails visibly and can be retried once the states are fixed.
 *
 * With `atomic`, the whole feed is committed in a single transaction instead: if any write fails, the run fails and
 * nothing is committed. This suits sources that cannot tolerate a partially applied run.
 *
 * The deltas are written to the store bound to the current execution.
 *
 * @param options The options governing how the deltas are committed
 * @param options.atomic Whether to commit all the deltas in a single transaction; defaults to `false`
 *
 * @returns A sink that writes a feed of deltas and resolves to a {@link Tally} of the changes made
 *
 * @throws {@link !Error Error} If no store is bound to the current execution
 *
 * @throws {@link !RangeError RangeError} While the feed is consumed, if the store rejects a state as invalid
 *
 * @throws {@link !Error Error} While the feed is consumed, with `atomic`, if any write fails; nothing is committed
 */
export function settle({

	atomic = false

}: {

	readonly atomic?: boolean

} = {}): Sink<Delta, Tally> {

	const store = service(createStore);


	return async feed => time(async () => immutable(atomic
		? await store.execute(client => feed(collect(client)))
		: await feed(collect(store))
	), ({ insert, remove, reject }, elapsed) => {

		const total = insert+remove+reject;

		const resources = total === 1 ? "resource" : "resources";
		const counts = `insert <${report(insert)}> | remove <${report(remove)}> | reject <${report(reject)}>`;

		logger.info`persisted <${report(total)}> ${resources} in <${report(elapsed)}> ms | ${counts}`;

	});


	function collect(client: StoreClient): Sink<Delta, Tally> {
		return reduce(async (tally, [entry, shape, state]) => {

			try {

				if ( state === null ) {

					await client.remove({ entry, shape });

					logger.info`removed resource <${entry}>`;

					return { ...tally, remove: tally.remove+1 };

				} else {

					await client.insert({ entry, shape, state });

					logger.info`inserted resource <${entry}>`;

					return { ...tally, insert: tally.insert+1 };

				}

			} catch ( error ) {

				// an atomic run must fail as a whole, and an invalid state would fail every write anyway, so both
				// end the run; any other failure costs only its own resource and leaves the feed running

				if ( atomic || error instanceof RangeError ) {

					throw error;

				} else {

					logger.warning`unable to mutate resource <${entry}> (${report(error)})`;

					return { ...tally, reject: tally.reject+1 };

				}

			}

		}, {

			insert: 0,
			remove: 0,
			reject: 0

		});
	}

}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a delta for a resource.
 *
 * A `null` state requests the removal of the resource.
 *
 * @typeParam S The shape of the resource, against which the state is checked
 *
 * @param entry The resource concerned
 * @param shape The shape of the resource
 * @param state The state to store for the resource, or `null` to remove it
 *
 * @returns An immutable {@link Delta} combining `entry`, `shape` and `state`
 */
export function delta<S extends Lazy<ResourceShape>>(entry: Reference, shape: S, state: null | Draft<S>): Delta<S> {

	return Object.freeze([entry, shape, state]);

}
