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
import { eager, type Frame, items as frames, type Match, type Model, type Slice } from "@metreeca/blue/value";
import type { Lazy } from "@metreeca/core";
import type { Feed } from "@metreeca/flow";
import { items } from "@metreeca/flow/feeds";
import { flat, map, trim } from "@metreeca/flow/tasks";
import { service } from "@metreeca/gear";
import type { StoreLookup } from "@metreeca/keep";
import { log, report, time } from "@metreeca/tape";
import { createStore } from "./index.js";


const logger = log(import.meta.url);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Retrieves the items of a collection held by a resource in the store.
 *
 * The request names a resource and a {@link Slice | slice} of it. A slice is a model with a single entry: a property
 * holding any number of resources, together with the members to retrieve from each of them. The feed returns the
 * items one by one, each typed as a {@link Frame | frame} of the slice, rather than the resource holding them.
 *
 * A model naming any further member, such as `id`, retrieves the resource itself rather than the items it holds.
 *
 * If the store does not hold the resource, or the collection holds no items, the feed is empty.
 *
 * The resource is read from the store bound to the current execution.
 *
 * @typeParam S The shape of the resource holding the collection, possibly deferred to break definition cycles
 * @typeParam T The slice naming the collection and the members to retrieve from its items
 *
 * @param request The resource to retrieve and the slice to retrieve it with
 *
 * @returns A feed of the items of the collection, each restricted to the members the slice names
 *
 * @throws {@link !Error Error} If no store is bound to the current execution
 */
export function lookup<
	S extends Lazy<ResourceShape>,
	T extends Slice<S, T>
>(request: StoreLookup<S, T>): Feed<Frame<S, T>>;

/**
 * Retrieves a resource from the store.
 *
 * The request names a resource and a {@link Model | model} of the members to retrieve from it. The feed returns the
 * resource once, typed as a {@link Match | match} of the model and restricted to the members it names.
 *
 * A model naming a single collection of resources and nothing else is a {@link Slice | slice}: the feed then returns
 * the items of the collection, each typed as a {@link Frame | frame}, rather than the resource holding them.
 *
 * If the store does not hold the resource, the feed is empty.
 *
 * The resource is read from the store bound to the current execution.
 *
 * @typeParam S The shape of the resource, possibly deferred to break definition cycles
 * @typeParam T The model naming the members to retrieve from the resource
 *
 * @param request The resource to retrieve and the model to retrieve it with
 *
 * @returns A feed of the resource, restricted to the members the model names
 *
 * @throws {@link !Error Error} If no store is bound to the current execution
 */
export function lookup<
	S extends Lazy<ResourceShape>,
	T extends Model<S, T>
>(request: StoreLookup<S, T>): Feed<Match<S, T>>;

/**
 * Retrieves a resource, or the items of a collection it holds, from the store.
 */
export function lookup<
	S extends Lazy<ResourceShape>,
	T extends Model<S, T>
>(request: StoreLookup<S, T>): Feed<Frame<S, T & Slice<S, T>> | Match<S, T>> {

	const store = service(createStore);

	return isSlice(request)
		? retrieve(request)(flat(map(match => frames(match, request.model))))
		: retrieve(request);


	function isSlice(request: StoreLookup<S, T>): request is StoreLookup<S, T & Slice<S, T>> {

		const { shape, model } = request;

		const fields = Object.keys(model);
		const member = fields.length === 1 ? eager(shape).members[fields[0]] : undefined;
		const range = member?.kind === "property" ? member.range : undefined;
		const kind = range && range.maxCount !== 1 ? eager(range.shape).kind : undefined;

		return kind === "reference" || kind === "resource";

	}

	function retrieve<M extends Model<S, M>>(request: StoreLookup<S, M>) {
		return trim(items(time(() => store.lookup(request), (_, elapsed) => {
			logger.info`looked up resource <${request.entry}> in <${report(elapsed)}> ms`;
		})));
	}

}
