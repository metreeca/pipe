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

import { reference } from "@metreeca/blue/reference";
import { id, multiple, optional, required, resource } from "@metreeca/blue/resource";
import { string } from "@metreeca/blue/string";
import type { Frame, Match } from "@metreeca/blue/value";
import type { Feed } from "@metreeca/flow";
import { toArray } from "@metreeca/flow/sinks";
import { bind, executor } from "@metreeca/gear";
import type { Store } from "@metreeca/keep";
import { describe, expect, expectTypeOf, it } from "vitest";
import { createStore } from "./index.js";
import { lookup } from "./lookup.js";


const Item = resource({ id: id(), label: required(string()) });
const Note = resource({ text: required(string()) });

const Catalogue = resource({

	id: id(),
	label: optional(string()),

	owner: optional(reference(Item)),
	members: multiple(reference(Item)),
	notes: multiple(Note),
	tags: multiple(string())

});


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

describe("lookup()", () => {

	const entry = "/catalogues/1";

	function stored(state: undefined | object, requests: unknown[] = []) {
		return executor(bind(createStore, () => ({

			lookup: async (request: unknown) => {

				requests.push(request);

				return state;

			}

		}) as unknown as Store)); // ;(cast) test mock implementing only the method under test
	}


	describe("with a collection model", () => {

		it("returns the referenced items one by one", async () => {

			const state = { members: [{ label: "First" }, { label: "Second" }] };

			expect(await stored(state)(async () =>
				lookup({ entry, shape: Catalogue, model: { members: { label: {} } } })(toArray())
			)).toEqual([{ label: "First" }, { label: "Second" }]);

		});

		it("returns the embedded items one by one", async () => {

			const state = { notes: [{ text: "First" }, { text: "Second" }] };

			expect(await stored(state)(async () =>
				lookup({ entry, shape: Catalogue, model: { notes: { text: {} } } })(toArray())
			)).toEqual([{ text: "First" }, { text: "Second" }]);

		});

		it("returns an empty feed if the collection holds no items", async () => {

			expect(await stored({ members: [] })(async () =>
				lookup({ entry, shape: Catalogue, model: { members: { label: {} } } })(toArray())
			)).toEqual([]);

		});

		it("returns an empty feed if the store does not hold the resource", async () => {

			expect(await stored(undefined)(async () =>
				lookup({ entry, shape: Catalogue, model: { members: { label: {} } } })(toArray())
			)).toEqual([]);

		});

		it("is typed as a frame of the slice", async () => {

			await stored(undefined)(async () =>
				expectTypeOf(lookup({ entry, shape: Catalogue, model: { members: { label: {} } } }))
					.toEqualTypeOf<Feed<Frame<typeof Catalogue, { members: { label: {} } }>>>()
			);

		});

	});

	describe("with a resource model", () => {

		it("returns the resource once", async () => {

			const state = { label: "Catalogue" };

			expect(await stored(state)(async () =>
				lookup({ entry, shape: Catalogue, model: { label: {} } })(toArray())
			)).toEqual([state]);

		});

		it.each([

			["a further member alongside a collection", { id: entry, members: [{ label: "First" }] }, { id: {}, members: { label: {} } }],
			["a single property holding one value", { owner: { label: "Owner" } }, { owner: { label: {} } }],
			["a single property holding plain values", { tags: ["first", "second"] }, { tags: {} }]

		] as const)("returns the resource for %s", async (_, state, model) => {

			expect(await stored(state)(async () =>
				lookup({ entry, shape: Catalogue, model })(toArray())
			)).toEqual([state]);

		});

		it("returns an empty feed if the store does not hold the resource", async () => {

			expect(await stored(undefined)(async () =>
				lookup({ entry, shape: Catalogue, model: { label: {}, members: {} } })(toArray())
			)).toEqual([]);

		});

		it("is typed as a match of the model", async () => {

			await stored(undefined)(async () =>
				expectTypeOf(lookup({ entry, shape: Catalogue, model: { label: {}, members: { label: {} } } }))
					.toEqualTypeOf<Feed<Match<typeof Catalogue, { label: {}, members: { label: {} } }>>>()
			);

		});

	});

	describe("store", () => {

		it("reads the resource from the bound store", async () => {

			const requests: unknown[] = [];
			const request = { entry, shape: Catalogue, model: { label: {} } };

			await stored(undefined, requests)(async () => lookup(request)(toArray()));

			expect(requests).toEqual([request]);

		});

		it("throws if no store is bound", async () => {

			await expect(executor()(async () =>
				lookup({ entry, shape: Catalogue, model: { label: {} } })
			)).rejects.toThrow(Error);

		});

	});

});
