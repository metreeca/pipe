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

import { required, resource } from "@metreeca/blue/resource";
import { string } from "@metreeca/blue/string";
import { items } from "@metreeca/flow/feeds";
import { bind, executor } from "@metreeca/gear";
import type { Store, StoreClient } from "@metreeca/keep";
import { describe, expect, it } from "vitest";
import { createStore } from "./index.js";
import { delta, type Delta, settle } from "./settle.js";


const Item = resource({ label: required(string()) });


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

describe("settle()", () => {

	type Write = readonly [via: "store" | "transaction", operation: "insert" | "remove", entry: string];

	/**
	 * Binds a store recording the writes it receives and failing those on the `failing` entries with `failure`.
	 */
	function stored(writes: Write[], { failing = [], failure = new Error("failed write") }: {
		readonly failing?: readonly string[]
		readonly failure?: Error
	} = {}) {

		function client(via: Write[0]): StoreClient {

			async function write(operation: Write[1], entry: string) {

				writes.push([via, operation, entry]);

				if ( failing.includes(entry) ) {
					throw failure;
				}

			}

			return {

				insert: async ({ entry }: { entry: string }) => write("insert", entry),
				remove: async ({ entry }: { entry: string }) => write("remove", entry)

			} as unknown as StoreClient; // ;(cast) test mock implementing only the methods under test

		}

		return executor(bind(createStore, () => ({

			...client("store"),

			execute: async <V>(task: (store: StoreClient) => V) => task(client("transaction"))

		}) as unknown as Store)); // ;(cast) test mock implementing only the methods under test

	}

	const deltas: readonly Delta[] = [
		delta("/items/1", Item, { label: "First" }),
		delta("/items/2", Item, null),
		delta("/items/3", Item, { label: "Third" })
	];


	describe("writing", () => {

		it("inserts the deltas holding a state and removes those holding null", async () => {

			const writes: Write[] = [];

			await stored(writes)(async () => items(deltas)(settle()));

			expect(writes).toEqual([
				["store", "insert", "/items/1"],
				["store", "remove", "/items/2"],
				["store", "insert", "/items/3"]
			]);

		});

		it("resolves to a tally of the changes made", async () => {

			expect(await stored([])(async () => items(deltas)(settle())))
				.toEqual({ insert: 2, remove: 1, reject: 0 });

		});

		it("resolves to an immutable tally", async () => {

			expect(Object.isFrozen(await stored([])(async () => items(deltas)(settle())))).toBeTruthy();

		});

		it("resolves to an empty tally for an empty feed", async () => {

			expect(await stored([])(async () => items([])(settle())))
				.toEqual({ insert: 0, remove: 0, reject: 0 });

		});

	});

	describe("failing", () => {

		it("counts a failed write as rejected and keeps the feed running", async () => {

			const writes: Write[] = [];

			const tally = await stored(writes, { failing: ["/items/1", "/items/2"] })(async () =>
				items(deltas)(settle())
			);

			expect(tally).toEqual({ insert: 1, remove: 0, reject: 2 });
			expect(writes).toHaveLength(3);

		});

		it("propagates a state rejected as invalid", async () => {

			await expect(stored([], { failing: ["/items/1"], failure: new RangeError("invalid state") })(async () =>
				items(deltas)(settle())
			)).rejects.toThrow(RangeError);

		});

	});

	describe("atomic", () => {

		it("writes every delta within a single transaction", async () => {

			const writes: Write[] = [];

			await stored(writes)(async () => items(deltas)(settle({ atomic: true })));

			expect(writes).toEqual([
				["transaction", "insert", "/items/1"],
				["transaction", "remove", "/items/2"],
				["transaction", "insert", "/items/3"]
			]);

		});

		it("resolves to a tally of the changes made", async () => {

			expect(await stored([])(async () => items(deltas)(settle({ atomic: true }))))
				.toEqual({ insert: 2, remove: 1, reject: 0 });

		});

		it("fails the run on any failed write", async () => {

			await expect(stored([], { failing: ["/items/2"] })(async () =>
				items(deltas)(settle({ atomic: true }))
			)).rejects.toThrow(Error);

		});

	});

	describe("store", () => {

		it("throws if no store is bound", async () => {

			await expect(executor()(async () => settle())).rejects.toThrow(Error);

		});

	});

});

describe("delta()", () => {

	it("combines entry, shape and state", async () => {

		expect(delta("/items/1", Item, { label: "First" })).toEqual(["/items/1", Item, { label: "First" }]);

	});

	it("accepts a null state requesting removal", async () => {

		expect(delta("/items/1", Item, null)).toEqual(["/items/1", Item, null]);

	});

	it("is immutable", async () => {

		expect(Object.isFrozen(delta("/items/1", Item, null))).toBeTruthy();

	});

});
