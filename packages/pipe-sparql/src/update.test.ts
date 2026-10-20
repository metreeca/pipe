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

import { pipe } from "@metreeca/flow";
import { items } from "@metreeca/flow/feeds";
import { bind, executor } from "@metreeca/gear";
import type { Repository, SPARQL } from "@metreeca/wire-sparql";
import { describe, expect, it } from "vitest";
import { createRepository } from "./index.js";
import { update } from "./update.js";


/**
 * Creates a repository recording the update requests it applies, rejecting the `failing` one with `error`.
 */
function repository(failing?: SPARQL, error?: Error): Repository & { readonly applied: readonly SPARQL[] } {

	const applied: SPARQL[] = [];

	return {
		applied,
		ask: async () => false,
		select: async () => [],
		construct: async () => [],
		update: async request => request === failing
			? Promise.reject(error)
			: void applied.push(request), // recording an applied request has no functional equivalent
		merge: async () => {},
		store: async () => {},
		execute: async () => { throw new Error("unexpected transaction"); },
		close: async () => {}
	};
}


describe("update()", () => {

	describe("updating", () => {

		it("applies each request in feed order, resolving to the number of applied requests", async () => {

			const target = repository();

			const count = await pipe(
				(items(["update 1", "update 2", "update 3"]))
				(update(target))
			);

			expect(target.applied).toEqual(["update 1", "update 2", "update 3"]);
			expect(count).toBe(3);

		});

		it("resolves to zero for an empty feed, applying nothing", async () => {

			const target = repository();

			const count = await pipe(
				(items<SPARQL>([]))
				(update(target))
			);

			expect(target.applied).toEqual([]);
			expect(count).toBe(0);

		});

		it("rejects with the error of a failing request, keeping the requests applied before it", async () => {

			const error = new Error("failing request");
			const target = repository("update 2", error);

			await expect(pipe(
				(items(["update 1", "update 2", "update 3"]))
				(update(target))
			)).rejects.toBe(error);

			expect(target.applied).toEqual(["update 1"]);

		});

	});

	describe("repository", () => {

		it("targets the bound repository when given none", async () => {

			const target = repository();

			await executor(bind(createRepository, () => target))(() => pipe(
				(items(["update 1"]))
				(update())
			));

			expect(target.applied).toEqual(["update 1"]);

		});

		it("throws on creation when given none and none is bound", async () => {

			await expect(executor()(async () => update())).rejects.toThrow(Error);

		});

	});

});
