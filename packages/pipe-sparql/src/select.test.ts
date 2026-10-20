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
import { toArray } from "@metreeca/flow/sinks";
import { bind, executor } from "@metreeca/gear";
import { named } from "@metreeca/trio";
import { type Repository, type SPARQL, type Tuple, tuple } from "@metreeca/wire-sparql";
import { describe, expect, it } from "vitest";
import { createRepository } from "./index.js";
import { select } from "./select.js";


const x = tuple({ "?s": named("https://example.com/x") });
const y = tuple({ "?s": named("https://example.com/y") });
const z = tuple({ "?s": named("https://example.com/z") });


/**
 * Creates a repository answering each `SELECT` query with the tuples mapped to it, and rejecting the query mapped to
 * an error.
 */
function repository(answers: Readonly<Record<SPARQL, readonly Tuple[] | Error>> = {}): Repository {
	return {
		ask: async () => false,
		select: async query => {
			const answer = answers[query] ?? [];
			return answer instanceof Error ? Promise.reject(answer) : answer;
		},
		construct: async () => [],
		update: async () => {},
		merge: async () => {},
		store: async () => {},
		execute: async () => { throw new Error("unexpected transaction"); },
		close: async () => {}
	};
}


describe("select()", () => {

	describe("querying", () => {

		it("emits the tuples matched by each query as a single feed, in query order", async () => {

			const target = repository({ "select x": [x], "select yz": [y, z] });

			const tuples = await pipe(
				(items(["select yz", "select x"]))
				(select(target))
				(toArray())
			);

			expect(tuples).toEqual([y, z, x]);

		});

		it("emits nothing for a query matching no tuples", async () => {

			const tuples = await pipe(
				(items(["select nothing"]))
				(select(repository()))
				(toArray())
			);

			expect(tuples).toEqual([]);

		});

		it("emits nothing for an empty feed", async () => {

			const tuples = await pipe(
				(items<SPARQL>([]))
				(select(repository()))
				(toArray())
			);

			expect(tuples).toEqual([]);

		});

		it("rejects with the error of a failing query", async () => {

			const error = new Error("failing query");

			await expect(pipe(
				(items(["select x", "select failing"]))
				(select(repository({ "select x": [x], "select failing": error })))
				(toArray())
			)).rejects.toBe(error);

		});

	});

	describe("repository", () => {

		it("targets the bound repository when given none", async () => {

			const target = repository({ "select x": [x] });

			const tuples = await executor(bind(createRepository, () => target))(() => pipe(
				(items(["select x"]))
				(select())
				(toArray())
			));

			expect(tuples).toEqual([x]);

		});

		it("throws on creation when given none and none is bound", async () => {

			await expect(executor()(async () => select())).rejects.toThrow(Error);

		});

	});

});
