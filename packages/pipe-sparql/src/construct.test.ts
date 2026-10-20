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
import { type Graph, named, triple } from "@metreeca/trio";
import type { Repository, SPARQL } from "@metreeca/wire-sparql";
import { describe, expect, it } from "vitest";
import { construct } from "./construct.js";
import { createRepository } from "./index.js";


const x = triple(named("https://example.com/x"), named("https://example.com/p"), named("https://example.com/y"));
const y = triple(named("https://example.com/y"), named("https://example.com/p"), named("https://example.com/z"));


/**
 * Creates a repository answering each `CONSTRUCT` query with the graph mapped to it, and rejecting the query mapped to
 * an error.
 */
function repository(answers: Readonly<Record<SPARQL, Graph | Error>> = {}): Repository {
	return {
		ask: async () => false,
		select: async () => [],
		construct: async query => {
			const answer = answers[query] ?? [];
			return answer instanceof Error ? Promise.reject(answer) : answer;
		},
		update: async () => {},
		merge: async () => {},
		store: async () => {},
		execute: async () => { throw new Error("unexpected transaction"); },
		close: async () => {}
	};
}


describe("construct()", () => {

	describe("querying", () => {

		it("emits the graph built by each query, in query order", async () => {

			const target = repository({ "construct x": [x], "construct y": [y], "construct xy": [x, y] });

			const graphs = await pipe(
				(items(["construct y", "construct xy", "construct x"]))
				(construct(target))
				(toArray())
			);

			expect(graphs).toEqual([[y], [x, y], [x]]);

		});

		it("emits an empty graph for a query building no triples", async () => {

			const graphs = await pipe(
				(items(["construct nothing"]))
				(construct(repository()))
				(toArray())
			);

			expect(graphs).toEqual([[]]);

		});

		it("emits nothing for an empty feed", async () => {

			const graphs = await pipe(
				(items<SPARQL>([]))
				(construct(repository()))
				(toArray())
			);

			expect(graphs).toEqual([]);

		});

		it("rejects with the error of a failing query", async () => {

			const error = new Error("failing query");

			await expect(pipe(
				(items(["construct x", "construct failing"]))
				(construct(repository({ "construct x": [x], "construct failing": error })))
				(toArray())
			)).rejects.toBe(error);

		});

	});

	describe("repository", () => {

		it("targets the bound repository when given none", async () => {

			const target = repository({ "construct x": [x] });

			const graphs = await executor(bind(createRepository, () => target))(() => pipe(
				(items(["construct x"]))
				(construct())
				(toArray())
			));

			expect(graphs).toEqual([[x]]);

		});

		it("throws on creation when given none and none is bound", async () => {

			await expect(executor()(async () => construct())).rejects.toThrow(Error);

		});

	});

});
