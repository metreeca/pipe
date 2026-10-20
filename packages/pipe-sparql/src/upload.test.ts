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

import type { IRI } from "@metreeca/core/resource";
import { type Feed, pipe } from "@metreeca/flow";
import { items } from "@metreeca/flow/feeds";
import { bind, executor } from "@metreeca/gear";
import { type Graph, named, triple, type Triple } from "@metreeca/trio";
import type { Repository, RepositoryClient } from "@metreeca/wire-sparql";
import { describe, expect, it } from "vitest";
import { createRepository } from "./index.js";
import { upload } from "./upload.js";


const context = "https://example.com/context";
const other = "https://example.com/other";

const x = triple(named("https://example.com/x"), named("https://example.com/p"), named("https://example.com/y"));
const y = triple(named("https://example.com/y"), named("https://example.com/p"), named("https://example.com/z"));
const z = triple(named("https://example.com/z"), named("https://example.com/p"), named("https://example.com/x"));


interface Write {

	readonly method: "merge" | "store"
	readonly graph: undefined | IRI
	readonly transaction: boolean

}

/**
 * Creates an in-memory repository holding the given graph content, keyed by graph IRI with `""` standing for the
 * default graph, and recording every graph store write it receives; writes beyond the first `limit` are rejected with
 * `error`, leaving the content as it was.
 */
function repository(content: Readonly<Record<string, Graph>> = {}, { limit = Infinity, error }: {

	readonly limit?: number
	readonly error?: Error

} = {}): Repository & {

	readonly graphs: Map<undefined | IRI, Graph>
	readonly writes: readonly Write[]

} {

	const graphs = new Map<undefined | IRI, Graph>(
		Object.entries(content).map(([graph, triples]) => [graph === "" ? undefined : graph, triples])
	);

	const writes: Write[] = [];

	const write = async (write: Write, triples: Graph): Promise<void> => {
		if ( writes.length >= limit ) {
			return Promise.reject(error);
		} else { // recording a write and updating the content have no functional equivalent
			writes.push(write);
			graphs.set(write.graph, write.method === "merge" ? [...graphs.get(write.graph) ?? [], ...triples] : triples);
		}
	};

	const client = (transaction: boolean): RepositoryClient => ({
		ask: async () => false,
		select: async () => [],
		construct: async () => [],
		update: async () => {},
		merge: async (triples: readonly Triple[], graph?: IRI) => write({ method: "merge", graph, transaction }, triples),
		store: async (triples: readonly Triple[], graph?: IRI) => write({ method: "store", graph, transaction }, triples)
	});

	return {
		...client(false),
		graphs,
		writes,
		execute: async task => task(client(true)),
		close: async () => {}
	};
}

/**
 * Creates a feed carrying `graphs`, invoking `probe` each time a graph is drawn after the first and once the feed is
 * drained, so a test observes what happened while the feed was still being produced.
 */
function feed(graphs: readonly Graph[], probe: () => void): Feed<Graph> {
	return items((async function* () {
		for (const [index, graph] of graphs.entries()) { // probing between draws has no functional equivalent
			if ( index > 0 ) { probe(); }
			yield graph;
		}
		probe();
	})());
}


describe("upload()", () => {

	describe("atomic", () => {

		it("adds the feed to the default graph, keeping its statements", async () => {

			const target = repository({ "": [x] });

			const count = await pipe((items([[y], [z]]))(upload({ repository: target })));

			expect(count).toBe(2);
			expect(target.graphs.get(undefined)).toEqual([x, y, z]);

		});

		it("adds the feed to the named context, leaving other graphs untouched", async () => {

			const target = repository({ [context]: [x], [other]: [x] });

			await pipe((items([[y]]))(upload({ repository: target, context })));

			expect(target.graphs.get(context)).toEqual([x, y]);
			expect(target.graphs.get(other)).toEqual([x]);

		});

		it("overwrites the target graph with clear, leaving other graphs untouched", async () => {

			const target = repository({ [context]: [x], [other]: [x] });

			const count = await pipe((items([[y], [z]]))(upload({ repository: target, context, clear: true })));

			expect(count).toBe(2);
			expect(target.graphs.get(context)).toEqual([y, z]);
			expect(target.graphs.get(other)).toEqual([x]);

		});

		it("resolves to zero on an empty feed, leaving the target graph as it was", async () => {

			const target = repository({ "": [x] });

			const count = await pipe((items<Graph>([]))(upload({ repository: target })));

			expect(count).toBe(0);
			expect(target.graphs.get(undefined)).toEqual([x]);

		});

		it("empties the target graph on an empty feed with clear", async () => {

			const target = repository({ "": [x] });

			const count = await pipe((items<Graph>([]))(upload({ repository: target, clear: true })));

			expect(count).toBe(0);
			expect(target.graphs.get(undefined)).toEqual([]);

		});

		it.each([
			[false, "merge"],
			[true, "store"]
		] as const)("with clear set to %s, stores the whole feed with a single %s within a transaction", async (
			clear,
			method
		) => {

			const target = repository();

			await pipe((items([[x], [y]]))(upload({ repository: target, context, clear })));

			expect(target.writes).toEqual([{ method, graph: context, transaction: true }]);

		});

		it("drains the feed before opening the transaction", async () => {

			const target = repository();
			const events: string[] = [];

			const probed: Repository = {
				...target,
				execute: task => {
					events.push("execute"); // recording an event has no functional equivalent
					return target.execute(task);
				}
			};

			await pipe((feed([[x], [y]], () => events.push("draw")))(upload({ repository: probed })));

			expect(events).toEqual(["draw", "draw", "execute"]);

		});

		it("rejects with the error of a failing write", async () => {

			const error = new Error("failing write");
			const target = repository({ "": [x] }, { limit: 0, error });

			await expect(pipe((items([[y]]))(upload({ repository: target })))).rejects.toBe(error);

		});

	});

	describe("batch", () => {

		it("adds each graph to the target graph outside transactions", async () => {

			const target = repository({ [context]: [x] });

			const count = await pipe((items([[y], [z]]))(upload({ repository: target, context, batch: true })));

			expect(count).toBe(2);
			expect(target.graphs.get(context)).toEqual([x, y, z]);
			expect(target.writes).toEqual([
				{ method: "merge", graph: context, transaction: false },
				{ method: "merge", graph: context, transaction: false }
			]);

		});

		it("stores each graph as soon as it arrives", async () => {

			const target = repository();
			const written: number[] = [];

			await pipe((feed([[x], [y]], () => written.push(target.writes.length)))(upload({
				repository: target,
				batch: true
			})));

			expect(written).toEqual([1, 2]);

		});

		it("overwrites the target graph with clear, storing the first graph and merging the rest", async () => {

			const target = repository({ [context]: [x] });

			const count = await pipe(
				(items([[y], [z]]))
				(upload({ repository: target, context, batch: true, clear: true }))
			);

			expect(count).toBe(2);
			expect(target.graphs.get(context)).toEqual([y, z]);
			expect(target.writes.map(({ method }) => method)).toEqual(["store", "merge"]);

		});

		it("resolves to zero on an empty feed, writing nothing", async () => {

			const target = repository({ [context]: [x] });

			const count = await pipe((items<Graph>([]))(upload({ repository: target, context, batch: true })));

			expect(count).toBe(0);
			expect(target.writes).toEqual([]);
			expect(target.graphs.get(context)).toEqual([x]);

		});

		it("empties the target graph on an empty feed with clear", async () => {

			const target = repository({ [context]: [x] });

			const count = await pipe((items<Graph>([]))(upload({ repository: target, context, batch: true, clear: true })));

			expect(count).toBe(0);
			expect(target.graphs.get(context)).toEqual([]);

		});

		it("rejects with the error of a failing write, keeping the graphs stored before it", async () => {

			const error = new Error("failing write");
			const target = repository({ [context]: [x] }, { limit: 1, error });

			await expect(pipe((items([[y], [z]]))(upload({ repository: target, context, batch: true })))).rejects.toBe(error);

			expect(target.graphs.get(context)).toEqual([x, y]);

		});

	});

	describe("repository", () => {

		it("targets the bound repository when given none", async () => {

			const target = repository();

			await executor(bind(createRepository, () => target))(() => pipe((items([[x]]))(upload())));

			expect(target.graphs.get(undefined)).toEqual([x]);

		});

		it("throws on creation when given none and none is bound", async () => {

			await expect(executor()(async () => upload())).rejects.toThrow(Error);

		});

	});

});
