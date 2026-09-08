/*
 * Copyright © 2025-2026 Metreeca srl
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

import { describe, expect, it } from "vitest";
import { items } from "../feeds/index.js";
import { Feed, pipe } from "../index.js";
import { toArray, toSet } from "../sinks/index.js";
import { drain } from "./drain.js";
import { map } from "./map.js";
import { peek } from "./peek.js";


/**
 * Creates a feed yielding the given items, recording each as it is drawn.
 */
function tracked(drawn: number[], values: readonly number[]): Feed<number> {
	return items(values)(peek(value => drawn.push(value)));
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

describe("drain()", () => {

	it("should emit the items the sink computes", async () => {

		const values = await items([1, 2, 3, 4])(drain(async feed => (await feed(toArray())).slice(-2)))(toArray());

		expect(values).toEqual([3, 4]);

	});

	it("should hand the sink the items of the feed", async () => {

		const drawn: number[][] = [];

		await items([1, 2, 3])(drain(async feed => {
			drawn.push([...await feed(toArray())]);
			return [];
		}))(toArray());

		expect(drawn).toEqual([[1, 2, 3]]);

	});

	it("should carry on with the items of a sink already available", async () => {

		const values = await items([1, 2, 2, 3])(drain(toSet()))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should emit items of a different type", async () => {

		const values = await items([1, 2])(drain(async feed => (await feed(toArray())).map(n => `<${n}>`)))(toArray());

		expect(values).toEqual(["<1>", "<2>"]);

	});

	it("should empty the feed where the sink computes nothing", async () => {

		const values = await items([1, 2, 3])(drain(async () => []))(toArray());

		expect(values).toEqual([]);

	});

	it("should emit the computed items where the feed draws none", async () => {

		const values = await items<number>([])(drain(async () => [0]))(toArray());

		expect(values).toEqual([0]);

	});

	it("should accept items supplied as a feed of their own", async () => {

		const values = await items([1, 2])(drain(async feed => items(await feed(toArray()))(map(n => n*10))))(toArray());

		expect(values).toEqual([10, 20]);

	});

	it("should draw nothing before the reported feed is advanced", async () => {

		const drawn: number[] = [];

		tracked(drawn, [1, 2, 3])(drain(toArray()));

		expect(drawn).toEqual([]);

	});

	it("should emit nothing before the sink resolves", async () => {

		const drawn: number[] = [];

		const iterator = tracked(drawn, [1, 2, 3])(drain(toArray()))[Symbol.asyncIterator]();

		await iterator.next();

		expect(drawn).toEqual([1, 2, 3]);

	});

	it("should stop emitting on early termination", async () => {

		const iterator = items([1, 2, 3])(drain(toArray()))[Symbol.asyncIterator]();

		await iterator.next();

		await expect(iterator.return?.()).resolves.toEqual({ done: true, value: undefined });

	});

	it("should propagate failures raised by the sink", async () => {

		const failing = items([1, 2, 3])(drain<number, number>(async () => {
			throw new Error("sink failed");
		}));

		await expect(failing(toArray())).rejects.toThrow("sink failed");

	});

	it("should propagate failures of the source", async () => {

		const failing = items((async function* (): AsyncGenerator<number> {
			yield 1;
			throw new Error("source failed");
		})());

		await expect(failing(drain(toArray()))(toArray())).rejects.toThrow("source failed");

	});

	it("should chain with further tasks", async () => {

		const values = await pipe(
			(items([1, 2, 3]))
			(drain(toSet()))
			(map(n => n*10))
			(toArray())
		);

		expect(values).toEqual([10, 20, 30]);

	});

});
