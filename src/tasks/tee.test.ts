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

import { sleep } from "@metreeca/core/async";
import { describe, expect, it } from "vitest";
import { items } from "../feeds/index.js";
import { Feed, pipe, Sink, Task } from "../index.js";
import { toArray } from "../sinks/index.js";
import { filter } from "./filter.js";
import { map } from "./map.js";
import { take } from "./take.js";
import { tee } from "./tee.js";


/**
 * Creates a feed yielding the given items, recording each as it is drawn.
 */
function tracked(drawn: number[], values: readonly number[]): Feed<number> {
	return items((async function* () {
		for (const value of values) {
			drawn.push(value);
			yield value;
		}
	})());
}

/**
 * Creates a task reporting the items after a delay, to observe how branches interleave.
 */
function slow(ms: number, factor: number): Task<number, number> {
	return map(async n => {
		await sleep(ms);
		return n*factor;
	});
}

/**
 * Creates a task recording its closing, to observe how the fan-out is torn down.
 */
function closing(closed: string[], name: string): Task<number, number> {
	return source => items((async function* () {
		try {
			for await (const item of source) { yield item; }
		} finally {
			closed.push(name);
		}
	})());
}

/**
 * Creates a feed drawing from a fixed iterator, as a custom feed honouring the contract by hand may.
 */
function custom<V>(iterator: AsyncIterator<V>): Feed<V> {

	function feed<R>(task: Task<V, R>): Feed<R>;
	function feed<R>(sink: Sink<V, R>): Promise<R>;

	function feed<R>(step: Task<V, R> | Sink<V, R>): unknown {
		return step(items({ [Symbol.asyncIterator]: () => iterator }));
	}

	return Object.assign(feed, { [Symbol.asyncIterator]: () => iterator });

}

/**
 * Sorts values in ascending order, to compare feeds whose interleaving is not defined.
 */
function ordered(values: readonly number[]): readonly number[] {
	return [...values].sort((x, y) => x-y);
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

describe("tee()", () => {

	it("should hand every item to every branch", async () => {

		const values = await items([1, 2, 3])(tee(map(n => n), map(n => n*10)))(toArray());

		expect(ordered(values)).toEqual([1, 2, 3, 10, 20, 30]);

	});

	it("should draw each item once, however many branches take it", async () => {

		const drawn: number[] = [];

		const values = await tracked(drawn, [1, 2, 3])(tee(map(n => n), map(n => n*10), map(n => n*100)))(toArray());

		expect(drawn).toEqual([1, 2, 3]);
		expect(ordered(values)).toEqual([1, 2, 3, 10, 20, 30, 100, 200, 300]);

	});

	it("should draw no further than one item ahead of the branches", async () => {

		const drawn: number[] = [];

		const feed = tracked(drawn, [1, 2, 3, 4, 5])(tee(map(n => n), slow(20, 10)));
		const iterator = feed[Symbol.asyncIterator]();

		await iterator.next(); // the immediate branch reported its first item
		await sleep(10); // the slow branch is still working on it

		expect(drawn.length).toBeLessThanOrEqual(2);

		await iterator.return?.();

	});

	it("should emit items as they become available", async () => {

		const values = await items([1])(tee(slow(30, 1), slow(10, 10)))(toArray());

		expect(values).toEqual([10, 1]);

	});

	it("should preserve the order of the items of each branch", async () => {

		const values = await items([1, 2, 3])(tee(map(n => n), slow(5, 10)))(toArray());

		expect(values.filter(value => value < 10)).toEqual([1, 2, 3]);
		expect(values.filter(value => value >= 10)).toEqual([10, 20, 30]);

	});

	it("should draw the whole feed in every branch", async () => {

		const values = await items([1, 2, 3])(tee(take(2), take(2)))(toArray());

		expect(ordered(values)).toEqual([1, 1, 2, 2]); // the quota is spent on the feed, not shared among the branches

	});

	it("should drop branches closing early", async () => {

		const values = await items([1, 2, 3])(tee(take(1), map(n => n*10)))(toArray());

		expect(ordered(values)).toEqual([1, 10, 20, 30]);

	});

	it("should keep drawing for branches reporting nothing", async () => {

		const values = await items([1, 2, 3])(tee(filter(() => false), map(n => n*10)))(toArray());

		expect(ordered(values)).toEqual([10, 20, 30]);

	});

	it("should report an empty feed with no branches", async () => {

		const drawn: number[] = [];

		const values = await tracked(drawn, [1, 2, 3])(tee<number, number>())(toArray());

		expect(values).toEqual([]);
		expect(drawn).toEqual([]);

	});

	it("should handle an empty source", async () => {

		const values = await items<number>([])(tee(map(n => n), map(n => n*10)))(toArray());

		expect(values).toEqual([]);

	});

	it("should close the branches and the source on early termination", async () => {

		const closed: string[] = [];

		const source = items((async function* () {
			try {
				yield 1;
				yield 2;
			} finally {
				closed.push("source");
			}
		})());

		const iterator = source(tee(closing(closed, "a"), closing(closed, "b")))[Symbol.asyncIterator]();

		await iterator.next();
		await iterator.return?.();

		expect([...closed].sort()).toEqual(["a", "b", "source"]);

	});

	it("should close the branches and the source on failure", async () => {

		const closed: string[] = [];

		const source = items((async function* () {
			try {
				yield 1;
				yield 2;
			} finally {
				closed.push("source");
			}
		})());

		const failing: Task<number, number> = feed => items((async function* () {
			for await (const item of feed) { throw new Error(`branch failed <${item}>`); }
		})());

		await expect(source(tee(failing, closing(closed, "b")))(toArray())).rejects.toThrow(Error);

		expect([...closed].sort()).toEqual(["b", "source"]);

	});

	it("should propagate failures of the source", async () => {

		const failing = items((async function* (): AsyncGenerator<number> {
			yield 1;
			throw new Error("source failed");
		})());

		await expect(failing(tee(map(n => n), map(n => n*10)))(toArray())).rejects.toThrow("source failed");

	});

	it("should report a failure raised while the consumer is idle on the next advance", async () => {

		const unhandled: unknown[] = [];
		const collect = (reason: unknown) => unhandled.push(reason);

		const failing: Task<number, number> = feed => items((async function* () {
			for await (const item of feed) {
				yield item;
				await sleep(10); // the branch fails after handing over the item
				throw new Error("branch failed");
			}
		})());

		const iterator = items([1, 2])(tee(failing))[Symbol.asyncIterator]();

		process.on("unhandledRejection", collect);

		try {

			await iterator.next();
			await sleep(50); // the consumer stays idle while the branch fails

		} finally {

			process.off("unhandledRejection", collect);

		}

		await expect(iterator.next()).rejects.toThrow("branch failed");

		expect(unhandled).toEqual([]);

	});

	it("should suppress failures reported while closing", async () => {

		const hostile = custom<number>({
			next: async () => ({ done: false, value: 1 }),
			return: () => { throw new Error("close failed"); }
		});

		const iterator = hostile(tee(map(n => n)))[Symbol.asyncIterator]();

		await iterator.next();

		await expect(iterator.return?.()).resolves.toEqual({ done: true, value: undefined });

	});

	it("should chain with further tasks", async () => {

		const values = await pipe(
			(items([1, 2]))
			(tee(map(n => n), map(n => n*10)))
			(map(n => n+1))
			(toArray())
		);

		expect(ordered(values)).toEqual([2, 3, 11, 21]);

	});

});
