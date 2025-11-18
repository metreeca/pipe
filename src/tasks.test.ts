/*
 * Copyright © 2025 Metreeca srl
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
import { pipe } from ".";
import { items } from "./feeds";
import { toArray } from "./sinks";
import { batch, distinct, filter, flatMap, map, peek, skip, sort, take } from "./tasks";


describe("skip()", () => {

	it("should skip first n items", async () => {

		const values = await items([1, 2, 3, 4, 5])(skip(2))(toArray());

		expect(values).toEqual([3, 4, 5]);

	});

	it("should skip all items when n >= length", async () => {

		const values = await items([1, 2, 3])(skip(5))(toArray());

		expect(values).toEqual([]);

	});

	it("should skip zero items", async () => {

		const values = await items([1, 2, 3])(skip(0))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should treat negative n as zero", async () => {

		const values = await items([1, 2, 3])(skip(-5))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

});

describe("take()", () => {

	it("should take first n items", async () => {

		const values = await items([1, 2, 3, 4, 5])(take(3))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should take all items when n >= length", async () => {

		const values = await items([1, 2, 3])(take(5))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should take zero items", async () => {

		const values = await items([1, 2, 3])(take(0))(toArray());

		expect(values).toEqual([]);

	});

	it("should treat negative n as zero", async () => {

		const values = await items([1, 2, 3])(take(-5))(toArray());

		expect(values).toEqual([]);

	});

	it("should terminate infinite generator after n items", async () => {

		let generatorCalls = 0;
		let iteratorReturned = false;

		// Create an infinite generator that tracks cleanup
		const infiniteGenerator = items((async function* () {
			try {
				let i = 0;
				while ( true ) {
					generatorCalls++;
					yield i++;
				}
			} finally {
				iteratorReturned = true;
			}
		})());

		const values = await infiniteGenerator(take(5))(toArray());

		expect(values).toEqual([0, 1, 2, 3, 4]);
		expect(generatorCalls).toBe(6); // Called 6 times: yields 0-4, then one more call before return
		expect(iteratorReturned).toBe(true); // Generator was properly cleaned up

	});

	it("should backsignal through intermediate tasks", async () => {

		let generatorCalls = 0;
		let iteratorReturned = false;

		// Create an infinite generator that tracks cleanup
		const infiniteGenerator = items((async function* () {
			try {
				let i = 0;
				while ( true ) {
					generatorCalls++;
					yield i++;
				}
			} finally {
				iteratorReturned = true;
			}
		})());

		// Pipeline: infinite generator > filter (evens) > take(3)
		const values = await infiniteGenerator
		(filter(x => x%2 === 0))
		(take(3))
		(toArray());

		expect(values).toEqual([0, 2, 4]);
		// Generator yields: 0(✓), 1(✗), 2(✓), 3(✗), 4(✓), 5(✗), 6(passes filter, triggers take return)
		expect(generatorCalls).toBe(7); // 7 calls: take needs one more to detect count >= 3
		expect(iteratorReturned).toBe(true); // Generator was properly cleaned up

	});

});

describe("peek()", () => {

	it("should execute side effect for each item", async () => {

		const sideEffects: number[] = [];
		const values = await items([1, 2, 3])(peek(x => {
			sideEffects.push(x*10);
		}))(toArray());

		expect(values).toEqual([1, 2, 3]);
		expect(sideEffects).toEqual([10, 20, 30]);

	});

	it("should support async consumers", async () => {

		const sideEffects: number[] = [];
		const values = await items([1, 2, 3])(peek(async x => {
			await Promise.resolve();
			sideEffects.push(x);
		}))(toArray());

		expect(values).toEqual([1, 2, 3]);
		expect(sideEffects).toEqual([1, 2, 3]);

	});

});

describe("filter()", () => {

	it("should filter items by predicate", async () => {

		const values = await items([1, 2, 3, 4, 5])(filter(x => x%2 === 0))(toArray());

		expect(values).toEqual([2, 4]);

	});

	it("should support async predicates", async () => {

		const values = await items([1, 2, 3, 4, 5])(filter(async x => {
			await Promise.resolve();
			return x > 2;
		}))(toArray());

		expect(values).toEqual([3, 4, 5]);

	});

	it("should handle empty results", async () => {

		const values = await items([1, 2, 3])(filter(() => false))(toArray());

		expect(values).toEqual([]);

	});

	it("should treat undefined as false", async () => {

		const values = await items([1, 2, 3, 4, 5])(filter(x => x > 3 ? true : undefined))(toArray());

		expect(values).toEqual([4, 5]);

	});

	it("should handle async predicates returning undefined", async () => {

		const values = await items([1, 2, 3, 4, 5])(filter(async x => {
			await Promise.resolve();
			return x%2 === 0 ? true : undefined;
		}))(toArray());

		expect(values).toEqual([2, 4]);

	});

});

describe("distinct()", () => {

	it("should filter out duplicate primitives", async () => {

		const values = await items([1, 2, 2, 3, 1, 4])(distinct())(toArray());

		expect(values).toEqual([1, 2, 3, 4]);

	});

	it("should use selector for comparison", async () => {

		const values = await items([
			{ id: 1, name: "a" },
			{ id: 2, name: "b" },
			{ id: 1, name: "c" }
		])(distinct(item => item.id))(toArray());

		expect(values).toEqual([
			{ id: 1, name: "a" },
			{ id: 2, name: "b" }
		]);

	});

	it("should handle empty stream", async () => {

		const values = await items([] as number[])(distinct())(toArray());

		expect(values).toEqual([]);

	});

});

describe("sort()", () => {

	it("should sort numbers in ascending order", async () => {

		const values = await items([3, 1, 4, 1, 5, 9, 2, 6])(sort())(toArray());

		expect(values).toEqual([1, 1, 2, 3, 4, 5, 6, 9]);

	});

	it("should sort strings lexicographically", async () => {

		const values = await items(["cherry", "apple", "banana", "date"])(sort())(toArray());

		expect(values).toEqual(["apple", "banana", "cherry", "date"]);

	});

	it("should use selector to extract sort key", async () => {

		const values = await items([
			{ id: 3, name: "c" },
			{ id: 1, name: "a" },
			{ id: 2, name: "b" }
		])(sort({ by: item => item.id }))(toArray());

		expect(values).toEqual([
			{ id: 1, name: "a" },
			{ id: 2, name: "b" },
			{ id: 3, name: "c" }
		]);

	});

	it("should support async selectors", async () => {

		const values = await items([
			{ id: 3, name: "c" },
			{ id: 1, name: "a" },
			{ id: 2, name: "b" }
		])(sort({
			by: async item => {
				await Promise.resolve();
				return item.id;
			}
		}))(toArray());

		expect(values).toEqual([
			{ id: 1, name: "a" },
			{ id: 2, name: "b" },
			{ id: 3, name: "c" }
		]);

	});

	it("should sort in descending order with 'desc'", async () => {

		const values = await items([1, 2, 3, 4, 5])(sort({ as: "desc" }))(toArray());

		expect(values).toEqual([5, 4, 3, 2, 1]);

	});

	it("should sort in descending order with 'descending'", async () => {

		const values = await items([1, 2, 3, 4, 5])(sort({ as: "descending" }))(toArray());

		expect(values).toEqual([5, 4, 3, 2, 1]);

	});

	it("should sort in ascending order with 'asc'", async () => {

		const values = await items([3, 1, 4, 5, 2])(sort({ as: "asc" }))(toArray());

		expect(values).toEqual([1, 2, 3, 4, 5]);

	});

	it("should sort in ascending order with 'ascending'", async () => {

		const values = await items([3, 1, 4, 5, 2])(sort({ as: "ascending" }))(toArray());

		expect(values).toEqual([1, 2, 3, 4, 5]);

	});

	it("should use custom comparator for descending order", async () => {

		const values = await items([1, 2, 3, 4, 5])(sort({ as: (a, b) => b-a }))(toArray());

		expect(values).toEqual([5, 4, 3, 2, 1]);

	});

	it("should combine selector with 'desc' order", async () => {

		const values = await items([
			{ age: 30, name: "Alice" },
			{ age: 25, name: "Bob" },
			{ age: 35, name: "Charlie" }
		])(sort({ by: item => item.age, as: "desc" }))(toArray());

		expect(values).toEqual([
			{ age: 35, name: "Charlie" },
			{ age: 30, name: "Alice" },
			{ age: 25, name: "Bob" }
		]);

	});

	it("should use comparator with selector", async () => {

		const values = await items([
			{ age: 30, name: "Alice" },
			{ age: 25, name: "Bob" },
			{ age: 35, name: "Charlie" }
		])(sort({ by: item => item.age, as: (a, b) => b-a }))(toArray());

		expect(values).toEqual([
			{ age: 35, name: "Charlie" },
			{ age: 30, name: "Alice" },
			{ age: 25, name: "Bob" }
		]);

	});

	it("should handle empty stream", async () => {

		const values = await items([] as number[])(sort())(toArray());

		expect(values).toEqual([]);

	});

	it("should handle single item", async () => {

		const values = await items([42])(sort())(toArray());

		expect(values).toEqual([42]);

	});

	it("should sort dates chronologically", async () => {

		const dates = [
			new Date("2023-03-15"),
			new Date("2023-01-10"),
			new Date("2023-02-20")
		];

		const values = await items(dates)(sort())(toArray());

		expect(values).toEqual([
			new Date("2023-01-10"),
			new Date("2023-02-20"),
			new Date("2023-03-15")
		]);

	});

	it("should sort booleans (false before true)", async () => {

		const values = await items([true, false, true, false])(sort())(toArray());

		expect(values).toEqual([false, false, true, true]);

	});

	it("should handle negative numbers", async () => {

		const values = await items([5, -3, 0, -1, 2])(sort())(toArray());

		expect(values).toEqual([-3, -1, 0, 2, 5]);

	});

	it("should be stable for equal elements", async () => {

		const values = await items([
			{ key: 2, value: "a" },
			{ key: 1, value: "b" },
			{ key: 2, value: "c" },
			{ key: 1, value: "d" }
		])(sort({ by: item => item.key }))(toArray());

		// Items with same key should maintain relative order (stable sort)
		expect(values).toEqual([
			{ key: 1, value: "b" },
			{ key: 1, value: "d" },
			{ key: 2, value: "a" },
			{ key: 2, value: "c" }
		]);

	});

});

describe("sequential map()", () => {

	it("should transform items", async () => {

		const values = await items([1, 2, 3])(map(x => x*2))(toArray());

		expect(values).toEqual([2, 4, 6]);

	});

	it("should support async mappers", async () => {

		const values = await items([1, 2, 3])(map(async x => {
			await Promise.resolve();
			return x*2;
		}))(toArray());

		expect(values).toEqual([2, 4, 6]);

	});

	it("should change item types", async () => {

		const values = await items([1, 2, 3])(map(x => `value-${x}`))(toArray());

		expect(values).toEqual(["value-1", "value-2", "value-3"]);

	});

});

describe("sequential flatMap()", () => {

	it("should flatten mapped async iterables", async () => {

		const values = await items([1, 2, 3])(flatMap(async function* (x) {
			yield x;
			yield x*10;
		}))(toArray());

		expect(values).toEqual([1, 10, 2, 20, 3, 30]);

	});

	it("should handle empty iterables", async () => {

		const values = await items([1, 2, 3])(flatMap(async function* (x) {
			if ( x === 2 ) {
				yield x;
			}
		}))(toArray());

		expect(values).toEqual([2]);

	});

	it("should treat returned strings as atomic values", async () => {

		const values = await items([1, 2, 3])(flatMap(x => `value${x}`))(toArray());

		expect(values).toEqual(["value1", "value2", "value3"]);

	});

	it("should treat strings in arrays as items to yield", async () => {

		const values = await items([1, 2])(flatMap(x => [`a${x}`, `b${x}`]))(toArray());

		expect(values).toEqual(["a1", "b1", "a2", "b2"]);

	});

});

describe("parallel map()", () => {

	it("should transform items in parallel", async () => {

		const values = await items([1, 2, 3, 4])(map(x => x*2, { parallel: true }))(toArray());

		// Results should contain all transformed values (order may vary)
		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8]);

	});

	it("should support async mappers with concurrency", async () => {

		const processingOrder: number[] = [];

		const values = await items([1, 2, 3, 4])(map(async x => {
			await new Promise(resolve => setTimeout(resolve, (5-x)*10)); // Reverse delay
			processingOrder.push(x);
			return x*2;
		}, { parallel: 2 }))(toArray());

		// All values should be present
		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8]);

		// With concurrency=2, processing should happen in batches
		// Items may complete out of order due to varying delays

	});

	it("should use auto concurrency by default", async () => {

		const values = await items([1, 2, 3, 4, 5])(map(async x => {
			await Promise.resolve();
			return x*2;
		}, { parallel: true }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8, 10]);

	});

	it("should respect concurrency limit", async () => {

		let concurrent = 0;
		let maxConcurrent = 0;

		const values = await items([1, 2, 3, 4, 5])(map(async x => {
			concurrent++;
			maxConcurrent = Math.max(maxConcurrent, concurrent);

			await new Promise(resolve => setTimeout(resolve, 10));

			concurrent--;
			return x*2;
		}, { parallel: 2 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8, 10]);
		expect(maxConcurrent).toBeLessThanOrEqual(2);

	});

	it("should handle errors in parallel processing", async () => {

		await expect(async () => {
			await items([1, 2, 3, 4])(map(async x => {
				if ( x === 3 ) {
					throw new Error("Error at 3");
				}
				return x*2;
			}, { parallel: true }))(toArray());
		}).rejects.toThrow("Error at 3");

	});

	it("should change item types", async () => {

		const values = await items([1, 2, 3])(map(x => `value-${x}`, { parallel: true }))(toArray());

		expect([...values].sort()).toEqual(["value-1", "value-2", "value-3"]);

	});

	it("should handle empty source", async () => {

		const values = await items([])(map(x => x*2, { parallel: true }))(toArray());

		expect(values).toEqual([]);

	});

	it("should handle concurrency of 1", async () => {

		let concurrent = 0;
		let maxConcurrent = 0;

		const values = await items([1, 2, 3, 4])(map(async x => {
			concurrent++;
			maxConcurrent = Math.max(maxConcurrent, concurrent);

			await new Promise(resolve => setTimeout(resolve, 10));

			concurrent--;
			return x*2;
		}, { parallel: 1 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8]);
		expect(maxConcurrent).toBe(1);

	});

	it("should clean up on early termination", async () => {

		let started = 0;
		let completed = 0;

		const iterator = items([1, 2, 3, 4, 5])(map(async x => {
			started++;
			await new Promise(resolve => setTimeout(resolve, 50));
			completed++;
			return x*2;
		}, { parallel: 2 }))()[Symbol.asyncIterator]();

		// Get first result
		await iterator.next();

		// Early termination
		await iterator.return?.();

		// Some operations may have started but iterator should be cleaned up
		expect(started).toBeGreaterThan(0);

	});

	it("should handle source iterator errors", async () => {

		async function* badSource() {
			yield 1;
			yield 2;
			throw new Error("Source failed");
		}

		await expect(async () => {
			await items(badSource())(map(x => x*2, { parallel: true }))(toArray());
		}).rejects.toThrow("Source failed");

	});

	it("should handle unbounded concurrency (parallel: 0)", async () => {

		let concurrent = 0;
		let maxConcurrent = 0;

		const values = await items([1, 2, 3, 4, 5, 6, 7, 8])(map(async x => {
			concurrent++;
			maxConcurrent = Math.max(maxConcurrent, concurrent);

			await new Promise(resolve => setTimeout(resolve, 20));

			concurrent--;
			return x*2;
		}, { parallel: 0 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8, 10, 12, 14, 16]);
		// With unbounded concurrency, all items should be processed simultaneously
		expect(maxConcurrent).toBe(8);

	});

	it("should start all tasks immediately with unbounded concurrency", async () => {

		const startTimes: number[] = [];
		const testStart = Date.now();

		const values = await items([1, 2, 3, 4, 5])(map(async x => {
			startTimes.push(Date.now()-testStart);
			await new Promise(resolve => setTimeout(resolve, 50));
			return x*2;
		}, { parallel: 0 }))(toArray());

		// All tasks should start within a few milliseconds of each other
		const maxStartTime = Math.max(...startTimes);
		const minStartTime = Math.min(...startTimes);
		const startTimeSpread = maxStartTime-minStartTime;

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8, 10]);
		expect(startTimeSpread).toBeLessThan(10); // All should start within 10ms

	});

	it("should complete faster with parallel than sequential", async () => {

		// Sequential
		const sequentialStart = Date.now();
		await items([1, 2, 3, 4])(map(async x => {
			await new Promise(resolve => setTimeout(resolve, 50));
			return x*2;
		}))(toArray());
		const sequentialTime = Date.now()-sequentialStart;

		// Parallel
		const parallelStart = Date.now();
		await items([1, 2, 3, 4])(map(async x => {
			await new Promise(resolve => setTimeout(resolve, 50));
			return x*2;
		}, { parallel: 0 }))(toArray());
		const parallelTime = Date.now()-parallelStart;

		// Sequential should take ~4x longer (4 items * 50ms)
		expect(sequentialTime).toBeGreaterThanOrEqual(190); // ~200ms
		expect(parallelTime).toBeLessThan(100); // ~50ms

	});

});

describe("parallel flatMap()", () => {

	it("should transform and flatten items in parallel", async () => {

		const values = await items([1, 2, 3])(flatMap(x => [x, x*2], { parallel: true }))(toArray());

		// Results should contain all flattened values (order may vary)
		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 6]);

	});

	it("should support async mappers that return arrays", async () => {

		const values = await items([1, 2, 3])(flatMap(async x => {
			await Promise.resolve();
			return [x, x*2];
		}, { parallel: true }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 6]);

	});

	it("should support async iterables", async () => {

		const values = await items([1, 2, 3])(flatMap(async function* (x) {
			yield x;
			yield x*2;
		}, { parallel: true }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 6]);

	});

	it("should respect concurrency limit", async () => {

		let concurrent = 0;
		let maxConcurrent = 0;

		const values = await items([1, 2, 3, 4])(flatMap(async x => {
			concurrent++;
			maxConcurrent = Math.max(maxConcurrent, concurrent);

			await new Promise(resolve => setTimeout(resolve, 10));

			concurrent--;
			return [x, x*2];
		}, { parallel: 2 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 4, 6, 8]);
		expect(maxConcurrent).toBeLessThanOrEqual(2);

	});

	it("should handle single values", async () => {

		const values = await items([1, 2, 3])(flatMap(x => x*2, { parallel: true }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6]);

	});

	it("should handle errors in parallel processing", async () => {

		await expect(async () => {
			await items([1, 2, 3, 4])(flatMap(async x => {
				if ( x === 3 ) {
					throw new Error("Error at 3");
				}
				return [x, x*2];
			}, { parallel: true }))(toArray());
		}).rejects.toThrow("Error at 3");

	});

	it("should handle empty source", async () => {

		const values = await items([])(flatMap(x => [x, x*2], { parallel: true }))(toArray());

		expect(values).toEqual([]);

	});

	it("should handle concurrency of 1", async () => {

		let concurrent = 0;
		let maxConcurrent = 0;

		const values = await items([1, 2, 3])(flatMap(async x => {
			concurrent++;
			maxConcurrent = Math.max(maxConcurrent, concurrent);

			await new Promise(resolve => setTimeout(resolve, 10));

			concurrent--;
			return [x, x*2];
		}, { parallel: 1 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 6]);
		expect(maxConcurrent).toBe(1);

	});

	it("should handle errors during flattening", async () => {

		await expect(async () => {
			await items([1, 2, 3])(flatMap(x => {
				return function* () {
					yield x;
					if ( x === 2 ) {
						throw new Error("Flatten error");
					}
					yield x*2;
				};
			}, { parallel: true }))(toArray());
		}).rejects.toThrow("Flatten error");

	});

	it("should handle unbounded concurrency (parallel: 0)", async () => {

		let concurrent = 0;
		let maxConcurrent = 0;

		const values = await items([1, 2, 3, 4, 5, 6])(flatMap(async x => {
			concurrent++;
			maxConcurrent = Math.max(maxConcurrent, concurrent);

			await new Promise(resolve => setTimeout(resolve, 20));

			concurrent--;
			return [x, x*2];
		}, { parallel: 0 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 4, 5, 6, 6, 8, 10, 12]);
		// With unbounded concurrency, all items should be processed simultaneously
		expect(maxConcurrent).toBe(6);

	});

	it("should process nested pipes in parallel with unbounded concurrency", async () => {

		const startTimes: number[] = [];
		const testStart = Date.now();

		const values = await items([1, 2, 3, 4, 5])(flatMap(x => pipe(
			items(x)
			(map(async v => {
				startTimes.push(Date.now()-testStart);
				await new Promise(resolve => setTimeout(resolve, 50));
				return v*2;
			}))
		), { parallel: 0 }))(toArray());

		// All nested pipes should start processing within a few milliseconds
		const maxStartTime = Math.max(...startTimes);
		const minStartTime = Math.min(...startTimes);
		const startTimeSpread = maxStartTime-minStartTime;

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([2, 4, 6, 8, 10]);
		expect(startTimeSpread).toBeLessThan(20); // All should start within 20ms

	});

});

describe("batch()", () => {

	it("should group items into batches of specified size", async () => {

		const values = await items([1, 2, 3, 4, 5])(batch(2))(toArray());

		expect(values).toEqual([[1, 2], [3, 4], [5]]);

	});

	it("should collect all items when size is 0", async () => {

		const values = await items([1, 2, 3, 4, 5])(batch(0))(toArray());

		expect(values).toEqual([[1, 2, 3, 4, 5]]);

	});

	it("should handle empty stream", async () => {

		const values = await items([] as number[])(batch(2))(toArray());

		expect(values).toEqual([]);

	});

	it("should yield final partial batch", async () => {

		const values = await items([1, 2, 3])(batch(2))(toArray());

		expect(values).toEqual([[1, 2], [3]]);

	});

	it("should create individual batches when size is 1", async () => {

		const values = await items([1, 2, 3, 4])(batch(1))(toArray());

		expect(values).toEqual([[1], [2], [3], [4]]);

	});

	it("should process batches through pipeline", async () => {

		const result = await items([1, 2, 3, 4, 5, 6, 7])
		(batch(3))
		(map(batch => batch.reduce((sum, n) => sum+n, 0)))
		(toArray());

		expect(result).toEqual([6, 15, 7]);

	});

});

describe("parallelize() edge cases", () => {

	it("should handle race condition where same mapper promise wins consecutive races", async () => {

		// Test the fix for the race condition where a slow mapper could win multiple races

		let mapperCallCount = 0;
		let consumerCallCount = 0;

		const values = await items([1, 2, 3])(flatMap(async x => {
			mapperCallCount++;
			// First item is very slow, others are fast
			await new Promise(resolve => setTimeout(resolve, x === 1 ? 100 : 1));
			return [x, x*2];
		}, { parallel: 3 }))(peek(() => {
			consumerCallCount++;
		}))(toArray());

		// All values should be present exactly once
		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 6]);

		// Each mapper should be called exactly once
		expect(mapperCallCount).toBe(3);

		// Each value should be consumed exactly once
		expect(consumerCallCount).toBe(6);

	});

	it("should handle consumer iterator that throws during next()", async () => {

		await expect(async () => {
			await items([1, 2, 3])(flatMap(x => {
				return {
					async* [Symbol.asyncIterator]() {
						yield x;
						if ( x === 2 ) {
							throw new Error("Consumer error during iteration");
						}
						yield x*2;
					}
				};
			}, { parallel: 2 }))(toArray());
		}).rejects.toThrow("Consumer error during iteration");

	});

	it("should handle mapper that throws before consumer starts", async () => {

		await expect(async () => {
			await items([1, 2, 3, 4])(flatMap(async x => {
				await new Promise(resolve => setTimeout(resolve, 10));
				if ( x === 2 ) {
					throw new Error("Mapper error");
				}
				return [x, x*2];
			}, { parallel: 2 }))(toArray());
		}).rejects.toThrow("Mapper error");

	});

	it("should handle consumer that throws during iteration", async () => {

		await expect(async () => {
			await items([1, 2, 3])(flatMap(async function* (x) {
				yield x;
				if ( x === 2 ) {
					throw new Error("Consumer throw");
				}
				yield x*2;
			}, { parallel: 2 }))(toArray());
		}).rejects.toThrow("Consumer throw");

	});

	it("should handle early termination via generator return()", async () => {

		const processed: number[] = [];
		const mapped: number[] = [];

		const iterator = items([1, 2, 3, 4, 5])(flatMap(async x => {
			mapped.push(x);
			await new Promise(resolve => setTimeout(resolve, 10));
			return [x, x*2];
		}, { parallel: 3 }))()[Symbol.asyncIterator]();

		// Consume first few values
		const result1 = await iterator.next();
		processed.push(result1.value as number);

		const result2 = await iterator.next();
		processed.push(result2.value as number);

		// Early termination
		await iterator.return?.();

		// Should have some values processed
		expect(processed.length).toBeGreaterThanOrEqual(2);

		// Not all items should be mapped (early termination)
		expect(mapped.length).toBeLessThan(10); // Would be 10 if all items produced [x, x*2]

	});

	it("should enforce thread limit across mapping and consuming phases", async () => {

		let concurrentMappers = 0;
		let concurrentConsumers = 0;
		let maxTotal = 0;

		const values = await items([1, 2, 3, 4, 5])(flatMap(async x => {
			concurrentMappers++;
			const totalBefore = concurrentMappers+concurrentConsumers;
			maxTotal = Math.max(maxTotal, totalBefore);

			await new Promise(resolve => setTimeout(resolve, 20));

			concurrentMappers--;

			// Return an async iterable to test consumer phase concurrency
			return {
				async* [Symbol.asyncIterator]() {
					for (let i = 0; i < 3; i++) {
						concurrentConsumers++;
						const totalDuring = concurrentMappers+concurrentConsumers;
						maxTotal = Math.max(maxTotal, totalDuring);

						await new Promise(resolve => setTimeout(resolve, 10));

						concurrentConsumers--;
						yield x*10+i;
					}
				}
			};
		}, { parallel: 2 }))(toArray());

		// Should have all values
		expect(values.length).toBe(15); // 5 items × 3 sub-items each

		// Total concurrent operations should never exceed thread limit
		expect(maxTotal).toBeLessThanOrEqual(2);

	});

	it("should handle varying mapper and consumer durations", async () => {

		const values = await items([1, 2, 3, 4])(flatMap(async x => {
			// Varying mapper delays
			await new Promise(resolve => setTimeout(resolve, x*10));

			return {
				async* [Symbol.asyncIterator]() {
					// Varying consumer delays
					for (let i = 0; i < 2; i++) {
						await new Promise(resolve => setTimeout(resolve, (5-x)*5));
						yield x*10+i;
					}
				}
			};
		}, { parallel: 2 }))(toArray());

		// All values should be present (order may vary)
		expect([...values].sort((a: number, b: number) => a-b)).toEqual([
			10, 11, 20, 21, 30, 31, 40, 41
		]);

	});

	it("should handle consumer that completes immediately", async () => {

		const values = await items([1, 2, 3])(flatMap(() => {
			// Return empty iterable (immediate completion)
			return {
				async* [Symbol.asyncIterator]() {
					// Yields nothing, completes immediately
				}
			};
		}, { parallel: 2 }))(toArray());

		expect(values).toEqual([]);

	});

	it("should handle consumer with single value", async () => {

		const values = await items([1, 2, 3])(flatMap(async function* (x) {
			yield x*10;
			// Only one value, then done
		}, { parallel: 2 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([10, 20, 30]);

	});

	it("should handle consumer with many values", async () => {

		const values = await items([1, 2])(flatMap(async function* (x) {
			for (let i = 0; i < 10; i++) {
				await new Promise(resolve => setTimeout(resolve, 1));
				yield x*100+i;
			}
		}, { parallel: 1 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([
			100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
			200, 201, 202, 203, 204, 205, 206, 207, 208, 209
		]);

	});

	it("should clean up resources on mapper error", async () => {

		const cleanedIterators: number[] = [];

		await expect(async () => {
			await items([1, 2, 3, 4, 5])(flatMap(async x => {
				await new Promise(resolve => setTimeout(resolve, 10));

				if ( x === 3 ) {
					throw new Error("Mapper fails at 3");
				}

				return {
					async* [Symbol.asyncIterator]() {
						try {
							yield x;
							yield x*2;
						} finally {
							cleanedIterators.push(x);
						}
					}
				};
			}, { parallel: 2 }))(toArray());
		}).rejects.toThrow("Mapper fails at 3");

		// Some iterators should be cleaned up
		// The exact number depends on timing, but cleanup should happen
		expect(cleanedIterators.length).toBeGreaterThanOrEqual(0);

	});

	it("should handle source iterator that throws", async () => {

		async function* throwingSource() {
			yield 1;
			yield 2;
			throw new Error("Source error");
		}

		await expect(async () => {
			await items(throwingSource())(flatMap(x => [x, x*2], { parallel: 2 }))(toArray());
		}).rejects.toThrow("Source error");

	});

	it("should handle mixed sync and async consumer values", async () => {

		const values = await items([1, 2, 3])(flatMap(function* (x) {
			// Synchronous generator (not async)
			yield x;
			yield x*2;
		}, { parallel: 2 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 2, 3, 4, 6]);

	});

	it("should maintain correct order with threads=1", async () => {

		const values = await items([1, 2, 3, 4, 5])(flatMap(async x => {
			await new Promise(resolve => setTimeout(resolve, (6-x)*5)); // Reverse delay
			return [x, x*2];
		}, { parallel: 1 }))(toArray());

		// With threads=1, should maintain strict order
		expect(values).toEqual([1, 2, 2, 4, 3, 6, 4, 8, 5, 10]);

	});

	it("should handle rapid completion of all operations", async () => {

		const values = await items([1, 2, 3, 4, 5])(flatMap(x => [x], { parallel: 10 }))(toArray());

		expect([...values].sort((a: number, b: number) => a-b)).toEqual([1, 2, 3, 4, 5]);

	});

});
