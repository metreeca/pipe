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
import {
	batch,
	chain,
	count,
	distinct,
	every,
	filter,
	find,
	flatMap,
	forEach,
	items,
	map,
	merge,
	peek,
	pipe,
	Pipe,
	range,
	reduce,
	skip,
	some,
	take,
	toArray,
	toMap,
	toSet
} from ".";


describe("Pipes", () => {

	it("should return promise value directly", async () => {

		const value=await pipe(Promise.resolve(42));

		expect(value).toBe(42);

	});

	it("should retrieve async iterable from pipe", async () => {

		const values=await items(pipe(items(range(1, 4))))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should return async iterable for manual iteration", async () => {

		const iterable=pipe((items([1, 2, 3, 4]))(filter(x => x > 1)));

		const values: number[]=[];

		for await (const value of iterable) {
			values.push(value);
		}

		expect(values).toEqual([2, 3, 4]);

	});

});

describe("Feeds", () => {

	describe("range()", () => {

		it("should generate ascending range", async () => {

			const values=await range(1, 5)(toArray());

			expect(values).toEqual([1, 2, 3, 4]);

		});

		it("should generate descending range", async () => {

			const values=await range(5, 1)(toArray());

			expect(values).toEqual([5, 4, 3, 2]);

		});

		it("should generate empty range when start equals end", async () => {

			const values=await range(3, 3)(toArray());

			expect(values).toEqual([]);

		});

		it("should work with negative numbers", async () => {

			const values=await range(-2, 2)(toArray());

			expect(values).toEqual([-2, -1, 0, 1]);

		});

		describe("should create a compliant pipe object", () => {

			it("should return async iterable when called without transform", async () => {
				expect(await pipe(range(1, 4)(toArray()))).toEqual([1, 2, 3]);
			});

			it("should apply task and return new pipe", async () => {
				expect(await pipe(range(1, 4)(map(x => x*2))(toArray()))).toEqual([2, 4, 6]);
			});

			it("should apply sink and return promise", async () => {
				expect(await range(1, 4)(reduce((acc, x) => acc+x, 0))).toBe(6);
			});

			it("should chain multiple tasks", async () => {
				expect(await pipe(
					range(1, 6)
					(filter(x => x%2 === 0))
					(map(x => x*2))
					(toArray())
				)).toEqual([4, 8]);
			});

		});

	});

	describe("items()", () => {

		it("should create pipe from single value", async () => {

			const values=await items(42)(toArray());

			expect(values).toEqual([42]);

		});

		it("should create pipe from array", async () => {

			const values=await items([1, 2, 3])(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		it("should create pipe from iterable", async () => {

			const values=await items(new Set([1, 2, 3]))(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		it("should create pipe from async iterable", async () => {

			const values=await items(range(1, 4))(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		describe("should create a compliant pipe object", () => {

			it("should return async iterable when called without transform", async () => {
				expect(await pipe(items(items([1, 2, 3])())(toArray()))).toEqual([1, 2, 3]);
			});

			it("should apply task and return new pipe", async () => {
				expect(await pipe(items([1, 2, 3])(map(x => x*2))(toArray()))).toEqual([2, 4, 6]);
			});

			it("should apply sink and return promise", async () => {
				expect(await items([1, 2, 3])(reduce((acc, x) => acc+x, 0))).toBe(6);
			});

			it("should chain multiple tasks", async () => {
				expect(await pipe(
					items([1, 2, 3, 4, 5])
					(filter(x => x%2 === 0))
					(map(x => x*2))
					(toArray())
				)).toEqual([4, 8]);
			});

		});

		describe("should filter undefined values", () => {

			it("should filter undefined from array feed", async () => {

				const result = await items([1, undefined, 2, undefined, 3])(toArray());

				expect(result).toEqual([1, 2, 3]);

			});

			it("should filter undefined from single value feed", async () => {

				const result = await items(undefined)(toArray());

				expect(result).toEqual([]);

			});

			it("should filter undefined from function feed", async () => {

				const result = await items(() => [10, undefined, 20, undefined])(toArray());

				expect(result).toEqual([10, 20]);

			});

			it("should filter undefined from iterable feed", async () => {

				const iterable = {
					* [Symbol.iterator]() {
						yield "a";
						yield undefined;
						yield "b";
						yield undefined;
						yield "c";
					}
				};

				const result = await items(iterable)(toArray());

				expect(result).toEqual(["a", "b", "c"]);

			});

			it("should filter undefined from async iterable feed", async () => {

				async function* gen() {
					yield "x";
					yield undefined;
					yield "y";
					yield undefined;
					yield "z";
				}

				const result = await items(gen())(toArray());

				expect(result).toEqual(["x", "y", "z"]);

			});

			it("should handle all undefined values", async () => {

				const result = await items([undefined, undefined, undefined])(toArray());

				expect(result).toEqual([]);

			});

			it("should preserve falsy values that are not undefined", async () => {

				const result = await items([0, false, "", null, undefined])(toArray());

				expect(result).toEqual([0, false, "", null]);

			});

			it("should filter undefined through chained operations", async () => {

				const result = await items([1, undefined, 2, undefined, 3] as number[])
				(map(x => x*2))
				(toArray());

				expect(result).toEqual([2, 4, 6]);

			});

			it("should filter undefined from custom task output", async () => {

				const parseNumbers = async function* (source: AsyncIterable<string>) {
					for await (const item of source) {
						const num = parseInt(item);
						yield isNaN(num) ? undefined : num;
					}
				};

				const result = await items(["1", "abc", "2", "xyz", "3"])(parseNumbers)(toArray());

				expect(result).toEqual([1, 2, 3]);

			});

		});

	});

	describe("merge()", () => {

		it("should merge multiple pipes", async () => {

			const values=await merge(range(1, 3), range(10, 12))(toArray());

			expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);

		});

		it("should handle empty pipes", async () => {

			const values=await merge(range(1, 1), range(2, 2))(toArray());

			expect(values).toEqual([]);

		});

		it("should clean up iterators on early termination", async () => {

			const cleanup: string[]=[];

			function tracked(name: string): Pipe<number> {
				return items((async function* () {
					try {
						yield 1;
						yield 2;
					} finally {
						cleanup.push(name);
					}
				})());
			}

			const merged=merge(tracked("a"), tracked("b"));
			const iterator=merged()[Symbol.asyncIterator]();

			await iterator.next();
			await iterator.return?.();

			expect(cleanup.length).toBe(2);

		});

		describe("should create a compliant pipe object", () => {

			it("should return async iterable when called without transform", async () => {
				const values=await pipe(merge(range(1, 3), range(10, 12))(toArray()));
				expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);
			});

			it("should apply task and return new pipe", async () => {
				const values=await pipe(merge(range(1, 3), range(10, 12))(map(x => x*2))(toArray()));
				expect([...values].sort((a, b) => a-b)).toEqual([2, 4, 20, 22]);
			});

			it("should apply sink and return promise", async () => {
				expect(await merge(range(1, 3), range(10, 12))(reduce((acc, x) => acc+x, 0))).toBe(24);
			});

			it("should chain multiple tasks", async () => {
				const values=await pipe(
					merge(range(1, 4), range(10, 13))
					(filter(x => x%2 === 0))
					(map(x => x*2))
					(toArray())
				);
				expect([...values].sort((a, b) => a-b)).toEqual([4, 20, 24]);
			});

		});

	});

	describe("chain()", () => {

		it("should chain multiple pipes in order", async () => {

			const values=await chain(range(1, 3), range(10, 12))(toArray());

			expect(values).toEqual([1, 2, 10, 11]);

		});

		it("should preserve source order", async () => {

			const values=await chain(range(5, 7), range(1, 3), range(10, 12))(toArray());

			expect(values).toEqual([5, 6, 1, 2, 10, 11]);

		});

		it("should handle empty pipes", async () => {

			const values=await chain(range(1, 1), range(2, 2))(toArray());

			expect(values).toEqual([]);

		});

		it("should handle single pipe", async () => {

			const values=await chain(range(1, 4))(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		it("should fully consume each source before next", async () => {

			const order: string[]=[];

			function tracked(name: string, values: number[]): Pipe<number> {
				return items((async function* () {
					for (const item of values) {
						order.push(`${name}:${item}`);
						yield item;
					}
				})());
			}

			await chain(tracked("a", [1, 2]), tracked("b", [3, 4]))(toArray());

			expect(order).toEqual(["a:1", "a:2", "b:3", "b:4"]);

		});

		describe("should create a compliant pipe object", () => {

			it("should return async iterable when called without transform", async () => {
				const values=await pipe(chain(range(1, 3), range(10, 12))(toArray()));
				expect(values).toEqual([1, 2, 10, 11]);
			});

			it("should apply task and return new pipe", async () => {
				const values=await pipe(chain(range(1, 3), range(10, 12))(map(x => x*2))(toArray()));
				expect(values).toEqual([2, 4, 20, 22]);
			});

			it("should apply sink and return promise", async () => {
				expect(await chain(range(1, 3), range(10, 12))(reduce((acc, x) => acc+x, 0))).toBe(24);
			});

			it("should chain multiple tasks", async () => {
				const values=await pipe(
					chain(range(1, 4), range(10, 13))
					(filter(x => x%2 === 0))
					(map(x => x*2))
					(toArray())
				);
				expect(values).toEqual([4, 20, 24]);
			});

		});

	});

});

describe("Tasks", () => {

	describe("skip()", () => {

		it("should skip first n items", async () => {

			const values=await items([1, 2, 3, 4, 5])(skip(2))(toArray());

			expect(values).toEqual([3, 4, 5]);

		});

		it("should skip all items when n >= length", async () => {

			const values=await items([1, 2, 3])(skip(5))(toArray());

			expect(values).toEqual([]);

		});

		it("should skip zero items", async () => {

			const values=await items([1, 2, 3])(skip(0))(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		it("should treat negative n as zero", async () => {

			const values=await items([1, 2, 3])(skip(-5))(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

	});

	describe("take()", () => {

		it("should take first n items", async () => {

			const values=await items([1, 2, 3, 4, 5])(take(3))(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		it("should take all items when n >= length", async () => {

			const values=await items([1, 2, 3])(take(5))(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		it("should take zero items", async () => {

			const values=await items([1, 2, 3])(take(0))(toArray());

			expect(values).toEqual([]);

		});

		it("should treat negative n as zero", async () => {

			const values=await items([1, 2, 3])(take(-5))(toArray());

			expect(values).toEqual([]);

		});

	});

	describe("peek()", () => {

		it("should execute side effect for each item", async () => {

			const sideEffects: number[]=[];
			const values=await items([1, 2, 3])(peek(x => {
				sideEffects.push(x*10);
			}))(toArray());

			expect(values).toEqual([1, 2, 3]);
			expect(sideEffects).toEqual([10, 20, 30]);

		});

		it("should support async consumers", async () => {

			const sideEffects: number[]=[];
			const values=await items([1, 2, 3])(peek(async x => {
				await Promise.resolve();
				sideEffects.push(x);
			}))(toArray());

			expect(values).toEqual([1, 2, 3]);
			expect(sideEffects).toEqual([1, 2, 3]);

		});

	});

	describe("filter()", () => {

		it("should filter items by predicate", async () => {

			const values=await items([1, 2, 3, 4, 5])(filter(x => x%2 === 0))(toArray());

			expect(values).toEqual([2, 4]);

		});

		it("should support async predicates", async () => {

			const values=await items([1, 2, 3, 4, 5])(filter(async x => {
				await Promise.resolve();
				return x > 2;
			}))(toArray());

			expect(values).toEqual([3, 4, 5]);

		});

		it("should handle empty results", async () => {

			const values=await items([1, 2, 3])(filter(() => false))(toArray());

			expect(values).toEqual([]);

		});

	});

	describe("distinct()", () => {

		it("should filter out duplicate primitives", async () => {

			const values=await items([1, 2, 2, 3, 1, 4])(distinct())(toArray());

			expect(values).toEqual([1, 2, 3, 4]);

		});

		it("should use selector for comparison", async () => {

			const values=await items([
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

			const values=await items([] as number[])(distinct())(toArray());

			expect(values).toEqual([]);

		});

	});

	describe("sequential map()", () => {

		it("should transform items", async () => {

			const values=await items([1, 2, 3])(map(x => x*2))(toArray());

			expect(values).toEqual([2, 4, 6]);

		});

		it("should support async mappers", async () => {

			const values=await items([1, 2, 3])(map(async x => {
				await Promise.resolve();
				return x*2;
			}))(toArray());

			expect(values).toEqual([2, 4, 6]);

		});

		it("should change item types", async () => {

			const values=await items([1, 2, 3])(map(x => `value-${x}`))(toArray());

			expect(values).toEqual(["value-1", "value-2", "value-3"]);

		});

	});

	describe("sequential flatMap()", () => {

		it("should flatten mapped async iterables", async () => {

			const values=await items([1, 2, 3])(flatMap(async function* (x) {
				yield x;
				yield x*10;
			}))(toArray());

			expect(values).toEqual([1, 10, 2, 20, 3, 30]);

		});

		it("should handle empty iterables", async () => {

			const values=await items([1, 2, 3])(flatMap(async function* (x) {
				if ( x === 2 ) {
					yield x;
				}
			}))(toArray());

			expect(values).toEqual([2]);

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

	});

	describe("batch()", () => {

		it("should group items into batches of specified size", async () => {

			const values=await items([1, 2, 3, 4, 5])(batch(2))(toArray());

			expect(values).toEqual([[1, 2], [3, 4], [5]]);

		});

		it("should collect all items when size is 0", async () => {

			const values=await items([1, 2, 3, 4, 5])(batch(0))(toArray());

			expect(values).toEqual([[1, 2, 3, 4, 5]]);

		});

		it("should handle empty stream", async () => {

			const values=await items([] as number[])(batch(2))(toArray());

			expect(values).toEqual([]);

		});

		it("should yield final partial batch", async () => {

			const values=await items([1, 2, 3])(batch(2))(toArray());

			expect(values).toEqual([[1, 2], [3]]);

		});

		it("should create individual batches when size is 1", async () => {

			const values=await items([1, 2, 3, 4])(batch(1))(toArray());

			expect(values).toEqual([[1], [2], [3], [4]]);

		});

		it("should process batches through pipeline", async () => {

			const result=await items([1, 2, 3, 4, 5, 6, 7])
			(batch(3))
			(map(batch => batch.reduce((sum, n) => sum+n, 0)))
			(toArray());

			expect(result).toEqual([6, 15, 7]);

		});

	});

});

describe("Sinks", () => {

	describe("reduce()", () => {

		it("should reduce with initial value", async () => {

			const sum=await items([1, 2, 3, 4])(reduce((acc, x) => acc+x, 0));

			expect(sum).toBe(10);

		});

		it("should reduce without initial value", async () => {

			const sum=await items([1, 2, 3, 4])(reduce((acc, x) => acc+x));

			expect(sum).toBe(10);

		});

		it("should return undefined for empty stream without initial", async () => {

			const result=await items([] as number[])(reduce((acc, x) => acc+x));

			expect(result).toBeUndefined();

		});

		it("should return initial for empty stream with initial", async () => {

			const result=await items([] as number[])(reduce((acc, x) => acc+x, 100));

			expect(result).toBe(100);

		});

		it("should support async reducers", async () => {

			const sum=await items([1, 2, 3])(reduce(async (acc, x) => {
				await Promise.resolve();
				return acc+x;
			}, 0));

			expect(sum).toBe(6);

		});

	});

	describe("find()", () => {

		it("should find first matching item", async () => {

			const result=await items([1, 2, 3, 4, 5])(find(x => x > 2));

			expect(result).toBe(3);

		});

		it("should return undefined when no match", async () => {

			const result=await items([1, 2, 3])(find(x => x > 10));

			expect(result).toBeUndefined();

		});

		it("should support async predicates", async () => {

			const result=await items([1, 2, 3, 4])(find(async x => {
				await Promise.resolve();
				return x === 3;
			}));

			expect(result).toBe(3);

		});

	});

	describe("some()", () => {

		it("should return true when any item matches", async () => {

			const result=await items([1, 2, 3, 4])(some(x => x > 3));

			expect(result).toBe(true);

		});

		it("should return false when no items match", async () => {

			const result=await items([1, 2, 3])(some(x => x > 10));

			expect(result).toBe(false);

		});

		it("should support async predicates", async () => {

			const result=await items([1, 2, 3])(some(async x => {
				await Promise.resolve();
				return x === 2;
			}));

			expect(result).toBe(true);

		});

	});

	describe("every()", () => {

		it("should return true when all items match", async () => {

			const result=await items([1, 2, 3, 4])(every(x => x > 0));

			expect(result).toBe(true);

		});

		it("should return false when any item doesn't match", async () => {

			const result=await items([1, 2, 3, 4])(every(x => x < 3));

			expect(result).toBe(false);

		});

		it("should support async predicates", async () => {

			const result=await items([1, 2, 3])(every(async x => {
				await Promise.resolve();
				return x > 0;
			}));

			expect(result).toBe(true);

		});

		it("should return true for empty stream", async () => {

			const result=await items([] as number[])(every(x => x > 10));

			expect(result).toBe(true);

		});

	});

	describe("toArray()", () => {

		it("should collect all items into array", async () => {

			const values=await items([1, 2, 3])(toArray());

			expect(values).toEqual([1, 2, 3]);

		});

		it("should handle empty stream", async () => {

			const values=await items([] as number[])(toArray());

			expect(values).toEqual([]);

		});

	});

	describe("toSet()", () => {

		it("should collect all items into set", async () => {

			const values=await items([1, 2, 3])(toSet());

			expect(values).toEqual(new Set([1, 2, 3]));

		});

		it("should remove duplicates", async () => {

			const values=await items([1, 2, 2, 3, 1, 4])(toSet());

			expect(values).toEqual(new Set([1, 2, 3, 4]));

		});

		it("should handle empty stream", async () => {

			const values=await items([] as number[])(toSet());

			expect(values).toEqual(new Set());

		});

		it("should preserve insertion order", async () => {

			const values=await items([3, 1, 2])(toSet());

			expect([...values]).toEqual([3, 1, 2]);

		});

	});

	describe("toMap()", () => {

		it("should collect items into map using key selector", async () => {

			const values=await items([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
				{ id: 3, name: "c" }
			])(toMap(item => item.id));

			expect(values).toEqual(new Map([
				[1, { id: 1, name: "a" }],
				[2, { id: 2, name: "b" }],
				[3, { id: 3, name: "c" }]
			]));

		});

		it("should collect items into map using key and value selectors", async () => {

			const values=await items([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
				{ id: 3, name: "c" }
			])(toMap(item => item.id, item => item.name));

			expect(values).toEqual(new Map([
				[1, "a"],
				[2, "b"],
				[3, "c"]
			]));

		});

		it("should support async key selectors", async () => {

			const values=await items([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" }
			])(toMap(async item => {
				await Promise.resolve();
				return item.id;
			}));

			expect(values).toEqual(new Map([
				[1, { id: 1, name: "a" }],
				[2, { id: 2, name: "b" }]
			]));

		});

		it("should support async value selectors", async () => {

			const values=await items([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" }
			])(toMap(
				item => item.id,
				async item => {
					await Promise.resolve();
					return item.name.toUpperCase();
				}
			));

			expect(values).toEqual(new Map([
				[1, "A"],
				[2, "B"]
			]));

		});

		it("should overwrite duplicate keys", async () => {

			const values=await items([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
				{ id: 1, name: "c" }
			])(toMap(item => item.id, item => item.name));

			expect(values).toEqual(new Map([
				[1, "c"],
				[2, "b"]
			]));

		});

		it("should handle empty stream", async () => {

			const values=await items([] as { id: number; name: string }[])(toMap(item => item.id));

			expect(values).toEqual(new Map());

		});

		it("should preserve insertion order", async () => {

			const values=await items([3, 1, 2])(toMap(x => x));

			expect([...values.keys()]).toEqual([3, 1, 2]);

		});

	});

	describe("count()", () => {

		it("should count all items in stream", async () => {

			const result = await items([1, 2, 3, 4, 5])(count());

			expect(result).toBe(5);

		});

		it("should return zero for empty stream", async () => {

			const result = await items([] as number[])(count());

			expect(result).toBe(0);

		});

		it("should count items after filtering", async () => {

			const result = await items([1, 2, 3, 4, 5, 6])(filter(x => x%2 === 0))(count());

			expect(result).toBe(3);

		});

		it("should count items in range", async () => {

			const result = await range(1, 101)(count());

			expect(result).toBe(100);

		});

		it("should count items after mapping", async () => {

			const result = await items([1, 2, 3])(map(x => x*2))(count());

			expect(result).toBe(3);

		});

	});

	describe("forEach()", () => {

		it("should execute consumer for each item", async () => {

			const sideEffects: number[]=[];

			await items([1, 2, 3])(forEach(x => {
				sideEffects.push(x);
			}));

			expect(sideEffects).toEqual([1, 2, 3]);

		});

		it("should support async consumers", async () => {

			const sideEffects: number[]=[];

			await items([1, 2, 3])(forEach(async x => {
				await Promise.resolve();
				sideEffects.push(x*2);
			}));

			expect(sideEffects).toEqual([2, 4, 6]);

		});

		it("should handle empty stream", async () => {

			const sideEffects: number[]=[];

			await items([] as number[])(forEach(x => {
				sideEffects.push(x);
			}));

			expect(sideEffects).toEqual([]);

		});

	});

});
