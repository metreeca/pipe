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
import { Pipe, pipe } from ".";
import { chain, items, iterate, merge, range } from "./feeds";
import { reduce, toArray } from "./sinks";
import { filter, map } from "./tasks";


describe("items()", () => {

	it("should create pipe from single value", async () => {

		const values = await items(42)(toArray());

		expect(values).toEqual([42]);

	});

	it("should create pipe from array", async () => {

		const values = await items([1, 2, 3])(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should create pipe from iterable", async () => {

		const values = await items(new Set([1, 2, 3]))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should create pipe from async iterable", async () => {

		const values = await items(range(1, 4))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should create pipe from Promise resolving to value", async () => {

		const values = await items(Promise.resolve(42))(toArray());

		expect(values).toEqual([42]);

	});

	it("should create pipe from Promise resolving to array", async () => {

		const values = await items(Promise.resolve([1, 2, 3]))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should create pipe from Promise resolving to iterable", async () => {

		const values = await items(Promise.resolve(new Set([1, 2, 3])))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should create pipe from Promise resolving to async iterable", async () => {

		const values = await items(Promise.resolve(range(1, 4)()))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should handle Promise resolving to undefined", async () => {

		const values = await items(Promise.resolve(undefined))(toArray());

		expect(values).toEqual([]);

	});

	it("should handle delayed Promise", async () => {

		const delayedData = new Promise<number[]>(resolve => {
			setTimeout(() => resolve([1, 2, 3]), 10);
		});

		const values = await items(delayedData)(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should handle Promise with async operations in pipeline", async () => {

		const values = await items(Promise.resolve([1, 2, 3]))
		(map(async x => {
			await new Promise(resolve => setTimeout(resolve, 10));
			return x*2;
		}))
		(toArray());

		expect(values).toEqual([2, 4, 6]);

	});

	it("should filter undefined from Promise-resolved data", async () => {

		const values = await items(Promise.resolve([1, undefined, 2, undefined, 3]))(toArray());

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

	describe("should treat strings as atomic values", () => {

		it("should yield string as single item, not character by character", async () => {

			const result = await items("hello")(toArray());

			expect(result).toEqual(["hello"]);

		});

		it("should yield empty string as single item", async () => {

			const result = await items("")(toArray());

			expect(result).toEqual([""]);

		});

		it("should yield strings from array individually", async () => {

			const result = await items(["foo", "bar", "baz"])(toArray());

			expect(result).toEqual(["foo", "bar", "baz"]);

		});

	});

	describe("should accept multiple scalar values", () => {

		it("should create pipe from variadic number arguments", async () => {

			const result = await items(1, 2, 3, 4)(toArray());

			expect(result).toEqual([1, 2, 3, 4]);

		});

		it("should create pipe from variadic string arguments", async () => {

			const result = await items("a", "b", "c")(toArray());

			expect(result).toEqual(["a", "b", "c"]);

		});

		it("should create pipe from variadic mixed arguments", async () => {

			const result = await items<number | string>(1, "a", 2, "b")(toArray());

			expect(result).toEqual([1, "a", 2, "b"]);

		});

		it("should filter undefined from variadic arguments", async () => {

			const result = await items<number | undefined>(1, undefined, 2, undefined, 3)(toArray());

			expect(result).toEqual([1, 2, 3]);

		});

		it("should preserve falsy values in variadic arguments", async () => {

			const result = await items<number | boolean | string | null | undefined>(
				0, false, "", null, undefined
			)(toArray());

			expect(result).toEqual([0, false, "", null]);

		});

		it("should work with single scalar argument", async () => {

			const result = await items(42)(toArray());

			expect(result).toEqual([42]);

		});

		it("should maintain backward compatibility with array argument", async () => {

			const result = await items([1, 2, 3])(toArray());

			expect(result).toEqual([1, 2, 3]);

		});

	});

});

describe("range()", () => {

	it("should generate ascending range", async () => {

		const values = await range(1, 5)(toArray());

		expect(values).toEqual([1, 2, 3, 4]);

	});

	it("should generate descending range", async () => {

		const values = await range(5, 1)(toArray());

		expect(values).toEqual([5, 4, 3, 2]);

	});

	it("should generate empty range when start equals end", async () => {

		const values = await range(3, 3)(toArray());

		expect(values).toEqual([]);

	});

	it("should work with negative numbers", async () => {

		const values = await range(-2, 2)(toArray());

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

describe("iterate()", () => {

	it("should repeatedly call generator until undefined", async () => {

		function counter() {
			let count = 0;
			return () => count >= 3 ? undefined : count++;
		}

		const values = await iterate(counter())(toArray());

		expect(values).toEqual([0, 1, 2]);

	});

	it("should stop on empty array", async () => {

		function counter() {
			let count = 0;
			return () => count >= 2 ? [] : [count++];
		}

		const values = await iterate(counter())(toArray());

		expect(values).toEqual([0, 1]);

	});

	it("should stop on empty iterator", async () => {

		function counter() {
			let count = 0;
			return () => count >= 2 ? new Set() : new Set([count++]);
		}

		const values = await iterate(counter())(toArray());

		expect(values).toEqual([0, 1]);

	});

	it("should flatten arrays from each call", async () => {

		function pager() {
			let page = 0;
			return () => {
				if ( page >= 3 ) {
					return undefined;
				}
				const start = page*2;
				page++;
				return [start, start+1];
			};
		}

		const values = await iterate(pager())(toArray());

		expect(values).toEqual([0, 1, 2, 3, 4, 5]);

	});

	it("should handle single values", async () => {

		function counter() {
			let count = 0;
			return () => count >= 3 ? undefined : count++;
		}

		const values = await iterate(counter())(toArray());

		expect(values).toEqual([0, 1, 2]);

	});

	it("should handle iterables", async () => {

		function pager() {
			let page = 0;
			return () => {
				if ( page >= 2 ) {
					return undefined;
				}
				const start = page*2;
				page++;
				return new Set([start, start+1]);
			};
		}

		const values = await iterate(pager())(toArray());

		expect(values).toEqual([0, 1, 2, 3]);

	});

	it("should handle pipes", async () => {

		function counter() {
			let count = 0;
			return () => count >= 2 ? undefined : range(count++, count);
		}

		const values = await iterate(counter())(toArray());

		expect(values).toEqual([0, 1]);

	});

	it("should treat strings as atomic values", async () => {

		function counter() {
			let count = 0;
			return () => count >= 3 ? undefined : `value${count++}`;
		}

		const values = await iterate(counter())(toArray());

		expect(values).toEqual(["value0", "value1", "value2"]);

	});

	it("should handle empty stream when first call returns undefined", async () => {

		const values = await iterate(() => undefined)(toArray());

		expect(values).toEqual([]);

	});

	it("should handle empty stream when first call returns empty array", async () => {

		const values = await iterate(() => [])(toArray());

		expect(values).toEqual([]);

	});

	it("should work with generator tracking state", async () => {

		const pages = ["page1", "page2", "page3"];
		let index = 0;

		const values = await iterate(() => {
			if ( index >= pages.length ) {
				return undefined;
			}
			return pages[index++];
		})(toArray());

		expect(values).toEqual(["page1", "page2", "page3"]);

	});

	it("should handle async generators", async () => {

		function asyncCounter() {
			let count = 0;
			return async () => {
				await new Promise(resolve => setTimeout(resolve, 10));
				return count >= 3 ? undefined : count++;
			};
		}

		const values = await iterate(asyncCounter())(toArray());

		expect(values).toEqual([0, 1, 2]);

	});

	it("should handle async generators with arrays", async () => {

		function asyncPager() {
			let page = 0;
			return async () => {
				await new Promise(resolve => setTimeout(resolve, 10));
				if ( page >= 3 ) {
					return undefined;
				}
				const start = page*2;
				page++;
				return [start, start+1];
			};
		}

		const values = await iterate(asyncPager())(toArray());

		expect(values).toEqual([0, 1, 2, 3, 4, 5]);

	});

	it("should handle async generators returning promises of pipes", async () => {

		function asyncCounter() {
			let count = 0;
			return async () => {
				await new Promise(resolve => setTimeout(resolve, 10));
				return count >= 2 ? undefined : range(count++, count);
			};
		}

		const values = await iterate(asyncCounter())(toArray());

		expect(values).toEqual([0, 1]);

	});

	it("should handle async generators that terminate with undefined", async () => {

		let callCount = 0;
		const values = await iterate(async () => {
			await new Promise(resolve => setTimeout(resolve, 10));
			callCount++;
			return undefined;
		})(toArray());

		expect(values).toEqual([]);
		expect(callCount).toBe(1);

	});

	it("should handle async generators that terminate with empty array", async () => {

		let callCount = 0;
		const values = await iterate(async () => {
			await new Promise(resolve => setTimeout(resolve, 10));
			callCount++;
			return [];
		})(toArray());

		expect(values).toEqual([]);
		expect(callCount).toBe(1);

	});

	it("should handle mixed sync and async patterns", async () => {

		function mixedGenerator() {
			let count = 0;
			return async () => {
				if ( count === 0 ) {
					count++;
					return 0; // synchronous value
				} else if ( count === 1 ) {
					count++;
					await new Promise(resolve => setTimeout(resolve, 10));
					return 1; // async value
				} else if ( count === 2 ) {
					count++;
					return [2, 3]; // synchronous array
				} else {
					return undefined;
				}
			};
		}

		const values = await iterate(mixedGenerator())(toArray());

		expect(values).toEqual([0, 1, 2, 3]);

	});

	describe("should create a compliant pipe object", () => {

		it("should return async iterable when called without transform", async () => {
			function counter() {
				let count = 0;
				return () => count >= 3 ? undefined : [count++];
			}

			const values = await pipe(
				iterate(counter())
				(toArray())
			);
			expect(values).toEqual([0, 1, 2]);
		});

		it("should apply task and return new pipe", async () => {
			function counter() {
				let count = 0;
				return () => count >= 3 ? undefined : count++;
			}

			const values = await pipe(
				iterate(counter())
				(map(x => x*2))
				(toArray())
			);
			expect(values).toEqual([0, 2, 4]);
		});

		it("should apply sink and return promise", async () => {
			function counter() {
				let count = 0;
				return () => count >= 4 ? undefined : count++;
			}

			expect(await pipe(
				iterate(counter())
				(reduce((acc, x) => acc+x, 0))
			)).toBe(6);
		});

		it("should chain multiple tasks", async () => {
			function counter() {
				let count = 0;
				return () => count >= 6 ? undefined : count++;
			}

			expect(await pipe(
				iterate(counter())
				(filter(x => x%2 === 0))
				(map(x => x*2))
				(toArray())
			)).toEqual([0, 4, 8]);
		});

	});

});

describe("chain()", () => {

	it("should chain multiple pipes in order", async () => {

		const values = await chain(range(1, 3), range(10, 12))(toArray());

		expect(values).toEqual([1, 2, 10, 11]);

	});

	it("should preserve source order", async () => {

		const values = await chain(range(5, 7), range(1, 3), range(10, 12))(toArray());

		expect(values).toEqual([5, 6, 1, 2, 10, 11]);

	});

	it("should handle empty pipes", async () => {

		const values = await chain(range(1, 1), range(2, 2))(toArray());

		expect(values).toEqual([]);

	});

	it("should handle single pipe", async () => {

		const values = await chain(range(1, 4))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should fully consume each source before next", async () => {

		const order: string[] = [];

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

	it("should accept arrays as data sources", async () => {

		const values = await chain([1, 2], [10, 11])(toArray());

		expect(values).toEqual([1, 2, 10, 11]);

	});

	it("should accept single values as data sources", async () => {

		const values = await chain(1, 2, 3)(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should accept mixed Data<V> types", async () => {

		const values = await chain([1, 2], items([3, 4]), 5)(toArray());

		expect(values).toEqual([1, 2, 3, 4, 5]);

	});

	it("should accept async iterables", async () => {

		async function* gen1() {
			yield 1;
			yield 2;
		}

		async function* gen2() {
			yield 10;
			yield 11;
		}

		const values = await chain(gen1(), gen2())(toArray());

		expect(values).toEqual([1, 2, 10, 11]);

	});

	it("should accept sync iterables", async () => {

		const set1 = new Set([1, 2]);
		const set2 = new Set([10, 11]);

		const values = await chain(set1, set2)(toArray());

		expect(values).toEqual([1, 2, 10, 11]);

	});

	it("should preserve order with mixed Data<V> types", async () => {

		const values = await chain([5, 6], range(1, 3), [10, 11])(toArray());

		expect(values).toEqual([5, 6, 1, 2, 10, 11]);

	});

	it("should accept Promise<Data<V>>", async () => {

		const promise1 = Promise.resolve([1, 2]);
		const promise2 = Promise.resolve([10, 11]);

		const values = await chain(promise1, promise2)(toArray());

		expect(values).toEqual([1, 2, 10, 11]);

	});

	it("should accept mixed sync and async data sources", async () => {

		const promise1 = Promise.resolve([1, 2]);
		const array = [3, 4];
		const promise2 = Promise.resolve(5);

		const values = await chain(promise1, array, promise2)(toArray());

		expect(values).toEqual([1, 2, 3, 4, 5]);

	});

	it("should preserve order when awaiting promises", async () => {

		const delayed = (ms: number, value: number[]) =>
			new Promise<number[]>(resolve => setTimeout(() => resolve(value), ms));

		const values = await chain(
			delayed(30, [1, 2]),
			delayed(10, [3, 4]),
			delayed(20, [5, 6])
		)(toArray());

		expect(values).toEqual([1, 2, 3, 4, 5, 6]);

	});

	it("should handle empty promise results", async () => {

		const values = await chain(
			Promise.resolve([]),
			Promise.resolve([1, 2]),
			Promise.resolve([])
		)(toArray());

		expect(values).toEqual([1, 2]);

	});

	it("should filter undefined values from promise results", async () => {

		const values = await chain(
			Promise.resolve([1, undefined, 2] as number[]),
			Promise.resolve([undefined, 3] as number[])
		)(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should handle promise rejections", async () => {

		const rejected = Promise.reject(new Error("test error"));
		const valid = Promise.resolve([1, 2]);

		await expect(chain(valid, rejected)(toArray())).rejects.toThrow("test error");

	});

	it("should handle complex mixed sources", async () => {

		async function* asyncGen() {
			yield 1;
			yield 2;
		}

		const values = await chain(
			Promise.resolve([3, 4]),
			asyncGen(),
			new Set([5, 6]),
			Promise.resolve(items([7, 8])),
			9
		)(toArray());

		expect(values).toEqual([3, 4, 1, 2, 5, 6, 7, 8, 9]);

	});

	it("should await each promise before processing next", async () => {

		const events: string[] = [];

		// Track when promises are awaited vs when they start
		const trackingPromise = (id: string, value: number[]) => {
			events.push(`created-${id}`);
			return new Promise<number[]>(resolve => {
				events.push(`started-${id}`);
				setTimeout(() => {
					events.push(`resolved-${id}`);
					resolve(value);
				}, 10);
			});
		};

		const p1 = trackingPromise("p1", [1]);
		const p2 = trackingPromise("p2", [2]);
		const p3 = trackingPromise("p3", [3]);

		const result = await chain(p1, p2, p3)(toArray());

		expect(result).toEqual([1, 2, 3]);

		// Verify promises were created before chain execution
		expect(events.slice(0, 6)).toEqual([
			"created-p1", "started-p1",
			"created-p2", "started-p2",
			"created-p3", "started-p3"
		]);

	});

	describe("should create a compliant pipe object", () => {

		it("should return async iterable when called without transform", async () => {
			const values = await pipe(chain(range(1, 3), range(10, 12))(toArray()));
			expect(values).toEqual([1, 2, 10, 11]);
		});

		it("should apply task and return new pipe", async () => {
			const values = await pipe(chain(range(1, 3), range(10, 12))(map(x => x*2))(toArray()));
			expect(values).toEqual([2, 4, 20, 22]);
		});

		it("should apply sink and return promise", async () => {
			expect(await chain(range(1, 3), range(10, 12))(reduce((acc, x) => acc+x, 0))).toBe(24);
		});

		it("should chain multiple tasks", async () => {
			const values = await pipe(
				chain(range(1, 4), range(10, 13))
				(filter(x => x%2 === 0))
				(map(x => x*2))
				(toArray())
			);
			expect(values).toEqual([4, 20, 24]);
		});

	});

});
describe("merge()", () => {

	it("should merge multiple pipes", async () => {

		const values = await merge(range(1, 3), range(10, 12))(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);

	});

	it("should handle empty pipes", async () => {

		const values = await merge(range(1, 1), range(2, 2))(toArray());

		expect(values).toEqual([]);

	});

	it("should clean up iterators on early termination", async () => {

		const cleanup: string[] = [];

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

		const merged = merge(tracked("a"), tracked("b"));
		const iterator = merged()[Symbol.asyncIterator]();

		await iterator.next();
		await iterator.return?.();

		expect(cleanup.length).toBe(2);

	});

	it("should accept arrays as data sources", async () => {

		const values = await merge([1, 2], [10, 11])(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);

	});

	it("should accept single values as data sources", async () => {

		const values = await merge(1, 2, 3)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 3]);

	});

	it("should accept mixed Data<V> types", async () => {

		const values = await merge([1, 2], items([3, 4]), 5)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 3, 4, 5]);

	});

	it("should accept async iterables", async () => {

		async function* gen1() {
			yield 1;
			yield 2;
		}

		async function* gen2() {
			yield 10;
			yield 11;
		}

		const values = await merge(gen1(), gen2())(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);

	});

	it("should accept sync iterables", async () => {

		const set1 = new Set([1, 2]);
		const set2 = new Set([10, 11]);

		const values = await merge(set1, set2)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);

	});

	it("should accept Promise<Data<V>>", async () => {

		const promise1 = Promise.resolve([1, 2]);
		const promise2 = Promise.resolve([10, 11]);

		const values = await merge(promise1, promise2)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);

	});

	it("should accept mixed sync and async data sources", async () => {

		const promise1 = Promise.resolve([1, 2]);
		const array = [3, 4];
		const promise2 = Promise.resolve(5);

		const values = await merge(promise1, array, promise2)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 3, 4, 5]);

	});

	it("should handle empty promise results", async () => {

		const values = await merge(
			Promise.resolve([]),
			Promise.resolve([1, 2])
		)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2]);

	});

	it("should filter undefined values from promise results", async () => {

		const values = await merge(
			Promise.resolve([1, undefined, 2] as number[]),
			Promise.resolve([undefined, 3] as number[])
		)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 3]);

	});

	it("should handle promise rejections", async () => {

		const rejected = Promise.reject(new Error("test error"));
		const valid = Promise.resolve([1, 2]);

		await expect(merge(rejected, valid)(toArray())).rejects.toThrow("test error");

	});

	it("should handle complex mixed sources", async () => {

		async function* asyncGen() {
			yield 1;
			yield 2;
		}

		const values = await merge(
			Promise.resolve([3, 4]),
			asyncGen(),
			new Set([5, 6]),
			Promise.resolve(items([7, 8])),
			9
		)(toArray());

		expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);

	});

	describe("should create a compliant pipe object", () => {

		it("should return async iterable when called without transform", async () => {
			const values = await pipe(merge(range(1, 3), range(10, 12))(toArray()));
			expect([...values].sort((a, b) => a-b)).toEqual([1, 2, 10, 11]);
		});

		it("should apply task and return new pipe", async () => {
			const values = await pipe(merge(range(1, 3), range(10, 12))(map(x => x*2))(toArray()));
			expect([...values].sort((a, b) => a-b)).toEqual([2, 4, 20, 22]);
		});

		it("should apply sink and return promise", async () => {
			expect(await merge(range(1, 3), range(10, 12))(reduce((acc, x) => acc+x, 0))).toBe(24);
		});

		it("should chain multiple tasks", async () => {
			const values = await pipe(
				merge(range(1, 4), range(10, 13))
				(filter(x => x%2 === 0))
				(map(x => x*2))
				(toArray())
			);
			expect([...values].sort((a, b) => a-b)).toEqual([4, 20, 24]);
		});

	});

});

