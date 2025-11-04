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
import { items, range } from "./feeds";
import { count, every, find, forEach, reduce, some, toArray, toMap, toSet, toString } from "./sinks";
import { filter, map } from "./tasks";


describe("some()", () => {

	it("should return true when any item matches", async () => {

		const result = await items([1, 2, 3, 4])(some(x => x > 3));

		expect(result).toBe(true);

	});

	it("should return false when no items match", async () => {

		const result = await items([1, 2, 3])(some(x => x > 10));

		expect(result).toBe(false);

	});

	it("should support async predicates", async () => {

		const result = await items([1, 2, 3])(some(async x => {
			await Promise.resolve();
			return x === 2;
		}));

		expect(result).toBe(true);

	});

	it("should terminate infinite generator when match found", async () => {

		let generatorCalls = 0;
		let iteratorReturned = false;

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

		const result = await infiniteGenerator(some(x => x > 5));

		expect(result).toBe(true);
		expect(generatorCalls).toBe(7); // Checked items 0-6 (6 is first > 5)
		expect(iteratorReturned).toBe(true); // Generator was properly cleaned up

	});

});

describe("every()", () => {

	it("should return true when all items match", async () => {

		const result = await items([1, 2, 3, 4])(every(x => x > 0));

		expect(result).toBe(true);

	});

	it("should return false when any item doesn't match", async () => {

		const result = await items([1, 2, 3, 4])(every(x => x < 3));

		expect(result).toBe(false);

	});

	it("should support async predicates", async () => {

		const result = await items([1, 2, 3])(every(async x => {
			await Promise.resolve();
			return x > 0;
		}));

		expect(result).toBe(true);

	});

	it("should terminate infinite generator when predicate fails", async () => {

		let generatorCalls = 0;
		let iteratorReturned = false;

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

		const result = await infiniteGenerator(every(x => x < 5));

		expect(result).toBe(false);
		expect(generatorCalls).toBe(6); // Checked items 0-5 (5 is first that fails x < 5)
		expect(iteratorReturned).toBe(true); // Generator was properly cleaned up

	});

	it("should return true for empty stream", async () => {

		const result = await items([] as number[])(every(x => x > 10));

		expect(result).toBe(true);

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

describe("find()", () => {

	it("should find first matching item", async () => {

		const result = await items([1, 2, 3, 4, 5])(find(x => x > 2));

		expect(result).toBe(3);

	});

	it("should return undefined when no match", async () => {

		const result = await items([1, 2, 3])(find(x => x > 10));

		expect(result).toBeUndefined();

	});

	it("should support async predicates", async () => {

		const result = await items([1, 2, 3, 4])(find(async x => {
			await Promise.resolve();
			return x === 3;
		}));

		expect(result).toBe(3);

	});

	it("should terminate infinite generator when match found", async () => {

		let generatorCalls = 0;
		let iteratorReturned = false;

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

		const result = await infiniteGenerator(find(x => x === 3));

		expect(result).toBe(3);
		expect(generatorCalls).toBe(4); // Checked items 0, 1, 2, 3
		expect(iteratorReturned).toBe(true); // Generator was properly cleaned up

	});

});

describe("forEach()", () => {

	it("should execute consumer for each item", async () => {

		const sideEffects: number[] = [];

		const count = await items([1, 2, 3])(forEach(x => {
			sideEffects.push(x);
		}));

		expect(sideEffects).toEqual([1, 2, 3]);
		expect(count).toBe(3);

	});

	it("should support async consumers", async () => {

		const sideEffects: number[] = [];

		const count = await items([1, 2, 3])(forEach(async x => {
			await Promise.resolve();
			sideEffects.push(x*2);
		}));

		expect(sideEffects).toEqual([2, 4, 6]);
		expect(count).toBe(3);

	});

	it("should handle empty stream", async () => {

		const sideEffects: number[] = [];

		const count = await items([] as number[])(forEach(x => {
			sideEffects.push(x);
		}));

		expect(sideEffects).toEqual([]);
		expect(count).toBe(0);

	});

});

describe("reduce()", () => {

	it("should reduce with initial value", async () => {

		const sum = await items([1, 2, 3, 4])(reduce((acc, x) => acc+x, 0));

		expect(sum).toBe(10);

	});

	it("should reduce without initial value", async () => {

		const sum = await items([1, 2, 3, 4])(reduce((acc, x) => acc+x));

		expect(sum).toBe(10);

	});

	it("should return undefined for empty stream without initial", async () => {

		const result = await items([] as number[])(reduce((acc, x) => acc+x));

		expect(result).toBeUndefined();

	});

	it("should return initial for empty stream with initial", async () => {

		const result = await items([] as number[])(reduce((acc, x) => acc+x, 100));

		expect(result).toBe(100);

	});

	it("should support async reducers", async () => {

		const sum = await items([1, 2, 3])(reduce(async (acc, x) => {
			await Promise.resolve();
			return acc+x;
		}, 0));

		expect(sum).toBe(6);

	});

});

describe("toArray()", () => {

	it("should collect all items into array", async () => {

		const values = await items([1, 2, 3])(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should handle empty stream", async () => {

		const values = await items([] as number[])(toArray());

		expect(values).toEqual([]);

	});

});

describe("toSet()", () => {

	it("should collect all items into set", async () => {

		const values = await items([1, 2, 3])(toSet());

		expect(values).toEqual(new Set([1, 2, 3]));

	});

	it("should remove duplicates", async () => {

		const values = await items([1, 2, 2, 3, 1, 4])(toSet());

		expect(values).toEqual(new Set([1, 2, 3, 4]));

	});

	it("should handle empty stream", async () => {

		const values = await items([] as number[])(toSet());

		expect(values).toEqual(new Set());

	});

	it("should preserve insertion order", async () => {

		const values = await items([3, 1, 2])(toSet());

		expect([...values]).toEqual([3, 1, 2]);

	});

});

describe("toMap()", () => {

	it("should collect items into map using key selector", async () => {

		const values = await items([
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

		const values = await items([
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

		const values = await items([
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

		const values = await items([
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

		const values = await items([
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

		const values = await items([] as { id: number; name: string }[])(toMap(item => item.id));

		expect(values).toEqual(new Map());

	});

	it("should preserve insertion order", async () => {

		const values = await items([3, 1, 2])(toMap(x => x));

		expect([...values.keys()]).toEqual([3, 1, 2]);

	});

});

describe("toString()", () => {

	it("should join items with comma separator by default", async () => {

		const result = await items([1, 2, 3])(toString());

		expect(result).toBe("1,2,3");

	});

	it("should join string items", async () => {

		const result = await items(["a", "b", "c"])(toString());

		expect(result).toBe("a,b,c");

	});

	it("should handle undefined separator as default", async () => {

		const result = await items([1, 2, 3])(toString(undefined));

		expect(result).toBe("1,2,3");

	});

	it("should handle custom separator", async () => {

		const result = await items([1, 2, 3])(toString(" - "));

		expect(result).toBe("1 - 2 - 3");

	});

	it("should handle empty separator", async () => {

		const result = await items(["a", "b", "c"])(toString(""));

		expect(result).toBe("abc");

	});

	it("should handle empty stream", async () => {

		const result = await items([] as number[])(toString());

		expect(result).toBe("");

	});

	it("should handle single item", async () => {

		const result = await items([42])(toString());

		expect(result).toBe("42");

	});

	it("should convert objects to strings", async () => {

		const result = await items([{ id: 1 }, { id: 2 }])(toString("|"));

		expect(result).toBe("[object Object]|[object Object]");

	});

	it("should handle null and undefined", async () => {

		// Note: undefined values are automatically filtered out by items()
		const result = await items([1, null, undefined, 2])(toString(","));

		expect(result).toBe("1,,2");

	});

	it("should handle items after filtering", async () => {

		const result = await items([1, 2, 3, 4, 5, 6])(filter(x => x%2 === 0))(toString("-"));

		expect(result).toBe("2-4-6");

	});

	it("should handle items after mapping", async () => {

		const result = await items([1, 2, 3])(map(x => x*10))(toString(" "));

		expect(result).toBe("10 20 30");

	});

	it("should work with boolean values", async () => {

		const result = await items([true, false, true])(toString(","));

		expect(result).toBe("true,false,true");

	});

	it("should handle newline separator", async () => {

		const result = await items(["line1", "line2", "line3"])(toString("\n"));

		expect(result).toBe("line1\nline2\nline3");

	});

});
