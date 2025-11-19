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
import { items } from "./feeds";
import { toArray } from "./sinks";
import { filter } from "./tasks";
import { and, not, or } from "./tests";


describe("not()", () => {

	it("should negate a predicate", () => {

		const isEven = (n: number) => n%2 === 0;
		const isOdd = not(isEven);

		expect(isOdd(3)).toBe(true);
		expect(isOdd(4)).toBe(false);

	});

	it("should work with filter task", async () => {

		const isEven = (n: number) => n%2 === 0;
		const values = await items([1, 2, 3, 4, 5])(filter(not(isEven)))(toArray());

		expect(values).toEqual([1, 3, 5]);

	});

	it("should handle boolean predicates", () => {

		const alwaysTrue = () => true;
		const alwaysFalse = not(alwaysTrue);

		expect(alwaysFalse("anything")).toBe(false);

	});

});


describe("and()", () => {

	it("should return true when all predicates are true", () => {

		const isPositive = (n: number) => n > 0;
		const isEven = (n: number) => n%2 === 0;
		const isPositiveEven = and(isPositive, isEven);

		expect(isPositiveEven(4)).toBe(true);

	});

	it("should return false when any predicate is false", () => {

		const isPositive = (n: number) => n > 0;
		const isEven = (n: number) => n%2 === 0;
		const isPositiveEven = and(isPositive, isEven);

		expect(isPositiveEven(-4)).toBe(false); // not positive
		expect(isPositiveEven(3)).toBe(false);  // not even

	});

	it("should work with filter task", async () => {

		const isPositive = (n: number) => n > 0;
		const isEven = (n: number) => n%2 === 0;
		const values = await items([-2, -1, 0, 1, 2, 3, 4])(filter(and(isPositive, isEven)))(toArray());

		expect(values).toEqual([2, 4]);

	});

	it("should handle empty predicate list", () => {

		const alwaysTrue = and<number>();

		expect(alwaysTrue(42)).toBe(true); // every() returns true for empty array

	});

	it("should handle single predicate", () => {

		const isEven = (n: number) => n%2 === 0;
		const combined = and(isEven);

		expect(combined(2)).toBe(true);
		expect(combined(3)).toBe(false);

	});

	it("should handle multiple predicates", () => {

		const isPositive = (n: number) => n > 0;
		const isEven = (n: number) => n%2 === 0;
		const lessThan10 = (n: number) => n < 10;
		const combined = and(isPositive, isEven, lessThan10);

		expect(combined(4)).toBe(true);
		expect(combined(12)).toBe(false); // not less than 10

	});

	it("should short-circuit on first false", () => {

		let secondCalled = false;

		const alwaysFalse = () => false;
		const trackCalls = () => {
			secondCalled = true;
			return true;
		};

		const combined = and(alwaysFalse, trackCalls);
		combined(1);

		expect(secondCalled).toBe(false);

	});

});


describe("or()", () => {

	it("should return true when any predicate is true", () => {

		const isNegative = (n: number) => n < 0;
		const isZero = (n: number) => n === 0;
		const isNonPositive = or(isNegative, isZero);

		expect(isNonPositive(-5)).toBe(true);
		expect(isNonPositive(0)).toBe(true);

	});

	it("should return false when all predicates are false", () => {

		const isNegative = (n: number) => n < 0;
		const isZero = (n: number) => n === 0;
		const isNonPositive = or(isNegative, isZero);

		expect(isNonPositive(5)).toBe(false);

	});

	it("should work with filter task", async () => {

		const isNegative = (n: number) => n < 0;
		const isGreaterThan5 = (n: number) => n > 5;
		const values = await items([1, 2, 3, 4, 5, 6, 7, -1, -2])(filter(or(isNegative, isGreaterThan5)))(toArray());

		expect(values).toEqual([6, 7, -1, -2]);

	});

	it("should handle empty predicate list", () => {

		const alwaysFalse = or<number>();

		expect(alwaysFalse(42)).toBe(false); // some() returns false for empty array

	});

	it("should handle single predicate", () => {

		const isEven = (n: number) => n%2 === 0;
		const combined = or(isEven);

		expect(combined(2)).toBe(true);
		expect(combined(3)).toBe(false);

	});

	it("should handle multiple predicates", () => {

		const isNegative = (n: number) => n < 0;
		const isZero = (n: number) => n === 0;
		const isGreaterThan100 = (n: number) => n > 100;
		const combined = or(isNegative, isZero, isGreaterThan100);

		expect(combined(-5)).toBe(true);
		expect(combined(0)).toBe(true);
		expect(combined(150)).toBe(true);
		expect(combined(50)).toBe(false);

	});

	it("should short-circuit on first true", () => {

		let secondCalled = false;

		const alwaysTrue = () => true;
		const trackCalls = () => {
			secondCalled = true;
			return false;
		};

		const combined = or(alwaysTrue, trackCalls);
		combined(1);

		expect(secondCalled).toBe(false);

	});

});


describe("predicate composition", () => {

	it("should compose not with and", async () => {

		const isEven = (n: number) => n%2 === 0;
		const isPositive = (n: number) => n > 0;
		const isOddAndPositive = and(not(isEven), isPositive);

		const values = await items([-3, -2, -1, 0, 1, 2, 3])(filter(isOddAndPositive))(toArray());

		expect(values).toEqual([1, 3]);

	});

	it("should compose not with or", async () => {

		const isEven = (n: number) => n%2 === 0;
		const isNegative = (n: number) => n < 0;
		const isOddOrNegative = or(not(isEven), isNegative);

		const values = await items([-2, -1, 0, 1, 2, 3])(filter(isOddOrNegative))(toArray());

		expect(values).toEqual([-2, -1, 1, 3]);

	});

	it("should compose and with or", async () => {

		const isEven = (n: number) => n%2 === 0;
		const isPositive = (n: number) => n > 0;
		const isNegative = (n: number) => n < 0;
		const isOdd = (n: number) => n%2 !== 0;

		// (even AND positive) OR (odd AND negative)
		const complex = or(and(isEven, isPositive), and(isOdd, isNegative));

		const values = await items([-4, -3, -2, -1, 0, 1, 2, 3, 4])(filter(complex))(toArray());

		expect(values).toEqual([-3, -1, 2, 4]);

	});

	it("should compose multiple nots", () => {

		const isEven = (n: number) => n%2 === 0;
		const doubleNot = not(not(isEven));

		expect(doubleNot(2)).toBe(true);
		expect(doubleNot(3)).toBe(false);

	});

});


describe("edge cases", () => {

	it("should handle string predicates", async () => {

		const startsWithA = (s: string) => s.startsWith("a");
		const hasLength3 = (s: string) => s.length === 3;

		const values = await items(["apple", "art", "banana", "ant"])(filter(and(startsWithA, hasLength3)))(toArray());

		expect(values).toEqual(["art", "ant"]);

	});

	it("should handle object predicates", async () => {

		interface User {
			name: string;
			age: number;
		}

		const isAdult = (u: User) => u.age >= 18;
		const nameStartsWithA = (u: User) => u.name.startsWith("A");

		const users = [
			{ name: "Alice", age: 25 },
			{ name: "Bob", age: 17 },
			{ name: "Amy", age: 30 },
			{ name: "Charlie", age: 15 }
		];

		const values = await items(users)(filter(and(isAdult, nameStartsWithA)))(toArray());

		expect(values).toEqual([
			{ name: "Alice", age: 25 },
			{ name: "Amy", age: 30 }
		]);

	});

});
