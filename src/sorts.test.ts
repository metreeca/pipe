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
import { ascending, by, chain, defined, descending, nullish, reverse } from "./sorts";


describe("ascending()", () => {

	it("should return -1 when a < b", () => {
		expect(ascending(1, 2)).toBe(-1);
	});

	it("should return 1 when a > b", () => {
		expect(ascending(2, 1)).toBe(1);
	});

	it("should return 0 when a === b", () => {
		expect(ascending(1, 1)).toBe(0);
	});

	it("should work with strings", () => {
		expect(ascending("a", "b")).toBe(-1);
		expect(ascending("b", "a")).toBe(1);
		expect(ascending("a", "a")).toBe(0);
	});

	it("should work with dates", () => {
		const date1 = new Date("2023-01-01");
		const date2 = new Date("2023-12-31");

		expect(ascending(date1, date2)).toBe(-1);
		expect(ascending(date2, date1)).toBe(1);
		expect(ascending(date1, date1)).toBe(0);
	});

	it("should place null before defined values by default", () => {
		expect(ascending(null, 1)).toBe(-1);
		expect(ascending(1, null)).toBe(1);
	});

	it("should place undefined before defined values by default", () => {
		expect(ascending(undefined, 1)).toBe(-1);
		expect(ascending(1, undefined)).toBe(1);
	});

	it("should treat null and undefined as equal by default", () => {
		expect(ascending(null, null)).toBe(0);
		expect(ascending(undefined, undefined)).toBe(0);
		expect(ascending(null, undefined)).toBe(0);
		expect(ascending(undefined, null)).toBe(0);
	});

});

describe("descending()", () => {

	it("should return 1 when a < b", () => {
		expect(descending(1, 2)).toBe(1);
	});

	it("should return -1 when a > b", () => {
		expect(descending(2, 1)).toBe(-1);
	});

	it("should return 0 when a === b", () => {
		expect(descending(1, 1)).toBe(0);
	});

	it("should work with strings", () => {
		expect(descending("a", "b")).toBe(1);
		expect(descending("b", "a")).toBe(-1);
		expect(descending("a", "a")).toBe(0);
	});

	it("should place null before defined values by default", () => {
		expect(descending(null, 1)).toBe(-1);
		expect(descending(1, null)).toBe(1);
	});

	it("should place undefined before defined values by default", () => {
		expect(descending(undefined, 1)).toBe(-1);
		expect(descending(1, undefined)).toBe(1);
	});

});

describe("by()", () => {

	it("should extract key and compare using default ascending", () => {
		type Person = { name: string; age: number };

		const comparator = by<Person, number>(p => p.age);

		expect(comparator({ name: "Alice", age: 30 }, { name: "Bob", age: 25 })).toBe(1);
		expect(comparator({ name: "Alice", age: 25 }, { name: "Bob", age: 30 })).toBe(-1);
		expect(comparator({ name: "Alice", age: 25 }, { name: "Bob", age: 25 })).toBe(0);
	});

	it("should use custom comparator when provided", () => {
		type Person = { name: string; age: number };

		const comparator = by<Person, number>(p => p.age, descending);

		expect(comparator({ name: "Alice", age: 30 }, { name: "Bob", age: 25 })).toBe(-1);
		expect(comparator({ name: "Alice", age: 25 }, { name: "Bob", age: 30 })).toBe(1);
	});

	it("should work with string keys", () => {
		type Person = { name: string; age: number };

		const comparator = by<Person, string>(p => p.name);

		expect(comparator({ name: "Alice", age: 30 }, { name: "Bob", age: 25 })).toBe(-1);
		expect(comparator({ name: "Bob", age: 25 }, { name: "Alice", age: 30 })).toBe(1);
		expect(comparator({ name: "Alice", age: 25 }, { name: "Alice", age: 30 })).toBe(0);
	});

	it("should handle nullish keys with default comparator", () => {
		type Item = { value: number | null };

		const comparator = by<Item, number | null>(i => i.value);

		expect(comparator({ value: null }, { value: 1 })).toBe(-1);  // null first
		expect(comparator({ value: 1 }, { value: null })).toBe(1);
		expect(comparator({ value: 1 }, { value: 2 })).toBe(-1);
	});

	it("should compose with other comparators", () => {
		type Person = { name: string; age: number };

		const comparator = chain<Person>(
			by(p => p.age),
			by(p => p.name)
		);

		const people = [
			{ name: "Charlie", age: 25 },
			{ name: "Alice", age: 25 },
			{ name: "Bob", age: 30 }
		];

		const sorted = [...people].sort(comparator);

		expect(sorted).toEqual([
			{ name: "Alice", age: 25 },    // Same age, sorted by name
			{ name: "Charlie", age: 25 },
			{ name: "Bob", age: 30 }
		]);
	});

});

describe("nullish()", () => {

	it("should place null before defined values", () => {
		const comparator = nullish(ascending);

		expect(comparator(null, 1)).toBe(-1);
		expect(comparator(1, null)).toBe(1);
	});

	it("should place undefined before defined values", () => {
		const comparator = nullish(ascending);

		expect(comparator(undefined, 1)).toBe(-1);
		expect(comparator(1, undefined)).toBe(1);
	});

	it("should treat null and undefined as equal", () => {
		const comparator = nullish(ascending);

		expect(comparator(null, null)).toBe(0);
		expect(comparator(undefined, undefined)).toBe(0);
		expect(comparator(null, undefined)).toBe(0);
		expect(comparator(undefined, null)).toBe(0);
	});

	it("should delegate to comparator for defined values", () => {
		const comparator = nullish(ascending);

		expect(comparator(1, 2)).toBe(-1);
		expect(comparator(2, 1)).toBe(1);
		expect(comparator(1, 1)).toBe(0);
	});

	it("should work with descending order", () => {
		const comparator = nullish(descending);

		expect(comparator(null, 1)).toBe(-1);
		expect(comparator(1, null)).toBe(1);
		expect(comparator(1, 2)).toBe(1);
		expect(comparator(2, 1)).toBe(-1);
	});

});

describe("defined()", () => {

	it("should place defined values before null", () => {
		const comparator = defined(ascending);

		expect(comparator(1, null)).toBe(-1);
		expect(comparator(null, 1)).toBe(1);
	});

	it("should place defined values before undefined", () => {
		const comparator = defined(ascending);

		expect(comparator(1, undefined)).toBe(-1);
		expect(comparator(undefined, 1)).toBe(1);
	});

	it("should treat null and undefined as equal", () => {
		const comparator = defined(ascending);

		expect(comparator(null, null)).toBe(0);
		expect(comparator(undefined, undefined)).toBe(0);
		expect(comparator(null, undefined)).toBe(0);
		expect(comparator(undefined, null)).toBe(0);
	});

	it("should delegate to comparator for defined values", () => {
		const comparator = defined(ascending);

		expect(comparator(1, 2)).toBe(-1);
		expect(comparator(2, 1)).toBe(1);
		expect(comparator(1, 1)).toBe(0);
	});

	it("should work with descending order", () => {
		const comparator = defined(descending);

		expect(comparator(1, null)).toBe(-1);
		expect(comparator(null, 1)).toBe(1);
		expect(comparator(1, 2)).toBe(1);
		expect(comparator(2, 1)).toBe(-1);
	});

	it("should override default nullish-first behavior of ascending", () => {
		// ascending has nullish-first by default
		expect(ascending(null, 1)).toBe(-1);
		expect(ascending(1, null)).toBe(1);

		// defined() wrapper overrides to put defined values first
		const comparator = defined(ascending);
		expect(comparator(null, 1)).toBe(1);  // Reversed: defined first
		expect(comparator(1, null)).toBe(-1); // Reversed: defined first
		expect(comparator(1, 2)).toBe(-1);    // Normal ascending for defined values
	});

	it("should override default nullish-first behavior of descending", () => {
		// descending has nullish-first by default
		expect(descending(null, 1)).toBe(-1);
		expect(descending(1, null)).toBe(1);

		// defined() wrapper overrides to put defined values first
		const comparator = defined(descending);
		expect(comparator(null, 1)).toBe(1);  // Reversed: defined first
		expect(comparator(1, null)).toBe(-1); // Reversed: defined first
		expect(comparator(1, 2)).toBe(1);     // Normal descending for defined values
	});

});

describe("reverse()", () => {

	it("should reverse ascending to descending", () => {
		const comparator = reverse(ascending);

		expect(comparator(1, 2)).toBe(1);
		expect(comparator(2, 1)).toBe(-1);
		expect(comparator(1, 1) === 0).toBe(true); // -0 === 0 is true
	});

	it("should reverse descending to ascending", () => {
		const comparator = reverse(descending);

		expect(comparator(1, 2)).toBe(-1);
		expect(comparator(2, 1)).toBe(1);
		expect(comparator(1, 1) === 0).toBe(true); // -0 === 0 is true
	});

	it("should work with custom comparators", () => {
		const customComparator = (a: number, b: number) => a-b;
		const reversed = reverse(customComparator);

		expect(reversed(1, 2)).toBe(1);
		expect(reversed(2, 1)).toBe(-1);
		expect(reversed(1, 1) === 0).toBe(true); // -0 === 0 is true
	});

});

describe("chain()", () => {

	it("should use first comparator when values differ", () => {
		const comparator = chain(ascending);

		expect(comparator(1, 2)).toBe(-1);
		expect(comparator(2, 1)).toBe(1);
	});

	it("should use second comparator when first returns 0", () => {
		const comparator = chain(
			(a: { x: number; y: number }, b: { x: number; y: number }) => a.x-b.x,
			(a: { x: number; y: number }, b: { x: number; y: number }) => a.y-b.y
		);

		expect(comparator({ x: 1, y: 2 }, { x: 1, y: 3 })).toBe(-1);
		expect(comparator({ x: 1, y: 3 }, { x: 1, y: 2 })).toBe(1);
		expect(comparator({ x: 1, y: 2 }, { x: 2, y: 1 })).toBe(-1);
	});

	it("should return 0 when all comparators return 0", () => {
		const comparator = chain(
			(a: number, b: number) => 0,
			(a: number, b: number) => 0
		);

		expect(comparator(1, 2)).toBe(0);
	});

	it("should work with no comparators", () => {
		const comparator = chain();

		expect(comparator(1, 2)).toBe(0);
	});

	it("should work with three or more comparators", () => {
		const comparator = chain(
			(a: { x: number; y: number; z: number }, b: { x: number; y: number; z: number }) => a.x-b.x,
			(a: { x: number; y: number; z: number }, b: { x: number; y: number; z: number }) => a.y-b.y,
			(a: { x: number; y: number; z: number }, b: { x: number; y: number; z: number }) => a.z-b.z
		);

		expect(comparator({ x: 1, y: 1, z: 1 }, { x: 1, y: 1, z: 2 })).toBe(-1);
		expect(comparator({ x: 1, y: 1, z: 2 }, { x: 1, y: 1, z: 1 })).toBe(1);
		expect(comparator({ x: 1, y: 1, z: 1 }, { x: 1, y: 2, z: 1 })).toBe(-1);
	});

	it("should combine with nullish wrapper", () => {
		const comparator = chain<number | null>(
			nullish((a, b) => a-b)
		);

		expect(comparator(null, 1)).toBe(-1);
		expect(comparator(1, null)).toBe(1);
		expect(comparator(1, 2)).toBe(-1);
	});

	it("should support multi-level sorting", () => {
		type Item = { category: string; priority: number; name: string };

		const items: Item[] = [
			{ category: "A", priority: 1, name: "z" },
			{ category: "B", priority: 2, name: "a" },
			{ category: "A", priority: 1, name: "a" },
			{ category: "A", priority: 2, name: "b" }
		];

		const comparator = chain<Item>(
			(a, b) => a.category.localeCompare(b.category),
			(a, b) => a.priority-b.priority,
			(a, b) => a.name.localeCompare(b.name)
		);

		const sorted = [...items].sort(comparator);

		expect(sorted).toEqual([
			{ category: "A", priority: 1, name: "a" },
			{ category: "A", priority: 1, name: "z" },
			{ category: "A", priority: 2, name: "b" },
			{ category: "B", priority: 2, name: "a" }
		]);
	});

});
