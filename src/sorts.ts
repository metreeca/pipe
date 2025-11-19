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

/**
 * Comparator functions and combinators for sorting operations.
 *
 * Provides a composable API for creating and combining comparison functions
 * used with {@link sort | sorting tasks} and other comparator-based operations.
 *
 * @module
 *
 * @groupDescription Comparators
 *
 * Basic comparison functions for ascending and descending order.
 *
 * @groupDescription Factories
 *
 * Functions that create new comparators from selectors or transformations.
 *
 * @groupDescription Combinators
 *
 * Functions that modify or combine existing comparators.
 */


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Compares values in ascending order.
 *
 * Null and undefined values are treated as less than any other value.
 * When both values are null or undefined, they are considered equal.
 *
 * @group Comparators
 *
 * @param a First value to compare
 * @param b Second value to compare
 *
 * @returns Negative if `a < b`, positive if `a > b`, zero if equal
 *
 * @remarks
 *
 * When comparing incompatible types (e.g., numbers with strings), JavaScript's comparison
 * operators perform type coercion which may produce unexpected results. Incomparable values
 * are treated as equal. Use with homogeneous collections for predictable sorting.
 */
export function ascending<V>(a: V, b: V): number {

	const na = a === undefined || a === null;
	const nb = b === undefined || b === null;

	return na && nb ? 0 : na ? -1 : nb ? 1 : a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Compares values in descending order.
 *
 * Null and undefined values are treated as less than any other value.
 * When both values are null or undefined, they are considered equal.
 *
 * @group Comparators
 *
 * @param a First value to compare
 * @param b Second value to compare
 *
 * @returns Positive if `a < b`, negative if `a > b`, zero if equal
 *
 * @remarks
 *
 * When comparing incompatible types (e.g., numbers with strings), JavaScript's comparison
 * operators perform type coercion which may produce unexpected results. Incomparable values
 * are treated as equal. Use with homogeneous collections for predictable sorting.
 */
export function descending<V>(a: V, b: V): number {

	const na = a === undefined || a === null;
	const nb = b === undefined || b === null;

	return na && nb ? 0 : na ? -1 : nb ? 1 : a < b ? 1 : a > b ? -1 : 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a comparator that compares values by a selected key.
 *
 * Extracts a comparable key from each value using the selector function,
 * then applies the specified comparator to the keys.
 *
 * @group Factories
 *
 * @typeParam V The type of values to compare
 * @typeParam K The type of keys to compare
 *
 * @param selector Function that extracts the comparison key from a value
 * @param comparator Comparator to apply to the extracted keys (defaults to {@link ascending})
 *
 * @returns A comparator function that compares values by their selected keys
 */
export function by<V, K>(selector: (value: V) => K, comparator: (a: K, b: K) => number = ascending) {
	return (a: V, b: V): number => comparator(selector(a), selector(b));
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Reverses the order of a comparator.
 *
 * Returns a new comparator that produces the opposite ordering of the original.
 *
 * @group Combinators
 *
 * @typeParam V The type of values to compare
 *
 * @param comparator Comparator to reverse
 *
 * @returns A comparator that produces the opposite ordering
 */
export function reverse<V>(comparator: (a: V, b: V) => number) {
	return (a: V, b: V): number => -comparator(a, b);
}

/**
 * Wraps a comparator to handle null and undefined values.
 *
 * Null and undefined values are treated as less than any other value and equal to each other.
 * Non-nullish values are compared using the provided comparator.
 *
 * @group Combinators
 *
 * @typeParam V The type of values to compare (may include null/undefined)
 *
 * @param comparator Comparator for non-nullish values
 *
 * @returns A comparator that handles null and undefined values by placing them first
 */
export function nullish<V>(comparator: (a: NonNullable<V>, b: NonNullable<V>) => number) {
	return (a: V, b: V): number => {

		const na = a === undefined || a === null;
		const nb = b === undefined || b === null;

		return na && nb ? 0 : na ? -1 : nb ? 1 : comparator(a as NonNullable<V>, b as NonNullable<V>);
	};
}

/**
 * Wraps a comparator to handle null and undefined values.
 *
 * Null and undefined values are treated as greater than any other value and equal to each other.
 * Non-nullish values are compared using the provided comparator.
 *
 * @group Combinators
 *
 * @typeParam V The type of values to compare (may include null/undefined)
 *
 * @param comparator Comparator for non-nullish values
 *
 * @returns A comparator that handles null and undefined values by placing them last
 */
export function defined<V>(comparator: (a: NonNullable<V>, b: NonNullable<V>) => number) {
	return (a: V, b: V): number => {

		const da = a !== undefined && a !== null;
		const db = b !== undefined && b !== null;

		return da && db ? comparator(a as NonNullable<V>, b as NonNullable<V>) : da ? -1 : db ? 1 : 0;
	};
}

/**
 * Combines multiple comparators into a single comparator.
 *
 * Applies comparators in sequence until one returns a non-zero result.
 * If all comparators return zero, the values are considered equal.
 *
 * @group Combinators
 *
 * @typeParam V The type of values to compare
 *
 * @param comparators Comparators to apply in sequence
 *
 * @returns A comparator that applies all provided comparators until a non-zero result is found
 */
export function chain<V>(...comparators: readonly ((a: V, b: V) => number)[]) {
	return (a: V, b: V): number => comparators.reduce(
		(order, comparator) => order !== 0 ? order : comparator(a, b),
		0
	);
}
