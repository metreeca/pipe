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
 * Predicate composition utilities.
 *
 * Provides functional utilities for composing and transforming predicate functions
 * used with filtering tasks and other predicate-based operations.
 *
 * @module tests
 */

/**
 * Negates a predicate.
 *
 * Creates a new predicate that returns the logical NOT of the input predicate.
 *
 * @typeParam V The type of value to test
 *
 * @param predicate The predicate to negate
 *
 * @returns A new predicate that returns `true` when the input predicate returns `false`
 *
 * @example
 *
 * ```typescript
 * const isEven = (n: number) => n % 2 === 0;
 * const isOdd = not(isEven);
 *
 * isOdd(3); // true
 * isOdd(4); // false
 * ```
 */
export function not<V>(predicate: (value: V) => boolean): (value: V) => boolean {
	return (value: V) => !predicate(value);
}

/**
 * Combines predicates with logical AND.
 *
 * Creates a new predicate that returns `true` only if all input predicates return `true`.
 * Short-circuits on the first `false` result.
 *
 * @typeParam V The type of value to test
 *
 * @param predicates The predicates to combine
 *
 * @returns A new predicate that returns `true` when all input predicates return `true`
 *
 * @example
 *
 * ```typescript
 * const isPositive = (n: number) => n > 0;
 * const isEven = (n: number) => n % 2 === 0;
 * const isPositiveEven = and(isPositive, isEven);
 *
 * isPositiveEven(4);  // true
 * isPositiveEven(-4); // false
 * isPositiveEven(3);  // false
 * ```
 */
export function and<V>(...predicates: Array<(value: V) => boolean>): (value: V) => boolean {
	return (value: V) => predicates.every(predicate => predicate(value));
}

/**
 * Combines predicates with logical OR.
 *
 * Creates a new predicate that returns `true` if any input predicate returns `true`.
 * Short-circuits on the first `true` result.
 *
 * @typeParam V The type of value to test
 *
 * @param predicates The predicates to combine
 *
 * @returns A new predicate that returns `true` when any input predicate returns `true`
 *
 * @example
 *
 * ```typescript
 * const isNegative = (n: number) => n < 0;
 * const isZero = (n: number) => n === 0;
 * const isNonPositive = or(isNegative, isZero);
 *
 * isNonPositive(-5); // true
 * isNonPositive(0);  // true
 * isNonPositive(5);  // false
 * ```
 */
export function or<V>(...predicates: Array<(value: V) => boolean>): (value: V) => boolean {
	return (value: V) => predicates.some(predicate => predicate(value));
}
