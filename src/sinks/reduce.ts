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

import type { Awaitable } from "@metreeca/core/async";
import { Sink } from "../index.js";


/**
 * Creates a sink folding the feed into a single item.
 *
 * The first item seeds the accumulator and every following one is folded into it, in source order.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is folded before the sink resolves, so an infinite feed never completes.
 * > - **Streaming**: no more than the accumulator is held in memory, whatever the size of the feed.
 * > - **Stateful**: the accumulator covers the items drawn, so a sink closing a nested or truncated feed sees those
 * >   alone.
 *
 * @typeParam V The type of items in the feed
 *
 * @param reducer The function folding an item into the accumulator and returning the updated one
 *
 * @returns A sink resolving to the final accumulator, to the only item of a singleton feed, or to `undefined` for
 *   an empty feed
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (reduce((total, n) => total+n))
 * );  // 15
 * ```
 */
export function reduce<V>(
	reducer: (accumulator: NoInfer<V>, item: NoInfer<V>) => Awaitable<V>
): Sink<V, undefined | V>;

/**
 * Creates a sink folding the feed into a value of a different type.
 *
 * Folds like {@link reduce} without an `initial` argument, seeding the accumulator with the supplied value rather
 * than with the first item, so the result type is free of the item type and an empty feed resolves to the seed.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is folded before the sink resolves, so an infinite feed never completes.
 * > - **Streaming**: no more than the accumulator is held in memory, whatever the size of the feed.
 * > - **Stateful**: the accumulator covers the items drawn, so a sink closing a nested or truncated feed sees those
 * >   alone, seeded afresh with `initial` at each.
 *
 * @typeParam V The type of items in the feed
 * @typeParam R The type of the accumulated result
 *
 * @param reducer The function folding an item into the accumulator and returning the updated one
 * @param initial The value seeding the accumulator
 *
 * @returns A sink resolving to the final accumulator
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3, 4, 5]))
 *   (reduce((total, n) => total+n, 10))
 * );  // 25
 * ```
 */
export function reduce<V, R>(reducer: (accumulator: R, item: NoInfer<V>) => Awaitable<R>, initial: R): Sink<V, R>;

/**
 * Creates a sink reducing the feed to a single value, with or without an initial value.
 */
export function reduce<V, R>(reducer: Function, initial?: R): Sink<V, undefined | V | R> {

	return async source => {

		let started = arguments.length > 1;
		let accumulator: V | R | undefined = started ? initial : undefined;

		for await (const item of source) {
			if ( started ) {
				accumulator = await reducer(accumulator, item);
			} else {
				accumulator = item;
				started = true;
			}
		}

		return accumulator;

	};

}
