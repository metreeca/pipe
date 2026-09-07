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

import { ascending } from "@metreeca/core/order";
import { Sink } from "../index.js";
import { reduce } from "./reduce.js";


/**
 * Creates a sink selecting the least item of the feed.
 *
 * Items are ranked in source order, seeding the result with the first one; equally ranking items resolve to the first
 * one in source order.
 *
 * An empty feed resolves to `undefined`; callers wanting a default supply it with `??`. As {@link ascending} ranks
 * `null` before any other value, a feed carrying `null` resolves to it unless the comparator states otherwise.
 *
 * > [!WARNING]
 * >
 * > - **Exhaustive**: every item is drawn before the sink resolves, so an infinite feed never completes.
 * > - **Streaming**: no more than the least ranking item seen so far is held in memory, whatever the size of the
 * >   feed.
 * > - **Stateful**: the outcome covers the items drawn, so a sink closing a nested or truncated feed sees those alone.
 *
 * > [!TIP]
 * >
 * > The {@link https://metreeca.github.io/core/modules/order.html order} module of `@metreeca/core` provides helper
 * > functions for assembling complex ranking criteria.
 *
 * @typeParam V The type of items in the feed
 *
 * @param comparator The function establishing the relative order of two items, defaulting to {@link ascending},
 *   which ranks values in natural order, placing `null` first
 *
 * @returns A sink resolving to the least ranking item of the feed, or to `undefined` if the feed carried no items
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([3, 1, 2]))
 *   (min())
 * );  // 1
 *
 * await pipe(
 *   (items([{ age: 30 }, { age: 20 }]))
 *   (min(by(x => x.age)))
 * );  // { age: 20 }
 *
 * await pipe(
 *   (items<number>([]))
 *   (min())
 * );  // undefined
 * ```
 */
export function min<V>(comparator: (a: NoInfer<V>, b: NoInfer<V>) => number = ascending): Sink<V, undefined | V> {

	return reduce((min: V, item: V) => comparator(item, min) < 0 ? item : min);

}
