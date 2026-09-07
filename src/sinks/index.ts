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

/**
 * Terminal operations that consume the items of a feed and compute the final result.
 *
 * Sinks close a pipe: applying one to a {@link index.Feed Feed} triggers execution and returns a promise resolving to
 * the final result. Those that can decide their outcome early stop consuming rather than draining the source, while
 * those collecting items into a container return it deeply immutable, freezing the container together with the items,
 * keys and values collected into it. Those reducing the feed to a single value, whether computed over its items or
 * selected among them, resolve to `undefined` when the feed carries none and no result is defined, leaving the
 * choice of a fallback to the caller; {@link seek} fails instead, so the item it hands back is usable as is.
 *
 * The item type of a sink is taken from the feed it is applied to, or from the {@link index.Sink Sink} type it is
 * declared under, never from the consumer, predicate, extractor, reducer or comparator handed to it: a function
 * accepting any item, `console.log` or `Boolean` among them, leaves the item type untouched, while one accepting a
 * different type is rejected. A sink composed on its own, outside a pipe, states its item type in the type it is
 * declared under or as an explicit type argument.
 *
 * Every sink is classified along three axes:
 *
 * - **incremental** or **exhaustive**, for how much of the feed it draws before resolving
 * - **streaming** or **materialising**, for what it holds in memory
 * - **stateless** or **stateful**, for whether its result depends on every item drawn
 *
 * > [!WARNING]
 * >
 * > An exhaustive sink never resolves on an infinite feed, and a materialising one may exhaust memory on a large
 * > feed, bounded or not. Every sink but {@link find}, {@link seek}, {@link some} and {@link every} is exhaustive,
 * > and the ones collecting items, into a container or into a single string, materialise the whole feed as well.
 * > Bound the feed upstream with {@link tasks.take take}.
 *
 * **Custom Sinks** close a pipe, consuming the items and returning a promise for the final result; a computation
 * delegating to operations already available applies them to the feed it draws from, which is drained by a single
 * pass, however repeatable the source behind it:
 *
 * ```typescript
 * import { pipe } from '@metreeca/flow';
 * import { items } from '@metreeca/flow/feeds';
 * import type { Sink } from '@metreeca/flow';
 *
 * function histogram<V>(): Sink<V, Map<V, number>> {
 *   return async source => {
 *     const counts = new Map<V, number>();
 *     for await (const item of source) { counts.set(item, (counts.get(item) ?? 0)+1); }
 *     return counts;
 *   };
 * }
 *
 * await pipe(
 *   (items(["a", "b", "a"]))
 *   (histogram())
 * );  // Map(2) { "a" => 2, "b" => 1 }
 * ```
 *
 * @module
 */

// predicates

export * from "./some.js";
export * from "./every.js";

// aggregators

export * from "./count.js";
export * from "./sum.js";
export * from "./avg.js";
export * from "./min.js";
export * from "./max.js";

// scanners

export * from "./find.js";
export * from "./seek.js";
export * from "./reduce.js";

// collectors

export * from "./toArray.js";
export * from "./toSet.js";
export * from "./toMap.js";
export * from "./toObject.js";
export * from "./toString.js";

// effects

export * from "./each.js";
