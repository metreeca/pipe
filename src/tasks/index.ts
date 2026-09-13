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
 * Intermediate operations that transform, filter or reshape the items of a feed.
 *
 * Tasks apply to a {@link index.Feed Feed} and yield a new feed, so they chain freely into longer pipes. Items are
 * processed lazily, sequentially and in source order, unless a task reorders them, interleaves the nested feeds
 * carrying them, hands each of them to several tasks at once or wraps another to run it concurrently, trading output
 * order for throughput.
 *
 * A task wrapping others either hands each of them the whole feed, as {@link flat}, {@link join} and {@link tee} do,
 * leaving whatever state they initialise on invocation to decide on every item, or invokes one once per run over a
 * share of the items, as {@link fork} does, scoping that state to the run rather than to the pipe as a whole. The feed
 * a task draws from and the one it reports are both drained by a single pass, however repeatable the source behind
 * them: a composition to be consumed twice is built afresh from the feed it opens with.
 *
 * The item type of a task is taken from the feed it is applied to, or from the {@link index.Task Task} type it is
 * declared under, never from the predicate, selector, mapper or comparator handed to it: a function accepting any
 * item, `console.log` or `Boolean` among them, leaves the feed type untouched, while one accepting a different type
 * is rejected. A task composed on its own, outside a pipe, states its item type in the type it is declared under or
 * as an explicit type argument.
 *
 * Every task is classified along three axes:
 *
 * - **incremental** or **exhaustive**, for how much of the feed it draws before emitting
 * - **streaming** or **materialising**, for what it holds in memory
 * - **stateless** or **stateful**, for whether its outcome depends on the items drawn before it
 *
 * > [!WARNING]
 * >
 * > An exhaustive task never completes on an infinite feed, and a materialising one may exhaust memory on a large
 * > feed, bounded or not. {@link sort}, {@link group}, an unbounded {@link batch} and a {@link recast} whose mapper
 * > draws the feed entire are both; {@link distinct}, {@link join} and an uncapped {@link fork} are incremental yet
 * > materialising. Bound the feed upstream with {@link take}, batch by a positive size, or cap the runs of a fork.
 *
 * > [!NOTE]
 * >
 * > A custom task is only required to report a feed honouring the {@link index.Feed Feed} contract, however it is
 * > obtained: handing the generator object to {@link feeds.items items()} is the shortest route there, while a
 * > transformation delegating to tasks already available composes the feed it draws from with them and reports what
 * > they report, a feed already. A transformation deciding on the feed as a whole is spared the generator altogether
 * > by {@link recast}, which carries on with the items a mapper computes over it.
 *
 * **Custom Tasks** extend a pipe, drawing the items of a feed and reporting a new one; the transformation is most
 * easily written as an async generator handed to {@link feeds.items items()}, with items to be dropped left unyielded:
 *
 * ```typescript
 * import { pipe } from '@metreeca/flow';
 * import { items } from '@metreeca/flow/feeds';
 * import { toArray } from '@metreeca/flow/sinks';
 * import type { Task } from '@metreeca/flow';
 *
 * function double<V extends number>(): Task<V, V> {
 *   return source => items((async function* () {
 *     for await (const item of source) { yield item*2 as V; }
 *   })());
 * }
 *
 * await pipe(
 *   (items([1, 2, 3]))
 *   (double())
 *   (toArray())
 * );  // [2, 4, 6]
 * ```
 *
 * @module
 */

// operators

export * from "./filter.js";
export * from "./distinct.js";
export * from "./sort.js";
export * from "./skip.js";
export * from "./take.js";
export * from "./peek.js";

// transformers

export * from "./map.js";
export * from "./batch.js";
export * from "./group.js";
export * from "./recast.js";

// splicers

export * from "./flat.js";
export * from "./join.js";
export * from "./fork.js";
export * from "./tee.js";
