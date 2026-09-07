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
 * Composable primitives for asynchronous data flows.
 *
 * A pipe is written as nested applications, one stage per step, over the three symmetric contracts declared here: a
 * {@link Feed} opens the data and accepts the steps advancing it, a {@link Task} moves it on by transforming,
 * filtering or reshaping the items, and a {@link Sink} closes it with the final result.
 *
 * A feed is opened from a value or a data source, supplied as it is or as a promise, then composed with any number of
 * tasks and an optional sink, bracketed by {@link pipe}. Closed by a sink, the pipe resolves to the final result;
 * left open, it ends with a feed, iterated with `for await` to draw the items it carries.
 *
 * Every operation the companion modules provide states where it stands on four axes, so that a pipe can be assembled
 * knowing what it completes on and what it costs: **bounded** or **infinite**, for how far a feed goes; **incremental**
 * or **exhaustive**, for how much of a feed a task or sink draws before emitting or resolving; **streaming** or
 * **materialising**, for what is held in memory; **stateless** or **stateful**, for whether the outcome depends on the
 * items drawn before it.
 *
 * Pipes compose all the way down, with no privileged core: the companion feed, task and sink modules are written
 * against the very contracts declared here, and so is anything you add. A custom feed opens a pipe over a source of
 * your own, a custom task extends one and a custom sink closes it, each chaining with the built-in steps in any order.
 * Every step is handed a feed in turn, so a custom one either draws the items itself or delegates the whole job, or
 * part of it, to steps already available.
 *
 * @module index
 */


/**
 * Async sequence of items.
 *
 * A feed is called to advance the pipe: with a {@link Task} to obtain a new feed carrying the transformed items, or
 * with a {@link Sink} to consume it and obtain the final result. A caller preferring to drive the pipe itself iterates
 * the feed with `for await`. Composition is lazy, as nothing is drawn from the source until a sink or an iteration
 * consumes the feed.
 *
 * A feed ends when the source it draws from stops yielding items: an array, a generator object or another feed ends it
 * on its own under the
 * {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols iteration protocols}.
 * A source producing one value per call, a polling function for instance, is bounded by the feed it is opened with,
 * either reporting an end marker as {@link feeds.inlet inlet} accepts or wrapped in a generator of its own, while a
 * source genuinely without end is bounded downstream, with a task such as {@link tasks.take take} or with a sink
 * deciding its outcome early.
 *
 * A feed is replayable only as far as its source is: one opened over a repeatable source, an array or a set among
 * them, carries the same items at each pass, while one opened over a source drained by iteration is empty after the
 * first. A feed handed to a {@link Task} or a {@link Sink}, and one a task reports, is drained by a single pass
 * whatever it draws from, so a pipe is replayed by composing it afresh from the feed it opens with.
 *
 * @typeParam V The type of items carried by the feed
 *
 * @see {@link https://tc39.es/ecma262/#sec-asynciterator-interface ECMAScript AsyncIterator interface}
 */
export interface Feed<V> extends AsyncIterable<V> {

	/**
	 * Applies a transformation task to the feed.
	 *
	 * @typeParam R The type of items `task` reports
	 *
	 * @param task The task to apply
	 *
	 * @returns A new feed carrying the items `task` reports
	 */<R>(task: Task<V, R>): Feed<R>;

	/**
	 * Applies a terminal sink to the feed.
	 *
	 * @typeParam R The type of result `sink` computes
	 *
	 * @param sink The sink to apply
	 *
	 * @returns A promise resolving to the result `sink` computes over the items
	 */<R>(sink: Sink<V, R>): Promise<R>;

}

/**
 * Intermediate operation applied to a feed.
 *
 * A task consumes the items of a {@link Feed} and reports a new feed carrying new ones, transforming, filtering,
 * reordering or regrouping items along the way; the reported feed accepts further tasks, so tasks chain into longer
 * pipes.
 *
 * The task draws from a feed, so it may either iterate it with `for await` or compose it with further tasks and sinks,
 * delegating the whole transformation or part of it to operations already available.
 *
 * > [!CAUTION]
 * >
 * > The feed handed over and the feed reported must both be assumed to be drained by a single pass.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of items reported, defaulting to `V` for tasks preserving the item type
 */
export interface Task<V, R = V> {

	/**
	 * Transforms the items.
	 *
	 * @param feed The feed carrying the items to process
	 *
	 * @returns A feed carrying the transformed items
	 */
	(feed: Feed<V>): Feed<R>;

}

/**
 * Terminal operation applied to a feed.
 *
 * A sink consumes the items of a {@link Feed}, driving the pipe that feeds it and computing the final result; one able
 * to decide its outcome early may stop consuming before the source runs dry.
 *
 * Stating the item type alone, as `Sink<V>`, names any sink drawing `V` items whatever it resolves to, so sinks are
 * held together under a common type or handed over as arguments with no result to agree on. A pipe closed by a sink
 * declared that way resolves to `unknown`: state the result type wherever it is meant to be read.
 *
 * The sink draws from a feed, so it may either iterate it with `for await` or compose it with further tasks and sinks,
 * delegating the whole computation or part of it to operations already available.
 *
 * > [!CAUTION]
 * >
 * > The feed handed over must be assumed to be drained by a single pass.
 *
 * @typeParam V The type of items drawn from the feed
 * @typeParam R The type of result computed over the items, defaulting to `unknown` for sinks whose result is of no
 *   interest
 */
export interface Sink<V, R = unknown> {

	/**
	 * Consumes the items.
	 *
	 * @param feed The feed carrying the items to consume
	 *
	 * @returns A promise resolving to the final result
	 */
	(feed: Feed<V>): Promise<R>;

}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a pipe.
 *
 * Brackets a feed, the tasks chained to it and the {@link Sink} optionally closing them, declaring the composition a
 * pipe so that it reads as one unit rather than as a run of juxtaposed calls.
 *
 * @typeParam V The type of the composition, either the {@link Feed} a pipe left open ends with or the promise the
 *   closing sink reports
 *
 * @param source The composition to bracket
 *
 * @returns `source`, as handed in: a feed ready for manual iteration, or a promise resolving to the result the
 *   closing sink computes over the items
 *
 * @example
 *
 * ```typescript
 * await pipe(
 *   (items([1, 2, 3]))
 *   (map(n => n*2))
 *   (toArray())
 * );  // [2, 4, 6], as the closing sink resolves the pipe
 *
 * for await (const item of pipe(
 *   (range(1, 4))
 *   (map(n => n*2))
 * )) {
 *   console.log(item);  // 2, 4, 6, drawn from the feed a pipe left open ends with
 * }
 * ```
 */
export function pipe<V extends Feed<unknown> | Promise<unknown>>(source: V): V {

	return source;

}
