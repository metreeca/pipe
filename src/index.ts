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
 * Asynchronous stream processing utilities.
 *
 * Provides a composable API for working with async iterables through pipes, tasks, and sinks.
 *
 * > [!WARNING]
 * >
 * > **All streams automatically filter out `undefined` values.** This filtering occurs at stream creation
 * > and when tasks are chained together, ensuring that `undefined` never flows through your pipeline.
 * >
 * > - `undefined` values are **removed** from all streams
 * > - Other falsy values (`null`, `0`, `false`, `""`) are **preserved**
 * > - Custom tasks yielding `undefined` have those values automatically filtered
 * > - This behavior is centralized in the `items()` function, which is used internally for all stream creation
 *
 * @remarks
 *
 * The library is designed to be extensible. You can create custom feeds, tasks, and sinks
 * to suit your specific needs by following the patterns demonstrated in each module.
 *
 * @module index
 */

import { isFunction } from "@metreeca/core";


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Flexible data source for stream processing.
 *
 * Represents various data formats that can be converted into async streams:
 *
 * - `undefined` - filtered out and not yielded
 * - Single value: `V` - yields one item
 * - Array: `readonly V[]` - yields items from the array
 * - Synchronous iterable: `Iterable<V>` - yields items from the iterable
 * - Asynchronous iterable: `AsyncIterable<V>` - yields items from the async iterable
 * - Pipe: {@link Pipe}`<V>` - yields items from the pipe's underlying async iterable
 *
 * @typeParam V The type of values in the data source
 *
 * @remarks
 *
 * Enables functions to accept data in the most convenient format for the caller,
 * internally normalizing it to async iterables for uniform processing.
 * Used by `items()` to create pipes and by `flatMap()` to flatten nested data sources.
 *
 * **`undefined` Handling**: `undefined` values are automatically filtered out by the `items()` function,
 * allowing tasks like `map()` to return `undefined` as a way to filter items from the stream.
 *
 * @example
 *
 * ```typescript
 * // All of these are valid Data<number> values:
 * const scalarData: Data<number> = 42;
 * const arrayData: Data<number> = [1, 2, 3];
 * const iterableData: Data<number> = new Set([1, 2, 3]);
 * const asyncData: Data<number> = (async function*() { yield 1; yield 2; })();
 * const pipeData: Data<number> = items([1, 2, 3]);
 * ```
 */
export type Data<V> =
	| V
	| readonly V[]
	| Iterable<V>
	| AsyncIterable<V>
	| Pipe<V>;


/**
 * Fluent interface for composing async stream operations.
 *
 * Supports three call patterns:
 *
 * - Apply a task: returns a new {@link Pipe} for continued chaining
 * - Apply a sink: returns a Promise with the final result
 * - Get iterator: returns the underlying AsyncIterable
 *
 * @typeParam V The type of values in the stream
 */
export interface Pipe<V> {

	/**
	 * Retrieves the underlying async iterable.
	 *
	 * @returns The async iterable for manual iteration
	 */
	(): AsyncIterable<V>;

	/**
	 * Applies a transformation task to the stream.
	 *
	 * @typeParam R The type of transformed values
	 *
	 * @param task The task to apply
	 *
	 * @returns A new pipe with transformed values
	 */<R>(task: Task<V, R>): Pipe<R>;

	/**
	 * Applies a terminal sink operation to consume the stream.
	 *
	 * @typeParam R The type of result
	 *
	 * @param sink The sink to apply
	 *
	 * @returns A promise resolving to the sink's result
	 */<R>(sink: Sink<V, R>): Promise<R>;

}

/**
 * Transformation that processes stream items and yields results.
 *
 * Tasks are intermediate operations that can be chained together.
 * They transform, filter, or otherwise process items while maintaining the stream.
 *
 * @typeParam V The type of input values
 * @typeParam R The type of output values (defaults to V)
 */
export interface Task<V, R = V> {

	/**
	 * Processes an async iterable and yields transformed results.
	 *
	 * @param value The source async iterable to process
	 *
	 * @returns An async iterable of transformed values (may include `undefined` for filtering)
	 */
	(value: AsyncIterable<V>): AsyncIterable<undefined | R>;

}

/**
 * Terminal operation that consumes a stream and produces a result.
 *
 * Sinks trigger stream execution and return a promise that resolves
 * when all items have been processed.
 *
 * @typeParam V The type of input values
 * @typeParam R The type of result (defaults to V)
 */
export interface Sink<V, R = V> {

	/**
	 * Consumes an async iterable and produces a final result.
	 *
	 * @param value The source async iterable to consume
	 *
	 * @returns A promise resolving to the final result
	 */
	(value: AsyncIterable<V>): Promise<R>;

}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Processes a promise, returning it as-is.
 *
 * @typeParam V The type of value in the promise
 *
 * @param source The promise to process
 *
 * @returns The same promise
 */
export function pipe<V>(source: Promise<V>): Promise<V>;

/**
 * Processes a pipe, retrieving its underlying async iterable.
 *
 * @typeParam V The type of values in the pipe
 *
 * @param source The pipe to process
 *
 * @returns The underlying async iterable
 */
export function pipe<V>(source: Pipe<V>): AsyncIterable<V>;

export function pipe(source: unknown): unknown {

	return isFunction(source) ? source() : source;

}
