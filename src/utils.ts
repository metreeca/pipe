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
 * Internal utilities shared across modules.
 *
 * @internal
 * @module
 */

import { isAsyncIterable, isFunction, isIterable, isString } from "@metreeca/core";
import { Data } from "./index.js";

/**
 * Helper to flatten Data<R> into individual items.
 *
 * @remarks
 *
 * Converts various data sources into an async generator:
 *
 * - Functions (Pipe instances): Invokes and yields from the returned async iterable
 * - Async iterables: Yields items directly from the async iterable
 * - Sync iterables (excluding strings): Yields items from the iterable
 * - All other values (primitives, objects, etc.): Yields the value as a single item
 *
 * **String Handling**: Strings are treated as atomic values and yielded whole, not
 * iterated character by character, ensuring consistent behavior where they represent
 * single data items rather than character sequences.
 *
 * @internal
 */
export async function* flatten<R>(data: Data<R>): AsyncGenerator<R, void, unknown> {

	if ( isString(data) ) {

		yield data as R;

	} else if ( isFunction(data) ) {

		yield* data();

	} else if ( isAsyncIterable<R>(data) ) {

		yield* data;

	} else if ( isIterable<undefined | R>(data) ) {

		yield* data;

	} else {

		yield data;

	}

}
