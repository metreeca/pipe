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

import { describe, expectTypeOf, it } from "vitest";
import { items } from "./feeds/items.js";
import type { Sink } from "./index.js";
import { toArray } from "./sinks/toArray.js";


describe("Sink", () => {

	it("should leave the result type unstated as unknown", async () => {

		expectTypeOf<Sink<number>>().toEqualTypeOf<Sink<number, unknown>>();

	});

	it("should accept a sink whatever it resolves to, where the result is of no interest", async () => {

		expectTypeOf<Sink<number, readonly number[]>>().toExtend<Sink<number>>();
		expectTypeOf<Sink<number, void>>().toExtend<Sink<number>>();

	});

	it("should reject a sink unable to consume the items", async () => {

		expectTypeOf<Sink<string, unknown>>().not.toExtend<Sink<number>>();

	});

	it("should resolve a pipe closed by a sink with an unstated result to an unknown value", async () => {

		const sink: Sink<number> = toArray();

		expectTypeOf(await (items([1, 2, 3]))(sink)).toEqualTypeOf<unknown>();

	});

});
