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
import { items } from "../feeds/items.js";
import type { Sink } from "../index.js";
import { each } from "./each.js";


describe("each()", () => {

	it("should carry the item type over to the consumer", async () => {

		await (items([1, 2, 3]))(each(item => expectTypeOf(item).toEqualTypeOf<number>()));

	});

	it("should leak no untyped item under a consumer accepting any item", async () => {

		const sink = each(console.log);

		expectTypeOf(sink).toEqualTypeOf<Sink<unknown, number>>();

	});

	it("should take the item type from the declared sink", async () => {

		const sink: Sink<number, number> = each(item => expectTypeOf(item).toEqualTypeOf<number>());

		expectTypeOf(sink).toEqualTypeOf<Sink<number, number>>();

	});

	it("should reject a consumer unable to handle the items", async () => {

		// @ts-expect-error — a string consumer cannot process numbers
		await (items([1, 2, 3]))(each((item: string) => item.length));

	});

});
