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
import { seek } from "./seek.js";


describe("seek()", () => {

	it("should carry the item type over to the predicate", async () => {

		await (items([1, 2, 3]))(seek(item => expectTypeOf(item).toEqualTypeOf<number>() && true));

	});

	it("should retrieve an item of the feed type under a predicate accepting any item", async () => {

		expectTypeOf(await (items([1, 2, 3]))(seek(Boolean))).toEqualTypeOf<number>();

	});

	it("should reject a predicate unable to test the items", async () => {

		// @ts-expect-error — a string predicate cannot test numbers
		await (items([1, 2, 3]))(seek((item: string) => item.length > 0));

	});

});
