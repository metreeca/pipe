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
import { toMap } from "./toMap.js";


describe("toMap()", () => {

	it("should carry the item type over to the extractors", async () => {

		await (items([1, 2, 3]))(toMap(
			item => expectTypeOf(item).toEqualTypeOf<number>() && "key",
			item => expectTypeOf(item).toEqualTypeOf<number>()
		));

	});

	it("should collect items of the feed type under a key extractor accepting any item", async () => {

		const map = await (items([1, 2, 3]))(toMap(String));

		// a nested `any` is identical to every type, so the values are checked as they are retrieved

		expectTypeOf(map.get("1")).toEqualTypeOf<undefined | number>();

	});

	it("should reject a key extractor unable to key the items", async () => {

		// @ts-expect-error — a string key extractor cannot key numbers
		await (items([1, 2, 3]))(toMap((item: string) => item.length));

	});

});
