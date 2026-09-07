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
import { group } from "./group.js";


describe("group()", () => {

	it("should carry the item type over to the key extractor", async () => {

		(items([1, 2, 3]))(group(item => expectTypeOf(item).toEqualTypeOf<number>() && "key"));

	});

	it("should keep the item type in the groups under a key extractor accepting any item", async () => {

		// a nested `any` is identical to every type, so the members are checked as they are drawn

		for await (const [ key, members ] of (items([1, 2, 3]))(group(String))) {
			expectTypeOf(key).toEqualTypeOf<string>();
			expectTypeOf(members[0]).toEqualTypeOf<number>();
		}

	});

	it("should reject a key extractor unable to key the items", async () => {

		// @ts-expect-error — a string key extractor cannot key numbers
		(items([1, 2, 3]))(group((item: string) => item.length));

	});

});
