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
import type { Feed } from "../index.js";
import { distinct } from "./distinct.js";


describe("distinct()", () => {

	it("should carry the item type over to the selector", async () => {

		(items([1, 2, 3]))(distinct(item => expectTypeOf(item).toEqualTypeOf<number>()));

	});

	it("should keep the feed type under a selector accepting any item", async () => {

		const feed = (items([1, 2, 3]))(distinct(String));

		expectTypeOf(feed).toEqualTypeOf<Feed<number>>();

		// a nested `any` is identical to every type, so the items are checked as they are drawn

		for await (const item of feed) {
			expectTypeOf(item).toEqualTypeOf<number>();
		}

	});

	it("should reject a selector unable to key the items", async () => {

		// @ts-expect-error — a string selector cannot key numbers
		(items([1, 2, 3]))(distinct((item: string) => item.length));

	});

});
