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
import type { Feed, Task } from "../index.js";
import { filter } from "./filter.js";
import { map } from "./map.js";
import { peek } from "./peek.js";
import { tee } from "./tee.js";


describe("tee()", () => {

	it("should carry the item type over to the branches", async () => {

		(items([1, 2, 3]))(tee(peek(item => expectTypeOf(item).toEqualTypeOf<number>())));

	});

	it("should keep the feed type under branches preserving it", async () => {

		const feed = (items([1, 2, 3]))(tee(filter(n => n > 1), peek(console.log)));

		expectTypeOf(feed).toEqualTypeOf<Feed<number>>();

	});

	it("should report the type of the items the branches report", async () => {

		const feed = (items([1, 2, 3]))(tee(map(n => `<${n}>`), map(n => `[${n}]`)));

		expectTypeOf(feed).toEqualTypeOf<Feed<string>>();

	});

	it("should take the item type from the declared task", async () => {

		const task: Task<number, string> = tee(map(n => `<${n}>`));

		expectTypeOf(task).toEqualTypeOf<Task<number, string>>();

	});

	it("should reject branches unable to draw the items", async () => {

		// @ts-expect-error — a string branch cannot draw numbers
		(items([1, 2, 3]))(tee(map((item: string) => item.length)));

	});

	it("should reject branches reporting different types", async () => {

		// @ts-expect-error — a branch reporting strings cannot join one reporting numbers
		(items([1, 2, 3]))(tee(map(n => n*2), map(n => `<${n}>`)));

	});

});
