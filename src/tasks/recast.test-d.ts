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
import { toArray, toSet } from "../sinks/index.js";
import { recast } from "./recast.js";
import { map } from "./map.js";


describe("recast()", () => {

	it("should carry the item type over to the mapper", async () => {

		(items([1, 2, 3]))(recast(async feed => {

			expectTypeOf(feed).toEqualTypeOf<Feed<number>>();

			return [];

		}));

	});

	it("should report the type of the items the mapper computes", async () => {

		const feed = (items([1, 2, 3]))(recast(async feed => (await feed(toArray())).map(n => `<${n}>`)));

		expectTypeOf(feed).toEqualTypeOf<Feed<string>>();

	});

	it("should report the type of the items a mapper hands back without awaiting", async () => {

		const feed = (items([1, 2, 3]))(recast(feed => feed(map(n => `<${n}>`))));

		expectTypeOf(feed).toEqualTypeOf<Feed<string>>();

	});

	it("should keep the feed type under a sink preserving it", async () => {

		const feed = (items([1, 2, 3]))(recast(toSet()));

		expectTypeOf(feed).toEqualTypeOf<Feed<number>>();

	});

	it("should take the item type from the declared task", async () => {

		const task: Task<number, string> = recast(async feed => (await feed(toArray())).map(n => `<${n}>`));

		expectTypeOf(task).toEqualTypeOf<Task<number, string>>();

	});

	it("should reject a mapper unable to draw the items", async () => {

		// @ts-expect-error — a string mapper cannot draw numbers
		(items([1, 2, 3]))(recast(async (feed: Feed<string>) => feed(toArray())));

	});

});
