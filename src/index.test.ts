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

import { describe, expect, it } from "vitest";
import { items, range } from "./feeds.js";
import { pipe } from "./index.js";
import { toArray } from "./sinks.js";
import { filter } from "./tasks.js";


describe("pipe()", () => {

	it("should return promise value directly", async () => {

		const value = await pipe(Promise.resolve(42));

		expect(value).toBe(42);

	});

	it("should retrieve async iterable from pipe", async () => {

		const values = await items(pipe(items(range(1, 4))))(toArray());

		expect(values).toEqual([1, 2, 3]);

	});

	it("should return async iterable for manual iteration", async () => {

		const iterable = pipe((items([1, 2, 3, 4]))(filter(x => x > 1)));

		const values: number[] = [];

		for await (const value of iterable) {
			values.push(value);
		}

		expect(values).toEqual([2, 3, 4]);

	});

});
