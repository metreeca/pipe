/*
 * Copyright © 2026 Metreeca srl
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

import type { IRI } from "@metreeca/core/resource";
import { items } from "@metreeca/flow/feeds";
import { toArray } from "@metreeca/flow/sinks";
import { describe, expect, it } from "vitest";
import { retire } from "./retire.js";


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

describe("retire()", () => {

	const current: readonly IRI[] = ["/items/1", "/items/2", "/items/3", "/items/4"];

	function scan(
		scanned: readonly IRI[],
		options: { readonly atomic?: boolean, readonly survival: number, readonly existing?: readonly IRI[] }
	) {
		return items(scanned)(retire({ ...options, existing: items(options.existing ?? current) }))(toArray());
	}


	it("returns the existing identifiers the scan no longer lists", async () => {

		expect(await scan(["/items/1", "/items/2", "/items/3"], { survival: 0 })).toEqual(["/items/4"]);

	});

	it("returns nothing where the scan lists every existing identifier", async () => {

		expect(await scan(current, { survival: 0 })).toEqual([]);

	});

	it("ignores scanned identifiers the store doesn't hold", async () => {

		expect(await scan(["/items/1", "/items/2"], { survival: 0, existing: ["/items/1"] })).toEqual([]);

	});

	it("returns each existing identifier once", async () => {

		expect(await scan([], { survival: 0, existing: ["/items/1", "/items/1", "/items/2"] }))
			.toEqual(["/items/1", "/items/2"]);

	});

	it("returns nothing for an empty store", async () => {

		expect(await scan([], { survival: 1, existing: [] })).toEqual([]);

	});

	it.each([-0.1, 1.1, Number.NaN])("rejects a survival rate of %s as the task is created", async survival => {

		expect(() => retire({ survival, existing: items([]) })).toThrow(TypeError);

	});


	describe("above the survival floor", () => {

		it("returns the retired identifiers where the scan reaches the floor exactly", async () => {

			expect(await scan(["/items/1", "/items/2", "/items/3"], { survival: 0.75 })).toEqual(["/items/4"]);

		});

		it("returns the retired identifiers under atomic", async () => {

			expect(await scan(["/items/1", "/items/2", "/items/3"], { atomic: true, survival: 0.75 }))
				.toEqual(["/items/4"]);

		});

		it("rounds a fractional floor up to a whole identifier", async () => {

			const existing: readonly IRI[] = ["/items/1", "/items/2", "/items/3"];

			expect(await scan(["/items/1", "/items/2"], { survival: 0.5, existing })).toEqual(["/items/3"]);

		});

		it("retires every existing identifier where the scan renames them all", async () => {

			expect(await scan(["/renamed/1", "/renamed/2", "/renamed/3", "/renamed/4"], { survival: 0.75 }))
				.toEqual(current);

		});

		it("weighs the whole scan rather than the identifiers it carries over", async () => {

			expect(await scan(["/items/1", "/renamed/2", "/renamed/3", "/renamed/4"], { survival: 0.75 }))
				.toEqual(["/items/2", "/items/3", "/items/4"]);

		});

	});


	describe("below the survival floor", () => {

		it("skips the retirements", async () => {

			expect(await scan(["/items/1"], { survival: 0.75 })).toEqual([]);

		});

		it("skips the retirements where the scan falls short of a fractional floor", async () => {

			const existing: readonly IRI[] = ["/items/1", "/items/2", "/items/3"];

			expect(await scan(["/items/1"], { survival: 0.5, existing })).toEqual([]);

		});

		it("skips the retirements where a short scan lists none of the existing identifiers", async () => {

			expect(await scan(["/renamed/1"], { survival: 0.75 })).toEqual([]);

		});

		it("fails the run under atomic", async () => {

			await expect(scan(["/items/1"], { atomic: true, survival: 0.75 })).rejects.toThrow(RangeError);

		});

	});


	describe("with a degenerate survival rate", () => {

		it("retires every existing identifier at 0, even after an empty scan", async () => {

			expect(await scan([], { survival: 0 })).toEqual(current);

		});

		it("retires nothing at 1 unless the scan lists every existing identifier", async () => {

			expect(await scan(["/items/1", "/items/2", "/items/3"], { survival: 1 })).toEqual([]);

		});

	});

});
