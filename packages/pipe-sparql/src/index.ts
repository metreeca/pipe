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

/**
 * SPARQL processing tasks.
 *
 * Provides ready-made {@link https://github.com/metreeca/flow @metreeca/flow} steps for working with SPARQL
 * repositories in pipelines run by the {@link https://github.com/metreeca/gear @metreeca/gear} executor.
 *
 * Each step targets the repository bound to {@link createRepository} unless given one of its own, so a job binds its
 * repository once and every step in the execution works against that repository.
 *
 * @module index
 *
 * @see {@link https://www.w3.org/TR/sparql11-overview/ SPARQL 1.1 Overview}
 */

import type { Repository } from "@metreeca/wire-sparql";


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates the default repository.
 *
 * Identifies the repository service the tasks and sinks of this package target.
 *
 * > [!IMPORTANT]
 * >
 * > No default is provided: a job must bind a repository connector before creating tasks that rely on it.
 *
 * @returns Never: the repository service has no default implementation
 *
 * @throws {@link !Error Error} Always, as no repository connector is bound to the service in the enclosing execution
 */
export function createRepository(): Repository {

	throw new Error("undefined repository service");

}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

export * from "./select.js";
export * from "./construct.js";
export * from "./update.js";
export * from "./upload.js";
