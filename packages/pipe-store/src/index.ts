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
 * Store processing tasks.
 *
 * Provides ready-made {@link https://github.com/metreeca/flow @metreeca/flow} steps for working with
 * {@link https://github.com/metreeca/keep @metreeca/keep} stores in pipelines run by the
 * {@link https://github.com/metreeca/gear @metreeca/gear} executor.
 *
 * A job binds a store connector to {@link createStore} once, and every step that needs a store uses that one.
 *
 * @module index
 */

import type { Store } from "@metreeca/keep";


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * Creates the default store.
 *
 * Identifies the store service that the steps of this package read from and write to.
 *
 * > [!IMPORTANT]
 * >
 * > No default is provided: a job must bind a store connector before creating tasks that rely on it.
 *
 * @returns Never: the store service has no default implementation
 *
 * @throws {@link !Error Error} Always, as no store connector is bound to the service in the enclosing execution
 */
export function createStore(): Store {

	throw new Error("undefined store service");

}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

export * from "./lookup.js";
export * from "./retire.js";
export * from "./settle.js";
