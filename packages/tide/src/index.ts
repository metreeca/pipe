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
 * Source access contracts and shared services.
 *
 * Defines the sources a job reads from and writes to, keeping the job independent of the system at hand: a job names a
 * source by its contract rather than importing a concrete client, and a binding substitutes a stubbed, throttled or
 * recorded implementation for the duration of an execution.
 *
 * > [!IMPORTANT]
 * >
 * > Jobs are set up and run with the executor, binder and service locator from
 * > {@link https://github.com/metreeca/gear @metreeca/gear}: this package contributes the source services those
 * > primitives bind and resolve, not an execution runtime of its own.
 *
 * @module index
 */

export {};
