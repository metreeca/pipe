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
 * URL processing tasks.
 *
 * Provides ready-made {@link https://github.com/metreeca/flow @metreeca/flow} steps for working with resources
 * identified by URLs in pipelines run by the {@link https://github.com/metreeca/gear @metreeca/gear} executor.
 *
 * Each step sends its requests through the fetch client bound in the enclosing execution, so a job can swap in a
 * throttled, cached or stubbed client once and every step in the execution works through that client.
 *
 * @module index
 *
 * @see {@link https://www.rfc-editor.org/rfc/rfc3986 RFC 3986 Uniform Resource Identifier (URI): Generic Syntax}
 */

export * from "./fetch.js";
export * from "./crawl.js";
