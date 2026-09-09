> [!CAUTION]
>
> - **ONLY** modify code when explicitly requested or clearly required.
> - **NEVER** make unsolicited changes or revert **unrelated** user edits.
> - **ALWAYS** monitor IDE diagnostics when working on a file

> [!CAUTION]
> Activating and following skill guidance is **MANDATORY** for every task. Before starting any work, identify and
> activate all relevant skills. Skill instructions are binding and override default behaviours. When in doubt about
> whether skill guidance is current, relevant skills MUST be reloaded.

# Overview

`@metreeca/tide` is a standalone, general-purpose monorepo collecting the source access framework core and its source
family task packages, each sitting directly under `packages/` (for example `packages/tide-sql/`).

Jobs run under the `@metreeca/gear` executor: this repository contributes retrieval and persistence tasks, **NEVER** an
execution runtime of its own. Reach for `executor`, `bind` and `service` from `@metreeca/gear` rather than
reimplementing them, and keep the service contracts compatible with the ones `@metreeca/gear` already resolves.

Content arrives and leaves as media-typed payloads: parsing and serialising them belongs to `@metreeca/mime`, and this
repository **NEVER** duplicates that work. A source package moves bytes and records across the boundary; interpreting
what they carry sits on the other side of it.

# References

- [@metreeca/core](https://github.com/metreeca/core) - Core utilities and shared types
- [@metreeca/flow](https://github.com/metreeca/flow) - Composable async iterable processing
- [@metreeca/tape](https://github.com/metreeca/tape) - Simplified facade for the LogTape logging framework
- [@metreeca/gear](https://github.com/metreeca/gear) - Ready-made tasks and shared services for ETL jobs, supplying the
  job executor and service locator this repository builds on
- [@metreeca/mime](https://github.com/metreeca/mime) - Ready-made tasks for retrieving and parsing content by media
  type, covering the parsing and serialisation this repository hands off

# NPM Scripts

- **`npm run clean`** - Remove dependencies and build artefacts
- **`npm run prime`** - Install dependencies from the lockfile
- **`npm run setup`** - Configure for local development
- **`npm run build`** - Compile sources and generate docs
- **`npm run check`** - Run the test suite
- **`npm run proof`** - Build and serve docs

# Package Layout

The root `package.json` `workspaces` glob (`packages/*`) covers the framework packages, each in its own directory
immediately under `packages/` (for example `packages/tide`).

Source packages are self-contained leaves named after the family of systems they reach, not after the driver they reach
it with: `tide-sql`, not `tide-postgres` or `tide-knex`. Each pulls in the core package transitively and only the
drivers its own family needs.

Retrieval and persistence live together in the package for the family they address: the split follows the system a task
talks to, not the direction the content moves in.

# Shared Utilities

Reach for `@metreeca/core` before writing a helper: its `strings`, `numbers`, `arrays` and `structures` entry points
already cover text tidying, escaping, splitting and templating alongside the common collection and value operations. A
hand-rolled equivalent duplicates tested code and drifts from it, missing the edge cases the shared one handles.

Keep a local helper only where the shared one genuinely doesn't fit, and record in its doc comment what the difference
is, so the next reader doesn't take it for an oversight.

# Service Resolution

Calls to `service()` are **NEVER** inlined into a larger expression: always bind the resolved instance to a `const` on a
line of its own, then use it. This keeps the resolution point visible, since it depends on the enclosing execution
rather than on the surrounding expression.

```typescript
const store = service(getStore); // ✅
const record = lazy(async () => store(await key(source)));

const record = lazy(async () => service(getStore)(await key(source))); // ❌
```

# Testing

The root `vitest.config.ts` aliases all workspace `@metreeca/tide*` packages to their TypeScript source via regex, so
vitest transpiles directly from `src/` without requiring a prior build step. The resolver maps each `@metreeca/tide*`
specifier to `packages/<package>/src`; the aliases are convention-based and require no manual updates when adding
packages or subpath exports.

# Version Management

All workspace packages share the root `package.json` version. Beyond the `version` fields the release flow already
cascades, update the internal `@metreeca/tide*` dependency ranges in every `packages/**/package.json` to match.

When adding, removing, or renaming packages, update the package table in the root `README.md` Usage section to match.
