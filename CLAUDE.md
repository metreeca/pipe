---
title: Project Guidelines
description: Development guidelines and conventions for the @metreeca/flow package.
---

> [!CAUTION]
>
> - **UNDER NO CIRCUMSTANCES** rearchitect or refactor unrelated code beyond the requested scope.
> - **NEVER** make unsolicited changes or revert **unrelated** user edits.
> - **ONLY** modify code when explicitly requested or clearly required.

> [!IMPORTANT]
>
> - **ALL** relevant skills **MUST** be used when applicable without continuous prompting.
> - **SKILL** guidance **ALWAYS** supersedes internal general-purpose knowledge.

# Documentation

> [!CAUTION]
> **NEVER** cross-reference one task symbol from another: no `@see` entries and no `{@link}` mentions pointing from a
> task to a sibling. Each task documents its own contract and stands on its own; the relations between tasks are
> covered by the module description and by the README. Where prose has to name a sibling, write it as a backticked
> call, as `flat()`, never as a link.

# NPM Scripts

- **`npm run clean`** - Remove dependencies and build artefacts
- **`npm run prime`** - Install dependencies from the lockfile
- **`npm run setup`** - Configure for local development
- **`npm run build`** - Compile sources and generate docs
- **`npm run check`** - Run the test suite
- **`npm run proof`** - Serve live docs
