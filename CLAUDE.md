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

# NPM Scripts

- **`npm run clean`** - Remove dependencies and build artefacts
- **`npm run prime`** - Install dependencies from the lockfile
- **`npm run setup`** - Configure for local development
- **`npm run build`** - Compile sources and generate docs
- **`npm run check`** - Run the test suite
- **`npm run proof`** - Serve live docs
