# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/metreeca/pipe/compare/v0.11.1...HEAD)

## [0.11.1](https://github.com/metreeca/pipe/compare/v0.11.0...v0.11.1) - 2026-10-20

### Fixed

- Publish `@metreeca/pipe-url` with the `0.11.0` content, which the registry refused under that version

## [0.11.0](https://github.com/metreeca/pipe/compare/v0.10.1...v0.11.0) - 2026-10-20

### Added

- `@metreeca/pipe-sparql` package with `select`, `construct`, `update` and `upload` steps for SPARQL endpoints
- `@metreeca/pipe-store` package with `lookup`, `retire` and `settle` steps for `@metreeca/keep` stores

### Changed

- Upgrade `@metreeca/core` to `^0.13.0`
- Upgrade `@metreeca/flow` to `^0.12.0`
- Upgrade `@metreeca/gear` to `^0.4.0`
- Upgrade `@metreeca/http` to `^0.5.0`
- Upgrade `@metreeca/tape` to `^0.12.0`

## [0.10.1](https://github.com/metreeca/pipe/compare/v0.10.0...v0.10.1) - 2026-10-14

### Changed

- Upgrade `@metreeca/core` to `^0.12.1`
- Upgrade `@metreeca/flow` to `^0.11.1`
- Upgrade `@metreeca/gear` to `^0.3.1`
- Upgrade `@metreeca/http` to `^0.4.1`
- Upgrade `@metreeca/tape` to `^0.11.1`

## [0.10.0](https://github.com/metreeca/pipe/releases/tag/v0.10.0) - 2026-10-01

Initial release of the source family task packages, running under the `@metreeca/gear` job executor.

- `@metreeca/pipe-url` — URL processing tasks, formerly `@metreeca/gear-url`
