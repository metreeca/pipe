# @metreeca/pipe-sparql

[![npm](https://img.shields.io/npm/v/@metreeca/pipe-sparql)](https://www.npmjs.com/package/@metreeca/pipe-sparql)

SPARQL processing tasks for [@metreeca/pipe](https://github.com/metreeca/pipe).

Provides ready-made [@metreeca/flow](https://github.com/metreeca/flow) steps for working with SPARQL repositories in
pipelines run by the [@metreeca/gear](https://github.com/metreeca/gear) job executor.

# Installation

```shell
npm install @metreeca/gear                       # the job executor
npm install @metreeca/pipe-sparql                # this package
npm install @metreeca/wire-sparql-<backend>      # repository connector for the target engine
```

> [!IMPORTANT]
>
> Node.js 22 or later is required.

> [!WARNING]
>
> TypeScript consumers must use `"moduleResolution": "nodenext"/"node16"/"bundler"` in `tsconfig.json`.
> The legacy `"node"` resolver is not supported.

Repositories are reached through the backend connectors listed by
[@metreeca/wire-sparql](https://metreeca.github.io/wire/modules/_metreeca_wire-sparql.html): install the one for the
engine the pipeline targets, together with any peer dependency that the connector itself requires.

# Usage

| Task                       | Description                     |
|----------------------------|---------------------------------|
| [`select()`][select]       | SPARQL `SELECT` query runner    |
| [`construct()`][construct] | SPARQL `CONSTRUCT` query runner |
| [`update()`][update]       | SPARQL Update request runner    |
| [`upload()`][upload]       | RDF graph uploader              |

[select]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-sparql.select.html

[construct]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-sparql.construct.html

[update]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-sparql.update.html

[upload]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-sparql.upload.html

[createRepository]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-sparql.createRepository.html

The repository service has no default: a job binds a repository connector to [`createRepository()`][createRepository]
once, and every task or sink given no repository of its own then reads from or writes to the repository it reaches.

# Support

- open an [issue](https://github.com/metreeca/pipe/issues) to report a problem or to suggest a new feature
- start a [discussion](https://github.com/metreeca/pipe/discussions) to ask a how-to question or to share an idea

# License

This project is licensed under the Apache 2.0 License –
see [LICENSE](https://github.com/metreeca/pipe?tab=Apache-2.0-1-ov-file) file for details.
