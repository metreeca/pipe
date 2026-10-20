# @metreeca/pipe-store

[![npm](https://img.shields.io/npm/v/@metreeca/pipe-store)](https://www.npmjs.com/package/@metreeca/pipe-store)

Store processing tasks for [@metreeca/pipe](https://github.com/metreeca/pipe).

Provides ready-made [@metreeca/flow](https://github.com/metreeca/flow) steps for working with
[@metreeca/keep](https://github.com/metreeca/keep) stores in pipelines run by the
[@metreeca/gear](https://github.com/metreeca/gear) job executor.

# Installation

```shell
npm install @metreeca/gear                       # the job executor
npm install @metreeca/pipe-store                 # this package
npm install @metreeca/keep-<connector>           # store connector for the target backend
```

> [!IMPORTANT]
>
> Node.js 22 or later is required.

> [!WARNING]
>
> TypeScript consumers must use `"moduleResolution": "nodenext"/"node16"/"bundler"` in `tsconfig.json`.
> The legacy `"node"` resolver is not supported.

# Usage

| Task                 | Description               |
|----------------------|---------------------------|
| [`lookup()`][lookup] | Store resource retriever  |
| [`retire()`][retire] | Retired resource selector |
| [`settle()`][settle] | Resource delta writer     |

[lookup]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-store.lookup.html

[retire]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-store.retire.html

[settle]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-store.settle.html

[createStore]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-store.createStore.html

The store service has no default: a job binds a store connector to [`createStore()`][createStore] once, and every task
or sink in the execution then reads from or writes to the store that connector reaches.

# Support

- open an [issue](https://github.com/metreeca/pipe/issues) to report a problem or to suggest a new feature
- start a [discussion](https://github.com/metreeca/pipe/discussions) to ask a how-to question or to share an idea

# License

This project is licensed under the Apache 2.0 License –
see [LICENSE](https://github.com/metreeca/pipe?tab=Apache-2.0-1-ov-file) file for details.
