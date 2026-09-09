# @metreeca/tide

[![npm](https://img.shields.io/npm/v/@metreeca/tide)](https://www.npmjs.com/package/@metreeca/tide)

Source access contracts and shared services for [@metreeca/tide](https://github.com/metreeca/tide).

A consumer sets up a [@metreeca/gear](https://github.com/metreeca/gear) executor, binding the sources a job reads from
and writes to to the implementations chosen for the run. The job's tasks then resolve each source through the locator,
naming it by its contract rather than importing a concrete client.

Binding a different implementation leaves the job unchanged: the same job runs against live systems, against recorded
content, or against any custom client honouring the same contracts.

# Installation

```shell
npm install @metreeca/gear  # the job executor
npm install @metreeca/tide  # this package
```

> [!IMPORTANT]
>
> Node.js 22 or later is required.

> [!WARNING]
>
> TypeScript consumers must use `"moduleResolution": "nodenext"/"node16"/"bundler"` in `tsconfig.json`.
> The legacy `"node"` resolver is not supported.

# Usage

| Module                 | Description                                 |
|------------------------|---------------------------------------------|
| [@metreeca/tide][tide] | Source access contracts and shared services |

[tide]: https://metreeca.github.io/tide/modules/_metreeca_tide.index.html

# Support

- open an [issue](https://github.com/metreeca/tide/issues) to report a problem or to suggest a new feature
- start a [discussion](https://github.com/metreeca/tide/discussions) to ask a how-to question or to share an idea

# License

This project is licensed under the Apache 2.0 License –
see [LICENSE](https://github.com/metreeca/tide?tab=Apache-2.0-1-ov-file) file for details.
