# @metreeca/pipe-url

[![npm](https://img.shields.io/npm/v/@metreeca/pipe-url)](https://www.npmjs.com/package/@metreeca/pipe-url)

URL processing tasks for [@metreeca/pipe](https://github.com/metreeca/pipe).

# Installation

```shell
npm install @metreeca/gear      # the job executor
npm install @metreeca/pipe-url  # this package
```

> [!IMPORTANT]
>
> Node.js 22 or later is required.

> [!WARNING]
>
> TypeScript consumers must use `"moduleResolution": "nodenext"/"node16"/"bundler"` in `tsconfig.json`.
> The legacy `"node"` resolver is not supported.

# Usage

| Task               | Description                    |
|--------------------|--------------------------------|
| [`fetch()`][fetch] | Resource fetcher               |
| [`crawl()`][crawl] | URL graph walker and harvester |

[fetch]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-url.fetch.html

[crawl]: https://metreeca.github.io/pipe/functions/_metreeca_pipe-url.crawl.html

# Support

- open an [issue](https://github.com/metreeca/pipe/issues) to report a problem or to suggest a new feature
- start a [discussion](https://github.com/metreeca/pipe/discussions) to ask a how-to question or to share an idea

# License

This project is licensed under the Apache 2.0 License –
see [LICENSE](https://github.com/metreeca/pipe?tab=Apache-2.0-1-ov-file) file for details.
