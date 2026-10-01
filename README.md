# @metreeca/pipe

Ready-made tasks for retrieving and persisting data from external sources.

**@metreeca/pipe** brings ready-made [@metreeca/flow](https://github.com/metreeca/flow) tasks for moving content between
a pipeline and the systems it works with: drawing records out of web pages, object stores and databases, and writing
results back to them. The tasks run under the [@metreeca/gear](https://github.com/metreeca/gear) job executor, which
supplies the shared services they draw on.

- **Ready-Made Tasks**: retrieval and persistence, chaining alongside any other task
- **Shared Services**: clients, credentials and connection pools, built on demand and released after the run
- **Custom Bindings**: a stubbed, throttled or recorded source swapped in for a run, leaving the job untouched
- **Minimal Footprint**: one package per source family, each pulling in only the drivers that family needs

> [!IMPORTANT]
>
> Pipelines are server-side workloads targeting [Node.js](https://nodejs.org/) 22 or later, relying on facilities such
> as the filesystem, the process environment and `fetch`. The packages are not intended for the browser.

# Installation

```shell
npm install @metreeca/gear            # job executor and shared services
npm install @metreeca/pipe-<source>   # task package, one per source family
```

> [!WARNING]
>
> TypeScript consumers must use `"moduleResolution": "nodenext"/"node16"/"bundler"` in `tsconfig.json`.
> The legacy `"node"` resolver is not supported.

Add a task package for each source family the pipeline reaches. Source packages are self-contained leaves, each pulling
in only the drivers its own family needs. The job executor comes from [@metreeca/gear](https://github.com/metreeca/gear),
which the task packages pull in transitively; install it directly to set up and run a job.

| Package              | Description          |
|----------------------|----------------------|
| [@metreeca/pipe-url] | URL processing tasks |

[@metreeca/pipe-url]: https://metreeca.github.io/pipe/modules/_metreeca_pipe-url.html

# Usage

> [!NOTE]
>
> Each package documents its own API in its README and API reference; for complete coverage, see the
> [API reference](https://metreeca.github.io/pipe/).

# Support

- open an [issue](https://github.com/metreeca/pipe/issues) to report a problem or to suggest a new feature
- start a [discussion](https://github.com/metreeca/pipe/discussions) to ask a how-to question or to share an idea

# License

This project is licensed under the Apache 2.0 License –
see [LICENSE](https://github.com/metreeca/pipe?tab=Apache-2.0-1-ov-file) file for details.
