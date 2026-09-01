# lance-r2-write-bench

A standalone, Apache-2.0 benchmark for concurrent Lance commits through
Cloudflare R2's S3-compatible endpoint.

> **Experiment in progress.** No multi-writer safety conclusion has been made.
> The repository will publish a scoped decision only after repeated live R2
> trials, interruption/resume verification, query-under-write measurements, and
> independent reader compatibility checks are complete.

## Boundary

The JavaScript Worker is a bounded ingress/orchestrator. It never creates Lance
files. Named `LanceWriter` container instances run the Rust service and use the
public LanceDB/Lance/object_store APIs to prepare and commit chunks. In
`independent` mode each producer maps to a distinct named container; in `funnel`
mode all commits map to writer zero. The mode is always present in requests and
results; there is no automatic fallback.

The Worker exposes an authenticated `POST /run` surface shaped for a future
Layer `Function` push-dispatch contract. Layer currently exposes `worker.url`
in its schema but does not dispatch to it; see
[hev/layer-pro#535](https://github.com/hev/layer-pro/issues/535). This harness is
therefore standalone and does not claim current Layer integration.

## Local checks

```sh
docker compose up -d minio minio-init
cargo test
npm install
npm test
npx wrangler deploy --dry-run
```

Copy `.env.example` to an ignored `.env` for the local Rust service. Cloudflare
secrets are set with `wrangler secret put`; do not put credentials in
`wrangler.jsonc` or commit `.dev.vars`.

## Versions

The initial experiment pins Lance `6.0.0`, LanceDB `0.29.0`, and object_store
`0.12.x`. All eventual claims and raw result records will also include the git
SHA, container digest, placement, dataset shape, writer identities, and exact
dependency versions.

## License

Apache-2.0. Synthetic data only. This project contains no Layer implementation,
Story dataset, client data, or production namespace configuration.
