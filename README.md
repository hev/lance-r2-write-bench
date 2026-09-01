# lance-r2-write-bench

A standalone, Apache-2.0 benchmark for concurrent Lance commits through
Cloudflare R2's S3-compatible endpoint.

## Decision

**Independent commits are safe within the measured bounds and retry policy.**
Keep independent fan-out; do not funnel by default.

On 2026-09-01, Lance 6.0.0 / LanceDB 0.29.0 / object_store 0.12.5 completed
two live R2 trials at each of 1, 2, 4, and 8 actual named writer instances.
Each trial wrote 6,000 deterministic 128-dimensional rows in 24 chunks. All
48,000 independent rows were present exactly once, every created Lance version
was readable, and all 96 indexed/exact reads during writes succeeded. The
eight-writer clean repeat reached 68.1 rows/s. The serialized eight-producer
funnel reached 45.9 rows/s, 32.5% lower throughput.

This is not a claim of arbitrary multi-writer safety. It covers R2 through its
S3 endpoint, at most five simultaneous commits per Worker invocation, at most
eight named writers, these exact dependency versions, the synthetic schema and
payload sizes in the evidence, and Lance's bounded internal retry policy. See
[RESULTS.md](RESULTS.md) and [`results/r2/summary.json`](results/r2/summary.json).

## Boundary

The JavaScript Worker is a bounded ingress/orchestrator. It never creates Lance
files. Named `LanceWriter` container instances run the Rust service and use the
public LanceDB/Lance/object_store APIs to prepare and commit chunks. In
`independent` mode each producer maps to a distinct named container; in `funnel`
mode all commits map to writer zero. The mode is always present in requests and
results; there is no automatic fallback. Funnel commits are protected by a
process-local mutex; producers remain concurrent but manifest commits serialize.

The versioned `synthetic-v1` source fixture is a small recipe, not a corpus.
The Worker reads and transforms that fixture into bounded range/chunk requests;
the Rust writer deterministically materializes rows. A Worker invocation issues
at most five container calls, leaving one of Cloudflare's six outbound slots
free, and returns a resume cursor. Correctness never depends on `waitUntil()` or
one immortal request.

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

Start the Rust service against MinIO, then submit stable `/chunks/commit`
requests or use the live-trial client against a deployed Worker:

```sh
BENCH_URL=https://your-worker.workers.dev \
BENCH_TOKEN="$BENCH_AUTH_TOKEN" \
RUN_ID=trial-8w ROWS=6000 DIMENSIONS=128 CHUNK_SIZE=250 \
BATCH_SIZE=128 WRITERS=8 MODE=independent SEED=281 \
node scripts/run_worker_trial.mjs > results.jsonl

node scripts/summarize_trial.mjs results.jsonl
```

`POST /run` accepts the same fields plus `cursor`, `max_chunks` (1–5),
`payload_shape`, `source_fixture`, and `max_retries`. `GET /status/:run_id`,
`POST /verify`, `POST /query`, and `POST /index` expose checkpoint, correctness,
read, and index operations. All routes require `Authorization: Bearer ...`.

Build and deployment use Depot and the mesh-account ECR only:

```sh
depot build --project t4vlld595v --platform linux/amd64 \
  --tag 186219257916.dkr.ecr.us-east-1.amazonaws.com/hev-lance-r2-write-bench:GIT_SHA \
  --push .
npx wrangler deploy --dry-run
npx wrangler deploy --containers-rollout immediate
```

Copy `.env.example` to an ignored `.env` for the local Rust service. Cloudflare
secrets are set with `wrangler secret put`; do not put credentials in
`wrangler.jsonc` or commit `.dev.vars`.

## Versions

The experiment pins Lance `6.0.0`, LanceDB `0.29.0`, and object_store `0.12.5`.
The deployed linux/amd64 image is pinned in `wrangler.jsonc` by ECR digest.

Lance 6 routes S3 commits through `ConditionalPutCommitHandler`, creates the
manifest with object_store `PutMode::Create`, and maps already-exists /
precondition failures to commit conflicts. Lance's `CommitConfig::default()`
has an internal 20-retry ceiling. The harness adds a configurable, bounded
outer retry (default 8) with deterministic exponential jitter. Because LanceDB
does not expose the internal conflict counter, results distinguish outer retries
from **observed rebases** (`committed_version - base_version - 1`); observed
rebases are evidence of overlap, not an exact conflict count.

Workers AI text evidence used `@cf/baai/bge-base-en-v1.5` (768 dimensions,
225 ms, 9 prompt tokens). Workers AI had no CLIP/SigLIP image embedding model.
RFC 0110's CLIP implementation has no standalone HTTP container route, so no
incompatible substitute was used; the owning gap is
[hev/layer-pro#536](https://github.com/hev/layer-pro/issues/536).

## License

Apache-2.0. Synthetic data only. This project contains no Layer implementation,
Story dataset, client data, or production namespace configuration.
