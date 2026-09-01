# Results — 2026-09-01

## Scope and environment

- Git history through `f85283e`; final Worker configuration may have later
  evidence-only commits.
- Writer image:
  `186219257916.dkr.ecr.us-east-1.amazonaws.com/hev-lance-r2-write-bench@sha256:a478e8d163bd08e611ebf03572e59a090116104e3e6f7e62b8ec0b253d63f60a`
- Depot builds: `3xl1x33c3n` (measurement fields) and `964mtnl0jm`
  (funnel mutex); linux/amd64, pushed only to ECR.
- Lance 6.0.0, LanceDB 0.29.0, object_store 0.12.5.
- Isolated R2 bucket `lance-r2-write-bench`, location `WNAM`; Worker ingress
  observed at `DEN`. No Story data, namespace, Worker, or AWS gateway path was
  read or modified.
- Each acceptance trial: 6,000 rows, 128 dimensions, seed recorded per result,
  250 rows/chunk, batch size 128, 24 chunks, five simultaneous outbound writer
  calls maximum, `independent` unless named funnel.
- Wrangler CPU limit: 30,000 ms. A redacted live tail sample records 0 ms CPU /
  1 ms wall for health and 1 ms CPU / 3,378 ms wall for a proxied exact query;
  container work is outside Worker CPU. Per-call wall time is in every JSONL.
- Peak writer RSS ranged from 104–120 MiB in clean 4/8-writer trials.

R2 does not expose per-prefix operation/byte counters to the Worker. This is
recorded as unavailable rather than estimated. The verifier reports logical
payload bytes (3,986,280 per independent acceptance dataset); Lance metadata,
index, and object-store transfer bytes are not mislabeled as observable.

## R2 ladder

| Writers | Repeat | Result | Rows/s | Rebases | Read errors | Notes |
|---:|---:|---|---:|---:|---:|---|
| 1 | 1 | valid | 33.6 | 42 | 0 | one instance, concurrent independent calls |
| 1 | 2 | valid | 30.4 | 42 | 0 | repeat |
| 2 | 1 | valid | 38.4 | 42 | 0 | two actual writer IDs |
| 2 | 2 | valid | 16.2 | 41 | 0 | forced rollout/restart included in elapsed time |
| 4 | 1 | valid | 26.3 | 36 | 0 | capacity failures/recovery included in elapsed time |
| 4 | 2 | valid | 94.0 | 36 | 0 | clean repeat |
| 8 | 1 | valid | 18.2 | 38 | 0 | capacity failures/recovery included in elapsed time |
| 8 | 2 | valid | 68.1 | 41 | 0 | clean repeat |
| 1 (funnel) | 1 | valid | 45.9 | 0 | 0 | eight producers, serialized commits |

All nine datasets contain 54,000/54,000 expected rows in aggregate. Every ID
appears exactly once; schema, vector width, seed, sampled rows, and checksums
match. All checkpoints finished committed: zero prepared, commit-attempted,
failed, or orphan states. Every Lance version was openable and countable.

The 108 reads issued during the nine runs all succeeded. Ninety were expected
snapshot-stale observations because they ran before the final version; none was
unreadable or partial. Combined client query latency was 6,321 ms p50, 19,971 ms
p95, and 26,097 ms maximum under concurrent local-client/R2 load. Exact and
indexed queries always returned source row 0 first for the deterministic source
0 vector.

The clean eight-writer repeat was 48.3% faster than the serialized funnel; the
funnel paid a 32.5% throughput penalty relative to independent. Independent is
therefore retained. Funnel remains explicit and tested, not a silent fallback.

## MinIO integration ladder

The local S3-compatible ladder preceded R2:

- One writer: 10/10 unique rows, duplicate chunk replay performed zero new
  commit attempts, three versions readable, exact source-3 query returned ID 3.
- Three independent OS processes: 130/130 unique rows across 13 committed
  checkpoints and three writer IDs; all 14 versions readable.
- Query-under-write: 2,000/2,000 rows after 15 concurrent chunks across three
  processes; 20/20 indexed/exact queries succeeded, versions 12/13 were observed
  during writes, and all final 18 versions were readable.
- Process termination/resume: a 100,000-row, 256-dimensional chunk reached a
  durable `prepared` checkpoint, its listener process was killed, and the same
  run/chunk was submitted after restart. The final 100,000 IDs are unique with
  no omissions or duplicates; both versions are readable.
- Corrected funnel: ten concurrent producers into one process created strictly
  sequential versions 2–11, zero rebases/retries, and 1,000/1,000 unique rows.

The current status and full-verification responses are under `results/minio/`.
The termination event itself was observed at the process boundary; the final
checkpoint and verifier artifacts are preserved rather than presenting a client
pause as process termination.

## Interruption, retry, and failure evidence

The second two-writer run was interrupted by an immediate Cloudflare container
rollout after one 250-row chunk had committed. The durable status object showed
exactly one committed checkpoint. Restarting at cursor 0 returned that chunk
with `commit_attempts: 0`, continued on a second named writer, and finished with
6,000 unique rows and all 26 versions readable.

At four and eight writers, the original app-level `max_instances` values (8,
then 16) counted old instances still draining. Cloudflare returned HTML 500s;
live tail reported `Maximum number of running container instances exceeded`.
Nine bounded client retries are preserved. Raising drain capacity to 32 while
keeping `BENCH_MAX_WRITERS=8` fixed the lifecycle constraint. Chunks that had
committed before a lost response returned with `commit_attempts: 0` on replay.
These were platform-capacity failures, not Lance corruption or exhausted commit
conflicts.

Other preserved failures:

- The first Docker build lacked `protoc`; the second also lacked the standard
  Google protobuf includes. The final builder installs `protobuf-compiler` and
  `libprotobuf-dev`.
- The first smoke indexed after 250 rows and Lance rejected PQ training because
  it requires at least 256 rows. The client now delays indexing until the floor.
- Rust 1.98 produced an incremental compiler ICE after a struct edit; a clean
  package rebuild succeeded. It did not affect a runtime trial.

LanceDB's public append result does not expose internal conflict/retry counts.
Outer calls recorded zero conflicts/retries/exhaustion; Lance's bounded internal
commit loop resolved the observed version-gap rebases. No result labels version
gaps as an exact conflict count.

## Compatibility and embedding boundary

The existing `story-photo/phase1-gateway` binary was run locally with
`HEVSEARCH_STORAGE_URI=s3://lance-r2-write-bench/runs` and bucket-scoped
temporary credentials. It opened `r2-smoke-2w-20260901/data.lance` at version
10 through the existing `search-embedded` path and returned three rows from an
ANN query. The raw response is
[`phase1-reader-compatibility.json`](results/r2/phase1-reader-compatibility.json).

[`workers-ai-text-embedding.json`](results/r2/workers-ai-text-embedding.json)
records a real Workers AI call: `@cf/baai/bge-base-en-v1.5`, 768 dimensions,
225 ms, and 9 prompt tokens. Migration vectors are always carried verbatim;
this call exercises only the boundary.

Workers AI offered no CLIP or SigLIP image embedding model. RFC 0110 implements
CLIP inside the gateway embed wire and explicitly adds no HTTP surface, so the
harness could not exercise an authenticated image container route. The expected
auth/request/response contract and product gap are filed in
[hev/layer-pro#536](https://github.com/hev/layer-pro/issues/536). Captions and
other vision-language models were not presented as compatible substitutes.

## Evidence map

- [`results/r2/summary.json`](results/r2/summary.json): machine-readable scope
  and all acceptance summaries.
- `results/r2/*.jsonl`: raw requests, writer IDs, checkpoints, reads, retries,
  wall time, placement, git SHA, and image digest.
- `results/r2/*-summary.json`: reproducible per-trial reductions.
- [`worker-tail-sample.redacted.json`](results/r2/worker-tail-sample.redacted.json):
  redacted Worker CPU/wall/outcome sample.
- `results/minio/`: final serialized-funnel responses and full verification.

The source fixture and all datasets are deterministic synthetic data. No secret
or dataset is committed; live R2 objects remain in the isolated benchmark bucket.
