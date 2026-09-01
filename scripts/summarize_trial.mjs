#!/usr/bin/env node
import { readFile } from "node:fs/promises";

const input = process.argv[2];
if (!input) throw new Error("usage: summarize_trial.mjs results.jsonl");
const records = (await readFile(input, "utf8")).trim().split("\n").filter(Boolean).map(JSON.parse);
const pages = records.filter((record) => record.event === "run_page");
const queries = records.filter((record) => record.event === "query");
const chunks = pages.flatMap((page) => page.body.chunks || []);
const latencies = queries.filter((query) => query.ok).map((query) => query.wall_ms).sort((a, b) => a - b);
const percentile = (p) => latencies.length ? latencies[Math.min(latencies.length - 1, Math.ceil(p * latencies.length) - 1)] : null;
const start = records.find((record) => record.event === "trial_start");
const end = records.findLast((record) => record.event === "trial_complete") || records.at(-1);
const elapsedSeconds = start && end ? (Date.parse(end.at) - Date.parse(start.at)) / 1000 : null;
const verification = records.find((record) => record.event === "verification")?.body || null;
const status = records.find((record) => record.event === "status")?.body || null;
const finalVersion = verification?.readable_versions?.at(-1) || null;
const summary = {
  run_id: start?.spec.run_id,
  started_at: start?.at,
  finished_at: end?.at,
  git_sha: start?.git_sha,
  image_digest: start?.image_digest,
  mode: start?.spec.mode,
  seed: start?.spec.seed,
  rows: start?.spec.rows,
  dimensions: start?.spec.dimensions,
  batch_size: start?.spec.batch_size,
  chunk_size: start?.spec.chunk_size,
  producer_count: start?.spec.writers,
  actual_writer_ids: [...new Set(chunks.map((chunk) => chunk.writer_id))].sort(),
  chunks: chunks.length,
  commit_attempts: chunks.reduce((sum, chunk) => sum + (chunk.result.commit_attempts || 0), 0),
  outer_conflict_retries: chunks.reduce((sum, chunk) => sum + (chunk.result.conflict_retries || 0), 0),
  observed_rebases: chunks.reduce((sum, chunk) => {
    const checkpoint = chunk.result.checkpoint;
    return sum + Math.max(0, (checkpoint.committed_lance_version || 0) - (checkpoint.base_lance_version || 0) - 1);
  }, 0),
  exhausted_retries: chunks.filter((chunk) => chunk.status >= 300).length,
  retry_delay_policy: "deterministic jitter: 25ms * 2^attempt (capped at attempt 6) + 0..40ms",
  elapsed_seconds: elapsedSeconds,
  rows_per_second: elapsedSeconds && verification ? verification.actual_rows / elapsedSeconds : null,
  payload_bytes: verification?.payload_bytes || null,
  bytes_per_second: elapsedSeconds && verification ? verification.payload_bytes / elapsedSeconds : null,
  peak_writer_rss_kb: Math.max(0, ...chunks.map((chunk) => chunk.result.peak_rss_kb || 0)) || null,
  checkpoint_states: status ? { prepared: status.prepared, commit_attempted: status.commit_attempted, committed: status.committed, failed: status.failed } : null,
  orphan_checkpoints: status ? status.prepared + status.commit_attempted + status.failed : null,
  lance_versions_created: finalVersion,
  query_successes: queries.filter((query) => query.ok).length,
  query_errors: queries.filter((query) => !query.ok).length,
  query_latency_ms: { p50: percentile(0.5), p95: percentile(0.95), max: latencies.at(-1) || null },
  stale_version_observations: finalVersion ? queries.filter((query) => query.ok && query.body.lance_version < finalVersion).length : null,
  unreadable_or_partial_observations: queries.filter((query) => !query.ok).length,
  r2_operation_counts: null,
  r2_operation_note: "R2 per-prefix request/byte counters are not exposed to this Worker; dataset payload bytes are reported above",
  verification,
};
process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
