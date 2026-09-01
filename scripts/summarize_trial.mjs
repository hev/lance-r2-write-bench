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
const summary = {
  run_id: records.find((record) => record.event === "trial_start")?.spec.run_id,
  mode: records.find((record) => record.event === "trial_start")?.spec.mode,
  producer_count: records.find((record) => record.event === "trial_start")?.spec.writers,
  actual_writer_ids: [...new Set(chunks.map((chunk) => chunk.writer_id))].sort(),
  chunks: chunks.length,
  commit_attempts: chunks.reduce((sum, chunk) => sum + (chunk.result.commit_attempts || 0), 0),
  outer_conflict_retries: chunks.reduce((sum, chunk) => sum + (chunk.result.conflict_retries || 0), 0),
  observed_rebases: chunks.reduce((sum, chunk) => {
    const checkpoint = chunk.result.checkpoint;
    return sum + Math.max(0, (checkpoint.committed_lance_version || 0) - (checkpoint.base_lance_version || 0) - 1);
  }, 0),
  query_successes: queries.filter((query) => query.ok).length,
  query_errors: queries.filter((query) => !query.ok).length,
  query_latency_ms: { p50: percentile(0.5), p95: percentile(0.95), max: latencies.at(-1) || null },
  verification: records.find((record) => record.event === "verification")?.body || null,
};
process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);

