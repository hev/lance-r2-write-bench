#!/usr/bin/env node

const baseUrl = process.env.BENCH_URL;
const token = process.env.BENCH_TOKEN;
if (!baseUrl || !token) throw new Error("BENCH_URL and BENCH_TOKEN are required");

const spec = {
  run_id: process.env.RUN_ID || `trial-${Date.now()}`,
  rows: Number(process.env.ROWS || 10000),
  dimensions: Number(process.env.DIMENSIONS || 128),
  chunk_size: Number(process.env.CHUNK_SIZE || 500),
  batch_size: Number(process.env.BATCH_SIZE || 256),
  writers: Number(process.env.WRITERS || 2),
  mode: process.env.MODE || "independent",
  seed: Number(process.env.SEED || 17),
  payload_shape: process.env.PAYLOAD_SHAPE || "vector-text",
  max_retries: Number(process.env.MAX_RETRIES || 8),
  max_chunks: Number(process.env.MAX_CHUNKS || 5),
};
let cursor = Number(process.env.START_CURSOR || 0);
const interruptAfterPages = Number(process.env.INTERRUPT_AFTER_PAGES || 0);

function emit(event, value = {}) {
  process.stdout.write(`${JSON.stringify({ at: new Date().toISOString(), event, ...value })}\n`);
}

async function api(path, init = {}) {
  const started = performance.now();
  const response = await fetch(new URL(path, baseUrl), {
    ...init,
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json", ...(init.headers || {}) },
  });
  const body = await response.json();
  if (!response.ok) throw new Error(`${path}: ${response.status} ${JSON.stringify(body)}`);
  return { body, wall_ms: performance.now() - started, colo: response.headers.get("cf-ray")?.split("-")[1] || null };
}

async function query(exact) {
  try {
    const result = await api("/query", { method: "POST", body: JSON.stringify({
      run_id: spec.run_id, dimensions: spec.dimensions, seed: spec.seed,
      source_index: 0, exact, limit: 10,
    }) });
    emit("query", { exact, ok: true, ...result });
  } catch (error) {
    emit("query", { exact, ok: false, error: String(error) });
  }
}

emit("trial_start", { spec, cursor, git_sha: process.env.GIT_SHA || null, image_digest: process.env.IMAGE_DIGEST || null });
let pages = 0;
let indexed = false;
while (cursor !== null) {
  const minimumIndexChunks = Math.ceil(256 / spec.chunk_size);
  const pageSpec = { ...spec, cursor, max_chunks: cursor === 0 ? Math.min(5, minimumIndexChunks) : spec.max_chunks };
  const pending = api("/run", { method: "POST", body: JSON.stringify(pageSpec) });
  if (indexed) await Promise.all([query(true), query(false)]);
  const result = await pending;
  emit("run_page", { cursor, ...result });
  cursor = result.body.next_cursor;
  pages += 1;
  if (!indexed) {
    const index = await api("/index", { method: "POST", body: JSON.stringify({ run_id: spec.run_id }) });
    emit("index_created", index);
    indexed = true;
  }
  if (interruptAfterPages > 0 && pages >= interruptAfterPages && cursor !== null) {
    emit("intentional_interrupt", { resume_cursor: cursor });
    process.exit(75);
  }
}
const status = await api(`/status/${encodeURIComponent(spec.run_id)}`);
emit("status", status);
const verify = await api("/verify", { method: "POST", body: JSON.stringify({
  run_id: spec.run_id, rows: spec.rows, dimensions: spec.dimensions, seed: spec.seed,
}) });
emit("verification", verify);
await Promise.all([query(true), query(false)]);
emit("trial_complete", { run_id: spec.run_id, valid: verify.body.valid });
if (!verify.body.valid) process.exitCode = 1;
