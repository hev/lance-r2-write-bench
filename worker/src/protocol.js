export function stableChunkId(runId, start, rows, seed) {
  return `${runId}:${seed}:${start}:${rows}`;
}

export function planChunks({ run_id, rows, chunk_size, seed }) {
  if (!run_id || !Number.isSafeInteger(rows) || rows < 1 || !Number.isSafeInteger(chunk_size) || chunk_size < 1) {
    throw new Error("run_id, positive rows, and positive chunk_size are required");
  }
  const chunks = [];
  for (let start = 0; start < rows; start += chunk_size) {
    const count = Math.min(chunk_size, rows - start);
    chunks.push({ chunk_id: stableChunkId(run_id, start, count, seed), start, rows: count });
  }
  return chunks;
}

export function timingSafeEqual(left, right) {
  const a = new TextEncoder().encode(String(left || ""));
  const b = new TextEncoder().encode(String(right || ""));
  if (a.byteLength !== b.byteLength) return false;
  let difference = 0;
  for (let i = 0; i < a.byteLength; i++) difference |= a[i] ^ b[i];
  return difference === 0;
}

export function checkpointTransitionAllowed(from, to) {
  return new Set([
    "prepared:commit-attempted",
    "prepared:committed",
    "commit-attempted:commit-attempted",
    "commit-attempted:committed",
    "commit-attempted:failed",
    "committed:committed",
  ]).has(`${from}:${to}`);
}

export function validateRun(input, maxWriters) {
  const mode = input.mode || "independent";
  if (!['independent', 'funnel'].includes(mode)) throw new Error("mode must be independent or funnel");
  const writers = Number(input.writers || 1);
  if (!Number.isInteger(writers) || writers < 1 || writers > maxWriters) throw new Error(`writers must be 1..${maxWriters}`);
  return {
    run_id: String(input.run_id),
    rows: Number(input.rows),
    dimensions: Number(input.dimensions || 128),
    chunk_size: Number(input.chunk_size || 1000),
    batch_size: Number(input.batch_size || 256),
    writers,
    mode,
    seed: Number(input.seed || 1),
    source_fixture: String(input.source_fixture || "synthetic-v1"),
    payload_shape: input.payload_shape || "vector-text",
    max_retries: Number(input.max_retries ?? 8),
    cursor: Number(input.cursor || 0),
    max_chunks: Number(input.max_chunks || 5),
  };
}

export function planInvocation(spec) {
  if (!Number.isSafeInteger(spec.cursor) || spec.cursor < 0) throw new Error("cursor must be a non-negative integer");
  if (!Number.isSafeInteger(spec.max_chunks) || spec.max_chunks < 1 || spec.max_chunks > 5) {
    throw new Error("max_chunks must be 1..5");
  }
  const all = planChunks(spec);
  const chunks = all.slice(spec.cursor, spec.cursor + spec.max_chunks);
  const nextCursor = spec.cursor + chunks.length;
  return { chunks, next_cursor: nextCursor < all.length ? nextCursor : null, total_chunks: all.length };
}
