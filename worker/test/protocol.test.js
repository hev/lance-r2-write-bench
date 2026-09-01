import assert from "node:assert/strict";
import test from "node:test";
import { planChunks, planInvocation, stableChunkId, validateRun } from "../src/protocol.js";

test("chunk planning is deterministic and covers source exactly", () => {
  const input = { run_id: "r1", rows: 10, chunk_size: 4, seed: 7 };
  assert.deepEqual(planChunks(input), planChunks(input));
  assert.deepEqual(planChunks(input).map(({ start, rows }) => [start, rows]), [[0,4],[4,4],[8,2]]);
  assert.equal(stableChunkId("r1", 0, 4, 7), "r1:7:0:4");
});

test("mode and writer ceiling are explicit", () => {
  assert.equal(validateRun({ run_id: "r", rows: 1, writers: 2, mode: "funnel" }, 8).mode, "funnel");
  assert.throws(() => validateRun({ run_id: "r", rows: 1, writers: 9 }, 8));
  assert.throws(() => validateRun({ run_id: "r", rows: 1, mode: "fallback" }, 8));
});

test("an invocation is bounded to five chunks and resumes by cursor", () => {
  const spec = validateRun({ run_id: "r", rows: 12, chunk_size: 2, max_chunks: 5 }, 8);
  const first = planInvocation(spec);
  assert.equal(first.chunks.length, 5);
  assert.equal(first.next_cursor, 5);
  const second = planInvocation({ ...spec, cursor: first.next_cursor });
  assert.equal(second.chunks.length, 1);
  assert.equal(second.next_cursor, null);
});
