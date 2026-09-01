import assert from "node:assert/strict";
import test from "node:test";
import { planChunks, stableChunkId, validateRun } from "../src/protocol.js";

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

