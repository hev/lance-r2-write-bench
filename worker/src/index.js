import { Container, getContainer } from "@cloudflare/containers";
import { env as processEnv } from "cloudflare:workers";
import { planChunks, timingSafeEqual, validateRun } from "./protocol.js";

export class LanceWriter extends Container {
  defaultPort = 3000;
  sleepAfter = "10m";
  envVars = {
    BENCH_STORAGE_URI: processEnv.BENCH_STORAGE_URI,
    BENCH_S3_ENDPOINT: processEnv.BENCH_S3_ENDPOINT,
    BENCH_S3_REGION: processEnv.BENCH_S3_REGION,
    BENCH_S3_ACCESS_KEY: processEnv.BENCH_S3_ACCESS_KEY,
    BENCH_S3_SECRET_KEY: processEnv.BENCH_S3_SECRET_KEY,
    AWS_SESSION_TOKEN: processEnv.AWS_SESSION_TOKEN,
    BENCH_AUTH_TOKEN: processEnv.BENCH_AUTH_TOKEN,
  };
}

function json(value, status = 200) {
  return Response.json(value, { status, headers: { "cache-control": "no-store" } });
}

function authorized(request, secret) {
  const value = request.headers.get("authorization") || "";
  return value.startsWith("Bearer ") && timingSafeEqual(value.slice(7), secret);
}

async function callWriter(env, writerId, path, body, token) {
  const writer = getContainer(env.WRITERS, writerId);
  return writer.fetch(new Request(`http://writer${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify({ ...body, writer_id: writerId }),
  }));
}

export default {
  async fetch(request, env) {
    if (!authorized(request, env.BENCH_AUTH_TOKEN)) return json({ error: "unauthorized" }, 401);
    const url = new URL(request.url);
    if (url.pathname === "/health" && request.method === "GET") return json({ ok: true });
    if (url.pathname === "/run" && request.method === "POST") {
      const spec = validateRun(await request.json(), Number(env.BENCH_MAX_WRITERS || 8));
      const chunks = planChunks(spec);
      const results = [];
      // Each wave stays below Workers' six simultaneous outbound connections.
      for (let offset = 0; offset < chunks.length; offset += 5) {
        const wave = chunks.slice(offset, offset + 5);
        const responses = await Promise.all(wave.map(async (chunk, index) => {
          const producer = (offset + index) % spec.writers;
          const writerIndex = spec.mode === "funnel" ? 0 : producer;
          const writerId = `${spec.run_id}-writer-${writerIndex}`;
          const response = await callWriter(env, writerId, "/chunks/commit", { ...spec, ...chunk }, env.BENCH_AUTH_TOKEN);
          return { writer_id: writerId, status: response.status, result: await response.json() };
        }));
        results.push(...responses);
        if (responses.some((item) => item.status >= 300)) return json({ spec, chunks: results, complete: false }, 502);
      }
      return json({ spec, chunks: results, complete: true });
    }
    if (url.pathname === "/embed/text" && request.method === "POST") {
      const input = await request.json();
      const started = Date.now();
      const output = await env.AI.run(env.BENCH_TEXT_MODEL, { text: [String(input.text)] });
      const vector = output.data?.[0] || [];
      return json({ model: env.BENCH_TEXT_MODEL, dimensions: vector.length, latency_ms: Date.now() - started, usage: output.usage || null, vector });
    }
    return json({ error: "not found" }, 404);
  }
};

