import { Container, getContainer } from "@cloudflare/containers";
import { env as processEnv } from "cloudflare:workers";
import { planInvocation, timingSafeEqual, validateRun } from "./protocol.js";
import { readAndTransformFixture } from "./fixtures.js";

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
    const platform = { colo: request.cf?.colo || null, region: request.cf?.region || null };
    if (url.pathname === "/health" && request.method === "GET") return json({ ok: true, platform });
    if (url.pathname === "/run" && request.method === "POST") {
      const spec = validateRun(await request.json(), Number(env.BENCH_MAX_WRITERS || 8));
      const source = readAndTransformFixture(spec.source_fixture, spec);
      spec.payload_shape = source.payload_shape;
      const invocation = planInvocation(spec);
      // Five writer calls leave one of the platform's six outbound slots free.
      const results = await Promise.all(invocation.chunks.map(async (chunk, index) => {
        const producer = (spec.cursor + index) % spec.writers;
        const writerIndex = spec.mode === "funnel" ? 0 : producer;
        const writerId = `${spec.run_id}-writer-${writerIndex}`;
        const response = await callWriter(env, writerId, "/chunks/commit", { ...spec, ...chunk }, env.BENCH_AUTH_TOKEN);
        return { producer_id: `${spec.run_id}-producer-${producer}`, writer_id: writerId, status: response.status, result: await response.json() };
      }));
      const failed = results.some((item) => item.status >= 300);
      return json({ spec, source, platform, chunks: results, next_cursor: failed ? spec.cursor : invocation.next_cursor,
        total_chunks: invocation.total_chunks, complete: !failed && invocation.next_cursor === null }, failed ? 502 : 200);
    }
    if (url.pathname.startsWith("/status/") && request.method === "GET") {
      const runId = decodeURIComponent(url.pathname.slice("/status/".length));
      const writer = getContainer(env.WRITERS, `${runId}-writer-0`);
      const response = await writer.fetch(new Request(`http://writer/runs/${encodeURIComponent(runId)}/status`, {
        headers: { authorization: `Bearer ${env.BENCH_AUTH_TOKEN}` },
      }));
      return new Response(response.body, { status: response.status, headers: response.headers });
    }
    if (["/verify", "/query", "/index"].includes(url.pathname) && request.method === "POST") {
      const input = await request.json();
      const runId = String(input.run_id || "");
      if (!runId) return json({ error: "run_id is required" }, 400);
      const response = await callWriter(env, `${runId}-writer-0`, url.pathname, input, env.BENCH_AUTH_TOKEN);
      return new Response(response.body, { status: response.status, headers: response.headers });
    }
    if (url.pathname === "/embed/text" && request.method === "POST") {
      const input = await request.json();
      const started = Date.now();
      const output = await env.AI.run(env.BENCH_TEXT_MODEL, { text: [String(input.text)] });
      const vector = output.data?.[0] || [];
      return json({ model: env.BENCH_TEXT_MODEL, dimensions: vector.length, latency_ms: Date.now() - started, usage: output.usage || null, platform, vector });
    }
    return json({ error: "not found" }, 404);
  }
};
