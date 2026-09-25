// Opt-in billable smoke check: PI_PROVIDER=... PI_MODEL=... node test/live-smoke.mjs
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(dirname(fileURLToPath(import.meta.url)));
const bridge = spawn(process.execPath, [join(here, "pi-bridge.mjs")], {
  cwd: here, env: process.env, stdio: ["pipe", "pipe", "pipe"],
});
let buffer = "";
let nextId = 0;
const pending = new Map();
bridge.stdout.on("data", (chunk) => {
  buffer += chunk.toString();
  const lines = buffer.split("\n");
  buffer = lines.pop() ?? "";
  for (const line of lines) {
    if (!line) continue;
    const message = JSON.parse(line);
    const slot = pending.get(message.id);
    if (slot) { pending.delete(message.id); slot(message); }
  }
});
function call(method, params) {
  return new Promise((resolve, reject) => {
    const id = ++nextId;
    const timer = setTimeout(() => reject(new Error(`${method} timed out`)), 30000);
    pending.set(id, (message) => { clearTimeout(timer); resolve(message); });
    bridge.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });
}
async function tool(name, args) {
  const response = await call("tools/call", { name, arguments: args });
  if (response.error) throw new Error(response.error.message);
  return response.result.structuredContent;
}
let jobId;
try {
  await call("initialize", { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "smoke", version: "1" } });
  const submitted = await tool("pi-submit", { prompt: process.argv[2] ?? "Reply with exactly OK. Do not use tools." });
  jobId = submitted.jobId;
  const deadline = Date.now() + 90000;
  let job;
  do {
    job = await tool("pi-wait", { jobId, waitMs: 10000 });
  } while (["queued", "running"].includes(job.status) && Date.now() < deadline);
  if (["queued", "running"].includes(job.status)) job = await tool("pi-cancel", { jobId });
  console.log(JSON.stringify({
    status: job.status, agentSettled: job.agentSettled, extensionErrors: job.extensionErrors,
    eventCount: job.eventCount, toolNames: job.toolNames, text: job.text, error: job.error,
  }));
  if (job.status !== "completed") process.exitCode = 1;
} catch (error) {
  console.error(error.message);
  if (jobId) await tool("pi-cancel", { jobId }).catch(() => {});
  process.exitCode = 1;
} finally {
  bridge.stdin.end();
  const timer = setTimeout(() => bridge.kill(), 3000);
  bridge.on("close", () => clearTimeout(timer));
}
