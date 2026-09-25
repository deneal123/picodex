import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const fixture = join(here, "test", "fixture-pi.mjs");

function startBridge(workspace, coordinationDir, fixtureDelayMs = 0) {
  const child = spawn(process.execPath, [join(here, "pi-bridge.mjs")], {
    cwd: workspace,
    env: {
      ...process.env,
      PI_SCRIPT: fixture,
      PI_WORKSPACE_ROOT: workspace,
      PI_COORDINATION_DIR: coordinationDir,
      PI_GLOBAL_MAX_CONCURRENCY: "8",
      PI_WORKSPACE_MAX_CONCURRENCY: "4",
      PI_MAX_CONCURRENCY: "4",
      PI_FIXTURE_DELAY_MS: String(fixtureDelayMs),
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const pending = new Map();
  let buffer = "";
  let sequence = 0;
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr = (stderr + chunk.toString()).slice(-4000); });
  child.stdout.on("data", (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (!line) continue;
      const response = JSON.parse(line);
      const slot = pending.get(response.id);
      if (slot) {
        pending.delete(response.id);
        clearTimeout(slot.timer);
        slot.resolve(response);
      }
    }
  });
  child.on("close", (code) => {
    for (const [id, slot] of pending) {
      pending.delete(id);
      clearTimeout(slot.timer);
      slot.reject(new Error(`bridge exited (${code}): ${stderr}`));
    }
  });
  const call = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`MCP ${method} timed out; stderr: ${stderr}`));
    }, 5000);
    pending.set(id, { resolve, reject, timer });
    child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });
  const tool = async (name, args = {}) => {
    const response = await call("tools/call", { name, arguments: args });
    assert.ok(response.result, response.error?.message);
    return response.result;
  };
  const close = async () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.stdin.end();
    await new Promise((resolve) => {
      const timer = setTimeout(() => child.kill(), 2000);
      child.once("close", () => { clearTimeout(timer); resolve(); });
    });
  };
  return { child, call, tool, close };
}

async function initialize(bridge) {
  const init = await bridge.call("initialize", {
    protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "test", version: "1" },
  });
  assert.equal(init.result.serverInfo.name, "picodex");
  return bridge.call("tools/list", {});
}

function toolText(result) {
  return result.content?.map((item) => item.text ?? "").join("\n") ?? "";
}

test("MCP lists async tools, overview, and returns a settled Pi job", async () => {
  const temp = await mkdtemp(join(tmpdir(), "picodex-bridge-basic-"));
  const workspace = join(temp, "workspace");
  await mkdir(workspace);
  const bridge = startBridge(workspace, join(temp, "coordination"));
  try {
    const listed = await initialize(bridge);
    assert.deepEqual(listed.result.tools.map((tool) => tool.name),
      ["pi", "pi-reply", "pi-submit", "pi-status", "pi-wait", "pi-cancel", "pi-overview"]);
    const submitted = await bridge.tool("pi-submit", { prompt: "hello" });
    const jobId = submitted.structuredContent.jobId;
    assert.ok(jobId);
    const waited = await bridge.tool("pi-wait", { jobId, waitMs: 3000 });
    assert.equal(waited.structuredContent.status, "completed");
    assert.equal(waited.structuredContent.text, "fixture: hello");
    assert.equal(waited.structuredContent.agentSettled, true);
  } finally {
    await bridge.close();
    await rm(temp, { recursive: true, force: true });
  }
});

test("separate stdio bridges share capacity, isolate workspaces, and reject foreign job IDs", async () => {
  const temp = await mkdtemp(join(tmpdir(), "picodex-bridge-shared-"));
  const coordinationDir = join(temp, "coordination");
  const workspaceA = join(temp, "workspace-a");
  const workspaceB = join(temp, "workspace-b");
  await Promise.all([mkdir(workspaceA), mkdir(workspaceB)]);
  const bridgeA = startBridge(workspaceA, coordinationDir, 650);
  const bridgeB = startBridge(workspaceB, coordinationDir, 650);
  try {
    await Promise.all([initialize(bridgeA), initialize(bridgeB)]);
    const jobA = (await bridgeA.tool("pi-submit", { prompt: "worker-a" })).structuredContent;
    const jobB = (await bridgeB.tool("pi-submit", { prompt: "worker-b" })).structuredContent;
    assert.ok(jobA.jobId && jobB.jobId);

    const overviewA = await bridgeA.tool("pi-overview");
    const overviewB = await bridgeB.tool("pi-overview");
    assert.equal(overviewA.structuredContent.shared.globalActive, 2);
    assert.equal(overviewB.structuredContent.shared.globalActive, 2);
    assert.equal(overviewA.structuredContent.shared.workspaceActive, 1);
    assert.equal(overviewB.structuredContent.shared.workspaceActive, 1);
    assert.notEqual(overviewA.structuredContent.shared.workspaceId, overviewB.structuredContent.shared.workspaceId);

    for (const name of ["pi-status", "pi-cancel"]) {
      const foreign = await bridgeB.tool(name, { jobId: jobA.jobId });
      assert.equal(foreign.isError, true, `${name} should reject a job owned by another bridge`);
      assert.match(toolText(foreign), /unknown jobId/i);
    }

    const [doneA, doneB] = await Promise.all([
      bridgeA.tool("pi-wait", { jobId: jobA.jobId, waitMs: 3000 }),
      bridgeB.tool("pi-wait", { jobId: jobB.jobId, waitMs: 3000 }),
    ]);
    assert.equal(doneA.structuredContent.status, "completed");
    assert.equal(doneB.structuredContent.status, "completed");
    assert.equal((await bridgeA.tool("pi-overview")).structuredContent.shared.globalActive, 0);
    assert.equal((await bridgeB.tool("pi-overview")).structuredContent.shared.globalActive, 0);

    const leasesDir = join(coordinationDir, "leases");
    assert.deepEqual(await readdir(leasesDir), [], "settled jobs must release every shared lease");
    assert.deepEqual((await readdir(coordinationDir)).sort(), ["leases", "policy.json"], "only the persistent policy and lease directory should remain");
  } finally {
    await Promise.all([bridgeA.close(), bridgeB.close()]);
    await rm(temp, { recursive: true, force: true });
  }
});

test("same workspace and explicit threadId are serialized across bridge processes", async () => {
  const temp = await mkdtemp(join(tmpdir(), "picodex-bridge-thread-"));
  const coordinationDir = join(temp, "coordination");
  const workspace = join(temp, "shared-workspace");
  await mkdir(workspace);
  const bridgeA = startBridge(workspace, coordinationDir, 700);
  const bridgeB = startBridge(workspace, coordinationDir, 100);
  try {
    await Promise.all([initialize(bridgeA), initialize(bridgeB)]);
    const threadId = "shared-session-17";
    const first = (await bridgeA.tool("pi-submit", { prompt: "first", threadId })).structuredContent;
    assert.equal(first.status, "running");

    const second = (await bridgeB.tool("pi-submit", { prompt: "second", threadId })).structuredContent;
    assert.equal(second.status, "queued");
    assert.equal(second.waitReason, "thread_active");

    const finishedFirst = await bridgeA.tool("pi-wait", { jobId: first.jobId, waitMs: 3000 });
    assert.equal(finishedFirst.structuredContent.status, "completed");
    const finishedSecond = await bridgeB.tool("pi-wait", { jobId: second.jobId, waitMs: 4000 });
    assert.equal(finishedSecond.structuredContent.status, "completed");
    assert.equal(finishedSecond.structuredContent.threadId, threadId);
    assert.equal((await bridgeA.tool("pi-overview")).structuredContent.shared.globalActive, 0);
    assert.deepEqual(await readdir(join(coordinationDir, "leases")), []);
    assert.deepEqual((await readdir(coordinationDir)).sort(), ["leases", "policy.json"], "only the persistent policy and lease directory should remain");
  } finally {
    await Promise.all([bridgeA.close(), bridgeB.close()]);
    await rm(temp, { recursive: true, force: true });
  }
});

test("cancelling a Pi process frees its shared thread only after the process exits", async () => {
  const temp = await mkdtemp(join(tmpdir(), "picodex-bridge-cancel-"));
  const coordinationDir = join(temp, "coordination");
  const workspace = join(temp, "shared-workspace");
  await mkdir(workspace);
  const bridgeA = startBridge(workspace, coordinationDir, 2000);
  const bridgeB = startBridge(workspace, coordinationDir, 100);
  try {
    await Promise.all([initialize(bridgeA), initialize(bridgeB)]);
    const threadId = "cancelled-session-17";
    const first = (await bridgeA.tool("pi-submit", { prompt: "cancel-me", threadId })).structuredContent;
    assert.equal(first.status, "running");

    const second = (await bridgeB.tool("pi-submit", { prompt: "after-cancel", threadId })).structuredContent;
    assert.equal(second.status, "queued");
    assert.equal(second.waitReason, "thread_active");

    const cancelled = await bridgeA.tool("pi-cancel", { jobId: first.jobId });
    assert.equal(cancelled.structuredContent.status, "cancelling");
    const finishedFirst = await bridgeA.tool("pi-wait", { jobId: first.jobId, waitMs: 4000 });
    assert.equal(finishedFirst.structuredContent.status, "cancelled");

    const finishedSecond = await bridgeB.tool("pi-wait", { jobId: second.jobId, waitMs: 4000 });
    assert.equal(finishedSecond.structuredContent.status, "completed");
    assert.equal(finishedSecond.structuredContent.text, "fixture: after-cancel");
    assert.equal((await bridgeA.tool("pi-overview")).structuredContent.shared.globalActive, 0);
    assert.deepEqual(await readdir(join(coordinationDir, "leases")), []);
  } finally {
    await Promise.all([bridgeA.close(), bridgeB.close()]);
    await rm(temp, { recursive: true, force: true });
  }
});
