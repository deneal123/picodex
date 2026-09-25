import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { createJobManager } from "./pi-jobs.mjs";

function fakeProcess() {
  const child = new EventEmitter();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.pid = null;
  return child;
}

function event(child, record) {
  child.stdout.write(`${JSON.stringify(record)}\n`);
}

function close(child, code = 0) {
  child.stdout.end();
  child.stderr.end();
  child.emit("close", code);
}

test("success requires agent_settled, not agent_end", async () => {
  const child = fakeProcess();
  const manager = createJobManager({ spawnPi: () => child });
  const job = manager.submit("hello");
  event(child, { type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "intermediate" }] } });
  event(child, { type: "agent_end", messages: [], willRetry: true });
  assert.equal(manager.status(job.jobId).status, "running");
  event(child, { type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "final" }], stopReason: "stop" } });
  event(child, { type: "agent_settled" });
  close(child);
  const result = await manager.wait(job.jobId);
  assert.equal(result.status, "completed");
  assert.equal(result.text, "final");
});

test("missing settlement fails despite exit zero", async () => {
  const child = fakeProcess();
  const manager = createJobManager({ spawnPi: () => child });
  const job = manager.submit("hello");
  event(child, { type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "answer" }] } });
  close(child);
  const result = await manager.wait(job.jobId);
  assert.equal(result.status, "failed");
  assert.match(result.error, /agent_settled/);
});

test("queue limits concurrency and queued jobs can be cancelled", async () => {
  const children = [];
  const manager = createJobManager({ maxConcurrency: 1, maxQueue: 1, spawnPi: () => {
    const child = fakeProcess();
    children.push(child);
    return child;
  } });
  const first = manager.submit("first");
  const second = manager.submit("second");
  assert.equal(manager.status(second.jobId).status, "queued");
  assert.throws(() => manager.submit("third"), /queue is full/);
  assert.equal(manager.cancel(second.jobId).status, "cancelled");
  event(children[0], { type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "done" }] } });
  event(children[0], { type: "agent_settled" });
  close(children[0]);
  assert.equal((await manager.wait(first.jobId)).status, "completed");
  assert.equal(children.length, 1);
});

test("assistant error and missing text are failures", async () => {
  for (const message of [
    { role: "assistant", content: [{ type: "text", text: "bad" }], stopReason: "error", errorMessage: "provider error" },
    { role: "assistant", content: [], stopReason: "stop" },
  ]) {
    const child = fakeProcess();
    const manager = createJobManager({ spawnPi: () => child });
    const job = manager.submit("hello");
    event(child, { type: "message_end", message });
    event(child, { type: "agent_settled" });
    close(child);
    assert.equal((await manager.wait(job.jobId)).status, "failed");
  }
});

test("extension and protocol errors cannot be erased by a later answer", async () => {
  for (const corrupt of ["not-json\n", `${JSON.stringify({ type: "extension_error", error: "SoL hook failed" })}\n`]) {
    const child = fakeProcess();
    const manager = createJobManager({ spawnPi: () => child });
    const job = manager.submit("hello");
    child.stdout.write(corrupt);
    event(child, { type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "looks fine" }], stopReason: "stop" } });
    event(child, { type: "agent_settled" });
    close(child);
    assert.equal((await manager.wait(job.jobId)).status, "failed");
  }
});

test("cancelled queue entries release capacity immediately", () => {
  const manager = createJobManager({ maxConcurrency: 1, maxQueue: 1, spawnPi: fakeProcess });
  manager.submit("running");
  const waiting = manager.submit("waiting");
  manager.cancel(waiting.jobId);
  assert.equal(manager.submit("replacement").status, "queued");
  manager.shutdown();
});

test("cancelling a running Pi holds its slot until the process closes", async () => {
  const children = [];
  const manager = createJobManager({ maxConcurrency: 1, spawnPi: () => {
    const child = fakeProcess();
    children.push(child);
    return child;
  } });
  const first = manager.submit("first");
  const second = manager.submit("second");
  assert.equal(manager.cancel(first.jobId).status, "cancelling");
  assert.equal(manager.status(second.jobId).status, "queued");
  assert.equal(children.length, 1);
  close(children[0], 1);
  assert.equal((await manager.wait(first.jobId)).status, "cancelled");
  assert.equal(children.length, 2);
  manager.cancel(second.jobId);
  close(children[1], 1);
});

test("overview exposes only counts and workspace identity", () => {
  const manager = createJobManager({ maxConcurrency: 1, bridgeId: "bridge-test", workspaceRoot: "R:\\sample", spawnPi: fakeProcess });
  manager.submit("one");
  manager.submit("two");
  const overview = manager.overview();
  assert.equal(overview.bridgeId, "bridge-test");
  assert.equal(overview.workspaceRoot, "R:\\sample");
  assert.equal(overview.own.running, 1);
  assert.equal(overview.own.queued, 1);
  assert.equal(JSON.stringify(overview).includes("one"), false);
  assert.equal(JSON.stringify(overview).includes("two"), false);
  manager.shutdown();
});

test("JSON records survive a split UTF-8 character", async () => {
  const child = fakeProcess();
  const manager = createJobManager({ spawnPi: () => child });
  const job = manager.submit("hello");
  const bytes = Buffer.from(`${JSON.stringify({ type: "message_end", message: {
    role: "assistant", content: [{ type: "text", text: "Привет" }], stopReason: "stop",
  } })}\n`);
  const split = bytes.indexOf(Buffer.from("П")[0]) + 1;
  child.stdout.write(bytes.subarray(0, split));
  child.stdout.write(bytes.subarray(split));
  event(child, { type: "agent_settled" });
  close(child);
  assert.equal((await manager.wait(job.jobId)).text, "Привет");
});
