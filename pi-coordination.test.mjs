import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, mkdirSync, utimesSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCoordination } from "./pi-coordination.mjs";

function fixture() {
  const directory = mkdtempSync(join(tmpdir(), "pi-coordination-"));
  const workspaceA = join(directory, "workspace-a");
  const workspaceB = join(directory, "workspace-b");
  mkdirSync(workspaceA);
  mkdirSync(workspaceB);
  return { directory: join(directory, "shared"), workspaceA, workspaceB, cleanup: () => rmSync(directory, { recursive: true, force: true }) };
}

function nonexistentPid() {
  for (let pid = process.pid + 1000; pid < process.pid + 2000; pid++) {
    try { process.kill(pid, 0); } catch (error) { if (error?.code === "ESRCH") return pid; }
  }
  throw new Error("could not find a known-dead process ID for the stale lease test");
}

test("two coordination instances share global/workspace limits and session exclusion", () => {
  const f = fixture();
  try {
    const a = createCoordination({ directory: f.directory, workspaceRoot: f.workspaceA, bridgeId: "bridge-a", globalMax: 2, workspaceMax: 1 });
    const sameWorkspace = createCoordination({ directory: f.directory, workspaceRoot: f.workspaceA, bridgeId: "bridge-b", globalMax: 2, workspaceMax: 1 });
    const otherWorkspace = createCoordination({ directory: f.directory, workspaceRoot: f.workspaceB, bridgeId: "bridge-c", globalMax: 2, workspaceMax: 1 });
    assert.equal(a.workspaceRoot, f.workspaceA);

    assert.deepEqual(a.tryAcquire({ jobId: "job-a", threadId: "session-a" }), { acquired: true });
    assert.deepEqual(sameWorkspace.tryAcquire({ jobId: "job-b", threadId: "session-a" }), { acquired: false, reason: "thread_active" });
    assert.deepEqual(sameWorkspace.tryAcquire({ jobId: "job-c", threadId: "session-b" }), { acquired: false, reason: "workspace_limit" });
    assert.deepEqual(otherWorkspace.tryAcquire({ jobId: "job-d", threadId: "session-a" }), { acquired: true });
    assert.deepEqual(createCoordination({ directory: f.directory, workspaceRoot: join(f.directory, "workspace-c"), globalMax: 2, workspaceMax: 1 })
      .tryAcquire({ jobId: "job-e", threadId: "session-c" }), { acquired: false, reason: "global_limit" });

    assert.equal(a.snapshot().globalActive, 2);
    assert.equal(a.snapshot().workspaceActive, 1);
    assert.equal(a.updateChild("job-a", process.pid), true);
    assert.equal(a.release("job-a"), false, "an active child must keep its lease");
    assert.equal(a.release("job-a", { childExited: true }), true, "confirmed child close overrides transient PID liveness");
    assert.equal(sameWorkspace.tryAcquire({ jobId: "job-f", threadId: "session-b" }).acquired, true);
    assert.equal(sameWorkspace.updateChild("job-f", nonexistentPid()), true);
    assert.equal(sameWorkspace.release("job-f"), true, "a dead child permits normal release");
  } finally {
    f.cleanup();
  }
});

test("shared coordination policy rejects bridges with conflicting limits", () => {
  const f = fixture();
  try {
    createCoordination({ directory: f.directory, workspaceRoot: f.workspaceA, globalMax: 8, workspaceMax: 4 });
    assert.throws(
      () => createCoordination({ directory: f.directory, workspaceRoot: f.workspaceB, globalMax: 12, workspaceMax: 4 }),
      /policy conflict.*globalMax=8.*requested globalMax=12/,
    );
  } finally {
    f.cleanup();
  }
});

test("lease files omit prompt and raw session identifiers; stale lease needs both PIDs gone", () => {
  const f = fixture();
  try {
    const a = createCoordination({ directory: f.directory, workspaceRoot: f.workspaceA, bridgeId: "private bridge label" });
    const b = createCoordination({ directory: f.directory, workspaceRoot: f.workspaceA, bridgeId: "observer" });
    const rawThreadId = "session-secret-7b843d";
    assert.equal(a.tryAcquire({ jobId: "job-opaque", threadId: rawThreadId }).acquired, true);
    const leasePath = join(f.directory, "leases", readdirSync(join(f.directory, "leases"))[0]);
    let text = readFileSync(leasePath, "utf8");
    assert.equal(text.includes(rawThreadId), false);
    assert.equal(text.includes("prompt"), false);

    const lease = JSON.parse(text);
    lease.ownerPid = nonexistentPid();
    lease.childPid = process.pid;
    writeFileSync(leasePath, JSON.stringify(lease));
    assert.equal(b.snapshot().globalActive, 1, "live child retains a lease after its bridge owner exits");

    lease.childPid = nonexistentPid();
    writeFileSync(leasePath, JSON.stringify(lease));
    assert.equal(b.snapshot().globalActive, 0, "lease is reclaimed when both owner and child are gone");
  } finally {
    f.cleanup();
  }
});

test("separate OS processes cannot exceed a shared global slot limit", async () => {
  const f = fixture();
  try {
    const moduleUrl = new URL("./pi-coordination.mjs", import.meta.url).href;
    const code = `
      import { createCoordination } from ${JSON.stringify(moduleUrl)};
      const [directory, workspaceRoot, jobId, threadId] = process.argv.slice(1);
      const manager = createCoordination({ directory, workspaceRoot, globalMax: 1, workspaceMax: 1 });
      const result = manager.tryAcquire({ jobId, threadId });
      process.stdout.write(JSON.stringify(result) + "\\n");
      if (result.acquired) await new Promise((resolve) => setTimeout(resolve, 1200));
    `;
    const run = (jobId) => new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ["--input-type=module", "-e", code, f.directory, f.workspaceA, jobId, `thread-${jobId}`], { stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "";
      let stderr = "";
      child.stdout.setEncoding("utf8").on("data", (chunk) => { stdout += chunk; });
      child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr += chunk; });
      child.on("error", reject);
      child.on("close", (codeValue) => {
        if (codeValue !== 0) return reject(new Error(stderr || `child exited ${codeValue}`));
        try { resolve(JSON.parse(stdout.trim())); } catch (error) { reject(error); }
      });
    });

    const results = await Promise.all([run("process-a"), run("process-b")]);
    assert.equal(results.filter((result) => result.acquired).length, 1);
    assert.equal(results.filter((result) => result.reason === "global_limit").length, 1);
  } finally {
    f.cleanup();
  }
});

test("an old ownerless mutex can be recovered after the grace period", () => {
  const f = fixture();
  try {
    const mutex = join(f.directory, "mutex");
    mkdirSync(mutex, { recursive: true });
    const old = new Date(Date.now() - 15000);
    utimesSync(mutex, old, old);
    const manager = createCoordination({ directory: f.directory, workspaceRoot: f.workspaceA, lockTimeoutMs: 1000 });
    assert.equal(manager.snapshot().globalActive, 0);
    assert.equal(manager.tryAcquire({ jobId: "after-reclaim", threadId: "session" }).acquired, true);
  } finally {
    f.cleanup();
  }
});

test("malformed lease records fail closed", () => {
  const f = fixture();
  try {
    const manager = createCoordination({ directory: f.directory, workspaceRoot: f.workspaceA });
    writeFileSync(join(f.directory, "leases", "corrupt.json"), "{broken");
    assert.throws(() => manager.snapshot(), /Unreadable Pi coordination lease/);
    assert.throws(() => manager.tryAcquire({ jobId: "blocked", threadId: "session" }), /Unreadable Pi coordination lease/);
  } finally {
    f.cleanup();
  }
});
