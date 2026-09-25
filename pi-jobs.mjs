import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { StringDecoder } from "node:string_decoder";

const numberSetting = (name, fallback, minimum, maximum) => {
  const parsed = Number(process.env[name]);
  return Number.isInteger(parsed) && parsed >= minimum && parsed <= maximum ? parsed : fallback;
};

export function createJobManager({
  spawnPi = spawn,
  now = () => Date.now(),
  piBin = process.env.PI_BIN || "pi",
  piScript = process.env.PI_SCRIPT,
  piProvider = process.env.PI_PROVIDER,
  piModel = process.env.PI_MODEL,
  maxConcurrency = numberSetting("PI_MAX_CONCURRENCY", 4, 1, 50),
  maxQueue = numberSetting("PI_MAX_QUEUE", 50, 1, 500),
  deadlineMs = numberSetting("PI_JOB_DEADLINE_MS", 1200000, 1000, 7200000),
  quietMs = numberSetting("PI_JOB_QUIET_MS", 300000, 1000, 3600000),
  coordination = null,
  bridgeId = randomUUID(),
  workspaceRoot = process.cwd(),
} = {}) {
  const jobs = new Map();
  const queue = [];
  let running = 0;
  let stopping = false;
  let retryTimer = null;
  const finishedStates = new Set(["completed", "failed", "cancelled", "timed_out"]);

  function publicJob(job) {
    return {
      jobId: job.jobId,
      bridgeId,
      workspaceRoot,
      threadId: job.threadId,
      status: job.status,
      queuedAt: job.queuedAt,
      startedAt: job.startedAt,
      updatedAt: job.updatedAt,
      finishedAt: job.finishedAt,
      elapsedMs: (job.finishedAt ?? now()) - (job.startedAt ?? job.queuedAt),
      event: job.event,
      eventCount: job.eventCount,
      toolNames: job.toolNames,
      agentSettled: job.agentSettled,
      text: job.text,
      error: job.error,
      exitCode: job.exitCode,
      extensionErrors: job.extensionErrors,
      stopRequested: job.stopStatus !== null,
      waitReason: job.waitReason,
    };
  }

  function notify(job) {
    const snapshot = publicJob(job);
    for (const listener of job.listeners) listener(snapshot);
    if (finishedStates.has(job.status)) job.listeners.clear();
  }

  function finish(job, status, error = null) {
    if (finishedStates.has(job.status)) return;
    job.status = status;
    job.error = error;
    job.prompt = null;
    job.finishedAt = now();
    job.updatedAt = job.finishedAt;
    clearInterval(job.watchdog);
    if (job.startedAt !== null) {
      running--;
      if (job.hasLease) {
        try { coordination.release(job.jobId, { childExited: job.childClosed }); }
        catch (releaseError) { console.error(`picodex: could not release Pi lease ${job.jobId}: ${releaseError.message}`); }
      }
    }
    notify(job);
    pump();
  }

  function maybeFinishStopped(job) {
    if (job.childClosed && !job.killPending && job.stopStatus && !finishedStates.has(job.status))
      finish(job, job.stopStatus, job.stopError);
  }

  function killTree(job) {
    const child = job.child;
    if (!child || !child.pid) return;
    if (process.platform === "win32") {
      job.killPending = true;
      // taskkill is given a numeric PID directly; it also stops Pi's tool children.
      const killer = spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
        stdio: "ignore", windowsHide: true,
      });
      killer.on("error", () => {
        child.kill();
        job.killPending = false;
        maybeFinishStopped(job);
      });
      killer.on("close", (code) => {
        if (code !== 0) child.kill();
        job.killPending = false;
        maybeFinishStopped(job);
      });
    } else {
      job.killPending = true;
      try { process.kill(-child.pid, "SIGTERM"); } catch { child.kill("SIGTERM"); }
      const timer = setTimeout(() => {
        try { process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); }
        job.killPending = false;
        maybeFinishStopped(job);
      }, 3000);
      timer.unref();
    }
  }

  function requestStop(job, status, error) {
    if (finishedStates.has(job.status) || job.stopStatus) return;
    job.stopStatus = status;
    job.stopError = error;
    job.status = status === "cancelled" ? "cancelling" : "stopping";
    job.updatedAt = now();
    notify(job);
    killTree(job);
    maybeFinishStopped(job);
  }

  function handleEvent(job, line) {
    if (!line.trim()) return;
    let event;
    try { event = JSON.parse(line); } catch {
      job.protocolError = "Pi emitted malformed JSON";
      return;
    }
    job.updatedAt = now();
    job.event = event.type ?? "unknown";
    job.eventCount++;
    if (event.type === "tool_execution_start" && typeof event.toolName === "string" && !job.toolNames.includes(event.toolName))
      job.toolNames.push(event.toolName);
    if (event.type === "extension_error") {
      job.extensionErrors++;
      job.extensionError = `Pi extension error: ${event.error ?? event.message ?? "unknown"}`;
    }
    if (event.type === "message_end" && event.message?.role === "assistant") {
      const message = event.message;
      const text = Array.isArray(message.content)
        ? message.content.filter((block) => block.type === "text").map((block) => block.text ?? "").join("")
        : "";
      if (text) job.text = text.slice(0, 100000);
      job.stopReason = message.stopReason ?? null;
      if (message.errorMessage) job.error = String(message.errorMessage);
      else if (job.stopReason !== "error") job.error = null;
    }
    if (event.type === "agent_settled") job.agentSettled = true;
    notify(job);
  }

  function start(job) {
    running++;
    job.status = "running";
    job.startedAt = now();
    job.updatedAt = job.startedAt;
    const args = [
      ...(piScript ? [piScript] : []),
      "--mode", "json", "-p",
      ...(piProvider ? ["--provider", piProvider] : []),
      ...(piModel ? ["--model", piModel] : []),
      "--session-id", job.threadId, job.prompt,
    ];
    let child;
    try {
      child = spawnPi(piScript ? process.execPath : piBin, args, {
        cwd: workspaceRoot, stdio: ["ignore", "pipe", "pipe"], windowsHide: true,
        detached: process.platform !== "win32",
      });
    } catch (error) {
      finish(job, "failed", `Pi startup failed: ${error.message}`);
      return;
    }
    job.child = child;
    if (job.hasLease && child.pid) {
      try { coordination.updateChild(job.jobId, child.pid); }
      catch (error) { requestStop(job, "failed", `Pi lease update failed: ${error.message}`); }
    }
    job.prompt = null;
    let buffer = "";
    const decoder = new StringDecoder("utf8");
    child.stdout.on("data", (chunk) => {
      if (chunk.length + buffer.length > 1000000) {
        requestStop(job, "failed", "Pi emitted an oversized JSON record or stream chunk");
        return;
      }
      buffer += decoder.write(chunk);
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) handleEvent(job, line.replace(/\r$/, ""));
    });
    child.stdout.on("error", (error) => {
      requestStop(job, "failed", `Pi stdout failed: ${error.message}`);
    });
    child.stderr.on("data", (chunk) => {
      job.stderrTail = (job.stderrTail + chunk.toString()).slice(-4000);
    });
    child.stderr.on("error", (error) => {
      job.stderrTail = `Pi stderr failed: ${error.message}`;
    });
    child.on("error", (error) => {
      if (!child.pid) finish(job, "failed", `Pi process error: ${error.message}`);
      else requestStop(job, "failed", `Pi process error: ${error.message}`);
    });
    child.on("close", (code) => {
      job.childClosed = true;
      buffer += decoder.end();
      if (buffer.trim()) handleEvent(job, buffer);
      job.exitCode = code;
      if (finishedStates.has(job.status)) return;
      if (job.stopStatus) { maybeFinishStopped(job); return; }
      if (code !== 0) finish(job, "failed", job.error ?? `Pi exited ${code}`);
      else if (!job.agentSettled) finish(job, "failed", job.error ?? "Pi exited without agent_settled");
      else if (job.protocolError || job.extensionError || job.error || job.stopReason === "error" || job.stopReason === "aborted")
        finish(job, "failed", job.protocolError ?? job.extensionError ?? job.error ?? `Pi assistant stopped: ${job.stopReason}`);
      else if (!job.text.trim()) finish(job, "failed", "Pi settled without assistant text");
      else finish(job, "completed");
    });
    job.watchdog = setInterval(() => {
      if (finishedStates.has(job.status)) return;
      const age = now() - job.startedAt;
      const quiet = now() - job.updatedAt;
      if (age > deadlineMs || quiet > quietMs) {
        requestStop(job, "timed_out", age > deadlineMs ? "Pi job deadline exceeded" : "Pi produced no events within quiet deadline");
      }
    }, 1000);
    job.watchdog.unref();
  }

  function pump() {
    if (stopping) return;
    const attempts = queue.length;
    for (let index = 0; index < attempts && running < maxConcurrency; index++) {
      const job = queue.shift();
      if (job.status !== "queued") continue;
      if (coordination) {
        let permit;
        try { permit = coordination.tryAcquire({ jobId: job.jobId, threadId: job.threadId }); }
        catch (error) { finish(job, "failed", `Pi coordination failed: ${error.message}`); continue; }
        if (!permit.acquired) {
          job.waitReason = permit.reason ?? "shared_capacity";
          queue.push(job);
          continue;
        }
        job.hasLease = true;
      }
      job.waitReason = null;
      start(job);
    }
    if (queue.length && running < maxConcurrency && !retryTimer) {
      retryTimer = setTimeout(() => { retryTimer = null; pump(); }, 250);
      retryTimer.unref();
    }
  }

  function submit(prompt, threadId = randomUUID()) {
    if (stopping) throw new Error("Pi bridge is shutting down");
    if (typeof prompt !== "string" || !prompt.trim()) throw new Error("prompt is required");
    if (typeof threadId !== "string" || !/^[\w-]{1,100}$/.test(threadId)) throw new Error("invalid threadId");
    if (queue.length >= maxQueue) throw new Error("Pi queue is full");
    if ([...jobs.values()].some((job) => job.threadId === threadId && !finishedStates.has(job.status)))
      throw new Error("threadId is already active");
    if (jobs.size >= 200) {
      for (const [id, job] of jobs) {
        if (finishedStates.has(job.status)) jobs.delete(id);
        if (jobs.size < 200) break;
      }
      if (jobs.size >= 200) throw new Error("Pi job history is full");
    }
    const job = {
      jobId: randomUUID(), threadId, prompt, status: "queued", queuedAt: now(),
      startedAt: null, updatedAt: now(), finishedAt: null, event: null, eventCount: 0,
      agentSettled: false, text: "", error: null, exitCode: null, stderrTail: "",
      toolNames: [],
      extensionErrors: 0, extensionError: null, protocolError: null, stopReason: null,
      listeners: new Set(), child: null, watchdog: null, stopStatus: null, stopError: null,
      childClosed: false, killPending: false, hasLease: false, waitReason: null,
    };
    jobs.set(job.jobId, job);
    queue.push(job);
    pump();
    return publicJob(job);
  }

  function status(jobId) {
    const job = jobs.get(jobId);
    if (!job) throw new Error("unknown jobId");
    return publicJob(job);
  }

  function wait(jobId, waitMs = 25000) {
    const job = jobs.get(jobId);
    if (!job) throw new Error("unknown jobId");
    if (finishedStates.has(job.status) || waitMs <= 0) return Promise.resolve(publicJob(job));
    return new Promise((resolve) => {
      const listener = (snapshot) => {
        if (!finishedStates.has(snapshot.status)) return;
        clearTimeout(timer);
        job.listeners.delete(listener);
        resolve(snapshot);
      };
      job.listeners.add(listener);
      const timer = setTimeout(() => {
        job.listeners.delete(listener);
        resolve(publicJob(job));
      }, Math.min(waitMs, 25000));
    });
  }

  function cancel(jobId) {
    const job = jobs.get(jobId);
    if (!job) throw new Error("unknown jobId");
    if (!finishedStates.has(job.status)) {
      if (job.status === "queued") {
        const index = queue.indexOf(job);
        if (index !== -1) queue.splice(index, 1);
      }
      if (job.status === "queued") finish(job, "cancelled", "Cancelled by caller");
      else requestStop(job, "cancelled", "Cancelled by caller");
    }
    return publicJob(job);
  }

  function shutdown() {
    stopping = true;
    clearTimeout(retryTimer);
    for (const job of jobs.values()) if (!finishedStates.has(job.status)) cancel(job.jobId);
  }

  function overview() {
    const counts = { queued: 0, running: 0, stopping: 0, finished: 0 };
    for (const job of jobs.values()) {
      if (job.status === "queued") counts.queued++;
      else if (job.status === "running") counts.running++;
      else if (job.status === "cancelling" || job.status === "stopping") counts.stopping++;
      else counts.finished++;
    }
    return {
      bridgeId, workspaceRoot, own: counts,
      shared: coordination?.snapshot() ?? null,
    };
  }

  return { submit, status, wait, cancel, shutdown, overview };
}
