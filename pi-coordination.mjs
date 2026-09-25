import { createHash, randomUUID } from "node:crypto";
import { homedir } from "node:os";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { join, resolve } from "node:path";

const DEFAULT_LOCK_TIMEOUT_MS = 10000;
const ORPHAN_MUTEX_GRACE_MS = 10000;
const sleepCell = new Int32Array(new SharedArrayBuffer(4));

function digest(value) {
  return createHash("sha256").update(value).digest("hex");
}

function processIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") return false;
    // EPERM means the process exists but cannot be signalled. Treat all other
    // ambiguous OS errors conservatively as alive.
    return true;
  }
}

function normalizedWorkspace(workspaceRoot) {
  if (typeof workspaceRoot !== "string" || !workspaceRoot.trim())
    throw new TypeError("workspaceRoot is required");
  let value = resolve(workspaceRoot);
  try { value = realpathSync.native(value); } catch { /* keep resolved path for not-yet-created workspaces */ }
  return value;
}

function readLease(path) {
  try {
    const value = JSON.parse(readFileSync(path, "utf8"));
    if (value?.version !== 1 || !Number.isInteger(value.ownerPid) || typeof value.workspaceId !== "string") return null;
    return value;
  } catch {
    return null;
  }
}

/**
 * Cross-process Pi worker leases stored in a shared local directory.
 * Lease files contain only opaque hashes, process IDs, and timestamps.
 */
export function createCoordination({
  directory = join(homedir(), ".pi", "picodex-coordination"),
  workspaceRoot,
  bridgeId = randomUUID(),
  globalMax = 8,
  workspaceMax = 4,
  lockTimeoutMs = DEFAULT_LOCK_TIMEOUT_MS,
} = {}) {
  if (typeof directory !== "string" || !directory.trim()) throw new TypeError("directory must be a nonempty path");
  if (!Number.isInteger(globalMax) || globalMax < 1) throw new TypeError("globalMax must be a positive integer");
  if (!Number.isInteger(workspaceMax) || workspaceMax < 1) throw new TypeError("workspaceMax must be a positive integer");
  if (!Number.isInteger(lockTimeoutMs) || lockTimeoutMs < 1) throw new TypeError("lockTimeoutMs must be a positive integer");

  const root = resolve(directory);
  const leasesDir = join(root, "leases");
  const mutexDir = join(root, "mutex");
  const canonicalWorkspaceRoot = normalizedWorkspace(workspaceRoot);
  const workspaceKey = process.platform === "win32" ? canonicalWorkspaceRoot.toLocaleLowerCase("en-US") : canonicalWorkspaceRoot;
  const workspaceId = digest(workspaceKey);
  const ownerId = digest(String(bridgeId));
  const localLeases = new Map();
  mkdirSync(leasesDir, { recursive: true });

  function withMutex(action) {
    const deadline = Date.now() + lockTimeoutMs;
    const token = randomUUID();
    while (true) {
      try {
        mkdirSync(mutexDir);
        writeFileSync(join(mutexDir, "owner.json"), JSON.stringify({ pid: process.pid, token }), { flag: "wx" });
        break;
      } catch (error) {
        if (error?.code !== "EEXIST") {
          // If this process created the directory but failed to write its
          // owner marker, leave it in place. A contender cannot prove it stale.
          if (existsSync(mutexDir)) throw error;
          throw error;
        }
        let lockOwner;
        try { lockOwner = JSON.parse(readFileSync(join(mutexDir, "owner.json"), "utf8")); } catch { lockOwner = null; }
        if (Number.isInteger(lockOwner?.pid) && !processIsAlive(lockOwner.pid)) {
          const tombstone = `${mutexDir}.stale-${randomUUID()}`;
          try {
            renameSync(mutexDir, tombstone);
            rmSync(tombstone, { recursive: true, force: true });
            continue;
          } catch (reclaimError) {
            if (reclaimError?.code !== "ENOENT" && reclaimError?.code !== "EEXIST") throw reclaimError;
          }
        } else if (!Number.isInteger(lockOwner?.pid)) {
          // mkdir and writing owner.json cannot be one filesystem operation.
          // Reclaim only a genuinely old ownerless directory, after a second
          // read and age check; never use age to override a recorded live PID.
          try {
            const firstMtime = statSync(mutexDir).mtimeMs;
            if (Date.now() - firstMtime >= ORPHAN_MUTEX_GRACE_MS) {
              let secondOwner;
              try { secondOwner = JSON.parse(readFileSync(join(mutexDir, "owner.json"), "utf8")); } catch { secondOwner = null; }
              const secondMtime = statSync(mutexDir).mtimeMs;
              if (!Number.isInteger(secondOwner?.pid) && secondMtime === firstMtime && Date.now() - secondMtime >= ORPHAN_MUTEX_GRACE_MS) {
                const tombstone = `${mutexDir}.orphan-${randomUUID()}`;
                try {
                  renameSync(mutexDir, tombstone);
                  rmSync(tombstone, { recursive: true, force: true });
                  continue;
                } catch (reclaimError) {
                  if (reclaimError?.code !== "ENOENT" && reclaimError?.code !== "EEXIST") throw reclaimError;
                }
              }
            }
          } catch (reclaimError) {
            if (reclaimError?.code !== "ENOENT") throw reclaimError;
          }
        }
        if (Date.now() >= deadline) throw new Error("Pi coordination mutex is busy or has an unreadable owner marker");
        Atomics.wait(sleepCell, 0, 0, 8);
      }
    }

    try {
      return action();
    } finally {
      try {
        const lockOwner = JSON.parse(readFileSync(join(mutexDir, "owner.json"), "utf8"));
        if (lockOwner.token === token && lockOwner.pid === process.pid) rmSync(mutexDir, { recursive: true, force: true });
      } catch {
        // Never remove a lock whose ownership cannot be verified.
      }
    }
  }

  function leaseFiles() {
    return readdirSync(leasesDir).filter((name) => name.endsWith(".json")).map((name) => join(leasesDir, name));
  }

  function pruneStale() {
    const leases = [];
    for (const path of leaseFiles()) {
      const lease = readLease(path);
      if (!lease) throw new Error(`Unreadable Pi coordination lease: ${path}`);
      const ownerAlive = processIsAlive(lease.ownerPid);
      const childAlive = Number.isInteger(lease.childPid) && lease.childPid > 0 && processIsAlive(lease.childPid);
      if (!ownerAlive && !childAlive) {
        rmSync(path, { force: true });
        continue;
      }
      leases.push({ path, lease });
    }
    return leases;
  }

  function writeLease(path, lease) {
    const temporary = `${path}.${randomUUID()}.tmp`;
    writeFileSync(temporary, `${JSON.stringify(lease)}\n`, { flag: "wx" });
    try { renameSync(temporary, path); }
    catch (error) { rmSync(temporary, { force: true }); throw error; }
  }

  function tryAcquire({ jobId, threadId } = {}) {
    if (typeof jobId !== "string" || !jobId.trim()) throw new TypeError("jobId is required");
    if (typeof threadId !== "string" || !threadId.trim()) throw new TypeError("threadId is required");
    const requestedThreadKey = digest(threadId);
    const existingLease = localLeases.get(jobId);
    if (existingLease) return existingLease.threadKey === requestedThreadKey
      ? { acquired: true }
      : { acquired: false, reason: "job_id_conflict" };

    return withMutex(() => {
      const live = pruneStale();
      const threadKey = requestedThreadKey;
      if (live.some(({ lease }) => lease.workspaceId === workspaceId && lease.threadKey === threadKey))
        return { acquired: false, reason: "thread_active" };
      if (live.length >= globalMax) return { acquired: false, reason: "global_limit" };
      if (live.filter(({ lease }) => lease.workspaceId === workspaceId).length >= workspaceMax)
        return { acquired: false, reason: "workspace_limit" };

      const leaseId = randomUUID();
      const path = join(leasesDir, `${leaseId}.json`);
      const lease = {
        version: 1,
        leaseId,
        ownerId,
        ownerPid: process.pid,
        childPid: null,
        workspaceId,
        threadKey,
        acquiredAt: new Date().toISOString(),
      };
      writeLease(path, lease);
      localLeases.set(jobId, { path, leaseId, threadKey });
      return { acquired: true };
    });
  }

  function updateChild(jobId, pid) {
    if (typeof jobId !== "string" || !jobId.trim()) throw new TypeError("jobId is required");
    if (pid !== null && (!Number.isInteger(pid) || pid <= 0)) throw new TypeError("pid must be a positive integer or null");
    const local = localLeases.get(jobId);
    if (!local) return false;
    return withMutex(() => {
      const lease = readLease(local.path);
      if (!lease || lease.leaseId !== local.leaseId || lease.ownerId !== ownerId || lease.ownerPid !== process.pid) return false;
      lease.childPid = pid;
      writeLease(local.path, lease);
      return true;
    });
  }

  function release(jobId, { childExited = false } = {}) {
    if (typeof jobId !== "string" || !jobId.trim()) throw new TypeError("jobId is required");
    const local = localLeases.get(jobId);
    if (!local) return false;
    const released = withMutex(() => {
      const lease = readLease(local.path);
      if (!lease || lease.leaseId !== local.leaseId || lease.ownerId !== ownerId || lease.ownerPid !== process.pid) return false;
      if (!childExited && Number.isInteger(lease.childPid) && lease.childPid > 0 && processIsAlive(lease.childPid)) return false;
      rmSync(local.path, { force: true });
      return true;
    });
    if (released) localLeases.delete(jobId);
    return released;
  }

  function snapshot() {
    return withMutex(() => {
      const live = pruneStale();
      return {
        globalActive: live.length,
        workspaceActive: live.filter(({ lease }) => lease.workspaceId === workspaceId).length,
        globalMax,
        workspaceMax,
        workspaceId,
      };
    });
  }

  // The limits govern every bridge sharing this coordination directory. Pin
  // them once under the mutex and reject later bridges with conflicting policy.
  withMutex(() => {
    const policyPath = join(root, "policy.json");
    const expected = { version: 1, globalMax, workspaceMax };
    if (existsSync(policyPath)) {
      let actual;
      try { actual = JSON.parse(readFileSync(policyPath, "utf8")); } catch { throw new Error("Pi coordination policy is unreadable"); }
      if (actual?.version !== expected.version || actual?.globalMax !== globalMax || actual?.workspaceMax !== workspaceMax)
        throw new Error(`Pi coordination policy conflict: shared limits are globalMax=${actual?.globalMax}, workspaceMax=${actual?.workspaceMax}; requested globalMax=${globalMax}, workspaceMax=${workspaceMax}`);
    } else {
      const temporary = `${policyPath}.${randomUUID()}.tmp`;
      writeFileSync(temporary, `${JSON.stringify(expected)}\n`, { flag: "wx" });
      try { renameSync(temporary, policyPath); }
      catch (error) { rmSync(temporary, { force: true }); throw error; }
    }
  });

  return { workspaceRoot: canonicalWorkspaceRoot, tryAcquire, updateChild, release, snapshot };
}
