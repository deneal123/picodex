# Picodex

Picodex is a local MCP coordinator for Codex sessions that delegate bounded tasks to Pi workers. Each bridge process is attached to one project workspace; multiple Codex sessions can share the same machine-wide Pi capacity without seeing each other's task contents.

Repository: [deneal123/picodex](https://github.com/deneal123/picodex). This project extends the MIT-licensed [pi-mcp-bridge](https://github.com/abatilo/pi-mcp-bridge); see [NOTICE.md](NOTICE.md).

The coordinator has offline two-process tests and a live Pi smoke check. Run the checks below after changing the bridge or its configuration.

## What it provides

- `pi-submit` queues a Pi task and returns a `jobId` and Pi `threadId` immediately.
- `pi-status` reads the current state and result for a job submitted through this bridge.
- `pi-wait` waits briefly for that job to finish.
- `pi-cancel` cancels a queued or running job started through this bridge.
- `pi-overview` reports this bridge's own running, queued, and finished counts, the current workspace's active count, and global running/capacity totals. It does not expose other workspaces' prompts or job IDs.
- `pi` and `pi-reply` remain available for callers that need a blocking call or continuation of a Pi session.

A successful Pi task requires the `agent_settled` event, a zero exit code, and a final assistant response. An `agent_end` event alone may precede automatic continuation.

## Requirements

- Node.js 20 or newer.
- Pi CLI installed, authenticated, and available on `PATH` (or set `PI_BIN`).
- A Codex MCP client able to launch a local stdio server.

The bridge uses Pi's configured model, authentication, and extensions. Do not put provider secrets in this repository or in MCP command arguments. Configure authentication through Pi's supported local setup.

## Configure one project session

Clone or place this repository at `R:\picodex`. In each project's Codex configuration, register a bridge with the **absolute bridge path** and that project's **workspace as `cwd`**. The process working directory matters because Pi resolves saved sessions relative to the workspace. If an existing global `pi-worker` entry is enabled, update or disable it so the session does not launch two redundant bridges.

For example, in Project A's trusted `.codex/config.toml`:

```toml
[mcp_servers.picodex]
command = "node"
args = ["R:\\picodex\\pi-bridge.mjs"]
cwd = "R:\\ProjectA"
```

In Project B's configuration, use the same bridge executable and a different workspace:

```toml
[mcp_servers.picodex]
command = "node"
args = ["R:\\picodex\\pi-bridge.mjs"]
cwd = "R:\\ProjectB"
```

Start each Codex session from its corresponding project. Both bridge processes use the same default coordination directory for the current Windows user (`%USERPROFILE%\\.pi\\picodex-coordination`), while Pi runs in the project workspace selected by `cwd`. This lets the bridges coordinate machine-wide capacity while keeping each session's project and Pi session context separate. If you override `PI_COORDINATION_DIR`, configure the same shared path in both processes.

Restart Codex after changing MCP configuration so the tools load. Use `pi-overview` to inspect the available capacity after the bridge starts.

## Make the skill available to a fresh Codex session

The reusable agent instructions live in [`skills/picodex/SKILL.md`](skills/picodex/SKILL.md). Link that directory into your personal Codex skills once (PowerShell):

```powershell
New-Item -ItemType Junction -Path "$env:USERPROFILE\.codex\skills\picodex" -Target 'R:\picodex\skills\picodex'
```

After linking, a fresh session can invoke `$picodex` and then check `pi-overview` before delegating work. The skill does not install Pi or grant access to another project's files.

## Capacity settings

Defaults for the shared coordinator:

| Setting | Default | Scope |
|---|---:|---|
| `PI_GLOBAL_MAX_CONCURRENCY` | 8 | All Picodex bridges sharing the coordination directory |
| `PI_WORKSPACE_MAX_CONCURRENCY` | 4 | One workspace across all its bridge processes |
| `PI_MAX_CONCURRENCY` | 4 | One bridge process |

The effective limit is the tightest applicable limit. With two project sessions, the global default allows eight workers total, subject to a maximum of four in either workspace and four per bridge. These are concurrency limits, not throughput or cost guarantees.

Additional controls are `PI_MAX_QUEUE` (50 per bridge), `PI_JOB_DEADLINE_MS` (20 minutes), and `PI_JOB_QUIET_MS` (5 minutes). Check the implementation's accepted ranges before overriding these values. A `policy.json` file pins the shared global/workspace limits in the coordination directory; bridges using that directory reject conflicting limits. To change shared limits, stop every bridge, verify `leases/` is empty, remove only `policy.json`, then restart all bridges with the same settings. A different `PI_COORDINATION_DIR` starts an independent capacity pool.

## Coordinate a batch

1. Call `pi-overview` and note the current global and workspace capacity.
2. Split the work into independent tasks with a clear output, scope, and evidence requirement.
3. Submit bounded tasks with `pi-submit`; keep each returned `jobId` with its `threadId` and purpose.
4. Track only jobs submitted by this bridge with `pi-wait` or `pi-status`. Use `pi-cancel` to stop your queued or active job if it is no longer needed; a running job reports `cancelling` until its process tree exits.
5. Review worker evidence and reconcile findings before changing project files.
6. Save a `threadId` when you need to continue the same Pi conversation later, and resume it only from the same project workspace.

`pi-overview` intentionally gives only aggregate information about other workspaces. A bridge cannot use it to discover, inspect, or cancel another session's job IDs. Each bridge's job receipts are in memory and are lost when that bridge restarts; Pi conversation history is persisted by Pi and can be resumed with its `threadId`.

## Local checks

From this directory:

```powershell
npm test
```

The suite exercises two independent stdio bridge processes with a shared coordination directory and fixture Pi jobs. A separate live check exercises the configured Pi provider and extensions: `node test/live-smoke.mjs` with your local `PI_PROVIDER`, `PI_MODEL`, and optional `PI_SCRIPT` settings.

## Public release

Use a commit-pinned checkout for reproducibility when configuring other projects. Do not substitute an unrelated npm package with the same name.
