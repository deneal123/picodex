---
name: picodex
description: Coordinate bounded Pi worker tasks through the Picodex MCP bridge from a Codex project session. Use when delegating, monitoring, or resuming Pi work; not for unrelated coding tasks.
---

# Picodex swarm coordination

Use Picodex when Pi workers can handle independent, bounded tasks and the current Codex session is responsible for integrating their findings. Read the repository [README](../../README.md) for installation, configuration, tool semantics, and the current implementation status.

## Session and project boundaries

- Confirm the bridge is running from the intended project workspace. A Pi `threadId` belongs to that workspace because Pi session lookup is cwd-scoped.
- Use `pi-overview` to see this bridge's own work and aggregate global/workspace capacity. Treat other workspaces as opaque: never try to infer their jobs, and do not cancel work you did not submit.
- Each Codex session has its own bridge and local job receipts. Shared coordination accounts for global and per-workspace process limits; it does not merge job ownership or results.
- Keep follow-up work on the original `threadId` and same project. Use a fresh session for an independent assignment.

## Delegate and monitor

1. Check `pi-overview` before submitting a large batch. If the shared queue or capacity is saturated, submit a smaller batch and monitor it before adding more.
2. Formulate each task with one concrete deliverable, relevant source/files, permitted scope, and a requested evidence summary. Keep tasks independent where possible and identify any prerequisites explicitly.
3. Call `pi-submit` and record the returned `jobId` and `threadId` alongside a short task label. The `jobId` is the handle for `pi-status`, `pi-wait`, and `pi-cancel`; the `threadId` resumes the Pi conversation.
4. Poll owned work with `pi-wait` or `pi-status`. A job is complete only when the bridge reports `completed`; completion depends on Pi's `agent_settled`, successful process exit, and final assistant text.
5. Cancel an owned job that is obsolete or out of scope. Do not reuse its `threadId` for simultaneous work.
6. Review worker claims against their source or artifact, resolve conflicts, and make project changes in the Codex session. Worker completion alone is not evidence that the project task is done.

## Capacity and coordination

The defaults are four running Pi processes per bridge, four per workspace, and eight globally across bridges sharing the coordination directory. The default coordination directory is `%USERPROFILE%\\.pi\\picodex-coordination`; separate project sessions on the same host must use the same directory to share global slots. A custom `PI_COORDINATION_DIR` must likewise be identical across those bridge processes.

Use `pi-overview` as a capacity view, not as a global job browser. It exposes own job counts, the current workspace's active count, and redacted global running/capacity totals, without foreign prompts or job IDs. If the overview or shared scheduling behavior is not present in the installed build, do not assume global slots are being enforced; run the README's checks.

## Keep task ownership clear

Maintain a short table or list in the current session for the tasks you submitted: label, `jobId`, `threadId`, status, and next action. This makes monitoring resilient to context switches. Never copy another session's job identifier from logs and attempt to control it. Do not send secrets or credentials in worker prompts.
