#!/usr/bin/env node
/**
 * Picodex — MCP server exposing blocking and queued Pi worker tools.
 *
 * Pi persists sessions across restarts. Job status is retained in bridge
 * memory for the current process lifetime.
 *
 * cwd is deliberately NOT a tool param — pi's session lookup is cwd-scoped, so
 * keeping process.cwd() fixed makes resume always succeed. Work on another repo
 * by registering a separate bridge from it, or let pi `cd` itself.
 *
 * Wire protocol: newline-delimited JSON-RPC 2.0 over stdin/stdout (MCP stdio),
 * implemented inline with zero runtime deps. stdout is NDJSON only; the ready
 * banner and all logs go to stderr.
 */

import { createJobManager } from "./pi-jobs.mjs";
import { createCoordination } from "./pi-coordination.mjs";
import { randomUUID } from "node:crypto";

const PI_BIN = process.env.PI_BIN || "pi";
const PI_SCRIPT = process.env.PI_SCRIPT;
const PI_PROVIDER = process.env.PI_PROVIDER;
const PI_MODEL = process.env.PI_MODEL;
const VERSION = "0.4.0";
const LATEST_PROTOCOL_VERSION = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS = [
  LATEST_PROTOCOL_VERSION, "2025-06-18", "2025-03-26", "2024-11-05", "2024-10-07",
];
const bridgeId = randomUUID();
const workspaceRoot = process.env.PI_WORKSPACE_ROOT || process.cwd();
const coordination = createCoordination({
  directory: process.env.PI_COORDINATION_DIR,
  workspaceRoot,
  bridgeId,
  globalMax: Number(process.env.PI_GLOBAL_MAX_CONCURRENCY) || 8,
  workspaceMax: Number(process.env.PI_WORKSPACE_MAX_CONCURRENCY) || 4,
});
const jobs = createJobManager({
  piBin: PI_BIN, piScript: PI_SCRIPT, piProvider: PI_PROVIDER, piModel: PI_MODEL,
  coordination, bridgeId, workspaceRoot: coordination.workspaceRoot,
});

const PI_TOOL = {
  name: "pi",
  description:
    "Delegate a coding task to a fresh, isolated pi coding-agent instance. " +
    "pi uses its own configured model, auth, and tools (~/.pi/agent). " +
    "Returns the agent's final text and a threadId; pass threadId to `pi-reply` to continue.",
  inputSchema: {
    type: "object",
    properties: { prompt: { type: "string", description: "The task for the pi agent." } },
    required: ["prompt"],
  },
};

const PI_REPLY_TOOL = {
  name: "pi-reply",
  description:
    "Continue a previous pi instance by threadId. The pi session is resumed " +
    "with full conversation history. Returns the agent's final text.",
  inputSchema: {
    type: "object",
    properties: {
      threadId: { type: "string", description: "The threadId returned by a previous `pi` call." },
      prompt: { type: "string", description: "The follow-up message for the pi agent." },
    },
    required: ["threadId", "prompt"],
  },
};

const JOB_ID = { type: "string", description: "The jobId returned by pi-submit." };
const PI_SUBMIT_TOOL = {
  name: "pi-submit",
  description: "Queue a Pi task and return immediately with jobId and threadId. Use pi-status or pi-wait to track it; pi-cancel can stop it.",
  inputSchema: {
    type: "object",
    properties: {
      prompt: { type: "string", description: "Bounded task for Pi." },
      threadId: { type: "string", description: "Optional existing Pi session to continue." },
    },
    required: ["prompt"],
  },
};
const PI_STATUS_TOOL = {
  name: "pi-status", description: "Get a Pi job's status and final result, if finished.",
  inputSchema: { type: "object", properties: { jobId: JOB_ID }, required: ["jobId"] },
};
const PI_WAIT_TOOL = {
  name: "pi-wait", description: "Wait up to 25 seconds for a Pi job to finish; returns current status if still running.",
  inputSchema: {
    type: "object",
    properties: { jobId: JOB_ID, waitMs: { type: "integer", minimum: 0, maximum: 25000 } },
    required: ["jobId"],
  },
};
const PI_CANCEL_TOOL = {
  name: "pi-cancel", description: "Cancel a queued or running Pi job and stop its process tree.",
  inputSchema: { type: "object", properties: { jobId: JOB_ID }, required: ["jobId"] },
};
const PI_OVERVIEW_TOOL = {
  name: "pi-overview",
  description: "Show this bridge's workspace and job counts plus redacted shared Pi capacity across Codex sessions.",
  inputSchema: { type: "object", properties: {} },
};

// Blocking legacy tools and asynchronous job tools share the same bounded
// manager. JSON events are consumed until Pi emits agent_settled and exits.
function jobResult(job) {
  return {
    content: [{ type: "text", text: JSON.stringify(job) }],
    structuredContent: job,
    isError: ["failed", "cancelled", "timed_out"].includes(job.status),
  };
}

// threadId is mirrored into structuredContent so clients parse it without
// fragile text-prefix scraping.
function toolResult(text, threadId, isError = false) {
  return {
    content: [{ type: "text", text }],
    structuredContent: { threadId, content: text },
    isError,
  };
}
const failure = (message, threadId = null) => toolResult(message, threadId, true);

// --- newline-delimited JSON-RPC 2.0 over stdio ---
const METHOD_NOT_FOUND = -32601;
const INVALID_PARAMS = -32602;
const INTERNAL_ERROR = -32603;
const INVALID_REQUEST = -32600;

const send = (msg) => process.stdout.write(JSON.stringify(msg) + "\n");
const sendResult = (id, result) => send({ jsonrpc: "2.0", id, result });
const sendError = (id, code, message) => send({ jsonrpc: "2.0", id, error: { code, message } });

// Tool-level failures return a CallToolResult with isError:true (not a JSON-RPC
// error), so the LLM can see and self-correct; protocol errors are reserved for
// malformed requests.
async function handleCallTool(request) {
  const { name } = request.params;
  const args = request.params.arguments ?? {};
  try {
    if (name === "pi-status") return jobResult(jobs.status(args.jobId));
    if (name === "pi-overview") return jobResult(jobs.overview());
    if (name === "pi-wait") return jobResult(await jobs.wait(args.jobId, args.waitMs ?? 25000));
    if (name === "pi-cancel") return jobResult(jobs.cancel(args.jobId));
    if (name === "pi-submit") return jobResult(jobs.submit(args.prompt, args.threadId));
    if (name !== "pi" && name !== "pi-reply") return failure(`Unknown tool: ${name}`);
    const threadId = name === "pi-reply" ? args.threadId : undefined;
    if (name === "pi-reply" && !threadId) return failure("Error: `threadId` is required for pi-reply.");
    const submitted = jobs.submit(args.prompt, threadId);
    let result;
    do { result = await jobs.wait(submitted.jobId); }
    while (result.status === "queued" || result.status === "running");
    if (result.status !== "completed")
      return failure(`pi ${result.status}: ${result.error ?? "unknown failure"}`, result.threadId);
    return toolResult(result.text, result.threadId);
  } catch (error) {
    return failure(`Pi job error: ${error.message}`);
  }
}

// Dispatch one parsed message. Notifications (no id) and stray responses (we
// send none) are ignored; requests always get a result or JSON-RPC error.
async function handleMessage(message) {
  if (message.id === undefined || message.result !== undefined || message.error !== undefined) return;

  const { id, method } = message;
  if (typeof method !== "string") return sendError(id, INVALID_REQUEST, "Invalid Request");

  switch (method) {
    case "initialize": {
      const requested = message.params?.protocolVersion;
      if (typeof requested !== "string") {
        return sendError(id, INVALID_PARAMS, "Invalid params: initialize requires a string `protocolVersion`.");
      }
      const protocolVersion = SUPPORTED_PROTOCOL_VERSIONS.includes(requested)
        ? requested
        : LATEST_PROTOCOL_VERSION;
      return sendResult(id, {
        protocolVersion,
        capabilities: { tools: {} },
        serverInfo: { name: "picodex", version: VERSION },
      });
    }
    case "ping":
      return sendResult(id, {});
    case "tools/list":
      return sendResult(id, { tools: [PI_TOOL, PI_REPLY_TOOL, PI_SUBMIT_TOOL, PI_STATUS_TOOL, PI_WAIT_TOOL, PI_CANCEL_TOOL, PI_OVERVIEW_TOOL] });
    case "tools/call":
      if (typeof message.params?.name !== "string") {
        return sendError(id, INVALID_PARAMS, "Invalid params: tools/call requires a string `name`.");
      }
      try {
        return sendResult(id, await handleCallTool(message));
      } catch (err) {
        return sendError(id, INTERNAL_ERROR, err?.message ?? "Internal error");
      }
    default:
      return sendError(id, METHOD_NOT_FOUND, "Method not found");
  }
}

// Buffer stdin across chunk boundaries; dispatch one JSON message per newline.
// Malformed JSON lines are ignored so a bad line can't kill the server.
let stdinBuffer = null;
process.stdin.on("data", (chunk) => {
  stdinBuffer = stdinBuffer ? Buffer.concat([stdinBuffer, chunk]) : chunk;
  if (stdinBuffer.length > 5000000) {
    console.error("pi-mcp-bridge: MCP input exceeds 5 MB");
    jobs.shutdown();
    process.exitCode = 1;
    process.stdin.destroy();
    return;
  }
  let newline;
  while ((newline = stdinBuffer.indexOf(0x0a)) !== -1) {
    const line = stdinBuffer.subarray(0, newline).toString("utf8").replace(/\r$/, "");
    stdinBuffer = stdinBuffer.subarray(newline + 1);
    if (!line.trim()) continue;
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      continue;
    }
    handleMessage(message).catch((err) => console.error(`pi-mcp-bridge: error handling message: ${err}`));
  }
});

// Swallow stream errors so a client disconnect can't crash the server.
process.stdin.on("error", () => {});
process.stdin.on("end", () => jobs.shutdown());
process.stdout.on("error", () => {});
process.on("SIGTERM", () => { jobs.shutdown(); process.exitCode = 0; });
process.on("SIGINT", () => { jobs.shutdown(); process.exitCode = 0; });

console.error(`picodex v${VERSION} ready (pi binary: ${PI_BIN})`);
