#!/usr/bin/env node
/**
 * Minimal stdio MCP client for bounded Picodex batches.
 * Reads [mcp_servers.pi-worker] from Codex config, starts the bridge in cwd,
 * checks shared capacity, and persists owned job receipts/results only.
 */
import { spawn } from "node:child_process";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { homedir } from "node:os";

const DEFAULT_CONFIG = resolve(homedir(), ".codex", "config.toml");
const ALLOWED_ENV = new Set([
  "PI_BIN", "PI_SCRIPT", "PI_PROVIDER", "PI_MODEL", "PI_COORDINATION_DIR",
  "PI_GLOBAL_MAX_CONCURRENCY", "PI_WORKSPACE_MAX_CONCURRENCY", "PI_MAX_CONCURRENCY",
  "PI_MAX_QUEUE", "PI_JOB_DEADLINE_MS", "PI_JOB_QUIET_MS", "PI_WORKSPACE_ROOT",
]);
const HELP = `Usage:
  node pi-batch.mjs --overview [--config PATH]
  node pi-batch.mjs --batch PATH --output ABSOLUTE_PATH [--config PATH] [--max-tasks N]

Batch JSON: {"tasks":[{"label":"short-name","prompt":"bounded task"}]}
The batch is capped at 10 tasks by default. Output is written incrementally.
`;

function argsFrom(argv) {
  const opts = { maxTasks: 10, overview: false };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--help" || key === "-h") { process.stdout.write(HELP); process.exit(0); }
    if (key === "--overview") { opts.overview = true; continue; }
    if (!["--config", "--batch", "--output", "--max-tasks"].includes(key)) throw new Error(`Unknown option: ${key}`);
    const value = argv[++i];
    if (!value || value.startsWith("--")) throw new Error(`Missing value for ${key}`);
    opts[key.slice(2).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = value;
  }
  if (!opts.overview && (!opts.batch || !opts.output)) throw new Error("--batch and --output are required unless --overview is used");
  opts.config ??= DEFAULT_CONFIG;
  opts.maxTasks = Number(opts.maxTasks);
  if (!Number.isInteger(opts.maxTasks) || opts.maxTasks < 1 || opts.maxTasks > 10) throw new Error("--max-tasks must be an integer from 1 to 10");
  return opts;
}

function tomlValue(raw) {
  const s = raw.trim();
  if (s.startsWith("'") && s.endsWith("'")) return s.slice(1, -1);
  if (s.startsWith('"') && s.endsWith('"')) return JSON.parse(s);
  if (s.startsWith("[")) {
    const body = s.slice(1, -1).trim();
    if (!body) return [];
    const parts = body.match(/'(?:[^']*)'|"(?:\\.|[^"\\])*"|[^,]+/g) ?? [];
    return parts.map(tomlValue);
  }
  if (/^(true|false)$/.test(s)) return s === "true";
  if (/^-?\d+(?:\.\d+)?$/.test(s)) return Number(s);
  throw new Error("Unsupported TOML value in pi-worker configuration");
}

function loadServerConfig(source) {
  let section = "";
  const configs = new Map([
    ["picodex", { root: {}, env: {} }],
    ["pi-worker", { root: {}, env: {} }],
  ]);
  for (const line of source.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const header = trimmed.match(/^\[([^\]]+)\]$/);
    if (header) { section = header[1]; continue; }
    const entry = trimmed.match(/^([A-Za-z0-9_-]+)\s*=\s*(.*?)\s*(?:#.*)?$/);
    if (!entry) continue;
    const [, key, raw] = entry;
    for (const [name, config] of configs) {
      if (section === `mcp_servers.${name}`) config.root[key] = tomlValue(raw);
      if (section === `mcp_servers.${name}.env` && ALLOWED_ENV.has(key)) config.env[key] = String(tomlValue(raw));
    }
  }
  const selected = [...configs.values()].find((config) => typeof config.root.command === "string" && Array.isArray(config.root.args));
  const { root, env } = selected ?? { root: {}, env: {} };
  if (typeof root.command !== "string" || !Array.isArray(root.args) || !root.args.length) {
    throw new Error("[mcp_servers.picodex] or [mcp_servers.pi-worker] command/args not found in Codex config");
  }
  if (!root.args.some((arg) => /(?:^|[\\/])pi-bridge\.mjs$/i.test(arg))) {
    throw new Error("pi-worker config does not launch pi-bridge.mjs");
  }
  return { command: root.command, args: root.args, env };
}

class McpStdio {
  constructor(child) {
    this.child = child;
    this.nextId = 0;
    this.pending = new Map();
    this.buffer = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => this.#receive(chunk));
    child.on("exit", (code) => {
      for (const { reject, timer } of this.pending.values()) { clearTimeout(timer); reject(new Error(`MCP server exited (${code})`)); }
      this.pending.clear();
    });
  }
  #receive(chunk) {
    this.buffer += chunk;
    for (;;) {
      const at = this.buffer.indexOf("\n");
      if (at < 0) break;
      const line = this.buffer.slice(0, at).trim(); this.buffer = this.buffer.slice(at + 1);
      if (!line) continue;
      let message;
      try { message = JSON.parse(line); } catch { continue; }
      const waiter = this.pending.get(message.id);
      if (!waiter) continue;
      this.pending.delete(message.id); clearTimeout(waiter.timer);
      if (message.error) waiter.reject(new Error(`MCP error ${message.error.code}`));
      else waiter.resolve(message.result);
    }
  }
  request(method, params = {}, timeoutMs = 30000) {
    const id = ++this.nextId;
    return new Promise((resolvePromise, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`MCP ${method} timed out`)); }, timeoutMs);
      this.pending.set(id, { resolve: resolvePromise, reject, timer });
      this.child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
    });
  }
  notify(method, params = {}) { this.child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`); }
  async initialize() {
    await this.request("initialize", { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "picodex-batch-client", version: "1.0.0" } });
    this.notify("notifications/initialized");
    const { tools } = await this.request("tools/list");
    for (const name of ["pi-overview", "pi-submit", "pi-wait"]) if (!tools?.some((x) => x.name === name)) throw new Error(`Picodex lacks required tool ${name}`);
  }
  async call(name, args = {}) {
    const result = await this.request("tools/call", { name, arguments: args }, 30000);
    if (result?.isError) throw new Error(`Picodex tool ${name} returned an error`);
    if (result?.structuredContent) return result.structuredContent;
    const text = result?.content?.find((x) => x.type === "text")?.text;
    if (!text) throw new Error(`Picodex tool ${name} returned no structured result`);
    try { return JSON.parse(text); } catch { throw new Error(`Picodex tool ${name} returned invalid JSON`); }
  }
}

function resolveOutputPath(output) {
  if (!isAbsolute(output)) throw new Error("--output must be an absolute path");
  return resolve(output);
}

async function persist(path, payload) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function main() {
  const opts = argsFrom(process.argv.slice(2));
  const workspace = resolve(process.cwd());
  const configPath = resolve(opts.config);
  const serverConfig = loadServerConfig(await readFile(configPath, "utf8"));
  const env = { ...process.env, ...serverConfig.env, PI_WORKSPACE_ROOT: workspace };
  const bridge = spawn(serverConfig.command, serverConfig.args, { cwd: workspace, env, stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
  // Consume diagnostics without echoing them: stderr may contain local provider details.
  bridge.stderr.resume();
  const mcp = new McpStdio(bridge);
  let jobs = [];
  let outputPath = null;
  let outputState = null;
  try {
    await mcp.initialize();
    const overview = await mcp.call("pi-overview");
    if (opts.overview) {
      process.stdout.write(`${JSON.stringify({ workspaceRoot: overview.workspaceRoot, own: overview.own, shared: overview.shared })}\n`);
      return;
    }
    outputPath = resolveOutputPath(opts.output);
    const batchPath = resolve(opts.batch);
    const parsed = JSON.parse((await readFile(batchPath, "utf8")).replace(/^\uFEFF/, ""));
    if (!Array.isArray(parsed.tasks) || parsed.tasks.length < 1 || parsed.tasks.length > opts.maxTasks) {
      throw new Error(`Batch must contain 1-${opts.maxTasks} tasks in "tasks"`);
    }
    const labels = new Set();
    const tasks = parsed.tasks.map((item, i) => {
      if (!item || typeof item.label !== "string" || !item.label.trim() || typeof item.prompt !== "string" || !item.prompt.trim()) throw new Error(`Task ${i + 1} requires non-empty label and prompt`);
      if (labels.has(item.label)) throw new Error(`Duplicate task label: ${item.label}`);
      labels.add(item.label);
      return { label: item.label, prompt: item.prompt };
    });

    const shared = overview.shared;
    if (!shared || !Number.isInteger(shared.globalMax) || !Number.isInteger(shared.workspaceMax)) throw new Error("pi-overview lacks shared coordination capacity; refusing to submit");
    const bridgeMax = Number(serverConfig.env.PI_MAX_CONCURRENCY) || 4;
    const bridgeActive = (overview.own?.running ?? 0) + (overview.own?.stopping ?? 0);
    const available = Math.max(0, Math.min(shared.globalMax - shared.globalActive, shared.workspaceMax - shared.workspaceActive, bridgeMax - bridgeActive));
    if (available < 1) throw new Error("No free Pi capacity in the configured shared limits; no jobs submitted");

    const state = { createdAt: new Date().toISOString(), workspaceRoot: workspace, batchFile: batchPath, overview, capacityAtStart: available, jobs: [] };
    outputState = state;
    jobs = state.jobs;
    await persist(outputPath, state);
    for (let offset = 0; offset < tasks.length; offset += available) {
      const wave = tasks.slice(offset, offset + available);
      const submitted = await Promise.all(wave.map(async (task) => {
        const job = await mcp.call("pi-submit", { prompt: task.prompt });
        const receipt = { label: task.label, jobId: job.jobId, threadId: job.threadId, status: job.status, submittedAt: new Date().toISOString(), result: null };
        jobs.push(receipt); await persist(outputPath, state);
        return receipt;
      }));
      await Promise.all(submitted.map(async (receipt) => {
        let result;
        do {
          result = await mcp.call("pi-wait", { jobId: receipt.jobId, waitMs: 25000 });
          receipt.status = result.status;
          receipt.updatedAt = new Date().toISOString();
          receipt.result = result;
          await persist(outputPath, state);
        } while (["queued", "running", "cancelling", "stopping"].includes(result.status));
      }));
    }
    process.stdout.write(`${JSON.stringify({ output: outputPath, submitted: jobs.length, statuses: jobs.map(({ label, jobId, status }) => ({ label, jobId, status })) })}\n`);
  } finally {
    if (outputPath && jobs.length) {
      // Final persistence is best effort; do not emit response text or stderr.
      try { await persist(outputPath, { ...outputState, updatedAt: new Date().toISOString() }); } catch { /* preserve primary result */ }
    }
    bridge.stdin.end();
  }
}

main().catch((error) => {
  process.stderr.write(`picodex batch client: ${error.message}\n`);
  process.exitCode = 1;
});
