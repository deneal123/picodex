const prompt = process.argv.at(-1);
const emit = (record) => process.stdout.write(`${JSON.stringify(record)}\n`);
emit({ type: "agent_start" });
const delayMs = Number.parseInt(process.env.PI_FIXTURE_DELAY_MS ?? "0", 10);
setTimeout(() => {
  emit({ type: "message_end", message: {
    role: "assistant", content: [{ type: "text", text: `fixture: ${prompt}` }], stopReason: "stop",
  } });
  emit({ type: "agent_end", messages: [], willRetry: false });
  emit({ type: "agent_settled" });
}, Number.isInteger(delayMs) && delayMs > 0 ? delayMs : 20);
