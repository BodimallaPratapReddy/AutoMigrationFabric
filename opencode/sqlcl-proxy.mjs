import { spawn } from "node:child_process";
import readline from "node:readline";

const sqlcl = spawn("/home/pratap/tools/sqlcl/bin/sql", ["-mcp"], {
  stdio: ["pipe", "pipe", "inherit"], env: process.env,
});

// Register lifecycle handlers BEFORE awaiting input. A dead child must close
// the MCP transport, rather than leaving OpenCode with a falsely live proxy.
sqlcl.on("error", error => {
  console.error("SQLcl MCP could not start:", error.code || error.name);
  process.exit(1);
});
sqlcl.on("close", (code, signal) => {
  console.error(`SQLcl MCP closed (code=${code}, signal=${signal || "none"})`);
  process.exit(code || (signal ? 1 : 0));
});
sqlcl.stdin.on("error", error => {
  console.error("SQLcl MCP input closed:", error.code || error.name);
  sqlcl.kill("SIGTERM");
});
for (const signal of ["SIGTERM", "SIGINT"]) {
  process.on(signal, () => {
    sqlcl.kill("SIGTERM");
    setTimeout(() => { sqlcl.kill("SIGKILL"); process.exit(1); }, 5000).unref();
  });
}

sqlcl.stdout.pipe(process.stdout);
const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (!line.trim()) continue;
  let output = line;
  try {
    const message = JSON.parse(line);
    if (message.method === "initialize") {
      const form = message.params?.capabilities?.elicitation?.form;
      if (form && typeof form === "object") delete form.applyDefaults;
    }
    output = JSON.stringify(message);
  } catch {
    // Preserve the existing transparent forwarding behavior.
  }
  if (sqlcl.stdin.destroyed) break;
  if (!sqlcl.stdin.write(output + "\n")) {
    await new Promise(resolve => {
      const done = () => {
        sqlcl.stdin.off("drain", done);
        sqlcl.stdin.off("close", done);
        resolve();
      };
      sqlcl.stdin.once("drain", done);
      sqlcl.stdin.once("close", done);
    });
  }
}
sqlcl.stdin.end();
