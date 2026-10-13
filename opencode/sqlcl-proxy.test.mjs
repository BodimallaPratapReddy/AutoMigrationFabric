import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('SQLcl exit closes the proxy even while the MCP client keeps stdin open', async () => {
  const source = await readFile(new URL('./sqlcl-proxy.mjs', import.meta.url), 'utf8');
  const folder = await mkdtemp(join(tmpdir(), 'sqlcl-proxy-test-'));
  const path = join(folder, 'proxy.mjs');
  const replacement = `const sqlcl = spawn(${JSON.stringify(process.execPath)}, ['-e', 'process.exit(7)'], { stdio: ['pipe','pipe','inherit'] });`;
  await writeFile(path, source.replace(/const sqlcl = spawn\([\s\S]*?\);/, replacement));
  const proxy = spawn(process.execPath, [path], { stdio: ['pipe','pipe','pipe'] });
  try {
    const code = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => { proxy.kill(); reject(new Error('Dead SQLcl left a live MCP proxy')); }, 3000);
      proxy.on('exit', code => { clearTimeout(timeout); resolve(code); });
      proxy.on('error', error => { clearTimeout(timeout); reject(error); });
    });
    assert.equal(code, 7);
  } finally {
    proxy.stdin.destroy();
    await rm(folder, { recursive: true, force: true });
  }
});
