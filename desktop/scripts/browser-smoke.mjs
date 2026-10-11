// Browser-only interaction checks with an explicit mock bridge. This does not
// replace Windows/WebView2/native-dialog or multi-process UDP acceptance tests.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const { chromium } = await import(process.env.P2PFS_PLAYWRIGHT || 'playwright');
const root = fileURLToPath(new URL('../frontend/', import.meta.url));
const server = createServer(async (req, res) => {
  try {
    if (req.url === '/wails/runtime.js') {
      res.setHeader('Content-Type', 'text/javascript');
      res.end(
        'export const Call = {ByName:(name,...args)=>window.__mock(name.split(".").pop(),args)};',
      );
      return;
    }
    const target = path.join(
      root,
      req.url === '/' ? 'index.html' : req.url.split('?')[0],
    );
    if (!target.startsWith(root)) {
      res.writeHead(403).end();
      return;
    }
    const data = await readFile(target);
    res.setHeader(
      'Content-Type',
      target.endsWith('.js')
        ? 'text/javascript'
        : target.endsWith('.css')
          ? 'text/css'
          : 'text/html',
    );
    res.end(data);
  } catch {
    res.writeHead(404).end();
  }
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.CHROMIUM_PATH || undefined,
  args: ['--no-sandbox'],
});
try {
  const page = await browser.newPage({
      viewport: { width: 1220, height: 900 },
    }),
    errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.addInitScript(() => {
    const id = 'a'.repeat(64),
      manifest = {
        id,
        name: '研究资料 <img src=x onerror=alert(1)>.zip',
        size: 8388608,
        sha256: 'b'.repeat(64),
        data_shards: 4,
        parity_shards: 2,
      };
    window.__state = {
      connected: false,
      config: {
        server_url: 'https://localhost:8443',
        data_dir: 'C:\\p2pfs\\data',
        name: '实验设备 A',
        insecure_local: false,
        force_relay: false,
      },
      tasks: [],
      files: [
        {
          manifest,
          sources: [
            { peer_id: 'peer-1', indices: [0, 1, 2], last_seen: 1791695400 },
            { peer_id: 'peer-2', indices: [3, 4, 5], last_seen: 1791695400 },
          ],
        },
      ],
      calls: [],
      destination: 'C:\\Downloads\\research.zip',
    };
    window.__mock = async (method, args) => {
      const s = window.__state;
      s.calls.push({ method, args });
      switch (method) {
        case 'Status':
          return {
            connected: s.connected,
            connection_status: s.connected ? '已连接' : '未连接',
            uuid: s.connected ? 'peer-1' : '',
            config: s.config,
            tasks: structuredClone(s.tasks),
          };
        case 'Storage':
          return { path: s.config.data_dir, bytes: 4194320, files: 4 };
        case 'Connect':
          await new Promise((r) => setTimeout(r, 50));
          s.connected = true;
          s.config = args[0];
          return null;
        case 'Disconnect':
          s.connected = false;
          s.tasks = [];
          return null;
        case 'Files':
          return structuredClone(s.files);
        case 'LocalFiles':
          return [
            {
              manifest,
              sharing: s.files[0].sources.some((v) => v.peer_id === 'peer-1'),
              indices: [0, 1, 2],
              bytes: 4194304,
            },
          ];
        case 'ChooseDestination':
          return s.destination;
        case 'ChooseDataDirectory':
          return 'C:\\ChosenData';
        case 'ChoosePublishFile':
          return '';
        case 'Download':
          await new Promise((r) => setTimeout(r, 100));
          s.tasks.push({
            id: 'task-1',
            file_id: id,
            destination: args[1],
            state: 'downloading',
            completed_shards: [0],
            bytes: 2097152,
            created_at: new Date().toISOString(),
            transfers: [
              {
                peer_id: 'peer-2',
                index: 0,
                path: 'direct',
                bytes: 2097152,
                milliseconds: 420,
              },
            ],
          });
          return 'task-1';
        case 'TaskAction':
          s.tasks[0].state = {
            pause: 'paused',
            resume: 'downloading',
            cancel: 'cancelled',
            retry: 'downloading',
          }[args[1]];
          return null;
        case 'Retain':
          return null;
        case 'Unshare':
          s.files[0].sources = s.files[0].sources.filter(
            (v) => v.peer_id !== 'peer-1',
          );
          return null;
        default:
          throw new Error('Unexpected mock method: ' + method);
      }
    };
  });
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  await page.locator('#view-settings').waitFor({ state: 'visible' });
  assert.equal(await page.locator('#publish').isDisabled(), true);
  await page.locator('#choose-data').click();
  await page.waitForFunction(
    () => document.getElementById('data-dir').value === 'C:\\ChosenData',
  );
  await page.locator('#connect').click();
  await page.locator('#view-files').waitFor({ state: 'visible' });
  await page.locator('.file-row').waitFor();
  assert.equal(await page.locator('.file-name img').count(), 0);
  assert.match(await page.locator('.file-name').innerText(), /<img/);
  await mkdir('bin/qa', { recursive: true });
  await page.screenshot({ path: 'bin/qa/files-mock.png', fullPage: true });
  await page.locator('#search').fill('missing');
  assert.equal(await page.locator('.file-row').count(), 0);
  await page.locator('#search').fill('AAA');
  assert.equal(await page.locator('.file-row').count(), 1);
  await page.locator('#search').fill('');
  await page.locator('[data-action=download]').dblclick();
  await page.locator('#view-tasks').waitFor({ state: 'visible' });
  await page.locator('.task-card').waitFor();
  assert.equal(
    await page.evaluate(
      () => window.__state.calls.filter((c) => c.method === 'Download').length,
    ),
    1,
  );
  await page.locator('[data-action=task-pause]').click();
  await page.locator('[data-action=task-resume]').waitFor();
  await page.locator('[data-action=task-resume]').click();
  await page.locator('[data-action=task-pause]').waitFor();
  await page.locator('[data-action=task-cancel]').click();
  await page.locator('#confirm-dialog').waitFor({ state: 'visible' });
  await page.keyboard.press('Escape');
  assert.equal(
    await page.evaluate(() => window.__state.tasks[0].state),
    'downloading',
  );
  await page.locator('[data-action=task-cancel]').click();
  await page.locator('#dialog-confirm').click();
  await page.locator('[data-action=task-retry]').waitFor();
  await page.locator('[data-action=task-retry]').click();
  await page.locator('[data-action=task-pause]').waitFor();
  await page.locator('.transfer-details summary').click();
  await page.screenshot({ path: 'bin/qa/tasks-mock.png', fullPage: true });
  await page.locator('.nav-item[data-view=files]').click();
  await page.evaluate(() => (window.__state.destination = ''));
  await page.locator('[data-action=download]').click();
  assert.equal(
    await page.evaluate(
      () => window.__state.calls.filter((c) => c.method === 'Download').length,
    ),
    1,
  );
  await page.locator('[data-action=retain]').click();
  await page.locator('#dialog-confirm').click();
  await page.waitForFunction(
    () =>
      document.getElementById('notice-text').textContent ===
      '请至少选择一个分片',
  );
  assert.equal(
    await page.evaluate(
      () => window.__state.calls.filter((c) => c.method === 'Retain').length,
    ),
    0,
  );
  await page.locator('[data-action=retain]').click();
  for (const i of [0, 2, 5])
    await page.locator(`#dialog-body input[value="${i}"]`).check();
  await page.locator('#dialog-confirm').click();
  await page.waitForFunction(() =>
    window.__state.calls.some((c) => c.method === 'Retain'),
  );
  assert.deepEqual(
    await page.evaluate(
      () => window.__state.calls.find((c) => c.method === 'Retain').args[1],
    ),
    [0, 2, 5],
  );
  await page.locator('[data-action=unshare]').click();
  await page.locator('#dialog-confirm').click();
  await page.waitForFunction(() =>
    window.__state.calls.some((c) => c.method === 'Unshare'),
  );
  assert.equal(
    await page.evaluate(
      () => window.__state.calls.find((c) => c.method === 'Unshare').args[1],
    ),
    false,
  );
  await page.locator('.nav-item[data-view=settings]').click();
  await page.locator('#disconnect').click();
  await page.keyboard.press('Escape');
  assert.equal(await page.evaluate(() => window.__state.connected), true);
  await page.locator('#disconnect').click();
  await page.locator('#dialog-confirm').click();
  await page.waitForFunction(() => !window.__state.connected);
  await page.locator('.nav-item[data-view=files]').click();
  await page.waitForFunction(
    () => document.querySelectorAll('.file-row').length === 0,
  );
  assert.equal(await page.locator('[data-action=download]').count(), 0);
  for (const width of [900, 1220]) {
    await page.setViewportSize({ width, height: 820 });
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
      `horizontal overflow at ${width}`,
    );
  }
  assert.deepEqual(errors, []);
  console.log(
    'PASS: browser-only mock-bridge flows, repeated click, modal cancellation, filtering, task controls, share controls, safe text rendering, 900/1220px layout.',
  );
} finally {
  await browser.close();
  server.close();
}
