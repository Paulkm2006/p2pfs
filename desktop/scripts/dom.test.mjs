import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { JSDOM } from 'jsdom';
import * as helpers from '../frontend/format.js';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(check) {
  for (let i = 0; i < 200; i++) {
    if (check()) return;
    await delay(10);
  }
  assert.fail('DOM condition did not settle');
}

async function exerciseDesktop(lineEnding) {
  const html = await readFile(
    new URL('../frontend/index.html', import.meta.url),
    'utf8',
  );
  const inputJS = await readFile(
    new URL('../frontend/app.js', import.meta.url),
    'utf8',
  );
  // Exercise both checkouts on every OS, rather than relying on core.autocrlf.
  const js = inputJS
    .replace(/\r\n?/g, '\n')
    .replace(/\n/g, lineEnding === 'CRLF' ? '\r\n' : '\n');
  const dom = new JSDOM(html, {
      runScripts: 'outside-only',
      url: 'http://wails.localhost',
    }),
    w = dom.window,
    d = w.document;
  const $ = (selector) => d.querySelector(selector),
    click = (selector) => {
      const node = $(selector);
      assert.ok(node, selector);
      node.click();
    };
  w.setInterval = () => 0;
  w.HTMLDialogElement.prototype.showModal = function () {
    this.setAttribute('open', '');
  };
  w.HTMLDialogElement.prototype.close = function (value = '') {
    this.returnValue = value;
    this.removeAttribute('open');
    this.dispatchEvent(new w.Event('close'));
  };
  $('#confirm-dialog form').addEventListener('submit', (event) => {
    event.preventDefault();
    $('#confirm-dialog').close(event.submitter?.value || 'cancel');
  });
  const id = 'a'.repeat(64),
    manifest = {
      id,
      name: '资料 <img src=x onerror=alert(1)>.zip',
      size: 1024,
      sha256: 'b'.repeat(64),
      data_shards: 4,
      parity_shards: 2,
    };
  const state = {
    connected: false,
    config: {
      server_url: 'https://localhost:8443',
      data_dir: 'C:\\data',
      name: 'Test',
      concurrency: 3,
    },
    tasks: [],
    files: [
      {
        manifest,
        sources: [
          { peer_id: 'self', indices: [0, 1], last_seen: 123 },
          { peer_id: 'other', indices: [2, 3, 4, 5], last_seen: 123 },
        ],
      },
    ],
    calls: [],
    destination: 'C:\\out.bin',
  };
  const record =
    (name, fn) =>
    (...args) => {
      state.calls.push({ name, args });
      return fn(...args);
    };
  const api = {
    status: async () => ({
      connected: state.connected,
      connection_status: state.connected ? '已连接' : '未连接',
      uuid: state.connected ? 'self' : '',
      config: state.config,
      tasks: structuredClone(state.tasks),
    }),
    storage: async () => ({
      path: state.config.data_dir,
      bytes: 1024,
      files: 2,
    }),
    connect: record('connect', async (config) => {
      await delay(30);
      state.config = config;
      state.connected = true;
    }),
    disconnect: record('disconnect', async () => {
      state.connected = false;
      state.tasks = [];
    }),
    files: async () => structuredClone(state.files),
    chooseDataDirectory: async () => '',
    choosePublishFile: async () => '',
    chooseDestination: async () => state.destination,
    download: record('download', async (_, destination) => {
      await delay(20);
      state.tasks = [
        {
          id: 't',
          file_id: id,
          destination,
          state: 'downloading',
          completed_shards: [0],
          bytes: 256,
          created_at: new Date().toISOString(),
          transfers: [
            {
              peer_id: 'other',
              index: 0,
              path: 'direct',
              bytes: 256,
              milliseconds: 10,
            },
          ],
        },
      ];
      return 't';
    }),
    taskAction: record('taskAction', async (_, action) => {
      state.tasks[0].state = {
        pause: 'paused',
        resume: 'downloading',
        retry: 'downloading',
        cancel: 'cancelled',
      }[action];
    }),
    retain: record('retain', async () => {}),
    unshare: record('unshare', async () => {
      state.files[0].sources = state.files[0].sources.filter(
        (s) => s.peer_id !== 'self',
      );
    }),
    localFiles: async () => [],
    exportStatistics: async () => '',
  };
  const source = js.replace(/^import[\s\S]*?;\r?\n/gm, '');
  await new w.Function(
    'api',
    'helpers',
    `const {bytes, shardSources, stateLabel, taskActions, filterFiles, sourceTime}=helpers;return (async()=>{${source}\n})();`,
  )(api, helpers);
  assert.equal($('#view-settings').hidden, false);
  assert.equal($('#publish').disabled, true);
  click('#connect');
  click('#connect');
  await until(() => !$('#view-files').hidden && $('.file-row'));
  assert.equal(state.calls.filter((c) => c.name === 'connect').length, 1);
  assert.equal($('.file-name img'), null);
  assert.match($('.file-name').textContent, /<img/);
  $('#search').value = 'missing';
  $('#search').dispatchEvent(new w.Event('input'));
  assert.equal($('.file-row'), null);
  $('#search').value = 'AAA';
  $('#search').dispatchEvent(new w.Event('input'));
  assert.ok($('.file-row'));
  $('#search').value = '';
  $('#search').dispatchEvent(new w.Event('input'));
  click('[data-action=download]');
  click('[data-action=download]');
  await until(() => !$('#view-tasks').hidden && $('[data-action=task-pause]'));
  assert.equal(state.calls.filter((c) => c.name === 'download').length, 1);
  click('[data-action=task-pause]');
  await until(
    () =>
      $('[data-action=task-resume]') &&
      !$('[data-action=task-resume]').disabled,
  );
  click('[data-action=task-resume]');
  await until(
    () =>
      $('[data-action=task-pause]') && !$('[data-action=task-pause]').disabled,
  );
  click('[data-action=task-cancel]');
  assert.equal($('#confirm-dialog').open, true);
  $('#confirm-dialog').close('cancel');
  await delay(20);
  assert.equal(state.tasks[0].state, 'downloading');
  click('[data-action=task-cancel]');
  click('#dialog-confirm');
  await until(
    () =>
      $('[data-action=task-retry]') && !$('[data-action=task-retry]').disabled,
  );
  click('[data-action=task-retry]');
  await until(() => state.tasks[0].state === 'downloading');
  click('.nav-item[data-view=files]');
  state.destination = '';
  click('[data-action=download]');
  await delay(20);
  assert.equal(state.calls.filter((c) => c.name === 'download').length, 1);
  click('[data-action=retain]');
  click('#dialog-confirm');
  await until(() => $('#notice-text').textContent === '请至少选择一个分片');
  await delay(20);
  assert.equal(state.calls.filter((c) => c.name === 'retain').length, 0);
  click('[data-action=retain]');
  for (const i of [0, 2, 5])
    $(`#dialog-body input[value="${i}"]`).checked = true;
  click('#dialog-confirm');
  await until(() => state.calls.some((c) => c.name === 'retain'));
  await delay(20);
  assert.equal(
    JSON.stringify(state.calls.find((c) => c.name === 'retain').args[1]),
    '[0,2,5]',
  );
  click('[data-action=unshare]');
  click('#dialog-confirm');
  await until(() => state.calls.some((c) => c.name === 'unshare'));
  assert.equal(state.calls.find((c) => c.name === 'unshare').args[1], false);
  click('.nav-item[data-view=settings]');
  click('#disconnect');
  $('#confirm-dialog').close('cancel');
  await delay(20);
  assert.equal(state.connected, true);
  click('#disconnect');
  click('#dialog-confirm');
  await until(() => !state.connected && !$('#connect').hidden);
  assert.equal($('.file-row'), null);
  assert.equal($('[data-action=download]'), null);
  assert.equal($('#publish').disabled, true);
  dom.window.close();
}

for (const lineEnding of ['LF', 'CRLF']) {
  test(`DOM-only mocked bridge (${lineEnding}): connect, safe rendering, task/share dialogs, duplicate actions, disconnect`, () =>
    exerciseDesktop(lineEnding));
}
