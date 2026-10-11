import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import {
  bytes,
  shardSources,
  taskActions,
  stateLabel,
  filterFiles,
} from '../frontend/format.js';

test('sizes use actual bytes including empty files', () => {
  assert.equal(bytes(0), '0 B');
  assert.equal(bytes(1024), '1.0 KiB');
  assert.equal(bytes(-1), '—');
  assert.equal(bytes(1 << 30), '1.0 GiB');
});
test('candidate shards deduplicate peer and index', () => {
  assert.deepEqual(
    shardSources({
      sources: [
        { peer_id: 'a', indices: [0, 0, 2] },
        { peer_id: 'a', indices: [0] },
        { peer_id: 'b', indices: [0, 5] },
      ],
    }),
    [2, 0, 1, 0, 0, 1],
  );
});
test('task completion is backend complete, not progress count', () => {
  assert.equal(stateLabel('complete'), '已完成');
  assert.deepEqual(taskActions('complete'), []);
  assert.deepEqual(taskActions('failed'), ['retry']);
  assert.deepEqual(taskActions('paused'), ['resume', 'cancel']);
  assert.deepEqual(taskActions('waiting'), ['pause', 'cancel']);
});
test('file filtering preserves same-name versions', () => {
  const files = [
    { manifest: { name: 'Report.pdf', id: 'aaa' } },
    { manifest: { name: 'Report.pdf', id: 'bbb' } },
  ];
  assert.equal(filterFiles(files, 'report').length, 2);
  assert.equal(filterFiles(files, 'BBB')[0].manifest.id, 'bbb');
});
test('every named frontend binding exists in the Windows Go service', async () => {
  const js = await readFile(
    new URL('../frontend/api.js', import.meta.url),
    'utf8',
  );
  const go = await readFile(
    new URL('../service_windows.go', import.meta.url),
    'utf8',
  );
  const calls = [...js.matchAll(/invoke\('([A-Za-z]+)'/g)].map(
    (match) => match[1],
  );
  assert.ok(calls.length >= 14);
  for (const call of calls)
    assert.match(go, new RegExp(`func \\(s \\*DesktopService\\) ${call}\\(`));
  assert.match(js, /main\.DesktopService/);
});
test('untrusted remote text never uses HTML injection APIs', async () => {
  const js = await readFile(
    new URL('../frontend/app.js', import.meta.url),
    'utf8',
  );
  assert.doesNotMatch(
    js,
    /innerHTML|outerHTML|insertAdjacentHTML|document\.write/,
  );
});
