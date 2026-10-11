import { api } from './api.js';
import {
  bytes,
  shardSources,
  stateLabel,
  taskActions,
  filterFiles,
  sourceTime,
} from './format.js';

const $ = (id) => document.getElementById(id);
const state = {
  status: null,
  files: [],
  selected: null,
  busy: new Set(),
  polling: null,
  generation: 0,
  syncing: false,
  taskSignature: '',
  fileSignature: '',
  localFiles: [],
};
const el = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
};
const button = (text, className, action, id) => {
  const b = el('button', `button ${className}`, text);
  b.type = 'button';
  b.dataset.action = action;
  if (id) b.dataset.id = id;
  return b;
};
const emptyDetail = [...$('file-detail').childNodes].map((node) =>
  node.cloneNode(true),
);
const currentFile = () =>
  state.files.find((file) => file.manifest.id === state.selected);

function notify(text, error = false) {
  $('notice-text').textContent = text;
  $('notice').classList.toggle('error', error);
  $('notice').hidden = false;
}
function message(error) {
  return typeof error === 'string'
    ? error
    : error?.message || '操作失败，请检查连接后重试';
}
function view(name) {
  for (const section of document.querySelectorAll('.view'))
    section.hidden = section.id !== `view-${name}`;
  for (const item of document.querySelectorAll('.nav-item')) {
    const active = item.dataset.view === name;
    item.classList.toggle('active', active);
    if (active) item.setAttribute('aria-current', 'page');
    else item.removeAttribute('aria-current');
  }
}
async function operation(key, action, control) {
  if (state.busy.has(key)) return;
  state.busy.add(key);
  if (control) control.disabled = true;
  try {
    await action();
  } catch (error) {
    notify(message(error), true);
  } finally {
    state.busy.delete(key);
    if (control?.isConnected) control.disabled = false;
    await poll(true);
  }
}
async function confirm(title, copy, content, confirmText = '确认') {
  const dialog = $('confirm-dialog');
  if (dialog.open) return false;
  $('dialog-title').textContent = title;
  $('dialog-copy').textContent = copy;
  $('dialog-body').replaceChildren();
  if (content) $('dialog-body').append(content);
  $('dialog-confirm').textContent = confirmText;
  dialog.returnValue = '';
  dialog.showModal();
  return new Promise((resolve) =>
    dialog.addEventListener(
      'close',
      () => resolve(dialog.returnValue === 'confirm'),
      { once: true },
    ),
  );
}
function fillConfig(config) {
  $('server-url').value = config.server_url;
  $('name').value = config.name;
  $('data-dir').value = config.data_dir;
  $('concurrency').value = config.concurrency || 3;
  $('upload-rate').value = (config.upload_bytes_per_second || 0) / 1024;
  $('force-relay').checked = config.force_relay;
  $('insecure-local').checked = config.insecure_local;
}
function renderStatus() {
  const s = state.status;
  if (!s) return;
  $('connection-dot').classList.toggle(
    'online',
    s.connected && s.connection_status === '已连接',
  );
  $('connection-label').textContent = s.connected
    ? s.connection_status === '已连接'
      ? '会话已连接'
      : '连接异常'
    : '未连接';
  $('device-name').textContent = s.connected
    ? s.config.name
    : '连接你的协作空间';
  $('device-name').title = s.connection_status || '';
  $('settings-state').textContent = s.connected
    ? s.connection_status || '已建立会话'
    : '未连接';
  $('uuid').textContent = s.uuid || '尚未连接';
  for (const id of [
    'server-url',
    'name',
    'data-dir',
    'force-relay',
    'insecure-local',
    'choose-data',
    'concurrency',
    'upload-rate',
  ])
    $(id).disabled = s.connected || state.busy.has('connect');
  $('connect').hidden = s.connected;
  $('disconnect').hidden = !s.connected;
  $('publish').disabled = !s.connected || state.busy.has('publish');
  $('refresh').disabled = !s.connected || state.syncing;
  $('task-count').textContent = (s.tasks || []).filter(
    (task) =>
      !['complete', 'completed', 'cancelled', 'canceled'].includes(task.state),
  ).length;
  const signature = JSON.stringify(s.tasks || []);
  if (signature !== state.taskSignature) {
    state.taskSignature = signature;
    renderTasks();
  }
}
function renderFiles() {
  const files = filterFiles(state.files, $('search').value);
  $('file-count').textContent = state.files.length;
  $('metric-files').firstChild.textContent = state.files.length;
  $('metric-peers').firstChild.textContent = new Set(
    state.files.flatMap((file) =>
      (file.sources || []).map((source) => source.peer_id),
    ),
  ).size;
  const list = $('file-list');
  list.replaceChildren();
  $('files-empty').hidden = files.length > 0;
  if (!files.length) {
    const connected = state.status?.connected;
    $('files-empty').querySelector('h2').textContent = connected
      ? $('search').value
        ? '没有匹配的文件'
        : '空间里还没有文件'
      : '从连接一台设备开始';
    $('files-empty').querySelector('p').textContent = connected
      ? $('search').value
        ? '试试其他文件名称或完整版本 ID。'
        : '发布一个本地文件，或等待其他设备上线。'
      : '配置协调服务后，即可发现和发布文件。';
    $('files-empty').querySelector('button').hidden = connected;
  }
  for (const file of files) {
    const m = file.manifest,
      counts = shardSources(file);
    const row = el(
      'button',
      `file-row${m.id === state.selected ? ' selected' : ''}`,
    );
    row.type = 'button';
    row.dataset.action = 'select';
    row.dataset.id = m.id;
    row.setAttribute('aria-label', `${m.name}，版本 ${m.id}`);
    row.setAttribute('aria-pressed', String(m.id === state.selected));
    const name = el('div', 'file-name', m.name);
    name.title = m.name;
    name.append(el('span', 'file-version', m.id.slice(0, 14)));
    const shards = el('div');
    const minis = el('div', 'shard-minis');
    for (const count of counts) minis.append(el('i', count ? 'available' : ''));
    shards.append(
      minis,
      el(
        'span',
        'shard-caption',
        `${counts.filter(Boolean).length} / 6 个索引`,
      ),
    );
    row.append(name, el('span', 'file-size', bytes(m.size)), shards);
    list.append(row);
  }
  renderDetail();
}
function renderDetail() {
  const file = currentFile();
  if (!file) {
    $('file-detail').replaceChildren(
      ...emptyDetail.map((node) => node.cloneNode(true)),
    );
    return;
  }
  const m = file.manifest,
    counts = shardSources(file),
    detail = $('file-detail');
  detail.replaceChildren();
  detail.append(
    el('h2', 'detail-title', m.name),
    el(
      'p',
      'detail-meta',
      `${bytes(m.size)} · RS ${m.data_shards} + ${m.parity_shards}`,
    ),
  );
  detail.append(el('div', 'detail-label', '候选分片分布'));
  const grid = el('div', 'shard-grid');
  counts.forEach((count, index) => {
    const tile = el(
      'div',
      `shard-tile${count ? ' available' : ''}`,
      String(index),
    );
    tile.title = `${index} 号分片：${count} 个候选来源`;
    tile.append(el('small', '', `${count} 源`));
    grid.append(tile);
  });
  detail.append(
    grid,
    el(
      'p',
      'fine',
      `发现 ${counts.filter(Boolean).length} 个不同分片索引，恢复需 4 个。`,
    ),
  );
  const actions = el('div', 'detail-actions');
  actions.append(
    button('↓  下载完整文件', 'primary', 'download', m.id),
    button('保留选定分片', 'secondary', 'retain', m.id),
  );
  const local = state.localFiles.find((f) => f.manifest.id === m.id);
  const declared = (file.sources || []).some(
    (source) => source.peer_id === state.status?.uuid,
  );
  if (local?.sharing || declared)
    actions.append(button('停止本机共享', 'danger', 'unshare', m.id));
  for (const b of actions.children)
    b.disabled =
      !state.status?.connected || state.busy.has(`${b.dataset.action}:${m.id}`);
  detail.append(
    actions,
    el('div', 'detail-label', '版本 ID'),
    el('div', 'mono wrap', m.id),
    el('div', 'detail-label', '完整文件 SHA-256'),
    el('div', 'mono wrap', m.sha256),
  );
  if (local)
    detail.append(
      el('div', 'detail-label', '上次检查的本机缓存'),
      el(
        'p',
        'fine',
        `${(local.indices || []).join(', ') || '无有效分片'} · ${bytes(local.bytes)}${local.sharing ? ' · 共享已启用' : ' · 共享已停止'}`,
      ),
    );
  detail.append(el('div', 'detail-label', '来源 / 最近心跳'));
  for (const source of file.sources || []) {
    const node = el(
      'div',
      'source',
      `${source.peer_id === state.status?.uuid ? '本机 · ' : ''}${source.peer_id}`,
    );
    node.append(
      el(
        'small',
        '',
        `索引 ${(source.indices || []).join(', ')} · ${sourceTime(source.last_seen)}`,
      ),
    );
    detail.append(node);
  }
}
function renderTasks() {
  const tasks = state.status?.tasks || [],
    list = $('tasks-list');
  list.replaceChildren();
  $('tasks-empty').hidden = tasks.length > 0;
  $('task-summary').textContent = tasks.length
    ? `${tasks.length} 个任务`
    : '暂无任务';
  for (const task of tasks) {
    const file = state.files.find((f) => f.manifest.id === task.file_id);
    const card = el('article', 'card task-card'),
      top = el('div', 'task-top'),
      title = el('div');
    title.append(
      el(
        'div',
        'task-title',
        file?.manifest.name || task.destination.split(/[\\/]/).pop(),
      ),
      el('div', 'mono', task.file_id.slice(0, 20)),
    );
    const completed = ['complete', 'completed'].includes(task.state);
    top.append(
      title,
      el(
        'span',
        `tag ${completed ? 'success' : task.state === 'failed' ? 'error' : ''}`,
        stateLabel(task.state),
      ),
    );
    const progressRow = el('div', 'progress-row'),
      track = el('div', 'progress-track'),
      progress = el('progress');
    progress.max = 4;
    progress.value = Math.min(new Set(task.completed_shards || []).size, 4);
    progress.setAttribute('aria-label', '已校验分片（至少需要 4 个）');
    track.append(progress);
    progressRow.append(
      track,
      el(
        'span',
        'progress-text',
        `${new Set(task.completed_shards || []).size} 个已校验分片 · 已接收 ${bytes(task.bytes)}`,
      ),
    );
    card.append(top, el('p', 'task-path wrap', task.destination), progressRow);
    if (task.error) card.append(el('div', 'task-error', task.error));
    if (task.transfers?.length) {
      const details = el('details', 'transfer-details');
      details.append(
        el('summary', '', `${task.transfers.length} 条实际传输记录`),
      );
      for (const transfer of task.transfers.slice(-12)) {
        const route =
          transfer.path === 'relay'
            ? 'UDP 中转'
            : transfer.path === 'direct'
              ? 'UDP 直连'
              : transfer.path || '路径未知';
        const row = el(
          'p',
          'fine wrap',
          `分片 ${transfer.index} · ${route} · ${bytes(transfer.bytes)} · ${transfer.milliseconds} ms${transfer.milliseconds > 0 ? ` · 本次平均 ${bytes((transfer.bytes * 1000) / transfer.milliseconds)}/s` : ''} · ${transfer.peer_id}${transfer.error ? ` · ${transfer.error}` : ''}`,
        );
        details.append(row);
      }
      if (task.transfers.length > 12)
        details.append(
          el('p', 'fine', '界面显示最近 12 条；完整记录保留在本机任务数据中。'),
        );
      card.append(details);
    }
    const actions = el('div', 'task-actions');
    for (const action of taskActions(task.state)) {
      const b = button(
        { pause: '暂停', resume: '继续', cancel: '取消', retry: '重试' }[
          action
        ],
        action === 'cancel' ? 'danger' : 'secondary',
        `task-${action}`,
        task.id,
      );
      b.disabled = state.busy.has(`task:${task.id}`);
      actions.append(b);
    }
    card.append(actions);
    list.append(card);
  }
}
async function poll(force = false) {
  if (state.polling) {
    await state.polling;
    if (force) return poll();
    return;
  }
  state.polling = (async () => {
    try {
      const initial = !state.status;
      state.status = await api.status();
      if (initial) {
        fillConfig(state.status.config);
        if (!state.status.connected) view('settings');
      }
      renderStatus();
    } catch (error) {
      notify(`无法读取本机状态：${message(error)}`, true);
    }
  })();
  try {
    await state.polling;
  } finally {
    state.polling = null;
  }
}
async function refreshFiles(showErrors = true) {
  if (!state.status?.connected || state.syncing) return;
  state.syncing = true;
  $('refresh').disabled = true;
  const generation = state.generation;
  try {
    const files = await api.files('');
    if (generation !== state.generation || !state.status?.connected) return;
    const signature = JSON.stringify(files);
    state.files = files || [];
    $('sync-time').textContent =
      `${new Date().toLocaleTimeString('zh-CN', { hour12: false })} 已同步`;
    if (signature !== state.fileSignature) {
      state.fileSignature = signature;
      if (!currentFile()) state.selected = state.files[0]?.manifest.id || null;
      renderFiles();
      renderTasks();
    }
  } catch (error) {
    $('sync-time').textContent = '同步失败 · 显示上次结果';
    if (showErrors) notify(`文件同步失败：${message(error)}`, true);
  } finally {
    state.syncing = false;
    $('refresh').disabled = !state.status?.connected;
  }
}
async function readStorage() {
  const storage = await api.storage();
  $('storage-bytes').textContent = bytes(storage.bytes);
  $('storage-detail').textContent =
    `${bytes(storage.bytes)} · ${storage.files} 个本机数据文件`;
  $('storage-path').textContent = storage.path;
}
async function refreshLocal() {
  if (state.status?.connected) state.localFiles = await api.localFiles();
  renderDetail();
}

$('export-statistics').addEventListener('click', (event) =>
  operation(
    'export',
    async () => {
      const path = await api.exportStatistics();
      if (path)
        notify(
          `诊断统计已导出至 ${path}。其中包含任务路径与设备 UUID，不包含会话凭据。`,
        );
    },
    event.currentTarget,
  ),
);
$('dismiss-notice').addEventListener('click', () => {
  $('notice').hidden = true;
});
$('search').addEventListener('input', renderFiles);
$('refresh').addEventListener('click', () => refreshFiles());
$('read-storage').addEventListener('click', (event) =>
  operation(
    'storage',
    async () => {
      await readStorage();
      await refreshLocal();
    },
    event.currentTarget,
  ),
);
$('choose-data').addEventListener('click', (event) =>
  operation(
    'directory',
    async () => {
      const path = await api.chooseDataDirectory();
      if (path) $('data-dir').value = path;
    },
    event.currentTarget,
  ),
);
$('config-form').addEventListener('submit', (event) => {
  event.preventDefault();
  operation(
    'connect',
    async () => {
      $('connecting-text').textContent = '正在建立会话…';
      try {
        await api.connect({
          server_url: $('server-url').value,
          data_dir: $('data-dir').value,
          name: $('name').value,
          force_relay: $('force-relay').checked,
          insecure_local: $('insecure-local').checked,
          concurrency: Number($('concurrency').value),
          upload_bytes_per_second: Number($('upload-rate').value) * 1024,
        });
        state.generation++;
        await poll(true);
        fillConfig(state.status.config);
        await refreshFiles();
        await readStorage();
        view('files');
        notify('已连接。设备会话已建立，可发布文件或创建下载任务。');
      } finally {
        $('connecting-text').textContent = '';
      }
    },
    $('connect'),
  );
});
$('disconnect').addEventListener('click', (event) =>
  operation(
    'disconnect',
    async () => {
      if (
        !(await confirm(
          '断开当前设备？',
          '下载与共享会停止。重新连接后可继续任务，完整分片会保留。',
          null,
          '断开连接',
        ))
      )
        return;
      state.generation++;
      await api.disconnect();
      state.files = [];
      state.localFiles = [];
      state.selected = null;
      state.fileSignature = '';
      await poll(true);
      renderFiles();
      $('sync-time').textContent = '连接后同步文件';
      notify('已断开。已校验的完整分片保留在本机。');
    },
    event.currentTarget,
  ),
);
$('publish').addEventListener('click', (event) =>
  operation(
    'publish',
    async () => {
      const path = await api.choosePublishFile();
      if (!path) return;
      notify('正在编码、校验并发布文件，请稍候…');
      const manifest = await api.publish(path);
      await refreshFiles();
      state.selected = manifest.id;
      renderFiles();
      await readStorage();
      notify(
        `已发布“${manifest.name}”。当前分片仍在本机，请在其他设备保留分片以建立多节点分布。`,
      );
    },
    event.currentTarget,
  ),
);
document.addEventListener('click', (event) => {
  const navigation = event.target.closest('[data-view]');
  if (navigation) {
    view(navigation.dataset.view);
    return;
  }
  const control = event.target.closest('[data-action]');
  if (!control || control.disabled) return;
  const { action, id } = control.dataset;
  if (action === 'select') {
    state.selected = id;
    renderFiles();
    return;
  }
  if (action.startsWith('task-')) {
    operation(
      `task:${id}`,
      async () => {
        if (
          action === 'task-cancel' &&
          !(await confirm(
            '取消这个下载？',
            '停止接收新数据，保留已校验分片，可稍后重试。',
            null,
            '取消下载',
          ))
        )
          return;
        await api.taskAction(id, action.slice(5));
        state.taskSignature = '';
      },
      control,
    );
    return;
  }
  operation(
    `${action}:${id}`,
    async () => {
      const file = state.files.find((f) => f.manifest.id === id);
      if (!file) throw new Error('文件列表已变化，请刷新后重试');
      if (action === 'download') {
        const destination = await api.chooseDestination(file.manifest.name);
        if (!destination) return;
        await api.download(id, destination);
        await poll(true);
        view('tasks');
        notify('下载任务已创建。正在发现来源并校验已有分片。');
      } else if (action === 'retain') {
        const options = el('div', 'shard-options');
        for (let index = 0; index < 6; index++) {
          const label = el('label');
          const input = el('input');
          input.type = 'checkbox';
          input.value = index;
          label.append(input, document.createTextNode(` 分片 ${index}`));
          options.append(label);
        }
        if (
          !(await confirm(
            '保留选定分片',
            '接收并校验后，本机将向协作空间提供这些分片。请选择至少一个索引。',
            options,
            '接收并共享',
          ))
        )
          return;
        const indices = [...options.querySelectorAll('input:checked')].map(
          (input) => Number(input.value),
        );
        if (!indices.length) throw new Error('请至少选择一个分片');
        notify('正在接收并校验选定分片，请保持连接…');
        await api.retain(id, indices);
        await refreshFiles();
        await readStorage();
        await refreshLocal();
        notify('选定分片已保留并上报。');
      } else if (action === 'unshare') {
        const label = el('label', 'checkbox'),
          input = el('input');
        input.type = 'checkbox';
        label.append(
          input,
          document.createTextNode('同时删除本机相关分片缓存（不删除原文件）'),
        );
        if (
          !(await confirm(
            '停止本机共享',
            '撤销本机的持有声明；其他设备已获得的文件不会被删除。',
            label,
            '停止共享',
          ))
        )
          return;
        await api.unshare(id, input.checked);
        await refreshFiles();
        await readStorage();
        await refreshLocal();
        notify('已停止本机共享。');
      }
    },
    control,
  );
});
await poll(true);
await readStorage().catch((error) =>
  notify(`数据目录统计失败：${message(error)}`, true),
);
setInterval(poll, 1500);
setInterval(() => refreshFiles(false), 10000);
