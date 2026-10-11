export function bytes(value) {
  if (!Number.isFinite(Number(value)) || Number(value) < 0) return '—';
  let n = Number(value),
    unit = 0;
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  while (n >= 1024 && unit < units.length - 1) {
    n /= 1024;
    unit++;
  }
  return `${unit ? n.toFixed(n < 10 ? 1 : 0) : n} ${units[unit]}`;
}
export function shardSources(file) {
  return Array.from(
    { length: 6 },
    (_, index) =>
      new Set(
        (file.sources || [])
          .filter((source) => (source.indices || []).includes(index))
          .map((source) => source.peer_id),
      ).size,
  );
}
export const taskLabels = {
  queued: '准备中',
  running: '正在下载',
  downloading: '正在下载',
  paused: '已暂停',
  waiting: '等待来源',
  waiting_sources: '等待来源',
  waiting_for_sources: '等待来源',
  reconstructing: '正在恢复文件',
  verifying: '正在校验',
  complete: '已完成',
  completed: '已完成',
  failed: '下载失败',
  error: '下载失败',
  cancelled: '已取消',
  canceled: '已取消',
};
export function stateLabel(state) {
  return taskLabels[state] || state || '未知状态';
}
export function taskActions(state) {
  if (['complete', 'completed'].includes(state)) return [];
  if (['cancelled', 'canceled', 'failed', 'error'].includes(state))
    return ['retry'];
  if (state === 'paused') return ['resume', 'cancel'];
  return ['pause', 'cancel'];
}
export function filterFiles(files, query) {
  const q = query.trim().toLowerCase();
  return files.filter(
    (file) =>
      file.manifest.name.toLowerCase().includes(q) ||
      file.manifest.id.toLowerCase().includes(q),
  );
}
export function sourceTime(value) {
  const date = new Date(Number(value) * 1000);
  return Number.isNaN(date.getTime())
    ? '更新时间未知'
    : date.toLocaleString('zh-CN', { hour12: false });
}
