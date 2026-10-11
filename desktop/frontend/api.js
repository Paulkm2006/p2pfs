// Wails v3's bundled runtime is served by application.BundledAssetFileServer.
// Official named binding format: <package>.<service>.<method>.
import { Call } from '/wails/runtime.js';
const invoke = (method, ...args) =>
  Call.ByName(`main.DesktopService.${method}`, ...args);
export const api = {
  status: () => invoke('Status'),
  connect: (config) => invoke('Connect', config),
  disconnect: () => invoke('Disconnect'),
  files: (query) => invoke('Files', query),
  publish: (path) => invoke('Publish', path),
  download: (id, path) => invoke('Download', id, path),
  taskAction: (id, action) => invoke('TaskAction', id, action),
  retain: (id, indices) => invoke('Retain', id, indices),
  unshare: (id, deleteCache) => invoke('Unshare', id, deleteCache),
  storage: () => invoke('Storage'),
  localFiles: () => invoke('LocalFiles'),
  exportStatistics: () => invoke('ExportStatistics'),
  choosePublishFile: () => invoke('ChoosePublishFile'),
  chooseDataDirectory: () => invoke('ChooseDataDirectory'),
  chooseDestination: (name) => invoke('ChooseDestination', name),
};
