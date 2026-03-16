const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("electronAPI", {
  getVersion: () => process.versions.electron,
  getPlatform: () => process.platform,
  onBackendReady: (callback) => ipcRenderer.on("backend-ready", callback),
});
