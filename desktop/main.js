const { app, BrowserWindow, shell } = require("electron");
const path = require("path");
const { spawn, execSync } = require("child_process");
const http = require("http");
const fs = require("fs");

let mainWindow = null;
let backendProcess = null;

const BACKEND_PORT = 8000;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;
const PROJECT_ROOT = path.resolve(__dirname, "..");

// ---------------------------------------------------------------------------
// Python & Backend
// ---------------------------------------------------------------------------

function findPython() {
  const candidates = [
    "python3",
    "/opt/homebrew/bin/python3",
    "/usr/local/bin/python3",
    "/usr/bin/python3",
    "python",
  ];
  for (const cmd of candidates) {
    try {
      execSync(`${cmd} --version`, { stdio: "ignore" });
      return cmd;
    } catch {
      // not found, try next
    }
  }
  return null;
}

function isBackendRunning() {
  return new Promise((resolve) => {
    const req = http.get(`${BACKEND_URL}/api/health`, (res) => {
      resolve(res.statusCode === 200);
    });
    req.on("error", () => resolve(false));
    req.setTimeout(1000, () => {
      req.destroy();
      resolve(false);
    });
  });
}

async function startBackend() {
  const alreadyRunning = await isBackendRunning();
  if (alreadyRunning) {
    console.log("Backend already running on port", BACKEND_PORT);
    return;
  }

  const python = findPython();
  if (!python) {
    console.error("Python not found. Please install Python 3.11+.");
    app.quit();
    return;
  }

  console.log(`Starting backend with ${python}...`);

  backendProcess = spawn(
    python,
    ["-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", String(BACKEND_PORT)],
    {
      cwd: PROJECT_ROOT,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, PYTHONUNBUFFERED: "1" },
    }
  );

  backendProcess.stdout.on("data", (data) => {
    console.log(`[backend] ${data.toString().trim()}`);
  });

  backendProcess.stderr.on("data", (data) => {
    console.log(`[backend] ${data.toString().trim()}`);
  });

  backendProcess.on("error", (err) => {
    console.error("Failed to start backend:", err.message);
  });

  backendProcess.on("exit", (code) => {
    console.log(`Backend exited with code ${code}`);
    backendProcess = null;
  });

  // Wait for backend to be ready (up to 30s)
  for (let i = 0; i < 60; i++) {
    await new Promise((r) => setTimeout(r, 500));
    const ready = await isBackendRunning();
    if (ready) {
      console.log("Backend is ready.");
      return;
    }
  }
  console.error("Backend failed to start within 30 seconds.");
}

function stopBackend() {
  if (backendProcess) {
    console.log("Stopping backend...");
    backendProcess.kill("SIGTERM");
    // Force kill after 3 seconds
    setTimeout(() => {
      if (backendProcess) {
        backendProcess.kill("SIGKILL");
      }
    }, 3000);
    backendProcess = null;
  }
}

// ---------------------------------------------------------------------------
// Window
// ---------------------------------------------------------------------------

function getFrontendPath() {
  // 1. Packaged app: resources/frontend/
  const packagedPath = path.join(process.resourcesPath || "", "frontend", "index.html");
  if (fs.existsSync(packagedPath)) {
    return { type: "file", path: packagedPath };
  }

  // 2. Local build: ../ui/dist/
  const localBuild = path.join(PROJECT_ROOT, "ui", "dist", "index.html");
  if (fs.existsSync(localBuild)) {
    return { type: "file", path: localBuild };
  }

  // 3. Dev server fallback
  return { type: "url", path: "http://localhost:3000" };
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 900,
    minHeight: 600,
    title: "Clone-Xs",
    titleBarStyle: "hiddenInset",
    backgroundColor: "#0d1117",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  const frontend = getFrontendPath();

  if (frontend.type === "file") {
    console.log("Loading frontend from:", frontend.path);
    mainWindow.loadFile(frontend.path);
  } else {
    console.log("Loading frontend from dev server:", frontend.path);
    mainWindow.loadURL(frontend.path);
    mainWindow.webContents.openDevTools();
  }

  // Open external links in system browser
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith("http://") || url.startsWith("https://")) {
      shell.openExternal(url);
      return { action: "deny" };
    }
    return { action: "allow" };
  });

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

// ---------------------------------------------------------------------------
// App Lifecycle
// ---------------------------------------------------------------------------

app.whenReady().then(async () => {
  await startBackend();
  createWindow();

  // Notify renderer that backend is ready
  if (mainWindow) {
    mainWindow.webContents.on("did-finish-load", () => {
      mainWindow.webContents.send("backend-ready");
    });
  }

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("will-quit", () => {
  stopBackend();
});

app.on("quit", () => {
  stopBackend();
});
