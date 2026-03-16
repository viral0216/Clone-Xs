---
sidebar_position: 17
---

# Desktop App

Clone-Xs includes a native desktop application built with Electron. It wraps the Web UI and auto-manages the Python backend — no terminal required.

## Supported Platforms

| Platform | Output |
|----------|--------|
| macOS (arm64) | `.app` bundle + `.zip` |
| Windows | NSIS installer + portable `.exe` |

## Quick Start (Development)

```bash
# Install Electron dependencies (one-time)
make desktop-install

# Launch the desktop app
make desktop-dev
```

The app will:
1. Find your Python installation automatically
2. Start the FastAPI backend on port 8000
3. Open the Web UI in a native window
4. Stop the backend when you close the app

## Building for Distribution

### macOS

```bash
make build-desktop-mac
```

Output: `desktop/dist/Clone-Xs-1.0.0-arm64-mac.zip`

### Windows

```bash
make build-desktop-win
```

Output:
- `desktop/dist/Clone-Xs-Setup-1.0.0.exe` (installer)
- `desktop/dist/Clone-Xs-1.0.0.exe` (portable)

To cross-compile Windows from macOS, install Wine first:

```bash
brew install --cask wine-stable
make build-desktop-win
```

## How It Works

The desktop app is a thin Electron wrapper around the existing web stack:

```
desktop/main.js  →  Spawns Python backend (uvicorn)
                 →  Waits for /api/health to respond
                 →  Creates BrowserWindow
                 →  Loads ui/dist/index.html (or dev server)
                 →  Kills backend on app exit
```

No changes are needed to the `ui/` or `api/` code. The same frontend and backend run identically in web and desktop modes.

## Project Structure

```
desktop/
  main.js        # Electron main process (backend lifecycle + window)
  preload.js     # Context bridge (platform info)
  package.json   # Electron + electron-builder config
  build/         # App icons (icon.png, icon.icns, icon.ico)
  dist/          # Build output (created by electron-builder)
```

## Custom Icons

Replace the placeholder icons in `desktop/build/` with your own:

- `icon.png` — 256x256 or 512x512 PNG
- `icon.icns` — macOS icon (generate with `iconutil`)
- `icon.ico` — Windows icon (256x256)
