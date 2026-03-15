import * as vscode from 'vscode';
import { exec } from 'child_process';

export function activate(context: vscode.ExtensionContext) {
    console.log('Catalog Clone extension activated');

    // Register commands
    const openPanel = vscode.commands.registerCommand('dbxClone.openPanel', () => {
        CatalogClonePanel.createOrShow(context.extensionUri);
    });

    const runClone = vscode.commands.registerCommand('dbxClone.runClone', () => {
        runCloneCommand('clone');
    });

    const runDiff = vscode.commands.registerCommand('dbxClone.runDiff', () => {
        runCloneCommand('diff');
    });

    const runPreflight = vscode.commands.registerCommand('dbxClone.runPreflight', () => {
        runCloneCommand('preflight');
    });

    const runPiiScan = vscode.commands.registerCommand('dbxClone.runPiiScan', () => {
        runCloneCommand('pii-scan');
    });

    const costEstimate = vscode.commands.registerCommand('dbxClone.costEstimate', () => {
        runCloneCommand('cost-estimate');
    });

    context.subscriptions.push(openPanel, runClone, runDiff, runPreflight, runPiiScan, costEstimate);
}

function runCloneCommand(subcommand: string) {
    const config = vscode.workspace.getConfiguration('dbxClone');
    const configPath = config.get<string>('configPath', 'config/clone_config.yaml');
    const pythonPath = config.get<string>('pythonPath', 'python3');

    const terminal = vscode.window.createTerminal('Catalog Clone');
    terminal.show();
    terminal.sendText(`${pythonPath} -m src.main ${subcommand} --config ${configPath}`);
}

class CatalogClonePanel {
    public static currentPanel: CatalogClonePanel | undefined;
    private readonly _panel: vscode.WebviewPanel;

    public static createOrShow(extensionUri: vscode.Uri) {
        const column = vscode.window.activeTextEditor
            ? vscode.window.activeTextEditor.viewColumn
            : undefined;

        if (CatalogClonePanel.currentPanel) {
            CatalogClonePanel.currentPanel._panel.reveal(column);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'catalogClone',
            'Catalog Clone',
            column || vscode.ViewColumn.One,
            { enableScripts: true }
        );

        CatalogClonePanel.currentPanel = new CatalogClonePanel(panel);
    }

    private constructor(panel: vscode.WebviewPanel) {
        this._panel = panel;
        this._panel.webview.html = this._getHtmlContent();

        this._panel.onDidDispose(() => {
            CatalogClonePanel.currentPanel = undefined;
        });

        this._panel.webview.onDidReceiveMessage(message => {
            switch (message.command) {
                case 'clone':
                    runCloneCommand(`clone --source ${message.source} --dest ${message.dest}`);
                    break;
                case 'diff':
                    runCloneCommand(`diff --source ${message.source} --dest ${message.dest}`);
                    break;
                case 'preflight':
                    runCloneCommand('preflight');
                    break;
            }
        });
    }

    private _getHtmlContent(): string {
        return `<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: var(--vscode-font-family); padding: 20px; color: var(--vscode-foreground); }
        h1 { color: var(--vscode-textLink-foreground); }
        .form-group { margin: 10px 0; }
        label { display: block; margin-bottom: 4px; font-weight: bold; }
        input, select { width: 100%; padding: 6px; border: 1px solid var(--vscode-input-border);
            background: var(--vscode-input-background); color: var(--vscode-input-foreground); }
        button { padding: 8px 16px; margin: 4px; cursor: pointer;
            background: var(--vscode-button-background); color: var(--vscode-button-foreground);
            border: none; }
        button:hover { background: var(--vscode-button-hoverBackground); }
        .actions { margin-top: 20px; }
    </style>
</head>
<body>
    <h1>Catalog Clone Utility</h1>

    <div class="form-group">
        <label>Source Catalog</label>
        <input type="text" id="source" placeholder="source_catalog" />
    </div>

    <div class="form-group">
        <label>Destination Catalog</label>
        <input type="text" id="dest" placeholder="dest_catalog" />
    </div>

    <div class="form-group">
        <label>Clone Type</label>
        <select id="cloneType">
            <option value="DEEP">DEEP</option>
            <option value="SHALLOW">SHALLOW</option>
        </select>
    </div>

    <div class="form-group">
        <label><input type="checkbox" id="dryRun" checked /> Dry Run</label>
    </div>

    <div class="actions">
        <button onclick="runCommand('clone')">Clone</button>
        <button onclick="runCommand('diff')">Diff</button>
        <button onclick="runCommand('preflight')">Preflight</button>
    </div>

    <script>
        const vscode = acquireVsCodeApi();

        function runCommand(cmd) {
            const source = document.getElementById('source').value;
            const dest = document.getElementById('dest').value;
            vscode.postMessage({ command: cmd, source, dest });
        }
    </script>
</body>
</html>`;
    }
}

export function deactivate() {}
