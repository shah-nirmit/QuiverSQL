import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';

let daemonClient: DaemonClient | undefined;

export async function activate(context: vscode.ExtensionContext) {
    console.log('Activating DQL Developer Tools...');

    daemonClient = new DaemonClient(context);
    try {
        await daemonClient.start();
        vscode.window.showInformationMessage('DQL Daemon started successfully.');
    } catch (e) {
        console.error('Failed to start DQL Daemon', e);
    }

    let pingCommand = vscode.commands.registerCommand('dql.pingDaemon', async () => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        try {
            const start = Date.now();
            const result = await daemonClient.sendRequest('ping');
            const duration = Date.now() - start;
            vscode.window.showInformationMessage(`Daemon responded with: ${result} (took ${duration}ms)`);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Daemon request failed: ${e.message || JSON.stringify(e)}`);
        }
    });

    let executeCommandUI = vscode.commands.registerCommand('dql.executeQueryUI', async () => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        let sql = '';
        const editor = vscode.window.activeTextEditor;
        if (editor) {
            const selection = editor.selection;
            if (!selection.isEmpty) {
                sql = editor.document.getText(selection);
            } else {
                sql = editor.document.getText();
            }
        }

        if (!sql.trim()) {
            const input = await vscode.window.showInputBox({
                prompt: 'Enter SQL Query (UI Grid)',
                placeHolder: 'SELECT * FROM employees LIMIT 100'
            });
            if (!input) return;
            sql = input;
        }

        const start = Date.now();
        try {
            const result = await daemonClient.sendRequest('execute_json', sql);
            const duration = Date.now() - start;
            
            // Show result in the rich Webview Data Grid
            const { ResultGridPanel } = await import('./webviewPanel');
            ResultGridPanel.createOrShow(context.extensionUri);
            if (ResultGridPanel.currentPanel) {
                ResultGridPanel.currentPanel.updateData(result as any[], duration);
            }

        } catch (e: any) {
            const duration = Date.now() - start;
            const { ResultGridPanel } = await import('./webviewPanel');
            ResultGridPanel.createOrShow(context.extensionUri);
            if (ResultGridPanel.currentPanel) {
                ResultGridPanel.currentPanel.updateError(e.message || JSON.stringify(e), duration);
            }
        }
    });

    let attachFileCommand = vscode.commands.registerCommand('dql.attachFile', async () => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        const fileUri = await vscode.window.showOpenDialog({
            canSelectMany: false,
            openLabel: 'Attach as Table',
            filters: {
                'Data Files': ['csv', 'json', 'parquet', 'ndjson']
            }
        });

        if (!fileUri || fileUri.length === 0) return;
        const filePath = fileUri[0].fsPath;

        let format = 'csv';
        if (filePath.endsWith('.parquet')) format = 'parquet';
        if (filePath.endsWith('.json') || filePath.endsWith('.ndjson')) format = 'json';

        const tableName = await vscode.window.showInputBox({
            prompt: 'Enter Table Name for this file',
            placeHolder: 'my_table'
        });

        if (!tableName) return;

        try {
            const result = await daemonClient.sendRequest('register_file', {
                table_name: tableName,
                path: filePath,
                format: format
            });
            vscode.window.showInformationMessage(result);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to attach file: ${e.message || JSON.stringify(e)}`);
        }
    });

    // CodeLens Provider to make it feel like ossdbtools
    const codeLensProvider = vscode.languages.registerCodeLensProvider('sql', {
        provideCodeLenses(_document: vscode.TextDocument, _token: vscode.CancellationToken) {
            // Provide a CodeLens at the top of the file
            const range = new vscode.Range(0, 0, 0, 0);
            const lens = new vscode.CodeLens(range, {
                title: "▶ Run Query",
                tooltip: "Execute this SQL query using DQL",
                command: "dql.executeQueryUI"
            });
            return [lens];
        }
    });

    context.subscriptions.push(pingCommand);
    context.subscriptions.push(executeCommandUI);
    context.subscriptions.push(attachFileCommand);
    context.subscriptions.push(codeLensProvider);
    context.subscriptions.push({ dispose: () => daemonClient?.stop() });
}

export function deactivate() {
    if (daemonClient) {
        daemonClient.stop();
    }
}
