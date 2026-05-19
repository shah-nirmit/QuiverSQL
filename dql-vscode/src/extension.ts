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

    let executeCommand = vscode.commands.registerCommand('dql.executeQuery', async () => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        const sql = await vscode.window.showInputBox({
            prompt: 'Enter SQL Query',
            placeHolder: 'SELECT 1 as num, \'test\' as str'
        });

        if (!sql) return;

        try {
            const start = Date.now();
            const result = await daemonClient.sendRequest('execute', sql);
            const duration = Date.now() - start;
            
            // Show result in an output channel instead of a toast because it could be large
            const outputChannel = vscode.window.createOutputChannel('DQL Results');
            outputChannel.appendLine(`-- Executed in ${duration}ms`);
            outputChannel.appendLine(sql);
            outputChannel.appendLine(result);
            outputChannel.show();

        } catch (e: any) {
            vscode.window.showErrorMessage(`Query failed: ${e.message || JSON.stringify(e)}`);
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

    context.subscriptions.push(pingCommand);
    context.subscriptions.push(executeCommand);
    context.subscriptions.push(attachFileCommand);
    context.subscriptions.push({ dispose: () => daemonClient?.stop() });
}

export function deactivate() {
    if (daemonClient) {
        daemonClient.stop();
    }
}
