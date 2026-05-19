import * as vscode from 'vscode';

export class ResultGridPanel {
    public static currentPanel: ResultGridPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel) {
        this._panel = panel;
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
    }

    public static createOrShow(extensionUri: vscode.Uri) {
        if (ResultGridPanel.currentPanel) {
            ResultGridPanel.currentPanel._panel.reveal(vscode.ViewColumn.Beside, true);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'dqlResultGrid',
            'Results',
            { viewColumn: vscode.ViewColumn.Beside, preserveFocus: true },
            {
                enableScripts: true,
                retainContextWhenHidden: true,
            }
        );

        ResultGridPanel.currentPanel = new ResultGridPanel(panel);
    }

    public updateData(data: any[], durationMs: number) {
        if (data.length === 0) {
            this._panel.webview.html = this._getHtmlForWebview('No rows affected.', durationMs);
            return;
        }

        const keys = Object.keys(data[0]);
        const columnDefs = keys.map(k => ({ field: k, sortable: true, filter: true, resizable: true }));

        this._panel.webview.html = this._getAgGridHtml(columnDefs, data, durationMs);
    }

    public updateError(errorMsg: string, durationMs: number) {
        this._panel.webview.html = this._getErrorHtml(errorMsg, durationMs);
    }

    public dispose() {
        ResultGridPanel.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _getHtmlForWebview(message: string, durationMs: number) {
        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Results</title>
            <style>
                body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 10px; color: var(--vscode-editor-foreground); background: var(--vscode-editor-background); }
                .message { color: #cccccc; font-size: 13px; margin-bottom: 5px; }
            </style>
        </head>
        <body>
            <div class="message">${message}</div>
            <div class="message"><br/>Total execution time: 00:00:00.${durationMs.toString().padStart(3, '0')}</div>
        </body>
        </html>`;
    }

    private _getAgGridHtml(columnDefs: any[], rowData: any[], durationMs: number) {
        let tableHeaders = '<th></th>'; // Row number column
        for (const col of columnDefs) {
            tableHeaders += `<th>${col.field}</th>`;
        }

        let tableRows = '';
        for (let i = 0; i < rowData.length; i++) {
            const row = rowData[i];
            tableRows += `<tr>`;
            tableRows += `<td class="row-num">${i + 1}</td>`;
            for (const col of columnDefs) {
                const val = row[col.field];
                tableRows += `<td>${val !== null && val !== undefined ? val : '<em>null</em>'}</td>`;
            }
            tableRows += `</tr>`;
        }

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Results</title>
            <style>
                body { 
                    margin: 0; 
                    padding: 0; 
                    height: 100vh; 
                    display: flex; 
                    flex-direction: column;
                    background-color: var(--vscode-editor-background);
                    color: var(--vscode-editor-foreground);
                    font-family: var(--vscode-editor-font-family, 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif);
                    font-size: 13px;
                }
                .tabs {
                    display: flex;
                    background-color: var(--vscode-editor-background);
                    border-bottom: 1px solid var(--vscode-panel-border);
                }
                .tab {
                    padding: 8px 12px;
                    font-size: 12px;
                    text-transform: uppercase;
                    cursor: pointer;
                    color: var(--vscode-tab-inactiveForeground);
                    border-bottom: 1px solid transparent;
                    user-select: none;
                }
                .tab.active {
                    color: var(--vscode-tab-activeForeground);
                    border-bottom: 1px solid var(--vscode-tab-activeForeground);
                }
                .tab-content {
                    flex-grow: 1;
                    display: none;
                    overflow: auto;
                }
                .tab-content.active {
                    display: block;
                }
                table {
                    border-collapse: collapse;
                    width: 100%;
                    min-width: 600px;
                }
                th {
                    position: sticky;
                    top: 0;
                    background: var(--vscode-editorWidget-background);
                    color: var(--vscode-editor-foreground);
                    font-weight: 600;
                    text-align: left;
                    padding: 4px 8px;
                    border: 1px solid var(--vscode-panel-border);
                    border-top: none;
                    z-index: 10;
                    box-shadow: 0 1px 0 var(--vscode-panel-border);
                }
                td {
                    padding: 4px 8px;
                    border: 1px solid var(--vscode-panel-border);
                    white-space: pre-wrap;
                    word-break: break-word;
                }
                .row-num {
                    background: var(--vscode-editorWidget-background);
                    color: var(--vscode-descriptionForeground);
                    text-align: right;
                    width: 30px;
                    user-select: none;
                }
                tr:hover td {
                    background-color: var(--vscode-list-hoverBackground);
                }
                .messages-view {
                    padding: 10px;
                    font-family: var(--vscode-editor-font-family, 'Consolas', monospace);
                    font-size: 13px;
                    color: var(--vscode-editor-foreground);
                }
                em {
                    color: var(--vscode-descriptionForeground);
                    font-style: italic;
                }
            </style>
        </head>
        <body>
            <div class="tabs">
                <div class="tab active" onclick="switchTab('results', this)">Results</div>
                <div class="tab" onclick="switchTab('messages', this)">Messages</div>
            </div>

            <div id="results" class="tab-content active">
                <table>
                    <thead>
                        <tr>${tableHeaders}</tr>
                    </thead>
                    <tbody>
                        ${tableRows}
                    </tbody>
                </table>
            </div>
            
            <div id="messages" class="tab-content">
                <div class="messages-view">
                    (${rowData.length} row(s) affected)<br/><br/>
                    Total execution time: 00:00:00.${durationMs.toString().padStart(3, '0')}
                </div>
            </div>

            <script>
                function switchTab(tabId, element) {
                    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
                    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                    document.getElementById(tabId).classList.add('active');
                    if (element) {
                        element.classList.add('active');
                    }
                }
            </script>
        </body>
        </html>`;
    }

    private _getErrorHtml(errorMsg: string, durationMs: number) {
        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Results</title>
            <style>
                body { 
                    margin: 0; 
                    padding: 0; 
                    height: 100vh; 
                    display: flex; 
                    flex-direction: column;
                    background-color: var(--vscode-editor-background);
                    color: var(--vscode-editor-foreground);
                    font-family: var(--vscode-editor-font-family, 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif);
                    font-size: 13px;
                }
                .tabs {
                    display: flex;
                    background-color: var(--vscode-editor-background);
                    border-bottom: 1px solid var(--vscode-panel-border);
                }
                .tab {
                    padding: 8px 12px;
                    font-size: 12px;
                    text-transform: uppercase;
                    cursor: pointer;
                    color: var(--vscode-tab-inactiveForeground);
                    border-bottom: 1px solid transparent;
                    user-select: none;
                }
                .tab.active {
                    color: var(--vscode-tab-activeForeground);
                    border-bottom: 1px solid var(--vscode-tab-activeForeground);
                }
                .tab-content {
                    flex-grow: 1;
                    display: none;
                    overflow: auto;
                }
                .tab-content.active {
                    display: block;
                }
                .messages-view {
                    padding: 10px;
                    font-family: var(--vscode-editor-font-family, 'Consolas', monospace);
                    font-size: 13px;
                }
                .error-text {
                    color: var(--vscode-errorForeground, #f48771);
                }
                .duration-text {
                    color: var(--vscode-editor-foreground);
                }
            </style>
        </head>
        <body>
            <div class="tabs">
                <div class="tab" onclick="switchTab('results', this)">Results</div>
                <div class="tab active" onclick="switchTab('messages', this)">Messages</div>
            </div>

            <div id="results" class="tab-content">
                <div style="padding: 10px; color: var(--vscode-descriptionForeground);">No results due to error.</div>
            </div>
            
            <div id="messages" class="tab-content active">
                <div class="messages-view">
                    <span class="error-text">Msg 1, Level 16, State 1, Line 1<br/>${errorMsg.replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\n/g, '<br/>')}</span><br/><br/>
                    <span class="duration-text">Total execution time: 00:00:00.${durationMs.toString().padStart(3, '0')}</span>
                </div>
            </div>

            <script>
                function switchTab(tabId, element) {
                    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
                    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                    document.getElementById(tabId).classList.add('active');
                    if (element) {
                        element.classList.add('active');
                    }
                }
            </script>
        </body>
        </html>`;
    }
}
