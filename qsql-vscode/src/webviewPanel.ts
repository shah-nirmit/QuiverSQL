import * as vscode from 'vscode';
import { QueryPage } from './models';

export class ResultGridPanel {
    public static currentPanel: ResultGridPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private _disposables: vscode.Disposable[] = [];
    private _onNextPage?: (queryId: string, pageIndex: number, pageSize: number) => void | Promise<void>;
    private _onCancel?: (queryId: string) => void | Promise<void>;

    private constructor(panel: vscode.WebviewPanel) {
        this._panel = panel;
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
        this._panel.webview.onDidReceiveMessage(message => {
            if (!message || typeof message !== 'object') {
                return;
            }

            if (
                message.type === 'nextPage' &&
                typeof message.queryId === 'string' &&
                typeof message.pageIndex === 'number' &&
                typeof message.pageSize === 'number'
            ) {
                void this._onNextPage?.(message.queryId, message.pageIndex, message.pageSize);
            }

            if (message.type === 'cancelQuery' && typeof message.queryId === 'string') {
                void this._onCancel?.(message.queryId);
            }
        }, null, this._disposables);
    }

    public static createOrShow(_extensionUri: vscode.Uri) {
        if (ResultGridPanel.currentPanel) {
            ResultGridPanel.currentPanel._panel.reveal(vscode.ViewColumn.Beside, true);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'qsqlResultGrid',
            'Results',
            { viewColumn: vscode.ViewColumn.Beside, preserveFocus: true },
            {
                enableScripts: true,
                retainContextWhenHidden: true,
            }
        );

        ResultGridPanel.currentPanel = new ResultGridPanel(panel);
    }

    public setPagingHandlers(handlers: {
        onNextPage?: (queryId: string, pageIndex: number, pageSize: number) => void | Promise<void>;
        onCancel?: (queryId: string) => void | Promise<void>;
    }) {
        this._onNextPage = handlers.onNextPage;
        this._onCancel = handlers.onCancel;
    }

    public updateLoading(message: string) {
        this._panel.webview.html = this._getHtmlForWebview(message, 0);
    }

    public updateData(data: any[], durationMs: number) {
        if (data.length === 0) {
            this._panel.webview.html = this._getHtmlForWebview('No rows affected.', durationMs);
            return;
        }

        const keys = Object.keys(data[0]);
        const page: QueryPage = {
            query_id: 'compat',
            schema: {
                fields: keys.map(k => ({ name: k, data_type: 'unknown', nullable: true }))
            },
            page_index: 0,
            page_size: data.length,
            is_last: true,
            data,
            metrics: {
                planning_time_ms: 0,
                execution_time_ms: durationMs,
                first_page_time_ms: durationMs,
                rows_produced: data.length,
                rows_returned: data.length
            }
        };

        this.updatePage(page, durationMs);
    }

    public updatePage(page: QueryPage, durationMs: number) {
        this._panel.webview.html = renderQueryPageHtml(page, durationMs);
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
        const safeMessage = escapeHtml(message);
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
            <div class="message">${safeMessage}</div>
            <div class="message"><br/>Total execution time: 00:00:00.${durationMs.toString().padStart(3, '0')}</div>
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
                    <span class="error-text">Msg 1, Level 16, State 1, Line 1<br/>${formatErrorMessage(errorMsg)}</span><br/><br/>
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

export function formatCellValue(value: any): string {
    if (value === null || value === undefined) {
        return '<em>null</em>';
    }

    if (typeof value === 'object') {
        return escapeHtml(JSON.stringify(value));
    }

    return escapeHtml(String(value));
}

export function getQueryPageColumns(page: QueryPage): string[] {
    if (page.schema.fields.length > 0) {
        return page.schema.fields.map(field => field.name);
    }

    return page.data.length > 0 ? Object.keys(page.data[0]) : [];
}

export function renderQueryPageHtml(page: QueryPage, durationMs: number): string {
    const columns = getQueryPageColumns(page);
    const tableHeaders = ['<th></th>']
        .concat(columns.map(col => `<th>${escapeHtml(col)}</th>`))
        .join('');
    const startRowNumber = page.page_index * page.page_size;
    const tableRows = page.data.map((row, index) => {
        const cells = columns
            .map(col => `<td>${formatCellValue(row[col])}</td>`)
            .join('');
        return `<tr><td class="row-num">${startRowNumber + index + 1}</td>${cells}</tr>`;
    }).join('');
    const emptyState = page.data.length === 0
        ? '<div class="empty-state">No rows returned.</div>'
        : '';
    const warning = page.warning
        ? `<div class="warning">${escapeHtml(page.warning)}</div>`
        : '';
    const nextDisabled = page.is_last ? ' disabled' : '';

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
            .tabs, .toolbar {
                display: flex;
                align-items: center;
                gap: 8px;
                background-color: var(--vscode-editor-background);
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            .toolbar {
                padding: 6px 8px;
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
            button {
                color: var(--vscode-button-foreground);
                background: var(--vscode-button-background);
                border: 0;
                padding: 3px 10px;
                cursor: pointer;
            }
            button:disabled {
                cursor: default;
                opacity: 0.5;
            }
            .tab-content {
                flex-grow: 1;
                display: none;
                overflow: auto;
            }
            .tab-content.active {
                display: block;
            }
            .warning {
                padding: 6px 8px;
                color: var(--vscode-editorWarning-foreground, #cca700);
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            .empty-state {
                padding: 10px;
                color: var(--vscode-descriptionForeground);
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
        ${warning}
        <div class="toolbar">
            <span>Page ${page.page_index + 1}</span>
            <button id="nextPage"${nextDisabled}>Next Page</button>
            <button id="cancelQuery">Cancel</button>
        </div>
        <div id="results" class="tab-content active">
            ${emptyState}
            <table>
                <thead><tr>${tableHeaders}</tr></thead>
                <tbody>${tableRows}</tbody>
            </table>
        </div>
        <div id="messages" class="tab-content">
            <div class="messages-view">
                (${page.metrics.rows_returned} row(s) returned on this page, ${page.metrics.rows_produced} total row(s) produced)<br/><br/>
                Planning time: ${page.metrics.planning_time_ms}ms<br/>
                Execution time: ${page.metrics.execution_time_ms}ms<br/>
                First page time: ${page.metrics.first_page_time_ms}ms<br/>
                Total UI round trip: 00:00:00.${durationMs.toString().padStart(3, '0')}
            </div>
        </div>
        <script>
            const vscode = acquireVsCodeApi();
            function switchTab(tabId, element) {
                document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.getElementById(tabId).classList.add('active');
                if (element) {
                    element.classList.add('active');
                }
            }
            document.getElementById('nextPage')?.addEventListener('click', () => {
                vscode.postMessage({
                    type: 'nextPage',
                    queryId: ${JSON.stringify(page.query_id)},
                    pageIndex: ${page.page_index + 1},
                    pageSize: ${page.page_size}
                });
            });
            document.getElementById('cancelQuery')?.addEventListener('click', () => {
                vscode.postMessage({
                    type: 'cancelQuery',
                    queryId: ${JSON.stringify(page.query_id)}
                });
            });
        </script>
    </body>
    </html>`;
}

export function formatErrorMessage(errorMsg: string): string {
    return escapeHtml(errorMsg).replace(/\n/g, '<br/>');
}

export function escapeHtml(value: string): string {
    return value
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}
