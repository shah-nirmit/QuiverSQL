import * as vscode from 'vscode';
import * as arrow from 'apache-arrow';
import { QueryPage, QueryError, isScanGuardError } from './models';
import {
    decodeResultPage,
    columnsForPage,
    rowCount,
    arrowTypeForColumn,
    cellAt,
    RenderablePage,
} from './resultPage';

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

    public updateQueryError(error: QueryError, durationMs: number) {
        this._panel.webview.html = this._getErrorHtml(error.message, durationMs, error);
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

    private _getErrorHtml(errorMsg: string, durationMs: number, error?: QueryError) {
        const scanGuardHint = error && isScanGuardError(error)
            ? `<div class="scan-guard-hint">
                <strong>&#9888; Scan budget exceeded</strong> &mdash; to resolve:
                <ul>
                    <li>Add a <code>LIMIT</code> clause to your query</li>
                    <li>Add a <code>WHERE</code> filter to narrow the result set</li>
                    <li>Raise the source scan budget in QuiverSQL settings</li>
                </ul>
            </div>`
            : '';
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
                .scan-guard-hint {
                    background-color: var(--vscode-inputValidation-warningBackground, rgba(255,204,0,0.15));
                    border: 1px solid var(--vscode-inputValidation-warningBorder, #b89500);
                    border-radius: 3px;
                    padding: 8px 12px;
                    margin-bottom: 10px;
                    font-family: var(--vscode-editor-font-family, 'Segoe UI', sans-serif);
                }
                .scan-guard-hint ul {
                    margin: 4px 0 0 0;
                    padding-left: 20px;
                }
                .scan-guard-hint code {
                    font-family: var(--vscode-editor-font-family, 'Consolas', monospace);
                    background: var(--vscode-textCodeBlock-background, rgba(0,0,0,0.1));
                    padding: 1px 4px;
                    border-radius: 2px;
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
                    ${scanGuardHint}
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

/**
 * Formats a cell value for HTML rendering. The optional second arg is the
 * Arrow `DataType` of the column when the page came over IPC; it drives
 * type-aware formatting (Phase 9):
 *
 *   - Int64 (`bigint`)          → exact base-10 string, no precision loss.
 *   - Float32/Float64           → standard JS `String(n)` with NaN/∞ handling.
 *   - Decimal128/256            → string passthrough (apache-arrow gives us a string).
 *   - Date32 / Date64           → ISO-8601 date string (`YYYY-MM-DD`).
 *   - Timestamp                 → ISO-8601 datetime, UTC.
 *   - Bool                      → `"true"` / `"false"`.
 *   - Utf8 / LargeUtf8          → HTML-escaped string.
 *   - Null / undefined          → muted `(null)` span.
 *   - Anything else             → fallback to legacy `String(value)`.
 *
 * When called without `dataType` (the JSON path), behaviour matches the
 * pre-Phase-9 formatter exactly so existing tests stay byte-identical.
 */
export function formatCellValue(value: unknown, dataType?: arrow.DataType): string {
    if (value === null || value === undefined) {
        return dataType
            ? '<span class="cell-null">(null)</span>'
            : '<em>null</em>';
    }

    if (dataType) {
        // bigint check first — apache-arrow returns Int64 cells as native
        // bigints, which JSON.stringify would otherwise blow up on.
        if (typeof value === 'bigint') {
            return escapeHtml(value.toString());
        }
        // arrow.DataType has a `typeId` enum we can switch on; the apache-arrow
        // type-id values are stable across minor versions.
        if (arrow.DataType.isTimestamp(dataType)) {
            // Cell values come back as milliseconds since epoch (`number`)
            // or `bigint` for the larger time units. Coerce both to a Date
            // and emit ISO. We assume UTC unless the timezone metadata
            // says otherwise — apache-arrow normalises this already.
            const ms = typeof value === 'bigint' ? Number(value) : Number(value);
            return escapeHtml(new Date(ms).toISOString());
        }
        if (arrow.DataType.isDate(dataType)) {
            const ms =
                typeof value === 'bigint' ? Number(value) : Number(value as number);
            return escapeHtml(new Date(ms).toISOString().slice(0, 10));
        }
        if (arrow.DataType.isBool(dataType)) {
            return value ? 'true' : 'false';
        }
        if (arrow.DataType.isDecimal(dataType)) {
            // apache-arrow may return Decimal cells as `Uint32Array` (raw
            // bytes) when the precision exceeds 53 bits; coerce via the
            // arrow utility when available, else fall back to String().
            return escapeHtml(String(value));
        }
    }

    if (typeof value === 'bigint') {
        // Even on the JSON path, the daemon never emits bigint today — but
        // future callers (or test fixtures) might. Stringify deterministically.
        return escapeHtml(value.toString());
    }

    if (typeof value === 'object') {
        return escapeHtml(JSON.stringify(value));
    }

    return escapeHtml(String(value));
}

/**
 * Legacy column-name helper, kept for callers that hand a raw `QueryPage`
 * (notably the test fixture in `detectQueries.test.ts`). New call sites
 * inside the renderer pipeline use `columnsForPage(RenderablePage)`
 * directly.
 */
export function getQueryPageColumns(page: QueryPage): string[] {
    if (page.schema.fields.length > 0) {
        return page.schema.fields.map(field => field.name);
    }

    return page.data.length > 0 ? Object.keys(page.data[0]) : [];
}

export function renderQueryPageHtml(page: QueryPage, durationMs: number): string {
    // Decode at the boundary so every downstream branch operates on the
    // RenderablePage discriminated union. JSON pages are a no-op pass-through;
    // Arrow IPC pages get base64-decoded + parsed once here.
    let decoded: RenderablePage;
    try {
        decoded = decodeResultPage(page);
    } catch (e: any) {
        // Surface decode errors as a structured page-level warning rather
        // than silently falling back to JSON. The Phase 9 plan explicitly
        // rules out auto-fallback (a malformed IPC payload is always a bug
        // we want to surface).
        const reason = e?.message ?? String(e);
        decoded = {
            kind: 'json',
            query_id: page.query_id,
            schema: page.schema,
            page_index: page.page_index,
            page_size: page.page_size,
            is_last: page.is_last,
            rows: page.data ?? [],
            metrics: page.metrics,
            warning: `Failed to decode Arrow IPC page: ${reason}. Falling back to legacy JSON rows for this page only.`,
        };
    }

    const columns = columnsForPage(decoded);
    const tableHeaders = ['<th></th>']
        .concat(columns.map(col => `<th>${escapeHtml(col)}</th>`))
        .join('');
    const startRowNumber = decoded.page_index * decoded.page_size;
    const totalRows = rowCount(decoded);
    const rowMarkup: string[] = [];
    for (let i = 0; i < totalRows; i++) {
        const cells = columns
            .map(col => {
                const value = cellAt(decoded, i, col);
                const arrowType = arrowTypeForColumn(decoded, col);
                return `<td>${formatCellValue(value, arrowType)}</td>`;
            })
            .join('');
        rowMarkup.push(
            `<tr><td class="row-num">${startRowNumber + i + 1}</td>${cells}</tr>`,
        );
    }
    const tableRows = rowMarkup.join('');
    const emptyState =
        totalRows === 0 ? '<div class="empty-state">No rows returned.</div>' : '';
    const warning = decoded.warning
        ? `<div class="warning">${escapeHtml(decoded.warning)}</div>`
        : '';
    const nextDisabled = decoded.is_last ? ' disabled' : '';

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
            <span>Page ${decoded.page_index + 1}</span>
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
                (${decoded.metrics.rows_returned} row(s) returned on this page, ${decoded.metrics.rows_produced} total row(s) produced)<br/><br/>
                Planning time: ${decoded.metrics.planning_time_ms}ms<br/>
                Execution time: ${decoded.metrics.execution_time_ms}ms<br/>
                First page time: ${decoded.metrics.first_page_time_ms}ms<br/>
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
                    queryId: ${JSON.stringify(decoded.query_id)},
                    pageIndex: ${decoded.page_index + 1},
                    pageSize: ${decoded.page_size}
                });
            });
            document.getElementById('cancelQuery')?.addEventListener('click', () => {
                vscode.postMessage({
                    type: 'cancelQuery',
                    queryId: ${JSON.stringify(decoded.query_id)}
                });
            });
        </script>
    </body>
    </html>`;
}

export function formatErrorMessage(errorMsg: string): string {
    return escapeHtml(errorMsg).replace(/\n/g, '<br/>');
}

/** Exported for unit testing: renders the error HTML for a given error message and optional QueryError. */
export function renderErrorHtml(errorMsg: string, durationMs: number, error?: QueryError): string {
    const scanGuardHint = error && isScanGuardError(error)
        ? `<div class="scan-guard-hint">Scan budget exceeded`
        : '';
    return `${scanGuardHint}${formatErrorMessage(errorMsg)}`;
}

export function escapeHtml(value: string): string {
    return value
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}
