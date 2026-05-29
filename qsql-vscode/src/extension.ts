import { SourceManager } from './sourceManager';
import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';
import { DataSourcesProvider } from './dataSourcesProvider';
import { LineageProvider } from './lineageProvider';

let daemonClient: DaemonClient | undefined;

export interface DetectedQuery {
    sql: string;
    range: vscode.Range;
}

export function detectQueries(document: vscode.TextDocument): DetectedQuery[] {
    const queries: DetectedQuery[] = [];
    const text = document.getText();
    
    let currentSql = '';
    let startPos = new vscode.Position(0, 0);
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let inLineComment = false;
    let inBlockComment = false;
    
    for (let i = 0; i < text.length; i++) {
        const char = text[i];
        const nextChar = text[i + 1] || '';
        
        // Handle comments and quotes
        if (inLineComment) {
            if (char === '\n') {
                inLineComment = false;
            }
            continue;
        }
        if (inBlockComment) {
            if (char === '*' && nextChar === '/') {
                inBlockComment = false;
                i++; // skip '/'
            }
            continue;
        }
        if (inSingleQuote) {
            if (char === "'") {
                if (nextChar === "'") {
                    currentSql += "'";
                    i++; // skip next quote
                } else {
                    inSingleQuote = false;
                }
            }
        } else if (inDoubleQuote) {
            if (char === '"') {
                inDoubleQuote = false;
            }
        } else {
            if (char === '-' && nextChar === '-') {
                inLineComment = true;
                i++; // skip second '-'
                continue;
            }
            if (char === '/' && nextChar === '*') {
                inBlockComment = true;
                i++; // skip '*'
                continue;
            }
            if (char === "'") {
                inSingleQuote = true;
            }
            if (char === '"') {
                inDoubleQuote = true;
            }
        }
        
        // Accumulate SQL character
        if (char === ';' && !inSingleQuote && !inDoubleQuote && !inLineComment && !inBlockComment) {
            const trimmed = currentSql.trim();
            if (trimmed.length > 0) {
                const endPos = document.positionAt(i);
                const queryStartOffset = text.indexOf(trimmed, document.offsetAt(startPos));
                const adjustedStartPos = queryStartOffset !== -1 ? document.positionAt(queryStartOffset) : startPos;
                
                queries.push({
                    sql: trimmed + ';',
                    range: new vscode.Range(adjustedStartPos, endPos)
                });
            }
            currentSql = '';
            startPos = document.positionAt(i + 1);
        } else {
            currentSql += char;
        }
    }
    
    const trimmed = currentSql.trim();
    if (trimmed.length > 0) {
        const endPos = document.positionAt(text.length);
        const queryStartOffset = text.indexOf(trimmed, document.offsetAt(startPos));
        const adjustedStartPos = queryStartOffset !== -1 ? document.positionAt(queryStartOffset) : startPos;
        
        queries.push({
            sql: trimmed,
            range: new vscode.Range(adjustedStartPos, endPos)
        });
    }
    
    return queries;
}


class QsqlPlanProvider implements vscode.TextDocumentContentProvider {
    private plans = new Map<string, string>();
    private _onDidChange = new vscode.EventEmitter<vscode.Uri>();
    readonly onDidChange = this._onDidChange.event;

    public setPlan(uri: vscode.Uri, planText: string) {
        this.plans.set(uri.toString(), planText);
        this._onDidChange.fire(uri);
    }

    provideTextDocumentContent(uri: vscode.Uri): string {
        return this.plans.get(uri.toString()) || 'No plan compiled yet.';
    }
}

export async function activate(context: vscode.ExtensionContext) {
    console.log('Activating QuiverSQL Developer Tools...');

    const dataSourcesProvider = new DataSourcesProvider();
    vscode.window.registerTreeDataProvider('qsqlDataSources', dataSourcesProvider);

    daemonClient = new DaemonClient(context);
    const sourceManager = new SourceManager(context, daemonClient);
    dataSourcesProvider.setContext(daemonClient, sourceManager, context.extensionUri);

    const lineageProvider = new LineageProvider(daemonClient, dataSourcesProvider);
    vscode.window.registerTreeDataProvider('qsqlLineage', lineageProvider);

    function refreshLineage(editor: vscode.TextEditor | undefined) {
        if (!editor || editor.document.languageId !== 'sql') {
            lineageProvider.clear();
            return;
        }
        const queries = detectQueries(editor.document);
        if (queries.length > 0) {
            const cursor = editor.selection.active;
            const matchingQuery = queries.find(q => q.range.contains(cursor));
            if (matchingQuery) {
                lineageProvider.update(matchingQuery.sql);
            } else {
                lineageProvider.update(queries[0].sql);
            }
        } else {
            const text = editor.document.getText();
            lineageProvider.update(text);
        }
    }

    try {
        await daemonClient.start();
        vscode.window.showInformationMessage('QuiverSQL Daemon started successfully.');
        
        // Replay persistent sources
        await sourceManager.replaySources();
        dataSourcesProvider.refresh();

        refreshLineage(vscode.window.activeTextEditor);
    } catch (e) {
        console.error('Failed to start QuiverSQL Daemon', e);
    }

    const planProvider = new QsqlPlanProvider();
    context.subscriptions.push(
        vscode.workspace.registerTextDocumentContentProvider('qsql-plan', planProvider)
    );

    // Shared launcher used by both `qsql.visualizePlan` (planner-only) and
    // `qsql.explainAnalyze` (Phase 10 — drives the physical plan to
    // completion and overlays runtime metrics on the plan tree).
    async function launchPlanVisualization(sqlArg: any, analyze: boolean) {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }
        let sql = typeof sqlArg === 'string' ? sqlArg : '';
        if (!sql) {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.selection;
                if (!selection.isEmpty) {
                    sql = editor.document.getText(selection);
                } else {
                    const queries = detectQueries(editor.document);
                    if (queries.length > 0) {
                        const cursor = editor.selection.active;
                        const matchingQuery = queries.find(q => q.range.contains(cursor));
                        if (matchingQuery) {
                            sql = matchingQuery.sql;
                        } else {
                            sql = queries[0].sql;
                        }
                    } else {
                        sql = editor.document.getText();
                    }
                }
            }
        }
        if (!sql.trim()) {
            vscode.window.showErrorMessage('No SQL query found to visualize.');
            return;
        }
        try {
            const { PlanVisualizationPanel } = await import('./planVisualizationPanel');
            PlanVisualizationPanel.createOrShow(context.extensionUri);
            const result = await daemonClient.explainQuery(sql, { analyze });
            if (PlanVisualizationPanel.currentPanel) {
                PlanVisualizationPanel.currentPanel.updatePlan(result);
            }
        } catch (e: any) {
            const { PlanVisualizationPanel } = await import('./planVisualizationPanel');
            PlanVisualizationPanel.createOrShow(context.extensionUri);
            if (PlanVisualizationPanel.currentPanel) {
                PlanVisualizationPanel.currentPanel.updateError(e.message || JSON.stringify(e));
            }
        }
    }

    const visualizePlanCommand = vscode.commands.registerCommand('qsql.visualizePlan', async (sqlArg?: any) => {
        await launchPlanVisualization(sqlArg, false);
    });

    const explainAnalyzeCommand = vscode.commands.registerCommand('qsql.explainAnalyze', async (sqlArg?: any) => {
        await launchPlanVisualization(sqlArg, true);
    });

    // Dynamic Lineage Event Listeners
    const activeEditorSub = vscode.window.onDidChangeActiveTextEditor(editor => {
        refreshLineage(editor);
    });

    const selectionSub = vscode.window.onDidChangeTextEditorSelection(event => {
        refreshLineage(event.textEditor);
    });

    let debounceTimer: NodeJS.Timeout | undefined;
    const documentEditSub = vscode.workspace.onDidChangeTextDocument(event => {
        if (vscode.window.activeTextEditor && event.document === vscode.window.activeTextEditor.document) {
            if (debounceTimer) {
                clearTimeout(debounceTimer);
            }
            debounceTimer = setTimeout(() => {
                refreshLineage(vscode.window.activeTextEditor);
            }, 500);
        }
    });


    const pingCommand = vscode.commands.registerCommand('qsql.pingDaemon', async () => {
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

    const showVersionCommand = vscode.commands.registerCommand('qsql.showVersion', async () => {
        const extensionVersion = context.extension.packageJSON.version || 'unknown';
        let daemonVersion = 'daemon unavailable';

        if (daemonClient) {
            try {
                const info = await daemonClient.sendRequest('version');
                daemonVersion = [
                    `daemon ${info.daemon || info.version || 'unknown'}`,
                    `core ${info.core || 'unknown'}`,
                    `connectors ${info.connectors || 'unknown'}`,
                    `rpc ${info.rpc || 'unknown'}`
                ].join(', ');
            } catch (e: any) {
                daemonVersion = `daemon unavailable: ${e.message || JSON.stringify(e)}`;
            }
        }

        vscode.window.showInformationMessage(`QuiverSQL extension ${extensionVersion}; ${daemonVersion}`);
    });

    const executeCommandUI = vscode.commands.registerCommand('qsql.executeQueryUI', async (sqlArg?: any) => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        let sql = typeof sqlArg === 'string' ? sqlArg : '';
        if (!sql) {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.selection;
                if (!selection.isEmpty) {
                    sql = editor.document.getText(selection);
                } else {
                    // Find all queries in the document and choose the one under the cursor
                    const queries = detectQueries(editor.document);
                    if (queries.length > 0) {
                        const cursor = editor.selection.active;
                        const matchingQuery = queries.find(q => q.range.contains(cursor));
                        if (matchingQuery) {
                            sql = matchingQuery.sql;
                        } else {
                            sql = queries[0].sql;
                        }
                    } else {
                        sql = editor.document.getText();
                    }
                }
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
            const { ResultGridPanel } = await import('./webviewPanel');
            ResultGridPanel.createOrShow(context.extensionUri);
            if (ResultGridPanel.currentPanel) {
                ResultGridPanel.currentPanel.updateLoading('Running query...');
                ResultGridPanel.currentPanel.setPagingHandlers({
                    onNextPage: async (queryId, pageIndex, pageSize) => {
                        if (!daemonClient || !ResultGridPanel.currentPanel) {
                            return;
                        }
                        const pageStart = Date.now();
                        try {
                            ResultGridPanel.currentPanel.updateLoading('Loading next page...');
                            const page = await daemonClient.getQueryPage(queryId, pageIndex, pageSize);
                            ResultGridPanel.currentPanel.updatePage(page, Date.now() - pageStart);
                        } catch (e: any) {
                            ResultGridPanel.currentPanel.updateError(e.message || JSON.stringify(e), Date.now() - pageStart);
                        }
                    },
                    onCancel: async (queryId) => {
                        if (!daemonClient || !ResultGridPanel.currentPanel) {
                            return;
                        }
                        const cancelStart = Date.now();
                        try {
                            const result = await daemonClient.cancelQuery(queryId);
                            ResultGridPanel.currentPanel.updateError(result.message, Date.now() - cancelStart);
                        } catch (e: any) {
                            ResultGridPanel.currentPanel.updateError(e.message || JSON.stringify(e), Date.now() - cancelStart);
                        }
                    }
                });
            }
            const config = vscode.workspace.getConfiguration('qsql');
            const pageSize = config.get<number>('defaultPageSize', 1000);
            const timeoutMs = config.get<number>('remoteQueryTimeout', 30) * 1000;
            const result = await daemonClient.startQuery(sql, { pageSize, timeoutMs });
            const duration = Date.now() - start;

            if (ResultGridPanel.currentPanel) {
                ResultGridPanel.currentPanel.updatePage(result, duration);
            }

        } catch (e: any) {
            const duration = Date.now() - start;
            const { ResultGridPanel } = await import('./webviewPanel');
            ResultGridPanel.createOrShow(context.extensionUri);
            if (ResultGridPanel.currentPanel) {
                if (typeof e?.code === 'number') {
                    ResultGridPanel.currentPanel.updateQueryError(e, duration);
                } else {
                    ResultGridPanel.currentPanel.updateError(e.message || JSON.stringify(e), duration);
                }
            }
        }
    });

    const attachFileCommand = vscode.commands.registerCommand('qsql.attachFile', async () => {
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
            
            // Persist the source and refresh
            await sourceManager.addSource(tableName, 'file', {
                path: filePath,
                format: format
            });
            dataSourcesProvider.refresh();
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to attach file: ${e.message || JSON.stringify(e)}`);
        }
    });

    const attachSQLiteCommand = vscode.commands.registerCommand('qsql.attachSQLite', async () => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        const fileUri = await vscode.window.showOpenDialog({
            canSelectMany: false,
            openLabel: 'Attach SQLite Database',
            filters: { 'SQLite Databases': ['db', 'sqlite', 'sqlite3'] }
        });

        if (!fileUri || fileUri.length === 0) return;
        const dbPath = fileUri[0].fsPath;

        const alias = await vscode.window.showInputBox({
            prompt: 'Database Alias (how it will be referenced in your QuiverSQL queries, e.g. sqlite_local)',
            placeHolder: 'sqlite_local'
        });
        if (!alias) return;

        try {
            const result = await daemonClient.sendRequest('register_sqlite', {
                db_path: dbPath,
                alias: alias
            });
            vscode.window.showInformationMessage(result);

            // Persist the source and refresh
            await sourceManager.addSource(alias, 'sqlite', {
                dbPath: dbPath
            });
            dataSourcesProvider.refresh();
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to attach SQLite database: ${e.message || JSON.stringify(e)}`);
        }
    });

    const connectWizardCommand = vscode.commands.registerCommand('qsql.connectWizard', async () => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        // Step 1: Select Data Source Type
        const sourceTypes = [
            { label: '$(file) CSV File', description: 'Attach a local Comma-Separated Values file', type: 'csv' },
            { label: '$(file-binary) Parquet File', description: 'Attach a local binary Parquet file', type: 'parquet' },
            { label: '$(json) JSON File', description: 'Attach a local JSON or NDJSON file', type: 'json' },
            { label: '$(file-text) Fixed-width File', description: 'Attach a fixed-width text file plus a JSON layout sidecar describing column spans', type: 'fixed_width' },
            { label: '$(database) SQLite Database', description: 'Attach a SQLite database file', type: 'sqlite' },
            { label: '$(database) Postgres', description: 'Attach a Postgres database or schema using a connection string', type: 'postgres' },
            { label: '$(database) MySQL', description: 'Attach a MySQL database using a connection string', type: 'mysql' },
            { label: '$(database) MariaDB', description: 'Attach a MariaDB database using a connection string', type: 'mariadb' }
        ];

        const selection = await vscode.window.showQuickPick(sourceTypes, {
            placeHolder: 'Select Data Source Type to connect'
        });

        if (!selection) return;
        const type = selection.type;

        if (type === 'sqlite') {
            // SQLite Connection Steps
            // Step 2a: Select SQLite DB file
            const fileUri = await vscode.window.showOpenDialog({
                canSelectMany: false,
                openLabel: 'Select SQLite Database',
                filters: { 'SQLite Databases': ['db', 'sqlite', 'sqlite3'] }
            });

            if (!fileUri || fileUri.length === 0) return;
            const dbPath = fileUri[0].fsPath;

            // Step 3: Enter Database Alias
            const alias = await vscode.window.showInputBox({
                prompt: 'Enter Database Alias (how it will be referenced in your QuiverSQL queries, e.g. sqlite_local)',
                placeHolder: 'sqlite_local',
                validateInput: (value) => value.trim().length === 0 ? 'Alias is required' : null
            });
            if (!alias) return;

            try {
                const result = await daemonClient.sendRequest('register_sqlite', {
                    db_path: dbPath,
                    alias: alias
                });
                vscode.window.showInformationMessage(result);

                // Persist the source and refresh
                await sourceManager.addSource(alias, 'sqlite', {
                    dbPath: dbPath
                });
                dataSourcesProvider.refresh();
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to attach SQLite database: ${e.message || JSON.stringify(e)}`);
            }

        } else if (type === 'postgres' || type === 'mysql' || type === 'mariadb') {
            const engineLabel = type === 'postgres' ? 'Postgres' : type === 'mysql' ? 'MySQL' : 'MariaDB';
            const connectionString = await vscode.window.showInputBox({
                prompt: `${engineLabel} connection string`,
                placeHolder: type === 'postgres'
                    ? 'postgres://user:password@localhost:5432/database'
                    : 'mysql://user:password@localhost:3306/database',
                password: true,
                validateInput: (value) => value.trim().length === 0 ? 'Connection string is required' : null
            });
            if (!connectionString) return;

            const schema = await vscode.window.showInputBox({
                prompt: type === 'postgres'
                    ? 'Schema name (blank uses public)'
                    : 'Database/schema name (blank uses the connection default)',
                placeHolder: type === 'postgres' ? 'public' : 'my_database'
            });

            const alias = await vscode.window.showInputBox({
                prompt: 'Enter Database Alias (how it will be referenced in your QuiverSQL queries, e.g. pg_local)',
                placeHolder: type === 'postgres' ? 'pg_local' : type === 'mysql' ? 'mysql_local' : 'mariadb_local',
                validateInput: (value) => value.trim().length === 0 ? 'Alias is required' : null
            });
            if (!alias) return;

            const method = type === 'postgres'
                ? 'register_postgres'
                : type === 'mariadb'
                    ? 'register_mariadb'
                    : 'register_mysql';

            try {
                const result = await daemonClient.sendRequest(method, {
                    connection_string: connectionString,
                    schema: schema?.trim() || undefined,
                    alias: alias
                });
                vscode.window.showInformationMessage(result);

                await sourceManager.addSource(alias, type, {
                    schema: schema?.trim() || undefined
                }, connectionString);
                dataSourcesProvider.refresh();
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to attach ${engineLabel} database: ${e.message || JSON.stringify(e)}`);
            }
        } else if (type === 'fixed_width') {
            // Fixed-width files need an extra JSON layout sidecar describing
            // each column's byte-span and SQL type. The wizard collects both
            // files plus an alias, then sends register_file with
            // options.layout_path so the daemon can build a
            // FixedWidthTableProvider (Phase 8B).
            const dataFileUri = await vscode.window.showOpenDialog({
                canSelectMany: false,
                openLabel: 'Select fixed-width data file',
                filters: {
                    'Fixed-width Data': ['txt', 'dat', 'fwf'],
                    'All Files': ['*']
                }
            });
            if (!dataFileUri || dataFileUri.length === 0) return;
            const dataPath = dataFileUri[0].fsPath;

            const layoutFileUri = await vscode.window.showOpenDialog({
                canSelectMany: false,
                openLabel: 'Select layout JSON file (column spans + types)',
                filters: {
                    'Layout JSON': ['json']
                }
            });
            if (!layoutFileUri || layoutFileUri.length === 0) return;
            const layoutPath = layoutFileUri[0].fsPath;

            const defaultAlias = dataPath.split(/[\\/]/).pop()?.split('.')[0] || 'fwf_table';
            const alias = await vscode.window.showInputBox({
                prompt: 'Enter Table Alias for this fixed-width file',
                placeHolder: defaultAlias,
                value: defaultAlias,
                validateInput: (value) => value.trim().length === 0 ? 'Alias is required' : null
            });
            if (!alias) return;

            try {
                const result = await daemonClient.sendRequest('register_file', {
                    table_name: alias,
                    path: dataPath,
                    format: 'fixed_width',
                    options: { layout_path: layoutPath }
                });
                vscode.window.showInformationMessage(result);

                await sourceManager.addSource(alias, 'file', {
                    path: dataPath,
                    format: 'fixed_width',
                    layoutPath: layoutPath
                });
                dataSourcesProvider.refresh();
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to attach fixed-width file: ${e.message || JSON.stringify(e)}`);
            }
        } else {
            // File Connection Steps
            // Step 2a: Select File
            const filters: Record<string, string[]> = {};
            if (type === 'csv') filters['CSV Files'] = ['csv'];
            else if (type === 'parquet') filters['Parquet Files'] = ['parquet'];
            else if (type === 'json') filters['JSON Files'] = ['json', 'ndjson'];

            const fileUri = await vscode.window.showOpenDialog({
                canSelectMany: false,
                openLabel: `Select ${type.toUpperCase()} File`,
                filters: filters
            });

            if (!fileUri || fileUri.length === 0) return;
            const filePath = fileUri[0].fsPath;

            // Extract default table alias from file name
            const defaultAlias = filePath.split(/[\\/]/).pop()?.split('.')[0] || 'my_table';

            // Step 3: Enter Table Alias
            const alias = await vscode.window.showInputBox({
                prompt: `Enter Table Alias for this ${type.toUpperCase()} file`,
                placeHolder: defaultAlias,
                value: defaultAlias,
                validateInput: (value) => value.trim().length === 0 ? 'Alias is required' : null
            });

            if (!alias) return;

            try {
                const result = await daemonClient.sendRequest('register_file', {
                    table_name: alias,
                    path: filePath,
                    format: type
                });
                vscode.window.showInformationMessage(result);

                // Persist the source and refresh
                await sourceManager.addSource(alias, 'file', {
                    path: filePath,
                    format: type
                });
                dataSourcesProvider.refresh();
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to attach file: ${e.message || JSON.stringify(e)}`);
            }
        }
    });

    // CodeLens Provider to make it feel like ossdbtools
    const codeLensProvider = vscode.languages.registerCodeLensProvider('sql', {
        provideCodeLenses(document: vscode.TextDocument, _token: vscode.CancellationToken) {
            const queries = detectQueries(document);
            const lenses: vscode.CodeLens[] = [];
            queries.forEach(q => {
                const range = new vscode.Range(q.range.start.line, 0, q.range.start.line, 0);
                lenses.push(new vscode.CodeLens(range, {
                    title: "$(play) Run Query",
                    tooltip: `Execute: ${q.sql.substring(0, 60)}${q.sql.length > 60 ? '...' : ''}`,
                    command: "qsql.executeQueryUI",
                    arguments: [q.sql]
                }));
                lenses.push(new vscode.CodeLens(range, {
                    title: "$(search) Explain Plan",
                    tooltip: `Visualize plan for: ${q.sql.substring(0, 60)}${q.sql.length > 60 ? '...' : ''}`,
                    command: "qsql.visualizePlan",
                    arguments: [q.sql]
                }));
                // Phase 10 — third CodeLens for EXPLAIN ANALYZE. Hidden by
                // default and only surfaces when the user has opted in via
                // `qsql.explainAnalyzeEnabled`. Keeps the UX from accidentally
                // running expensive queries.
                const analyzeOn = vscode.workspace
                    .getConfiguration('qsql')
                    .get<boolean>('explainAnalyzeEnabled', false);
                if (analyzeOn) {
                    lenses.push(new vscode.CodeLens(range, {
                        title: "$(beaker) Explain (ANALYZE)",
                        tooltip: `Run the query under the scan guard and overlay runtime metrics: ${q.sql.substring(0, 60)}${q.sql.length > 60 ? '...' : ''}`,
                        command: "qsql.explainAnalyze",
                        arguments: [q.sql]
                    }));
                }
            });
            return lenses;
        }
    });


    const removeSourceCommand = vscode.commands.registerCommand('qsql.removeSource', async (item: any) => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        let name = '';
        if (item && item.source && item.source.name) {
            name = item.source.name;
        } else {
            const profiles = sourceManager.getProfiles();
            if (profiles.length === 0) {
                vscode.window.showInformationMessage('No active data sources to remove.');
                return;
            }
            const selection = await vscode.window.showQuickPick(
                profiles.map(p => p.name),
                { placeHolder: 'Select a data source to remove' }
            );
            if (!selection) return;
            name = selection;
        }

        try {
            const result = await daemonClient.removeSource(name);
            if (result.removed) {
                vscode.window.showInformationMessage(`Data source '${name}' removed successfully.`);
            } else {
                vscode.window.showWarningMessage(`Data source '${name}' was not found on daemon but will be cleaned up locally.`);
            }
            await sourceManager.removeSource(name);
            dataSourcesProvider.refresh();
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to remove data source: ${e.message || JSON.stringify(e)}`);
            await sourceManager.removeSource(name);
            dataSourcesProvider.refresh();
        }
    });

    const loadMoreTablesCommand = vscode.commands.registerCommand('qsql.loadMoreTables', async (sourceName: string) => {
        try {
            await dataSourcesProvider.loadMoreTables(sourceName);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to load more tables: ${e.message || JSON.stringify(e)}`);
        }
    });

    context.subscriptions.push(removeSourceCommand);
    context.subscriptions.push(loadMoreTablesCommand);

    context.subscriptions.push(pingCommand);
    context.subscriptions.push(showVersionCommand);
    context.subscriptions.push(executeCommandUI);
    context.subscriptions.push(visualizePlanCommand);
    context.subscriptions.push(explainAnalyzeCommand);
    context.subscriptions.push(attachFileCommand);
    context.subscriptions.push(attachSQLiteCommand);
    context.subscriptions.push(connectWizardCommand);
    context.subscriptions.push(codeLensProvider);
    context.subscriptions.push(activeEditorSub);
    context.subscriptions.push(selectionSub);
    const configChangeSub = vscode.workspace.onDidChangeConfiguration(async (event) => {
        if (event.affectsConfiguration('qsql')) {
            console.log('QuiverSQL configuration changed. Restarting daemon...');
            if (daemonClient) {
                daemonClient.stop();
                try {
                    await daemonClient.start();
                    vscode.window.showInformationMessage('QuiverSQL Daemon restarted with new configurations.');
                    // Replay persistent sources since daemon restarted
                    await sourceManager.replaySources();
                    dataSourcesProvider.refresh();
                } catch (err: any) {
                    vscode.window.showErrorMessage(`Failed to restart QuiverSQL Daemon: ${err.message || err}`);
                }
            }
        }
    });
    context.subscriptions.push(configChangeSub);
    context.subscriptions.push(documentEditSub);

    context.subscriptions.push({ dispose: () => {
        if (debounceTimer) clearTimeout(debounceTimer);
        daemonClient?.stop();
    }});

}

export function deactivate() {
    if (daemonClient) {
        daemonClient.stop();
    }
}
