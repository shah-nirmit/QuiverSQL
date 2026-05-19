import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';
import { DataSourcesProvider } from './dataSourcesProvider';

let daemonClient: DaemonClient | undefined;

interface DetectedQuery {
    sql: string;
    range: vscode.Range;
}

function detectQueries(document: vscode.TextDocument): DetectedQuery[] {
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
        } else if (inBlockComment) {
            if (char === '*' && nextChar === '/') {
                inBlockComment = false;
                i++; // skip '/'
            }
        } else if (inSingleQuote) {
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


export async function activate(context: vscode.ExtensionContext) {
    console.log('Activating DQL Developer Tools...');

    const dataSourcesProvider = new DataSourcesProvider();
    vscode.window.registerTreeDataProvider('dqlDataSources', dataSourcesProvider);

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

    let executeCommandUI = vscode.commands.registerCommand('dql.executeQueryUI', async (sqlArg?: string) => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        let sql = sqlArg || '';
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
            
            // Register source in the tree view explorer
            dataSourcesProvider.register({
                tableName: tableName,
                sourceType: format as any,
                location: filePath
            });
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to attach file: ${e.message || JSON.stringify(e)}`);
        }
    });

    let attachSQLiteCommand = vscode.commands.registerCommand('dql.attachSQLite', async () => {
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

        const tableName = await vscode.window.showInputBox({
            prompt: 'SQLite table name to expose (must exist in the DB)',
            placeHolder: 'users'
        });
        if (!tableName) return;

        const alias = await vscode.window.showInputBox({
            prompt: 'Alias for this table in DQL queries (leave blank to use table name)',
            placeHolder: tableName,
            value: tableName
        });

        try {
            const result = await daemonClient.sendRequest('register_sqlite', {
                db_path: dbPath,
                table_name: tableName,
                alias: alias || tableName
            });
            vscode.window.showInformationMessage(result);

            // Register SQLite source in the tree view explorer
            dataSourcesProvider.register({
                tableName: alias || tableName,
                sourceType: 'sqlite',
                location: `${dbPath} :: ${tableName}`
            });
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to attach SQLite table: ${e.message || JSON.stringify(e)}`);
        }
    });

    let connectWizardCommand = vscode.commands.registerCommand('dql.connectWizard', async () => {
        if (!daemonClient) {
            vscode.window.showErrorMessage('Daemon is not running.');
            return;
        }

        // Step 1: Select Data Source Type
        const sourceTypes = [
            { label: '$(file) CSV File', description: 'Attach a local Comma-Separated Values file', type: 'csv' },
            { label: '$(file-binary) Parquet File', description: 'Attach a local binary Parquet file', type: 'parquet' },
            { label: '$(json) JSON File', description: 'Attach a local JSON or NDJSON file', type: 'json' },
            { label: '$(database) SQLite Database', description: 'Attach a table from a SQLite database file', type: 'sqlite' }
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

            // Step 2b: Enter Table Name to Expose
            const tableName = await vscode.window.showInputBox({
                prompt: 'SQLite table name to expose (must exist in the database)',
                placeHolder: 'users',
                validateInput: (value) => value.trim().length === 0 ? 'Table name is required' : null
            });
            if (!tableName) return;

            // Step 3: Enter Table Alias
            const alias = await vscode.window.showInputBox({
                prompt: 'Enter Table Alias (how it will be referenced in your DQL queries)',
                placeHolder: tableName,
                value: tableName,
                validateInput: (value) => value.trim().length === 0 ? 'Alias is required' : null
            });
            if (!alias) return;

            try {
                const result = await daemonClient.sendRequest('register_sqlite', {
                    db_path: dbPath,
                    table_name: tableName,
                    alias: alias
                });
                vscode.window.showInformationMessage(result);

                // Register source in the tree view explorer
                dataSourcesProvider.register({
                    tableName: alias,
                    sourceType: 'sqlite',
                    location: `${dbPath} :: ${tableName}`
                });
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to attach SQLite table: ${e.message || JSON.stringify(e)}`);
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
                
                // Register source in the tree view explorer
                dataSourcesProvider.register({
                    tableName: alias,
                    sourceType: type as any,
                    location: filePath
                });
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to attach file: ${e.message || JSON.stringify(e)}`);
            }
        }
    });

    // CodeLens Provider to make it feel like ossdbtools
    const codeLensProvider = vscode.languages.registerCodeLensProvider('sql', {
        provideCodeLenses(document: vscode.TextDocument, _token: vscode.CancellationToken) {
            const queries = detectQueries(document);
            return queries.map(q => {
                const range = new vscode.Range(q.range.start.line, 0, q.range.start.line, 0);
                return new vscode.CodeLens(range, {
                    title: "▶ Run Query",
                    tooltip: `Execute: ${q.sql.substring(0, 60)}${q.sql.length > 60 ? '...' : ''}`,
                    command: "dql.executeQueryUI",
                    arguments: [q.sql]
                });
            });
        }
    });

    context.subscriptions.push(pingCommand);
    context.subscriptions.push(executeCommandUI);
    context.subscriptions.push(attachFileCommand);
    context.subscriptions.push(attachSQLiteCommand);
    context.subscriptions.push(connectWizardCommand);
    context.subscriptions.push(codeLensProvider);
    context.subscriptions.push({ dispose: () => daemonClient?.stop() });
}

export function deactivate() {
    if (daemonClient) {
        daemonClient.stop();
    }
}
