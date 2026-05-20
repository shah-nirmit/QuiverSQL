import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';
import { CatalogSource } from './models';
import { SourceManager } from './sourceManager';

export class DataSourceItem extends vscode.TreeItem {
    constructor(public readonly source: CatalogSource) {
        super(source.name, vscode.TreeItemCollapsibleState.None);

        const typeLabel: Record<string, string> = {
            csv:         'CSV File',
            parquet:     'Parquet File',
            json:        'JSON File',
            ndjson:      'NDJSON File',
            sqlite:      'SQLite Table',
            fixed_width: 'Fixed Width File',
            postgres:    'Postgres Table',
            mysql:       'MySQL Table',
            mariadb:     'MariaDB Table',
        };

        const icon: Record<string, string> = {
            csv:         'table',
            parquet:     'file-binary',
            json:        'json',
            ndjson:      'json',
            sqlite:      'database',
            fixed_width: 'file-text',
            postgres:    'database',
            mysql:       'database',
            mariadb:     'database',
        };

        const isError = source.status === 'error';
        const typeStr = typeLabel[source.kind] || source.kind;

        if (isError) {
            this.description = `-> ${typeStr} (Error)`;
            this.tooltip = new vscode.MarkdownString(
                `**${source.name}**\n\n` +
                `Type: ${typeStr}\n\n` +
                `Status: ⚠️ **Error**\n\n` +
                `Details: \`${source.error || 'Unknown error during registration'}\``
            );
            this.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('testing.iconFailed'));
        } else {
            this.description = `-> ${typeStr}`;
            
            let location = '';
            if (source.connection_details) {
                if (source.connection_details.path) {
                    location = source.connection_details.path;
                } else if (source.connection_details.db_path) {
                    location = `${source.connection_details.db_path} :: ${source.connection_details.table_name}`;
                } else if (source.connection_details.dbPath) {
                    location = `${source.connection_details.dbPath} :: ${source.connection_details.tableName}`;
                } else {
                    location = JSON.stringify(source.connection_details);
                }
            }

            this.tooltip = new vscode.MarkdownString(
                `**${source.name}**\n\n` +
                `Type: ${typeStr}\n\n` +
                `Location: \`${location}\``
            );
            this.iconPath = new vscode.ThemeIcon(icon[source.kind] || 'database');
        }

        this.contextValue = 'qsqlDataSource';
    }
}

export class DataSourcesProvider
    implements vscode.TreeDataProvider<DataSourceItem>
{
    private _onDidChangeTreeData =
        new vscode.EventEmitter<DataSourceItem | undefined | null | void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private daemonClient?: DaemonClient;
    private sourceManager?: SourceManager;
    private sources: CatalogSource[] = [];

    public setContext(daemonClient: DaemonClient, sourceManager: SourceManager) {
        this.daemonClient = daemonClient;
        this.sourceManager = sourceManager;
    }

    public refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    public getSources(): CatalogSource[] {
        return this.sources;
    }

    getTreeItem(element: DataSourceItem): vscode.TreeItem {
        return element;
    }

    async getChildren(): Promise<DataSourceItem[]> {
        if (!this.daemonClient || !this.sourceManager) {
            const empty = new vscode.TreeItem(
                'DataSourcesProvider context not set.',
                vscode.TreeItemCollapsibleState.None
            );
            return [empty as unknown as DataSourceItem];
        }

        try {
            // 1. Fetch active sources from daemon
            let daemonSources: CatalogSource[] = [];
            try {
                daemonSources = await this.daemonClient.listSources();
            } catch (e) {
                console.error('Failed to list sources from daemon:', e);
            }

            // 2. Fetch persistent profiles
            const profiles = this.sourceManager.getProfiles();

            // 3. Merge them
            const mergedSources: CatalogSource[] = [];
            const processedNames = new Set<string>();

            // Process daemon sources first
            for (const ds of daemonSources) {
                processedNames.add(ds.name);
                const replayErr = this.sourceManager.replayErrors.get(ds.name);
                if (replayErr) {
                    mergedSources.push({
                        ...ds,
                        status: 'error',
                        error: replayErr
                    });
                } else {
                    mergedSources.push(ds);
                }
            }

            // Process any persistent profiles that didn't make it to active daemon list
            for (const profile of profiles) {
                if (!processedNames.has(profile.name)) {
                    processedNames.add(profile.name);
                    const replayErr = this.sourceManager.replayErrors.get(profile.name) || 'Replay failed / Offline';
                    mergedSources.push({
                        name: profile.name,
                        kind: profile.kind === 'file' ? (profile.details.format as any) : profile.kind as any,
                        connection_details: profile.details,
                        status: 'error',
                        error: replayErr
                    });
                }
            }

            if (mergedSources.length === 0) {
                this.sources = [];
                const empty = new vscode.TreeItem(
                    'No data sources attached yet.',
                    vscode.TreeItemCollapsibleState.None
                );
                empty.iconPath = new vscode.ThemeIcon('info');
                return [empty as unknown as DataSourceItem];
            }

            this.sources = mergedSources;
            return mergedSources.map(s => new DataSourceItem(s));
        } catch (e: any) {
            this.sources = [];
            const errorItem = new vscode.TreeItem(
                `Error loading sources: ${e.message || e}`,
                vscode.TreeItemCollapsibleState.None
            );
            errorItem.iconPath = new vscode.ThemeIcon('error');
            return [errorItem as unknown as DataSourceItem];
        }
    }
}
