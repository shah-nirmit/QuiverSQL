import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';
import { CatalogSource } from './models';
import { SourceManager } from './sourceManager';
import { treeIconFor, labelFor } from './providerIcons';

const TABLE_TREE_PAGE_SIZE = 250;

export class DataSourceItem extends vscode.TreeItem {
    constructor(
        public readonly source: CatalogSource,
        public readonly tableName?: string,
        public readonly loadMore: boolean = false,
        extensionUri?: vscode.Uri,
    ) {
        super(
            loadMore ? 'Load more tables...' : (tableName ? tableName : source.name),
            tableName || loadMore ? vscode.TreeItemCollapsibleState.None : (source.tables && source.tables.length > 0 ? vscode.TreeItemCollapsibleState.Collapsed : vscode.TreeItemCollapsibleState.None)
        );

        const typeLabel: Record<string, string> = {
            csv:         'CSV File',
            parquet:     'Parquet File',
            json:        'JSON File',
            ndjson:      'NDJSON File',
            sqlite:      'SQLite DB',
            fixed_width: 'Fixed Width File',
            postgres:    'Postgres DB',
            mysql:       'MySQL DB',
            mariadb:     'MariaDB DB',
        };

        const isError = source.status === 'error';
        const typeStr = typeLabel[source.kind] || source.kind;

        if (this.loadMore) {
            this.description = 'More';
            this.tooltip = `Load more tables from ${source.name}`;
            this.iconPath = new vscode.ThemeIcon('ellipsis');
            this.contextValue = 'qsqlDataSourceLoadMore';
            this.command = {
                command: 'qsql.loadMoreTables',
                title: 'Load more tables',
                arguments: [source.name]
            };
        } else if (this.tableName) {
            this.description = 'Table';
            this.tooltip = `Table: ${source.name}.${tableName}`;
            this.iconPath = new vscode.ThemeIcon('table');
            this.contextValue = 'qsqlDataSourceTable';
        } else {
            if (isError) {
                this.description = `-> ${typeStr} (Error)`;
                this.tooltip = new vscode.MarkdownString(
                    `**${source.name}**\n\n` +
                    `Type: ${typeStr}\n\n` +
                    `Status: **Error**\n\n` +
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
                        location = source.connection_details.db_path;
                    } else if (source.connection_details.dbPath) {
                        location = source.connection_details.dbPath;
                    } else {
                        location = JSON.stringify(source.connection_details);
                    }
                }

                this.tooltip = new vscode.MarkdownString(
                    `**${source.name}** (${labelFor(source.kind)})\n\n` +
                    `Type: ${typeStr}\n\n` +
                    `Location: \`${location}\``
                );
                // When `extensionUri` is provided, treeIconFor returns a
                // brand-specific SVG URI from media/icons/. When not (e.g.
                // unit tests that construct items without going through
                // DataSourcesProvider), it falls back to the generic
                // `database` codicon — preserves the pre-refactor look for
                // older call sites.
                this.iconPath = extensionUri
                    ? treeIconFor(extensionUri, source.kind)
                    : new vscode.ThemeIcon('database');
            }

            this.contextValue = 'qsqlDataSource';
        }
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
    private extensionUri?: vscode.Uri;
    private sources: CatalogSource[] = [];
    private loadedTables = new Map<string, string[]>();
    private hasMoreTables = new Map<string, boolean>();

    public setContext(
        daemonClient: DaemonClient,
        sourceManager: SourceManager,
        extensionUri?: vscode.Uri,
    ) {
        this.daemonClient = daemonClient;
        this.sourceManager = sourceManager;
        this.extensionUri = extensionUri;
    }

    public refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    public getSources(): CatalogSource[] {
        return this.sources;
    }

    public async loadMoreTables(sourceName: string): Promise<void> {
        if (!this.daemonClient) {
            return;
        }

        const source = this.sources.find(s => s.name === sourceName);
        if (!source) {
            return;
        }

        const loaded = this.loadedTables.get(sourceName) ?? [];
        const page = await this.daemonClient.listSourceTables(
            sourceName,
            loaded.length,
            TABLE_TREE_PAGE_SIZE
        );
        const next = [...loaded];
        for (const table of page.tables) {
            if (!next.includes(table)) {
                next.push(table);
            }
        }
        this.loadedTables.set(sourceName, next);
        this.hasMoreTables.set(
            sourceName,
            page.truncated || (page.total_known !== undefined && next.length < page.total_known)
        );
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: DataSourceItem): vscode.TreeItem {
        return element;
    }

    async getChildren(element?: DataSourceItem): Promise<DataSourceItem[]> {
        if (element) {
            if (element.loadMore || element.tableName) {
                return [];
            }
            if (element.source.tables && element.source.tables.length > 0) {
                const tables = this.tablesForSource(element.source);
                const children = tables.map(
                    table => new DataSourceItem(element.source, table, false, this.extensionUri),
                );
                if (this.hasMoreTables.get(element.source.name)) {
                    children.push(new DataSourceItem(element.source, undefined, true, this.extensionUri));
                }
                return children;
            }
            return [];
        }

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
            this.pruneTableState(mergedSources);
            return mergedSources.map(
                s => new DataSourceItem(s, undefined, false, this.extensionUri),
            );
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

    private tablesForSource(source: CatalogSource): string[] {
        const existing = this.loadedTables.get(source.name);
        if (existing) {
            return existing;
        }

        const tables = (source.tables ?? []).slice(0, TABLE_TREE_PAGE_SIZE);
        this.loadedTables.set(source.name, tables);
        const hasMore =
            Boolean(source.connection_details?.tables_truncated) ||
            (source.tables ?? []).length > tables.length;
        this.hasMoreTables.set(source.name, hasMore);
        return tables;
    }

    private pruneTableState(sources: CatalogSource[]): void {
        const names = new Set(sources.map(source => source.name));
        for (const name of this.loadedTables.keys()) {
            if (!names.has(name)) {
                this.loadedTables.delete(name);
                this.hasMoreTables.delete(name);
            }
        }
    }
}
