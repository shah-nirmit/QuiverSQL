import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';
import { DataSourcesProvider } from './dataSourcesProvider';

export interface LineageInfo {
    table_name: string;
    columns: string[];
}

export interface QueryLineage {
    tables: string[];
    relations: LineageInfo[];
}

export class LineageItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly contextValue: string,
        public readonly iconName?: string,
        public readonly children?: LineageItem[]
    ) {
        super(label, collapsibleState);
        if (iconName) {
            this.iconPath = new vscode.ThemeIcon(iconName);
        }
    }
}

export class LineageProvider implements vscode.TreeDataProvider<LineageItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<LineageItem | undefined | null | void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private rootNodes: LineageItem[] = [];
    private activeSql: string = '';

    constructor(
        private readonly daemonClient: DaemonClient,
        private readonly dataSourcesProvider: DataSourcesProvider
    ) {}

    /** Set/update the current SQL and perform real-time lineage analysis. */
    async update(sql: string): Promise<void> {
        const trimmed = sql.trim();
        if (!trimmed) {
            this.setEmptyState('No active query block found.');
            return;
        }

        if (trimmed === this.activeSql) {
            // Avoid duplicate requests
            return;
        }
        this.activeSql = trimmed;

        try {
            const lineage = await this.daemonClient.sendRequest('get_lineage', trimmed) as QueryLineage;
            this.buildTree(lineage);
        } catch (e: any) {
            const errMsg = e.message || JSON.stringify(e);
            this.setErrorState(errMsg);
        }
    }

    /** Reset state to empty. */
    clear(): void {
        this.activeSql = '';
        this.setEmptyState('No active query block found.');
    }

    private setEmptyState(message: string): void {
        const emptyItem = new vscode.TreeItem(message, vscode.TreeItemCollapsibleState.None);
        emptyItem.iconPath = new vscode.ThemeIcon('info');
        this.rootNodes = [emptyItem as LineageItem];
        this._onDidChangeTreeData.fire();
    }

    private setErrorState(errorMessage: string): void {
        const errorRoot = new LineageItem(
            'Validation / Planning Error',
            vscode.TreeItemCollapsibleState.Expanded,
            'lineageError',
            'warning'
        );

        const errorDetail = new LineageItem(
            errorMessage,
            vscode.TreeItemCollapsibleState.None,
            'lineageErrorDetail',
            'error'
        );
        errorDetail.tooltip = errorMessage;

        // Cast because we want errorDetail to be a child
        (errorRoot as any).children = [errorDetail];
        
        this.rootNodes = [errorRoot];
        this._onDidChangeTreeData.fire();
    }

    private buildTree(lineage: QueryLineage): void {
        if (!lineage || !lineage.relations || lineage.relations.length === 0) {
            this.setEmptyState('No tables or columns referenced in this statement.');
            return;
        }

        const registered = this.dataSourcesProvider.getSources();

        this.rootNodes = lineage.relations.map(rel => {
            const matchedSource = registered.find(s => s.tableName.toLowerCase() === rel.table_name.toLowerCase());
            let iconName = 'table'; // Default fallback
            if (matchedSource) {
                switch (matchedSource.sourceType) {
                    case 'csv':
                        iconName = 'table';
                        break;
                    case 'sqlite':
                        iconName = 'database';
                        break;
                    case 'parquet':
                        iconName = 'file-binary';
                        break;
                    case 'json':
                        iconName = 'json';
                        break;
                }
            }

            const colItems = rel.columns.map(col => new LineageItem(
                col,
                vscode.TreeItemCollapsibleState.None,
                'lineageColumn',
                'symbol-field'
            ));

            return new LineageItem(
                `Table: ${rel.table_name}`,
                vscode.TreeItemCollapsibleState.Expanded,
                'lineageTable',
                iconName,
                colItems
            );
        });

        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: LineageItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: LineageItem): LineageItem[] {
        if (element) {
            return element.children || [];
        }
        return this.rootNodes;
    }
}
