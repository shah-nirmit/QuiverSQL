import * as vscode from 'vscode';

// -----------------------------------------------------------------
// Data model
// -----------------------------------------------------------------

export type SourceType = 'csv' | 'parquet' | 'json' | 'sqlite';

export interface RegisteredSource {
    tableName: string;
    sourceType: SourceType;
    /** Human-readable location: file path for files, "db_path :: table" for SQLite */
    location: string;
}

// -----------------------------------------------------------------
// Tree Item
// -----------------------------------------------------------------

class DataSourceItem extends vscode.TreeItem {
    constructor(public readonly source: RegisteredSource) {
        super(source.tableName, vscode.TreeItemCollapsibleState.None);

        const typeLabel: Record<SourceType, string> = {
            csv:     'CSV File',
            parquet: 'Parquet File',
            json:    'JSON File',
            sqlite:  'SQLite Table',
        };

        const icon: Record<SourceType, string> = {
            csv:     'table',
            parquet: 'file-binary',
            json:    'json',
            sqlite:  'database',
        };

        // Label:       employees
        // Description: → SQLite Table
        // Tooltip:     full path / connection info
        this.description = `→ ${typeLabel[source.sourceType]}`;
        this.tooltip = new vscode.MarkdownString(
            `**${source.tableName}**\n\n` +
            `Type: ${typeLabel[source.sourceType]}\n\n` +
            `Source: \`${source.location}\``
        );
        this.iconPath = new vscode.ThemeIcon(icon[source.sourceType]);
        this.contextValue = 'dqlDataSource';
    }
}

// -----------------------------------------------------------------
// Provider
// -----------------------------------------------------------------

export class DataSourcesProvider
    implements vscode.TreeDataProvider<DataSourceItem>
{
    private _onDidChangeTreeData =
        new vscode.EventEmitter<DataSourceItem | undefined | null | void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private sources: RegisteredSource[] = [];

    public getSources(): RegisteredSource[] {
        return this.sources;
    }

    /** Register or update a data source entry. */
    register(source: RegisteredSource): void {
        // Replace if same table name already exists
        const idx = this.sources.findIndex(s => s.tableName === source.tableName);
        if (idx >= 0) {
            this.sources[idx] = source;
        } else {
            this.sources.push(source);
        }
        this._onDidChangeTreeData.fire();
    }

    /** Remove a data source entry. */
    remove(tableName: string): void {
        this.sources = this.sources.filter(s => s.tableName !== tableName);
        this._onDidChangeTreeData.fire();
    }

    /** Clear all entries (e.g. on daemon restart). */
    clear(): void {
        this.sources = [];
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: DataSourceItem): vscode.TreeItem {
        return element;
    }

    getChildren(): DataSourceItem[] {
        if (this.sources.length === 0) {
            // Show a placeholder item so the panel isn't empty
            const empty = new vscode.TreeItem(
                'No data sources attached yet.',
                vscode.TreeItemCollapsibleState.None
            );
            empty.iconPath = new vscode.ThemeIcon('info');
            // Cast needed because getChildren must return DataSourceItem[]
            return [empty as unknown as DataSourceItem];
        }
        return this.sources.map(s => new DataSourceItem(s));
    }
}
