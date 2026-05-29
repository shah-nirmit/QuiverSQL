import { CatalogSource } from './models';
import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';
import { DataSourcesProvider } from './dataSourcesProvider';

export interface LineageInfo {
    table_name: string;
    columns: string[];
}

// Phase 10 — fully-qualified `(table, column)` pointer used by every
// rich-lineage field. Mirrors `qsql_core::engine::ColumnRef` on the wire.
export interface ColumnRef {
    table: string;
    column: string;
}

// Phase 10 — one entry per SELECT-list expression. The `sources` array
// lists every column the expression depends on; `expression_summary` is a
// short human-readable display of the expression itself.
export interface OutputColumn {
    name: string;
    sources: ColumnRef[];
    expression_summary: string;
}

// Phase 10 — one `(left_col, right_col)` pair from a JOIN ON clause.
export interface JoinKey {
    left_col: ColumnRef;
    right_col: ColumnRef;
}

// Phase 10 — one entry per `LogicalPlan::Join` traversed.
export interface JoinLineage {
    kind: string; // "Inner" | "Left" | "Right" | "Full" | "Cross" | "LeftSemi" | ...
    left_table: string;
    right_table: string;
    on: JoinKey[];
}

// Phase 10 — one entry per aggregate function in the query.
export interface AggregateLineage {
    function: string; // uppercase, e.g. "SUM", "COUNT", "AVG"
    alias: string | null;
    inputs: ColumnRef[];
}

export interface QueryLineage {
    tables: string[];
    relations: LineageInfo[];
    // Phase 10 — additive rich fields. All optional/skip-if-empty on the
    // wire, so older daemons that haven't shipped Phase 10 yet just omit
    // them and the tree gracefully falls back to the Sources-only layout.
    output_columns?: OutputColumn[];
    joins?: JoinLineage[];
    aggregates?: AggregateLineage[];
    aliases?: Record<string, string>;
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
            const lineage = await this.daemonClient.sendRequest<QueryLineage>('get_lineage', { sql: trimmed });
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

    /**
     * Phase 10 — render the rich lineage shape as a four-section tree:
     *
     *   Output Columns (N)   — one entry per SELECT-list expression
     *   Sources (N)          — legacy "tables → columns" view
     *   Joins (N)            — one entry per Join node + ON-clause keys
     *   Aggregates (N)       — one entry per aggregate function
     *
     * Empty sections are skipped, which gives a graceful forward-compat
     * fall-back: when an older daemon returns only `tables`+`relations`,
     * only the `Sources` section appears and the tree looks identical to
     * what shipped before Phase 10.
     */
    private buildTree(lineage: QueryLineage): void {
        if (!lineage || !lineage.relations || lineage.relations.length === 0) {
            this.setEmptyState('No tables or columns referenced in this statement.');
            return;
        }

        const sections: LineageItem[] = [];

        // ----- Output Columns -----
        const outputColumns = lineage.output_columns ?? [];
        if (outputColumns.length > 0) {
            const children = outputColumns.map(oc => {
                const sourceChildren = oc.sources.map(ref => new LineageItem(
                    `${ref.table || '?'}.${ref.column}`,
                    vscode.TreeItemCollapsibleState.None,
                    'lineageOutputSource',
                    'symbol-field'
                ));
                const item = new LineageItem(
                    oc.name,
                    sourceChildren.length > 0
                        ? vscode.TreeItemCollapsibleState.Collapsed
                        : vscode.TreeItemCollapsibleState.None,
                    'lineageOutputColumn',
                    'symbol-property',
                    sourceChildren
                );
                item.description = oc.expression_summary;
                item.tooltip = `${oc.name} ← ${oc.expression_summary}`;
                return item;
            });
            sections.push(new LineageItem(
                `Output Columns (${outputColumns.length})`,
                vscode.TreeItemCollapsibleState.Expanded,
                'lineageSectionOutputColumns',
                'output',
                children
            ));
        }

        // ----- Sources (legacy tables view) -----
        const registered = this.dataSourcesProvider.getSources();
        const sourceItems = lineage.relations.map(rel => {
            const matchedSource = registered.find(
                (s: CatalogSource) => s.name.toLowerCase() === rel.table_name.toLowerCase()
            );
            let iconName = 'table';
            if (matchedSource) {
                const iconMap: Record<string, string> = {
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
                iconName = iconMap[matchedSource.kind] || 'database';
            }
            const colItems = rel.columns.map(col => new LineageItem(
                col,
                vscode.TreeItemCollapsibleState.None,
                'lineageColumn',
                'symbol-field'
            ));
            return new LineageItem(
                rel.table_name,
                vscode.TreeItemCollapsibleState.Expanded,
                'lineageTable',
                iconName,
                colItems
            );
        });
        sections.push(new LineageItem(
            `Sources (${lineage.relations.length})`,
            vscode.TreeItemCollapsibleState.Expanded,
            'lineageSectionSources',
            'database',
            sourceItems
        ));

        // ----- Joins -----
        const joins = lineage.joins ?? [];
        if (joins.length > 0) {
            const children = joins.map(j => {
                const onChildren = j.on.map(k => {
                    const left = `${k.left_col.table || '?'}.${k.left_col.column}`;
                    const right = `${k.right_col.table || '?'}.${k.right_col.column}`;
                    return new LineageItem(
                        `${left} = ${right}`,
                        vscode.TreeItemCollapsibleState.None,
                        'lineageJoinKey',
                        'key'
                    );
                });
                const item = new LineageItem(
                    `${j.kind} JOIN ${j.left_table} ↔ ${j.right_table}`,
                    onChildren.length > 0
                        ? vscode.TreeItemCollapsibleState.Collapsed
                        : vscode.TreeItemCollapsibleState.None,
                    'lineageJoin',
                    'git-merge',
                    onChildren
                );
                item.tooltip = `${j.kind} join on ${onChildren.length} key(s)`;
                return item;
            });
            sections.push(new LineageItem(
                `Joins (${joins.length})`,
                vscode.TreeItemCollapsibleState.Expanded,
                'lineageSectionJoins',
                'git-merge',
                children
            ));
        }

        // ----- Aggregates -----
        const aggregates = lineage.aggregates ?? [];
        if (aggregates.length > 0) {
            const children = aggregates.map(a => {
                const inputs = a.inputs
                    .map(ref => `${ref.table || '?'}.${ref.column}`)
                    .join(', ');
                const aliasLabel = a.alias ? ` AS ${a.alias}` : '';
                const display = `${a.function}(${inputs || '*'})${aliasLabel}`;
                const inputChildren = a.inputs.map(ref => new LineageItem(
                    `${ref.table || '?'}.${ref.column}`,
                    vscode.TreeItemCollapsibleState.None,
                    'lineageAggregateInput',
                    'symbol-field'
                ));
                const item = new LineageItem(
                    display,
                    inputChildren.length > 0
                        ? vscode.TreeItemCollapsibleState.Collapsed
                        : vscode.TreeItemCollapsibleState.None,
                    'lineageAggregate',
                    'symbol-operator',
                    inputChildren
                );
                return item;
            });
            sections.push(new LineageItem(
                `Aggregates (${aggregates.length})`,
                vscode.TreeItemCollapsibleState.Expanded,
                'lineageSectionAggregates',
                'symbol-operator',
                children
            ));
        }

        this.rootNodes = sections;
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
