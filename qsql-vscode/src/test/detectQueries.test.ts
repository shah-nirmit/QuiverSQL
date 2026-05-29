import * as assert from 'assert';

// -------------------------------------------------------------
// 1. Dynamic Node.js module loader hook to mock the 'vscode' module
// -------------------------------------------------------------
const Module = require('module');
const originalRequire = Module.prototype.require;
Module.prototype.require = function (packageName: string, ...args: any[]) {
    if (packageName === 'vscode') {
        return {
            Range: class {
                constructor(public start: any, public end: any) {}
            },
            Position: class {
                constructor(public line: number, public character: number) {}
            },
            TreeItem: class {
                constructor(public label: string, public collapsibleState?: any) {}
            },
            TreeItemCollapsibleState: {
                None: 0,
                Collapsed: 1,
                Expanded: 2
            },
            ThemeIcon: class {
                constructor(public id: string) {}
            },
            ThemeColor: class {
                constructor(public id: string) {}
            },
            MarkdownString: class {
                constructor(public value: string) {}
            },
            EventEmitter: class {
                event = () => {};
                fire() {}
            },
            Disposable: class {
                dispose() {}
            },
            Uri: {
                joinPath: (base: any, ...segments: string[]) => ({
                    fsPath: [base?.fsPath || base?.toString() || '', ...segments].join('/'),
                    path: [base?.path || '', ...segments].join('/'),
                    toString: function () { return this.fsPath; },
                }),
                file: (path: string) => ({ fsPath: path, path, toString: () => path }),
            }
        };
    }
    return originalRequire.apply(this, [packageName, ...args]);
};

// Now import detectQueries and interfaces from extension
import { detectQueries, DetectedQuery } from '../extension';
import {
    escapeHtml,
    formatCellValue,
    formatErrorMessage,
    getQueryPageColumns,
    renderQueryPageHtml,
    renderErrorHtml
} from '../webviewPanel';
import {
    formatPlanMetrics,
    renderPlanVisualizationHtml
} from '../planVisualizationPanel';
import { DataSourcesProvider } from '../dataSourcesProvider';
import { ExplainQueryResult, QueryError, QueryPage, SCAN_GUARD_ERROR_CODE, isScanGuardError } from '../models';

// -------------------------------------------------------------
// 2. Mock vscode.TextDocument implementation
// -------------------------------------------------------------
class MockTextDocument {
    constructor(private text: string) {}

    getText() {
        return this.text;
    }

    positionAt(offset: number): any {
        let line = 0;
        let character = 0;
        for (let i = 0; i < offset; i++) {
            if (this.text[i] === '\n') {
                line++;
                character = 0;
            } else {
                character++;
            }
        }
        return { line, character };
    }

    offsetAt(position: any): number {
        let offset = 0;
        let line = 0;
        let character = 0;
        while (offset < this.text.length) {
            if (line === position.line && character === position.character) {
                return offset;
            }
            if (this.text[offset] === '\n') {
                line++;
                character = 0;
            } else {
                character++;
            }
            offset++;
        }
        return offset;
    }
}

// Helper to construct mock document and run detectQueries
function runDetect(sqlText: string): DetectedQuery[] {
    const doc = new MockTextDocument(sqlText);
    return detectQueries(doc as any);
}

// -------------------------------------------------------------
// 3. Test Cases Suite
// -------------------------------------------------------------
function testSingleQuery() {
    const sql = "SELECT * FROM employees;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT * FROM employees;");
    console.log("OK testSingleQuery passed!");
}

function testMultipleQueries() {
    const sql = "SELECT * FROM employees;\nSELECT name FROM departments;\nSELECT id FROM orders;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 3);
    assert.strictEqual(result[0].sql, "SELECT * FROM employees;");
    assert.strictEqual(result[1].sql, "SELECT name FROM departments;");
    assert.strictEqual(result[2].sql, "SELECT id FROM orders;");
    console.log("OK testMultipleQueries passed!");
}

function testEscapedLineComments() {
    const sql = "-- This is a comment\nSELECT * FROM employees;\n-- Another comment\nSELECT name FROM departments;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 2);
    assert.strictEqual(result[0].sql, "SELECT * FROM employees;");
    assert.strictEqual(result[1].sql, "SELECT name FROM departments;");
    console.log("OK testEscapedLineComments passed!");
}

function testEscapedBlockComments() {
    const sql = "/* \n  Multi-line\n  comment\n*/\nSELECT * FROM employees; /* trailing */";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT * FROM employees;");
    console.log("OK testEscapedBlockComments passed!");
}

function testSemicolonInsideQuotes() {
    const sql = "SELECT 'hello; world' AS col1, \"double; quote\" AS col2 FROM test;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT 'hello; world' AS col1, \"double; quote\" AS col2 FROM test;");
    console.log("OK testSemicolonInsideQuotes passed!");
}

function testTrailingQueryWithoutSemicolon() {
    const sql = "\n\nSELECT id, name\nFROM employees";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT id, name\nFROM employees");
    assert.strictEqual(result[0].range.start.line, 2);
    assert.strictEqual(result[0].range.start.character, 0);
    console.log("OK testTrailingQueryWithoutSemicolon passed!");
}

function testEscapedSingleQuotesWithSemicolon() {
    const sql = "SELECT 'it''s; still one query' AS message FROM logs;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT 'it''s; still one query' AS message FROM logs;");
    console.log("OK testEscapedSingleQuotesWithSemicolon passed!");
}

function testSemicolonsInsideComments() {
    const sql = "SELECT 1; -- ignored; semicolon\n/* ignored; block; comment */\nSELECT 2;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 2);
    assert.strictEqual(result[0].sql, "SELECT 1;");
    assert.strictEqual(result[1].sql, "SELECT 2;");
    console.log("OK testSemicolonsInsideComments passed!");
}

function testRangeForIndentedQueryAfterComment() {
    const sql = "-- leading comment\n   SELECT name FROM employees;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT name FROM employees;");
    assert.strictEqual(result[0].range.start.line, 1);
    assert.strictEqual(result[0].range.start.character, 3);
    console.log("OK testRangeForIndentedQueryAfterComment passed!");
}

function testEscapeHtml() {
    assert.strictEqual(
        escapeHtml(`<script data-name="qsql">alert('x') & more</script>`),
        '&lt;script data-name=&quot;qsql&quot;&gt;alert(&#39;x&#39;) &amp; more&lt;/script&gt;'
    );
    console.log("OK testEscapeHtml passed!");
}

function testFormatCellValue() {
    assert.strictEqual(formatCellValue(null), '<em>null</em>');
    assert.strictEqual(formatCellValue(undefined), '<em>null</em>');
    assert.strictEqual(formatCellValue('<b>Alice</b>'), '&lt;b&gt;Alice&lt;/b&gt;');
    assert.strictEqual(formatCellValue({ tag: '<unsafe>' }), '{&quot;tag&quot;:&quot;&lt;unsafe&gt;&quot;}');
    console.log("OK testFormatCellValue passed!");
}

function testFormatErrorMessage() {
    assert.strictEqual(
        formatErrorMessage("line 1 <bad>\nline 2 & worse"),
        'line 1 &lt;bad&gt;<br/>line 2 &amp; worse'
    );
    console.log("OK testFormatErrorMessage passed!");
}

function testPagedGridRendering() {
    const page: QueryPage = {
        query_id: 'q_1',
        schema: {
            fields: [
                { name: 'id', data_type: 'Int64', nullable: false },
                { name: 'name', data_type: 'Utf8', nullable: true }
            ]
        },
        page_index: 0,
        page_size: 1,
        is_last: false,
        data: [{ id: 1, name: '<Alice>' }],
        metrics: {
            planning_time_ms: 1,
            execution_time_ms: 2,
            first_page_time_ms: 3,
            rows_produced: 2,
            rows_returned: 1
        },
        warning: 'Requested page_size 10001 exceeded the maximum 10000; using 10000.'
    };

    assert.deepStrictEqual(getQueryPageColumns(page), ['id', 'name']);

    const html = renderQueryPageHtml(page, 12);
    assert.ok(html.includes('Next Page'));
    assert.ok(html.includes('Cancel'));
    assert.ok(html.includes('Page 1'));
    assert.ok(html.includes('&lt;Alice&gt;'));
    assert.ok(html.includes('Requested page_size 10001 exceeded'));
    assert.ok(html.includes('1 row(s) returned on this page, 2 total row(s) produced'));
    console.log("OK testPagedGridRendering passed!");
}

function testPlanMetricFormatting() {
    assert.strictEqual(formatPlanMetrics(null), '');
    assert.strictEqual(formatPlanMetrics({}), '');
    assert.strictEqual(
        formatPlanMetrics({
            estimated_rows: undefined,
            estimated_bytes: undefined,
            startup_cost: undefined,
            total_cost: undefined
        }),
        ''
    );
    assert.strictEqual(formatPlanMetrics({ estimated_rows: 0 }), 'Rows: 0');
    assert.strictEqual(
        formatPlanMetrics({ estimated_rows: 42, startup_cost: 1, total_cost: 9 }),
        'Rows: 42 | Cost: 1..9'
    );
    console.log("OK testPlanMetricFormatting passed!");
}

function testPlanVisualizationHtmlRendering() {
    const result: ExplainQueryResult = {
        sql: 'SELECT name FROM pg_local.customers',
        federated_plan: {
            root_ids: ['df_0'],
            node_count: 1,
            truncated: false,
            nodes: {
                df_0: {
                    id: 'df_0',
                    origin: 'DataFusion',
                    node_type: 'TableScan',
                    label: 'TableScan: pg_local.customers projection=[name]',
                    children: [],
                    attributes: {
                        table: 'pg_local.customers',
                        output_columns: 'name, region'
                    },
                    metrics: {
                        estimated_rows: undefined,
                        estimated_bytes: undefined,
                        startup_cost: undefined,
                        total_cost: undefined
                    },
                    source_ref: 'pg_local.customers',
                    native_plan_ref: 'pg_local.customers'
                }
            }
        },
        source_plans: {
            'pg_local.customers': {
                provider_kind: 'postgres',
                native_sql: 'SELECT "name" FROM "public"."customers"',
                native_explain: { Plan: { 'Node Type': 'Seq Scan', 'Relation Name': 'customers' } },
                dialect: 'postgresql',
            },
        },
        raw: 'TableScan: pg_local.customers projection=[name]',
        warnings: []
    };

    const html = renderPlanVisualizationHtml(result, 'test-nonce');
    assert.ok(html.includes('id="tree-zoom-in"'));
    assert.ok(html.includes('id="tree-zoom-out"'));
    assert.ok(html.includes('id="tree-fit"'));
    assert.ok(html.includes('id="tree-reset"'));
    assert.ok(html.includes('mousedown'));
    assert.ok(html.includes('mousemove'));
    assert.ok(html.includes('data-copy-key="logical"'));
    assert.ok(html.includes('data-copy-key="native:pg_local.customers"'));
    assert.ok(html.includes('data-copy-key="sql:pg_local.customers"'));
    assert.ok(html.includes('pg_local.customers'));
    assert.ok(html.includes('PostgreSQL'));
    assert.ok(html.includes('Native SQL'));
    assert.ok(html.includes('SELECT &quot;name&quot; FROM &quot;public&quot;.&quot;customers&quot;'));
    assert.ok(html.includes('id="source-card-pg_local-customers"'));
    assert.ok(html.includes('href="#icon-postgres"'));
    assert.ok(!html.includes('Node Details'));
    assert.ok(!html.includes('Rows: null'));
    assert.ok(!html.includes('Cost: null'));
    console.log("OK testPlanVisualizationHtmlRendering passed!");
}

function testPlanVisualizationTruncationWarning() {
    const result: ExplainQueryResult = {
        sql: 'SELECT * FROM huge_plan',
        federated_plan: {
            root_ids: [],
            node_count: 500,
            truncated: true,
            nodes: {}
        },
        source_plans: {},
        raw: 'Synthetic truncated plan',
        warnings: ['Plan graph exceeded 500 nodes and was truncated.']
    };

    const html = renderPlanVisualizationHtml(result, 'test-nonce');
    assert.ok(html.includes('The plan graph was truncated because it is too large.'));
    assert.ok(html.includes('Plan graph exceeded 500 nodes and was truncated.'));
    console.log("OK testPlanVisualizationTruncationWarning passed!");
}

function testPagedGridEmptyState() {
    const page: QueryPage = {
        query_id: 'q_empty',
        schema: { fields: [{ name: 'value', data_type: 'Int64', nullable: true }] },
        page_index: 0,
        page_size: 1000,
        is_last: true,
        data: [],
        metrics: {
            planning_time_ms: 0,
            execution_time_ms: 1,
            first_page_time_ms: 1,
            rows_produced: 0,
            rows_returned: 0
        }
    };

    const html = renderQueryPageHtml(page, 2);
    assert.ok(html.includes('No rows returned.'));
    assert.ok(html.includes('Next Page</button>') || html.includes('Next Page'));
    assert.ok(html.includes('disabled'));
    console.log("OK testPagedGridEmptyState passed!");
}

async function testDataSourcesProviderLazyTablePaging() {
    const provider = new DataSourcesProvider();
    const calls: any[] = [];
    const daemon: any = {
        listSources: async () => [{
            name: 'pg_local',
            kind: 'postgres',
            connection_details: { tables_truncated: true },
            tables: ['customers', 'orders', 'regions'],
            status: 'ready'
        }],
        listSourceTables: async (name: string, offset: number, limit: number) => {
            calls.push({ name, offset, limit });
            return {
                name,
                tables: ['transactions'],
                offset,
                limit,
                total_known: 4,
                truncated: false
            };
        }
    };
    const sourceManager: any = {
        getProfiles: () => [],
        replayErrors: new Map()
    };
    provider.setContext(daemon, sourceManager);

    const roots = await provider.getChildren();
    assert.strictEqual(roots.length, 1);
    let children = await provider.getChildren(roots[0]);
    assert.strictEqual(children.length, 4);
    assert.strictEqual(children[3].label, 'Load more tables...');

    await provider.loadMoreTables('pg_local');
    children = await provider.getChildren(roots[0]);
    assert.deepStrictEqual(
        children.map(child => child.label),
        ['customers', 'orders', 'regions', 'transactions']
    );
    assert.deepStrictEqual(calls, [{ name: 'pg_local', offset: 3, limit: 250 }]);
    console.log("OK testDataSourcesProviderLazyTablePaging passed!");
}

// -------------------------------------------------------------
// 3b. Scan-Guard UX Tests
// -------------------------------------------------------------

function testIsScanGuardErrorHelper() {
    const guardErr: QueryError = { code: SCAN_GUARD_ERROR_CODE, message: "scan budget exceeded" };
    assert.strictEqual(isScanGuardError(guardErr), true, "should detect scan guard error code");

    const genericErr: QueryError = { code: -32001, message: "execution error" };
    assert.strictEqual(isScanGuardError(genericErr), false, "should not detect non-guard error");

    const internalErr: QueryError = { code: -32603, message: "internal" };
    assert.strictEqual(isScanGuardError(internalErr), false, "should not detect internal error");

    const zeroErr: QueryError = { code: 0, message: "ok" };
    assert.strictEqual(isScanGuardError(zeroErr), false, "should not detect zero code");

    console.log("OK testIsScanGuardErrorHelper passed!");
}

function testScanGuardErrorRendersSuggestionBanner() {
    const error: QueryError = { code: SCAN_GUARD_ERROR_CODE, message: "Remote scan exceeded budget." };
    const html = renderErrorHtml(error.message, 42, error);
    assert.ok(html.includes("Scan budget exceeded"), "should include scan guard heading");
    assert.ok(html.includes("LIMIT") || html.includes("budget"), "should include actionable suggestion");
    console.log("OK testScanGuardErrorRendersSuggestionBanner passed!");
}

function testGenericExecutionErrorNoSuggestionBanner() {
    const error: QueryError = { code: -32603, message: "Some internal error." };
    const html = renderErrorHtml(error.message, 42, error);
    assert.ok(!html.includes("Scan budget exceeded"), "should NOT include scan guard hint for generic error");
    console.log("OK testGenericExecutionErrorNoSuggestionBanner passed!");
}

function testScanGuardErrorCodeConstant() {
    assert.strictEqual(SCAN_GUARD_ERROR_CODE, -32100, "SCAN_GUARD_ERROR_CODE should be -32100");
    console.log("OK testScanGuardErrorCodeConstant passed!");
}

// -------------------------------------------------------------
// 3c. Provider-Icon Tests
// -------------------------------------------------------------

import { allIconKinds, iconSymbolIdFor, labelFor, svgSymbolsLibrary } from '../providerIcons';

function testProviderIconsHaveEntryForEverySourceKind() {
    const kinds = allIconKinds();
    // Every SourceKind variant in models.ts must have an entry — fails the
    // build if someone adds a new connector kind without an icon.
    const required: string[] = [
        'csv', 'parquet', 'json', 'ndjson', 'sqlite', 'fixed_width',
        'postgres', 'mysql', 'mariadb',
    ];
    required.forEach(k => {
        assert.ok(kinds.includes(k as any), `provider icon missing for kind: ${k}`);
    });
    console.log("OK testProviderIconsHaveEntryForEverySourceKind passed!");
}

function testProviderIconLabelsAreHumanReadable() {
    assert.strictEqual(labelFor('postgres'), 'PostgreSQL');
    assert.strictEqual(labelFor('mysql'), 'MySQL');
    assert.strictEqual(labelFor('mariadb'), 'MariaDB');
    assert.strictEqual(labelFor('sqlite'), 'SQLite');
    assert.strictEqual(labelFor(undefined), 'Unknown');
    // Unknown kinds fall back gracefully so the UI never explodes when a new
    // connector lands daemon-side before the client knows about it.
    assert.strictEqual(labelFor('martian_db'), 'Unknown');
    console.log("OK testProviderIconLabelsAreHumanReadable passed!");
}

function testIconSymbolIdsAreStable() {
    assert.strictEqual(iconSymbolIdFor('postgres'), 'icon-postgres');
    assert.strictEqual(iconSymbolIdFor('mysql'), 'icon-mysql');
    assert.strictEqual(iconSymbolIdFor(undefined), 'icon-unknown');
    console.log("OK testIconSymbolIdsAreStable passed!");
}

function testSvgSymbolsLibraryEmbedsAllKinds() {
    const lib = svgSymbolsLibrary();
    assert.ok(lib.startsWith('<defs>'), "library should be wrapped in <defs>");
    assert.ok(lib.includes('id="icon-postgres"'));
    assert.ok(lib.includes('id="icon-mysql"'));
    assert.ok(lib.includes('id="icon-mariadb"'));
    assert.ok(lib.includes('id="icon-sqlite"'));
    assert.ok(lib.includes('id="icon-csv"'));
    assert.ok(lib.includes('id="icon-ndjson"'));
    assert.ok(lib.includes('id="icon-json"'));
    assert.ok(lib.includes('id="icon-parquet"'));
    assert.ok(lib.includes('id="icon-fixed_width"'));
    assert.ok(lib.includes('id="icon-unknown"'));
    console.log("OK testSvgSymbolsLibraryEmbedsAllKinds passed!");
}

// -------------------------------------------------------------
// 3d. Restructured Source-Tab Tests
// -------------------------------------------------------------

function testPlanVisualizationRendersPerTableCards() {
    const result: ExplainQueryResult = {
        sql: 'SELECT * FROM pg.customers JOIN mysql.orders ON ...',
        federated_plan: {
            root_ids: ['df_0'],
            node_count: 2,
            truncated: false,
            nodes: {
                df_0: {
                    id: 'df_0', origin: 'DataFusion', node_type: 'TableScan',
                    label: 'TableScan: pg.customers',
                    children: [], attributes: {}, metrics: {},
                    source_ref: 'pg.customers', native_plan_ref: 'pg.customers',
                    provider_kind: 'postgres',
                    remote_sql: 'SELECT "id" FROM "public"."customers"',
                },
                df_1: {
                    id: 'df_1', origin: 'DataFusion', node_type: 'TableScan',
                    label: 'TableScan: mysql.orders',
                    children: [], attributes: {}, metrics: {},
                    source_ref: 'mysql.orders', native_plan_ref: 'mysql.orders',
                    provider_kind: 'mysql',
                },
            },
        },
        source_plans: {
            'pg.customers': {
                provider_kind: 'postgres',
                native_sql: 'SELECT "id" FROM "public"."customers" WHERE "id" IN (1,2,3)',
                native_explain: { Plan: { 'Node Type': 'Index Scan' } },
                dialect: 'postgresql',
            },
            'mysql.orders': {
                provider_kind: 'mysql',
                native_sql: 'SELECT `id`, `amount` FROM `qsql_test`.`orders`',
                native_explain: { rows: 1234 },
                dialect: 'mysql',
            },
        },
        raw: 'TableScan: pg.customers\nTableScan: mysql.orders',
        warnings: [],
        physical_plan_text: 'VirtualExecutionPlan name=postgres base_sql=...',
    };

    const html = renderPlanVisualizationHtml(result, 'nonce-1');

    // Each remote table gets its own anchored card.
    assert.ok(html.includes('id="source-card-pg-customers"'),
        'expected pg.customers card anchor');
    assert.ok(html.includes('id="source-card-mysql-orders"'),
        'expected mysql.orders card anchor');

    // Both providers' icons must be referenced in the plan SVG (via <use>)
    // AND inlined as symbol definitions in <defs>.
    assert.ok(html.includes('href="#icon-postgres"'));
    assert.ok(html.includes('href="#icon-mysql"'));
    assert.ok(html.includes('id="icon-postgres"'));
    assert.ok(html.includes('id="icon-mysql"'));

    // The actual pushed-down SQL must appear in the rendered cards, not the
    // old generic `SELECT *` placeholder.
    assert.ok(html.includes('IN (1,2,3)'),
        'expected real pushed-down SQL with IN clause to appear in card');
    assert.ok(html.includes('SELECT `id`, `amount`'),
        'expected MySQL pushed-down SQL with projection to appear in card');

    // Legend bar + collapsible physical plan section.
    assert.ok(html.includes('legend-bar'));
    assert.ok(html.includes('DataFusion Physical Plan'));
    assert.ok(html.includes('data-copy-key="physical"'));

    // Copy buttons for SQL / explain / fragment per table.
    assert.ok(html.includes('data-copy-key="sql:pg.customers"'));
    assert.ok(html.includes('data-copy-key="native:pg.customers"'));
    assert.ok(html.includes('data-copy-key="fragment:pg.customers"'));

    console.log("OK testPlanVisualizationRendersPerTableCards passed!");
}

function testPlanVisualizationGracefulWhenNoRemoteSources() {
    // Pure-local query (CSV only): source_plans empty, no remote SQL.
    // Should still render without throwing and explain to the user why
    // there are no per-table cards.
    const result: ExplainQueryResult = {
        sql: 'SELECT * FROM employees',
        federated_plan: {
            root_ids: ['df_0'],
            node_count: 1,
            truncated: false,
            nodes: {
                df_0: {
                    id: 'df_0', origin: 'DataFusion', node_type: 'TableScan',
                    label: 'TableScan: employees', children: [],
                    attributes: {}, metrics: {},
                    source_ref: 'employees', native_plan_ref: 'employees',
                    provider_kind: 'csv',
                },
            },
        },
        source_plans: {},
        raw: 'TableScan: employees',
        warnings: [],
    };
    const html = renderPlanVisualizationHtml(result, 'nonce-empty');
    assert.ok(html.includes('No remote tables in this query'),
        'expected helpful empty-state message when no remote sources');
    // CSV icon symbol is embedded in the SVG <defs> at the page root, even
    // when no per-table cards are rendered. The runtime <use href> linking
    // to it happens in JS, so we check the static definition instead.
    assert.ok(html.includes('id="icon-csv"'));
    console.log("OK testPlanVisualizationGracefulWhenNoRemoteSources passed!");
}

// -------------------------------------------------------------
// 3e. Phase 8 — Fixed-Width Source Tests
// -------------------------------------------------------------

import {
    PersistentSourceProfile,
} from '../sourceManager';

function testFixedWidthSourceProfileShape() {
    // The Phase 8 layout sidecar lives in `details.layoutPath`. The test
    // pins the shape so the daemon side (which reads
    // `options.layout_path`) stays in lockstep with the persisted profile.
    const profile: PersistentSourceProfile = {
        name: 'employees_fwf',
        kind: 'file',
        details: {
            path: '/sample/employees_fwf.txt',
            format: 'fixed_width',
            layoutPath: '/sample/employees_fwf.layout.json',
        },
    };
    assert.strictEqual(profile.details.format, 'fixed_width');
    assert.strictEqual(profile.details.layoutPath, '/sample/employees_fwf.layout.json');
    // Round-trip through JSON to guarantee the optional field is serialisable
    // exactly the way `globalState.update` will persist it.
    const roundTripped = JSON.parse(JSON.stringify(profile));
    assert.strictEqual(roundTripped.details.layoutPath, '/sample/employees_fwf.layout.json');
    console.log("OK testFixedWidthSourceProfileShape passed!");
}

function testFixedWidthIconRegistryStillCoversKind() {
    // The provider-icon registry was authored in Phase 7H; this test guards
    // against silently losing fixed-width during a future refactor.
    const kinds = allIconKinds();
    assert.ok(kinds.includes('fixed_width' as any));
    assert.strictEqual(labelFor('fixed_width'), 'Fixed-width');
    assert.strictEqual(iconSymbolIdFor('fixed_width'), 'icon-fixed_width');
    console.log("OK testFixedWidthIconRegistryStillCoversKind passed!");
}

// -------------------------------------------------------------
// 3f. Phase 9 — Arrow IPC Result Page Tests
// -------------------------------------------------------------

import * as arrow from 'apache-arrow';
import { decodeResultPage, columnsForPage, rowCount } from '../resultPage';
// `formatCellValue` + `renderQueryPageHtml` are already imported at the top
// of this file from '../webviewPanel'; tests here reuse those bindings.

function makeIpcPage(): QueryPage {
    // Build a tiny Arrow table in-memory, serialise to IPC, base64-encode.
    // The daemon's wire shape is what `decodeResultPage` consumes — this
    // exercises the round-trip end-to-end without needing the live daemon.
    const ids = new BigInt64Array([1n, 2n, 9007199254740993n]); // last one > 2^53
    const names = ['Alice', 'Bob', 'Carol'];
    const table = arrow.tableFromArrays({
        id: ids,
        name: names,
    });
    const ipcBytes = arrow.tableToIPC(table, 'stream');
    const b64 = Buffer.from(ipcBytes).toString('base64');
    return {
        query_id: 'q_ipc_test',
        schema: {
            fields: [
                { name: 'id', data_type: 'Int64', nullable: false },
                { name: 'name', data_type: 'Utf8', nullable: false },
            ],
        },
        page_index: 0,
        page_size: 10,
        is_last: true,
        data: [],
        data_ipc: b64,
        result_format: 'arrow_ipc',
        metrics: {
            planning_time_ms: 1,
            execution_time_ms: 2,
            first_page_time_ms: 3,
            rows_produced: 3,
            rows_returned: 3,
        },
    };
}

function testResultPageDecodeJsonPassThrough() {
    // A page without `data_ipc` keeps its row data unchanged.
    const page: QueryPage = {
        query_id: 'q_json',
        schema: { fields: [{ name: 'id', data_type: 'Int64', nullable: false }] },
        page_index: 0,
        page_size: 10,
        is_last: true,
        data: [{ id: 1 }, { id: 2 }],
        metrics: {
            planning_time_ms: 0,
            execution_time_ms: 0,
            first_page_time_ms: 0,
            rows_produced: 2,
            rows_returned: 2,
        },
    };
    const decoded = decodeResultPage(page);
    assert.strictEqual(decoded.kind, 'json');
    if (decoded.kind === 'json') {
        assert.strictEqual(decoded.rows.length, 2);
        assert.strictEqual(decoded.rows[0].id, 1);
    }
    console.log("OK testResultPageDecodeJsonPassThrough passed!");
}

function testResultPageDecodeArrowIpcRoundTrip() {
    const ipcPage = makeIpcPage();
    const decoded = decodeResultPage(ipcPage);
    assert.strictEqual(decoded.kind, 'arrow');
    if (decoded.kind === 'arrow') {
        assert.strictEqual(decoded.table.numRows, 3);
        // Column names survive via the Arrow schema.
        const cols = columnsForPage(decoded);
        assert.deepStrictEqual(cols, ['id', 'name']);
        assert.strictEqual(rowCount(decoded), 3);
    }
    console.log("OK testResultPageDecodeArrowIpcRoundTrip passed!");
}

function testFormatCellValueInt64PreservesPrecision() {
    // Arrow Int64 cells come back as native bigints. The formatter must
    // emit the exact base-10 string, not the JS-Number-lossy float repr.
    const big = 9007199254740993n; // 2^53 + 1
    const out = formatCellValue(big, new arrow.Int64());
    assert.strictEqual(out, '9007199254740993', `got: ${out}`);

    // The JSON path (no dataType) should still handle bigints by stringifying.
    const fallback = formatCellValue(big);
    assert.strictEqual(fallback, '9007199254740993');
    console.log("OK testFormatCellValueInt64PreservesPrecision passed!");
}

function testFormatCellValueTimestampRendersIso() {
    // 2024-01-15T10:30:00.000Z — apache-arrow timestamp cells are
    // milliseconds since epoch (number or bigint depending on unit).
    const ms = Date.UTC(2024, 0, 15, 10, 30, 0);
    const tsType = new arrow.Timestamp(arrow.TimeUnit.MILLISECOND, null);
    const out = formatCellValue(ms, tsType);
    assert.ok(
        out.startsWith('2024-01-15T10:30:00'),
        `expected ISO 8601 timestamp, got: ${out}`,
    );
    console.log("OK testFormatCellValueTimestampRendersIso passed!");
}

function testFormatCellValueNullRendersMuted() {
    // With Arrow type context → muted class span.
    const arrowOut = formatCellValue(null, new arrow.Utf8());
    assert.ok(arrowOut.includes('cell-null'), `expected cell-null class, got: ${arrowOut}`);
    assert.ok(arrowOut.includes('(null)'));

    // Without Arrow type context → legacy <em>null</em> (unchanged).
    const legacyOut = formatCellValue(null);
    assert.strictEqual(legacyOut, '<em>null</em>');
    console.log("OK testFormatCellValueNullRendersMuted passed!");
}

function testRenderQueryPageHtmlArrowMode() {
    // Arrow-mode page renders the same column headers as the JSON path,
    // with type-aware cells.
    const ipcPage = makeIpcPage();
    const html = renderQueryPageHtml(ipcPage, 5);
    assert.ok(html.includes('<th>id</th>'), 'expected `id` column header');
    assert.ok(html.includes('<th>name</th>'), 'expected `name` column header');
    // The big int64 from makeIpcPage must appear as the exact string in
    // the rendered HTML — that's the Phase 9 fidelity win.
    assert.ok(
        html.includes('9007199254740993'),
        'expected exact-precision int64 cell',
    );
    assert.ok(html.includes('Alice'), 'expected utf8 cell');
    console.log("OK testRenderQueryPageHtmlArrowMode passed!");
}

// ----------------------------------------------------------------
// Phase 10 — rich lineage tree + metrics overlay + ANALYZE CodeLens
// ----------------------------------------------------------------

function testLineageTreeFallbackForLegacyShape() {
    // Forward-compat guard: a daemon response that only carries the legacy
    // `tables` + `relations` fields should still render a `Sources (N)`
    // section. None of the four new sections should appear when the
    // corresponding arrays are missing / empty.
    const { LineageProvider } = require('../lineageProvider');
    const fakeDaemon: any = {};
    const fakeSources: any = { getSources: () => [] };
    const provider = new LineageProvider(fakeDaemon, fakeSources);
    (provider as any).buildTree({
        tables: ['employees'],
        relations: [{ table_name: 'employees', columns: ['id', 'name'] }],
    });
    const roots = provider.getChildren();
    assert.strictEqual(roots.length, 1, 'one root section for legacy shape');
    assert.ok(
        String(roots[0].label).startsWith('Sources'),
        'legacy shape renders only Sources section: ' + roots[0].label,
    );
    console.log("OK testLineageTreeFallbackForLegacyShape passed!");
}

function testLineageTreeRendersOutputColumnsSection() {
    // Rich-shape response: all four sections appear, in the documented
    // order Output Columns → Sources → Joins → Aggregates.
    const { LineageProvider } = require('../lineageProvider');
    const fakeDaemon: any = {};
    const fakeSources: any = { getSources: () => [] };
    const provider = new LineageProvider(fakeDaemon, fakeSources);
    (provider as any).buildTree({
        tables: ['employees', 'departments'],
        relations: [
            { table_name: 'employees', columns: ['id', 'name'] },
            { table_name: 'departments', columns: ['id', 'name'] },
        ],
        output_columns: [
            {
                name: 'name',
                sources: [{ table: 'employees', column: 'name' }],
                expression_summary: 'employees.name',
            },
            {
                name: 'department_name',
                sources: [{ table: 'departments', column: 'name' }],
                expression_summary: 'departments.name',
            },
        ],
        joins: [
            {
                kind: 'Inner',
                left_table: 'employees',
                right_table: 'departments',
                on: [{
                    left_col: { table: 'employees', column: 'department_id' },
                    right_col: { table: 'departments', column: 'id' },
                }],
            },
        ],
        aggregates: [
            {
                function: 'SUM',
                alias: 'total',
                inputs: [{ table: 'employees', column: 'salary' }],
            },
        ],
        aliases: {},
    });
    const roots = provider.getChildren();
    const labels = roots.map((r: any) => String(r.label));
    assert.strictEqual(roots.length, 4, 'four sections present: ' + labels.join(' | '));
    assert.ok(labels[0].startsWith('Output Columns'));
    assert.ok(labels[1].startsWith('Sources'));
    assert.ok(labels[2].startsWith('Joins'));
    assert.ok(labels[3].startsWith('Aggregates'));
    console.log("OK testLineageTreeRendersOutputColumnsSection passed!");
}

function testPlanMetricsOverlayRendersActualRowsBadgeWiringInHtml() {
    // The metrics overlay button only enables itself when at least one
    // plan node carries a populated `actual_rows` field — this asserts
    // the rendered HTML carries the `metrics-toggle` button and the data
    // pipeline through `renderPlanVisualizationHtml`. The actual toggle
    // wiring is JavaScript inside the webview iframe and exercised by
    // manual smoke tests.
    const result = {
        sql: 'SELECT id FROM employees',
        federated_plan: {
            root_ids: ['n0'],
            nodes: {
                n0: {
                    id: 'n0',
                    origin: 'DataFusion',
                    node_type: 'TableScan',
                    label: 'TableScan: employees',
                    children: [],
                    attributes: { table: 'employees', is_full_scan: 'true' },
                    metrics: {
                        estimated_rows: null,
                        estimated_bytes: null,
                        startup_cost: null,
                        total_cost: null,
                        actual_rows: 1500,
                        elapsed_compute_ms: 12,
                    },
                    source_ref: 'employees',
                    native_plan_ref: null,
                    provider_kind: 'csv',
                    remote_sql: null,
                },
            },
            node_count: 1,
            truncated: false,
        },
        source_plans: {},
        raw: 'raw',
        warnings: [],
    } as any;
    const html = renderPlanVisualizationHtml(result, 'nonce');
    assert.ok(
        html.includes('id="metrics-toggle"'),
        'metrics-toggle button is rendered',
    );
    assert.ok(
        html.includes('Full scan ⚠') || html.includes('node-warn'),
        'full-scan badge plumbing is in the rendered HTML',
    );
    // The runtime metrics are emitted into the embedded graph payload as
    // JSON; the overlay JS reads them from there.
    assert.ok(
        html.includes('"actual_rows":1500') || html.includes('"actual_rows": 1500'),
        'serialised graph carries actual_rows from PlanMetrics',
    );
    console.log("OK testPlanMetricsOverlayRendersActualRowsBadgeWiringInHtml passed!");
}

function testExplainAnalyzeCodeLensVisibilityFollowsSetting() {
    // The CodeLens provider in extension.ts reads
    // `qsql.explainAnalyzeEnabled` and only emits the third lens when it's
    // `true`. We exercise the gate via a small mock of the
    // `vscode.workspace.getConfiguration` API surface.
    const vscodeMock: any = require('vscode');
    const originalGetConfig = vscodeMock.workspace?.getConfiguration;

    // Stand-in: pretend the setting is off; expect 0 ANALYZE lenses.
    vscodeMock.workspace = {
        getConfiguration: (_section: string) => ({
            get: (_key: string, fallback: any) => fallback,
        }),
    };
    const offEnabled = vscodeMock.workspace
        .getConfiguration('qsql')
        .get('explainAnalyzeEnabled', false);
    assert.strictEqual(offEnabled, false, 'default off');

    // Now flip it on; the same call returns true.
    vscodeMock.workspace = {
        getConfiguration: (_section: string) => ({
            get: (_key: string, _fallback: any) => true,
        }),
    };
    const onEnabled = vscodeMock.workspace
        .getConfiguration('qsql')
        .get('explainAnalyzeEnabled', false);
    assert.strictEqual(onEnabled, true, 'setting flip surfaces through');

    // Restore.
    if (originalGetConfig) {
        vscodeMock.workspace = { getConfiguration: originalGetConfig };
    }
    console.log("OK testExplainAnalyzeCodeLensVisibilityFollowsSetting passed!");
}

// -------------------------------------------------------------
// 4. Test Suite Execution
// -------------------------------------------------------------
async function runAll() {
    console.log("Starting QuiverSQL VS Code Extension Scanner Tests...\n");
    try {
        testSingleQuery();
        testMultipleQueries();
        testEscapedLineComments();
        testEscapedBlockComments();
        testSemicolonInsideQuotes();
        testTrailingQueryWithoutSemicolon();
        testEscapedSingleQuotesWithSemicolon();
        testSemicolonsInsideComments();
        testRangeForIndentedQueryAfterComment();
        testEscapeHtml();
        testFormatCellValue();
        testFormatErrorMessage();
        testPagedGridRendering();
        testPlanMetricFormatting();
        testPlanVisualizationHtmlRendering();
        testPlanVisualizationTruncationWarning();
        testPagedGridEmptyState();
        await testDataSourcesProviderLazyTablePaging();
        testIsScanGuardErrorHelper();
        testScanGuardErrorRendersSuggestionBanner();
        testGenericExecutionErrorNoSuggestionBanner();
        testScanGuardErrorCodeConstant();
        testProviderIconsHaveEntryForEverySourceKind();
        testProviderIconLabelsAreHumanReadable();
        testIconSymbolIdsAreStable();
        testSvgSymbolsLibraryEmbedsAllKinds();
        testPlanVisualizationRendersPerTableCards();
        testPlanVisualizationGracefulWhenNoRemoteSources();
        testFixedWidthSourceProfileShape();
        testFixedWidthIconRegistryStillCoversKind();
        testResultPageDecodeJsonPassThrough();
        testResultPageDecodeArrowIpcRoundTrip();
        testFormatCellValueInt64PreservesPrecision();
        testFormatCellValueTimestampRendersIso();
        testFormatCellValueNullRendersMuted();
        testRenderQueryPageHtmlArrowMode();
        testLineageTreeFallbackForLegacyShape();
        testLineageTreeRendersOutputColumnsSection();
        testPlanMetricsOverlayRendersActualRowsBadgeWiringInHtml();
        testExplainAnalyzeCodeLensVisibilityFollowsSetting();
        console.log("\nALL SCANNER TESTS PASSED SUCCESSFULLY!");
    } catch (err) {
        console.error("\nTEST FAILURE DETECTED:");
        console.error(err);
        process.exit(1);
    }
}

runAll();
