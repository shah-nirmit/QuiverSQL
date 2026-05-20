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
            ThemeIcon: class {
                constructor(public id: string) {}
            },
            EventEmitter: class {
                event = () => {};
                fire() {}
            },
            Disposable: class {
                dispose() {}
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
    renderQueryPageHtml
} from '../webviewPanel';
import { QueryPage } from '../models';

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

// -------------------------------------------------------------
// 4. Test Suite Execution
// -------------------------------------------------------------
function runAll() {
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
        testPagedGridEmptyState();
        console.log("\nALL SCANNER TESTS PASSED SUCCESSFULLY!");
    } catch (err) {
        console.error("\nTEST FAILURE DETECTED:");
        console.error(err);
        process.exit(1);
    }
}

runAll();
