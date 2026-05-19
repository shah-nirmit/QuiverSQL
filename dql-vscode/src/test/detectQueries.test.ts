import * as assert from 'assert';

// -------------------------------------------------------------
// 1. Dynamic Node.js module loader hook to mock the 'vscode' module
// -------------------------------------------------------------
const Module = require('module');
const originalRequire = Module.prototype.require;
Module.prototype.require = function (packageName: string) {
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
    return originalRequire.apply(this, arguments);
};

// Now import detectQueries and interfaces from extension
import { detectQueries, DetectedQuery } from '../extension';

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
    console.log("✔ testSingleQuery passed!");
}

function testMultipleQueries() {
    const sql = "SELECT * FROM employees;\nSELECT name FROM departments;\nSELECT id FROM orders;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 3);
    assert.strictEqual(result[0].sql, "SELECT * FROM employees;");
    assert.strictEqual(result[1].sql, "SELECT name FROM departments;");
    assert.strictEqual(result[2].sql, "SELECT id FROM orders;");
    console.log("✔ testMultipleQueries passed!");
}

function testEscapedLineComments() {
    const sql = "-- This is a comment\nSELECT * FROM employees;\n-- Another comment\nSELECT name FROM departments;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 2);
    assert.strictEqual(result[0].sql, "SELECT * FROM employees;");
    assert.strictEqual(result[1].sql, "SELECT name FROM departments;");
    console.log("✔ testEscapedLineComments passed!");
}

function testEscapedBlockComments() {
    const sql = "/* \n  Multi-line\n  comment\n*/\nSELECT * FROM employees; /* trailing */";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT * FROM employees;");
    console.log("✔ testEscapedBlockComments passed!");
}

function testSemicolonInsideQuotes() {
    const sql = "SELECT 'hello; world' AS col1, \"double; quote\" AS col2 FROM test;";
    const result = runDetect(sql);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].sql, "SELECT 'hello; world' AS col1, \"double; quote\" AS col2 FROM test;");
    console.log("✔ testSemicolonInsideQuotes passed!");
}

// -------------------------------------------------------------
// 4. Test Suite Execution
// -------------------------------------------------------------
function runAll() {
    console.log("Starting DQL VS Code Extension Scanner Tests...\n");
    try {
        testSingleQuery();
        testMultipleQueries();
        testEscapedLineComments();
        testEscapedBlockComments();
        testSemicolonInsideQuotes();
        console.log("\n🎉 ALL SCANNER TESTS PASSED SUCCESSFULLY!");
    } catch (err) {
        console.error("\n❌ TEST FAILURE DETECTED:");
        console.error(err);
        process.exit(1);
    }
}

runAll();
