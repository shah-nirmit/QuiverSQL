import * as assert from 'assert';

// 1. Setup Module Mocks before importing anything
const Module = require('module');
const originalRequire = Module.prototype.require;

const vscodeMock = {
    window: {
        showErrorMessage: (msg: string) => {
            console.log(`[Mock Error Message]: ${msg}`);
        },
        showInformationMessage: (msg: string) => {
            console.log(`[Mock Info Message]: ${msg}`);
        }
    },
    workspace: {
        getConfiguration: () => ({
            get: (key: string, defaultValue: any) => defaultValue
        })
    }
};

let mockProcessInstance: any = null;
const childProcessMock = {
    spawn: (_command: string, _args?: string[]) => {
        const events = new Map<string, ((...args: any[]) => any)[]>();
        const stdoutEvents = new Map<string, ((...args: any[]) => any)[]>();
        const stderrEvents = new Map<string, ((...args: any[]) => any)[]>();

        const mockProcess = {
            on: (event: string, cb: (...args: any[]) => any) => {
                if (!events.has(event)) events.set(event, []);
                events.get(event)!.push(cb);
            },
            stdout: {
                on: (event: string, cb: (...args: any[]) => any) => {
                    if (!stdoutEvents.has(event)) stdoutEvents.set(event, []);
                    stdoutEvents.get(event)!.push(cb);
                }
            },
            stderr: {
                on: (event: string, cb: (...args: any[]) => any) => {
                    if (!stderrEvents.has(event)) stderrEvents.set(event, []);
                    stderrEvents.get(event)!.push(cb);
                }
            },
            stdin: {
                write: (data: string, cb?: (err?: Error) => void) => {
                    if (cb) cb();
                    process.nextTick(() => {
                        if (mockProcess.onRequestWritten) {
                            mockProcess.onRequestWritten(data);
                        }
                    });
                }
            },
            kill: () => {
                const closeHandlers = events.get('close');
                if (closeHandlers) {
                    for (const handler of closeHandlers) {
                        handler(0);
                    }
                }
            },
            emitStdout: (data: string) => {
                const dataHandlers = stdoutEvents.get('data');
                if (dataHandlers) {
                    for (const handler of dataHandlers) {
                        handler(data);
                    }
                }
            },
            emitStderr: (data: string) => {
                const stderrHandlers = stderrEvents.get('data');
                if (stderrHandlers) {
                    for (const handler of stderrHandlers) {
                        handler(data);
                    }
                }
            },
            emitSpawn: () => {
                const spawnHandlers = events.get('spawn');
                if (spawnHandlers) {
                    for (const handler of spawnHandlers) {
                        handler();
                    }
                }
            },
            emitError: (err: Error) => {
                const errorHandlers = events.get('error');
                if (errorHandlers) {
                    for (const handler of errorHandlers) {
                        handler(err);
                    }
                }
            },
            onRequestWritten: null as ((data: string) => void) | null
        };
        mockProcessInstance = mockProcess;
        return mockProcess;
    }
};

const fsMock = {
    existsSync: (_path: string) => true
};

Module.prototype.require = function (packageName: string, ...args: any[]) {
    if (packageName === 'vscode') {
        return vscodeMock;
    }
    if (packageName === 'child_process') {
        return childProcessMock;
    }
    if (packageName === 'fs') {
        return fsMock;
    }
    return originalRequire.apply(this, [packageName, ...args]);
};

// 2. Import elements to test
import { DaemonClient } from '../daemonClient';
import { QueryError, QueryPage } from '../models';

// Helper to instantiate and start DaemonClient
async function createClient(): Promise<DaemonClient> {
    const context: any = { extensionPath: '/mock/path' };
    const client = new DaemonClient(context);
    
    const startPromise = client.start();
    
    // Trigger simulated spawn
    process.nextTick(() => {
        if (mockProcessInstance) {
            mockProcessInstance.emitSpawn();
        }
    });
    
    await startPromise;
    return client;
}

// -------------------------------------------------------------
// 3. Test Cases
// -------------------------------------------------------------

async function testSuccessfulQuery() {
    console.log("Running: testSuccessfulQuery");
    const client = await createClient();
    
    // Register request handler to simulate success response
    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = JSON.parse(data.trim());
        const mockResult: QueryPage = {
            page_index: 0,
            is_last: true,
            data: [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]
        };
        const res = {
            jsonrpc: "2.0",
            result: mockResult,
            id: req.id
        };
        mockProcessInstance.emitStdout(JSON.stringify(res) + "\n");
    };

    const response = await client.sendRequest<QueryPage>('execute_json', { sql: "SELECT * FROM users" });
    
    assert.strictEqual(response.page_index, 0);
    assert.strictEqual(response.is_last, true);
    assert.strictEqual(response.data.length, 2);
    assert.strictEqual(response.data[0].name, "Alice");
    
    client.stop();
    console.log("OK: testSuccessfulQuery passed!");
}

async function testStandardErrorBubble() {
    console.log("Running: testStandardErrorBubble");
    const client = await createClient();

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = JSON.parse(data.trim());
        const res = {
            jsonrpc: "2.0",
            error: {
                code: -32601,
                message: "Method not found"
            },
            id: req.id
        };
        mockProcessInstance.emitStdout(JSON.stringify(res) + "\n");
    };

    try {
        await client.sendRequest('some_unknown_method');
        assert.fail("Should have thrown a rejected QueryError Promise");
    } catch (err: any) {
        const queryError = err as QueryError;
        assert.strictEqual(queryError.code, -32601);
        assert.strictEqual(queryError.message, "Method not found");
        assert.strictEqual(queryError.details, undefined);
    }

    client.stop();
    console.log("OK: testStandardErrorBubble passed!");
}

async function testErrorWithDetails() {
    console.log("Running: testErrorWithDetails");
    const client = await createClient();

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = JSON.parse(data.trim());
        const res = {
            jsonrpc: "2.0",
            error: {
                code: 4001,
                message: "Syntax error at or near 'FROM'",
                data: {
                    details: "Parser error details at line 1, column 12"
                }
            },
            id: req.id
        };
        mockProcessInstance.emitStdout(JSON.stringify(res) + "\n");
    };

    try {
        await client.sendRequest('execute_json', { sql: "SELECT * FROM" });
        assert.fail("Should have thrown a rejected QueryError Promise");
    } catch (err: any) {
        const queryError = err as QueryError;
        assert.strictEqual(queryError.code, 4001);
        assert.strictEqual(queryError.message, "Syntax error at or near 'FROM'");
        assert.strictEqual(queryError.details, "Parser error details at line 1, column 12");
    }

    client.stop();
    console.log("OK: testErrorWithDetails passed!");
}

async function testErrorWithStringDataDetails() {
    console.log("Running: testErrorWithStringDataDetails");
    const client = await createClient();

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = JSON.parse(data.trim());
        const res = {
            jsonrpc: "2.0",
            error: {
                code: 5000,
                message: "Fatal query failure",
                data: "Out of memory"
            },
            id: req.id
        };
        mockProcessInstance.emitStdout(JSON.stringify(res) + "\n");
    };

    try {
        await client.sendRequest('execute_json', { sql: "SELECT * FROM large" });
        assert.fail("Should have thrown a rejected QueryError Promise");
    } catch (err: any) {
        const queryError = err as QueryError;
        assert.strictEqual(queryError.code, 5000);
        assert.strictEqual(queryError.message, "Fatal query failure");
        assert.strictEqual(queryError.details, "Out of memory");
    }

    client.stop();
    console.log("OK: testErrorWithStringDataDetails passed!");
}

// -------------------------------------------------------------
// 4. Run Suite
// -------------------------------------------------------------
async function runAll() {
    console.log("Starting QuiverSQL VS Code Client Unit Tests...\n");
    try {
        await testSuccessfulQuery();
        await testStandardErrorBubble();
        await testErrorWithDetails();
        await testErrorWithStringDataDetails();
        console.log("\nALL CLIENT TESTS PASSED SUCCESSFULLY!");
    } catch (err) {
        console.error("\nTEST FAILURE DETECTED:");
        console.error(err);
        process.exit(1);
    }
}

runAll();
