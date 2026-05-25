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
            emitClose: (code: number) => {
                const closeHandlers = events.get('close');
                if (closeHandlers) {
                    for (const handler of closeHandlers) {
                        handler(code);
                    }
                }
            },
            emitStdout: (data: string | Buffer) => {
                const dataHandlers = stdoutEvents.get('data');
                if (dataHandlers) {
                    for (const handler of dataHandlers) {
                        handler(Buffer.isBuffer(data) ? data : Buffer.from(data, 'utf8'));
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
import { SourceManager } from '../sourceManager';

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

function parseRpcRequestFrame(data: string): any {
    const headerMatch = data.match(/^Content-Length:\s*(\d+)\r?\n\r?\n/i);
    assert.ok(headerMatch, `expected Content-Length request frame, got ${data}`);
    const headerLength = headerMatch[0].length;
    const bodyLength = Number(headerMatch[1]);
    return JSON.parse(data.slice(headerLength, headerLength + bodyLength));
}

function framedResponse(response: any): string {
    const body = JSON.stringify(response);
    return `Content-Length: ${Buffer.byteLength(body, 'utf8')}\r\n\r\n${body}`;
}

// -------------------------------------------------------------
// 3. Test Cases
// -------------------------------------------------------------

async function testSuccessfulQuery() {
    console.log("Running: testSuccessfulQuery");
    const client = await createClient();
    
    // Register request handler to simulate success response
    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = parseRpcRequestFrame(data);
        const mockResult: QueryPage = {
            query_id: "q_1",
            schema: {
                fields: [
                    { name: "id", data_type: "Int64", nullable: false },
                    { name: "name", data_type: "Utf8", nullable: true }
                ]
            },
            page_index: 0,
            page_size: 1000,
            is_last: true,
            data: [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }],
            metrics: {
                planning_time_ms: 1,
                execution_time_ms: 2,
                first_page_time_ms: 3,
                rows_produced: 2,
                rows_returned: 2
            }
        };
        const res = {
            jsonrpc: "2.0",
            result: mockResult,
            id: req.id
        };
        mockProcessInstance.emitStdout(framedResponse(res));
    };

    const response = await client.sendRequest<QueryPage>('execute_json', { sql: "SELECT * FROM users" });
    
    assert.strictEqual(response.page_index, 0);
    assert.strictEqual(response.is_last, true);
    assert.strictEqual(response.data.length, 2);
    assert.strictEqual(response.data[0].name, "Alice");
    
    client.stop();
    console.log("OK: testSuccessfulQuery passed!");
}

async function testPagedQueryHelpersSendExpectedRpc() {
    console.log("Running: testPagedQueryHelpersSendExpectedRpc");
    const client = await createClient();
    const methods: string[] = [];

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = parseRpcRequestFrame(data);
        methods.push(req.method);

        if (req.method === 'query_start') {
            assert.strictEqual(req.params.sql, "SELECT * FROM users");
            assert.strictEqual(req.params.page_size, 250);
            assert.strictEqual(req.params.timeout_ms, 5000);
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: "2.0",
                result: makeQueryPage("q_1", 0),
                id: req.id
            }) + "\n");
            return;
        }

        if (req.method === 'query_page') {
            assert.strictEqual(req.params.query_id, "q_1");
            assert.strictEqual(req.params.page_index, 1);
            assert.strictEqual(req.params.page_size, 250);
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: "2.0",
                result: makeQueryPage("q_1", 1),
                id: req.id
            }) + "\n");
            return;
        }

        if (req.method === 'query_cancel') {
            assert.strictEqual(req.params.query_id, "q_1");
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: "2.0",
                result: {
                    query_id: "q_1",
                    cancelled: true,
                    message: "Query cancellation requested"
                },
                id: req.id
            }) + "\n");
        }
    };

    const firstPage = await client.startQuery("SELECT * FROM users", { pageSize: 250, timeoutMs: 5000 });
    assert.strictEqual(firstPage.query_id, "q_1");

    const secondPage = await client.getQueryPage("q_1", 1, 250);
    assert.strictEqual(secondPage.page_index, 1);

    const cancelResult = await client.cancelQuery("q_1");
    assert.strictEqual(cancelResult.cancelled, true);
    assert.deepStrictEqual(methods, ['query_start', 'query_page', 'query_cancel']);

    client.stop();
    console.log("OK: testPagedQueryHelpersSendExpectedRpc passed!");
}

async function testPendingRequestsRejectedOnDaemonClose() {
    console.log("Running: testPendingRequestsRejectedOnDaemonClose");
    const client = await createClient();

    mockProcessInstance.onRequestWritten = (_data: string) => {
        process.nextTick(() => {
            mockProcessInstance.emitClose(17);
        });
    };

    try {
        await client.sendRequest('query_start', { sql: "SELECT * FROM slow" });
        assert.fail("Should have rejected pending request when daemon closed");
    } catch (err: any) {
        const queryError = err as QueryError;
        assert.strictEqual(queryError.code, -32010);
        assert.strictEqual(queryError.message, "QuiverSQL Daemon exited with code 17");
    }

    console.log("OK: testPendingRequestsRejectedOnDaemonClose passed!");
}

async function testStandardErrorBubble() {
    console.log("Running: testStandardErrorBubble");
    const client = await createClient();

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = parseRpcRequestFrame(data);
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
        const req = parseRpcRequestFrame(data);
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
        const req = parseRpcRequestFrame(data);
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

async function testContentLengthResponseParsing() {
    console.log("Running: testContentLengthResponseParsing");
    const client = await createClient();

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = parseRpcRequestFrame(data);
        const body = JSON.stringify({
            jsonrpc: "2.0",
            result: { ok: true, text: "line one\nline two" },
            id: req.id
        });
        const frame = `Content-Length: ${Buffer.byteLength(body, 'utf8')}\r\n\r\n${body}`;
        mockProcessInstance.emitStdout(frame.slice(0, 20));
        mockProcessInstance.emitStdout(frame.slice(20));
    };

    const response = await client.sendRequest<any>('ping');
    assert.strictEqual(response.ok, true);
    assert.strictEqual(response.text, "line one\nline two");

    client.stop();
    console.log("OK: testContentLengthResponseParsing passed!");
}

function makeQueryPage(queryId: string, pageIndex: number): QueryPage {
    return {
        query_id: queryId,
        schema: {
            fields: [
                { name: "id", data_type: "Int64", nullable: false },
                { name: "name", data_type: "Utf8", nullable: true }
            ]
        },
        page_index: pageIndex,
        page_size: 250,
        is_last: pageIndex > 0,
        data: [{ id: pageIndex + 1, name: pageIndex === 0 ? "Alice" : "Bob" }],
        metrics: {
            planning_time_ms: 1,
            execution_time_ms: 2,
            first_page_time_ms: 3,
            rows_produced: 2,
            rows_returned: 1
        }
    };
}

// -------------------------------------------------------------
// 4. Run Suite
// -------------------------------------------------------------
async function testSourceCatalogMethods() {
    console.log("Running: testSourceCatalogMethods");
    const client = await createClient();
    const methods: string[] = [];

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = parseRpcRequestFrame(data);
        methods.push(req.method);

        if (req.method === 'list_sources') {
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: "2.0",
                result: [
                    {
                        name: "my_csv",
                        kind: "csv",
                        connection_details: { path: "/path/to/file.csv", format: "csv" },
                        status: "ok"
                    }
                ],
                id: req.id
            }) + "\n");
            return;
        }

        if (req.method === 'list_source_tables') {
            assert.strictEqual(req.params.name, "my_csv");
            assert.strictEqual(req.params.offset, 250);
            assert.strictEqual(req.params.limit, 250);
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: "2.0",
                result: {
                    name: "my_csv",
                    tables: ["part_2"],
                    offset: 250,
                    limit: 250,
                    total_known: 251,
                    truncated: false
                },
                id: req.id
            }) + "\n");
            return;
        }

        if (req.method === 'remove_source') {
            assert.strictEqual(req.params.name, "my_csv");
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: "2.0",
                result: {
                    name: "my_csv",
                    removed: true
                },
                id: req.id
            }) + "\n");
            return;
        }

        if (req.method === 'get_source_metadata') {
            assert.strictEqual(req.params.name, "my_csv");
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: "2.0",
                result: {
                    name: "my_csv",
                    kind: "csv",
                    connection_details: { path: "/path/to/file.csv", format: "csv" },
                    status: "ok",
                    capabilities: {
                        projection: true,
                        filter: true,
                        limit: true,
                        aggregate: false,
                        joins: false,
                        dialect_name: "generic"
                    }
                },
                id: req.id
            }) + "\n");
        }
    };

    const sources = await client.listSources();
    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].name, "my_csv");
    assert.strictEqual(sources[0].kind, "csv");

    const tablePage = await client.listSourceTables("my_csv", 250, 250);
    assert.deepStrictEqual(tablePage.tables, ["part_2"]);

    const removeResult = await client.removeSource("my_csv");
    assert.strictEqual(removeResult.name, "my_csv");
    assert.strictEqual(removeResult.removed, true);

    const metadata = await client.getSourceMetadata("my_csv");
    assert.strictEqual(metadata.name, "my_csv");
    assert.strictEqual(metadata.capabilities?.projection, true);
    assert.deepStrictEqual(methods, ['list_sources', 'list_source_tables', 'remove_source', 'get_source_metadata']);

    client.stop();
    console.log("OK: testSourceCatalogMethods passed!");
}

async function testSourceManagerSqlSecretReplay() {
    console.log("Running: testSourceManagerSqlSecretReplay");
    let storedProfiles: any[] | undefined;
    const secrets = new Map<string, string>();
    const requests: Array<{ method: string; params: any }> = [];
    const context: any = {
        globalState: {
            get: (_key: string) => storedProfiles,
            update: async (_key: string, value: any[]) => {
                storedProfiles = value;
            }
        },
        secrets: {
            store: async (key: string, value: string) => {
                secrets.set(key, value);
            },
            get: async (key: string) => secrets.get(key),
            delete: async (key: string) => {
                secrets.delete(key);
            }
        }
    };
    const daemon: any = {
        sendRequest: async (method: string, params: any) => {
            requests.push({ method, params });
            return "ok";
        }
    };

    const manager = new SourceManager(context, daemon);
    await manager.addSource(
        "pg_local",
        "postgres",
        { schema: "public" },
        "postgres://user:secret@localhost:5432/db"
    );

    const profiles = manager.getProfiles();
    assert.strictEqual(profiles.length, 1);
    assert.strictEqual(profiles[0].kind, "postgres");
    assert.strictEqual((profiles[0].details as any).connectionString, undefined);
    assert.ok(profiles[0].secretKey);

    await manager.replaySources();
    assert.strictEqual(requests.length, 1);
    assert.strictEqual(requests[0].method, "register_postgres");
    assert.strictEqual(requests[0].params.alias, "pg_local");
    assert.strictEqual(requests[0].params.schema, "public");
    assert.strictEqual(requests[0].params.connection_string, "postgres://user:secret@localhost:5432/db");

    await manager.removeSource("pg_local");
    assert.strictEqual(manager.getProfiles().length, 0);
    assert.strictEqual(secrets.size, 0);
    console.log("OK: testSourceManagerSqlSecretReplay passed!");
}


async function testExplainQuery() {
    console.log("Running testExplainQuery...");
    const client = new DaemonClient({ extensionPath: "/fake/path" } as any);

    const startPromise = client.start();
    setTimeout(() => {
        if (mockProcessInstance && mockProcessInstance.emitSpawn) {
            mockProcessInstance.emitSpawn();
        }
    }, 10);
    await startPromise;

    mockProcessInstance.onRequestWritten = (data: string) => {
        const req = parseRpcRequestFrame(data);
        if (req.method === 'explain_query') {
            mockProcessInstance.emitStdout(JSON.stringify({
                jsonrpc: '2.0',
                id: req.id,
                result: {
                    sql: req.params.sql,
                    federated_plan: {
                        root_ids: ['1'],
                        nodes: { '1': { id: '1', origin: 'datafusion', node_type: 'Projection', label: 'SELECT *', children: [], attributes: {}, metrics: {} } },
                        node_count: 1,
                        truncated: false
                    },
                    source_plans: {},
                    raw: 'raw plan text',
                    warnings: []
                }
            }) + '\n');
        }
    };

    const result = await client.explainQuery("SELECT 1", false);
    assert.strictEqual(result.sql, "SELECT 1");
    assert.strictEqual(result.federated_plan.node_count, 1);
    assert.strictEqual(result.federated_plan.nodes['1'].node_type, "Projection");
    assert.strictEqual(result.raw, "raw plan text");

    client.stop();
    console.log("OK: testExplainQuery passed!");
}

async function runAll() {
    console.log("Starting QuiverSQL VS Code Client Unit Tests...\n");
    try {
        await testExplainQuery();
        await testSuccessfulQuery();
        await testSourceCatalogMethods();
        await testSourceManagerSqlSecretReplay();
        await testPagedQueryHelpersSendExpectedRpc();
        await testStandardErrorBubble();
        await testErrorWithDetails();
        await testErrorWithStringDataDetails();
        await testContentLengthResponseParsing();
        await testPendingRequestsRejectedOnDaemonClose();
        console.log("\nALL CLIENT TESTS PASSED SUCCESSFULLY!");
    } catch (err) {
        console.error("\nTEST FAILURE DETECTED:");
        console.error(err);
        process.exit(1);
    }
}

runAll();
