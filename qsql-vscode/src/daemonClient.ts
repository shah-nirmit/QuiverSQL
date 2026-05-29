import * as cp from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';
import {
    QueryCancelResult,
    QueryError,
    QueryPage,
    QueryPageRequest,
    QueryStartRequest,
    CatalogSource,
    ListSourceTablesResult,
    RemoveSourceResult,
    RemoveSourceRequest,
    GetSourceMetadataRequest,
    ExplainQueryRequest,
    ExplainQueryResult
} from './models';

let requestIdCounter = 0;

interface RpcRequest {
    jsonrpc: '2.0';
    method: string;
    params?: any;
    id?: number;
}

interface RpcError {
    code: number;
    message: string;
    data?: any;
    details?: string;
}

interface RpcResponse {
    jsonrpc: '2.0';
    result?: any;
    error?: RpcError;
    id?: number;
}

export class DaemonClient {
    private process?: cp.ChildProcess;
    private pendingRequests = new Map<number, { resolve: (val: any) => void; reject: (err: QueryError) => void }>();
    private buffer: Buffer = Buffer.alloc(0);

    constructor(private context: vscode.ExtensionContext) {}

    public async start(): Promise<void> {
        return new Promise((resolve, reject) => {
            const daemonPath = this.resolveDaemonPath();
            if (!daemonPath) {
                const message = 'QuiverSQL daemon not found. Build qsql-daemon or set qsql.daemonPath.';
                vscode.window.showErrorMessage(message);
                reject(new Error(message));
                return;
            }

            const config = vscode.workspace.getConfiguration('qsql');
            const env: Record<string, string> = {
                ...process.env,
                QSQL_DEFAULT_PAGE_SIZE: config.get<number>('defaultPageSize', 1000).toString(),
                QSQL_MAX_PAGE_SIZE: config.get<number>('maxPageSize', 10000).toString(),
                QSQL_QUERY_MEMORY_LIMIT_BYTES: config.get<number>('queryMemoryLimit', 268435456).toString(),
                QSQL_REMOTE_SCAN_MAX_ROWS: config.get<number>('remoteScanMaxRows', 1000000).toString(),
                QSQL_REMOTE_SCAN_MAX_BYTES: config.get<number>('remoteScanMaxBytes', 1073741824).toString(),
                QSQL_REMOTE_QUERY_TIMEOUT_SECS: config.get<number>('remoteQueryTimeout', 30).toString(),
            };
            // The daemon also honors `QSQL_EXPLAIN_TRACE=1` for one-line-per-node
            // physical-plan tracing to stderr — useful when diagnosing Explain
            // capture issues. Not exposed as a VS Code setting (it's a
            // developer diagnostic): set it in your shell before launching
            // VS Code or in a launch.json `env` block if you need it.

            const proc = cp.spawn(daemonPath, [], { env });
            this.process = proc;

            proc.on('error', (err) => {
                vscode.window.showErrorMessage(`Failed to start QuiverSQL Daemon: ${err.message}`);
                reject(err);
            });

            proc.stdout?.on('data', (data: Buffer) => {
                this.buffer = Buffer.concat([this.buffer, data]);
                this.processBuffer();
            });

            proc.stderr?.on('data', (data) => {
                console.error(`QuiverSQL Daemon stderr: ${data}`);
            });

            proc.on('close', (code) => {
                console.log(`QuiverSQL Daemon exited with code ${code}`);
                if (this.process === proc) {
                    this.process = undefined;
                    this.rejectPendingRequests({
                        code: -32010,
                        message: `QuiverSQL Daemon exited with code ${code}`,
                        details: undefined
                    });
                }
            });

            proc.on('spawn', () => {
                // Resolve as soon as the OS confirms the process has spawned
                resolve();
            });

            // Fallback for older Node versions
            setTimeout(resolve, 100);
        });
    }

    private resolveDaemonPath(): string | undefined {
        const configuredPath = vscode.workspace
            .getConfiguration('qsql')
            .get<string>('daemonPath', '')
            .trim();

        const candidates = [
            configuredPath,
            this.getBundledDaemonPath(),
            this.getWorkspaceDebugDaemonPath()
        ].filter((candidate): candidate is string => candidate.length > 0);

        return candidates.find(candidate => fs.existsSync(candidate));
    }

    private getBundledDaemonPath(): string {
        return path.join(this.context.extensionPath, 'bin', this.getDaemonBinaryName());
    }

    private getWorkspaceDebugDaemonPath(): string {
        return path.join(
            this.context.extensionPath,
            '..',
            'qsql-workspace',
            'target',
            'debug',
            this.getDaemonBinaryName()
        );
    }

    private getDaemonBinaryName(): string {
        return os.platform() === 'win32' ? 'qsql-daemon.exe' : 'qsql-daemon';
    }

    private processBuffer() {
        while (this.buffer.length > 0) {
            const headerStr = this.buffer.toString('utf8', 0, Math.min(this.buffer.length, 1024));
            const headerMatch = headerStr.match(/^Content-Length:\s*(\d+)\r?\n\r?\n/i);
            
            if (headerMatch) {
                const headerLength = Buffer.byteLength(headerMatch[0], 'utf8');
                const bodyLength = Number(headerMatch[1]);
                
                if (this.buffer.length < headerLength + bodyLength) {
                    return;
                }
                
                const body = this.buffer.subarray(headerLength, headerLength + bodyLength).toString('utf8');
                this.buffer = this.buffer.subarray(headerLength + bodyLength);
                this.processResponseFrame(body);
                continue;
            }
            if (/^Content-Length:/i.test(headerStr)) {
                return;
            }

            const newlineIndex = this.buffer.indexOf('\n'.charCodeAt(0));
            if (newlineIndex === -1) {
                return;
            }
            const line = this.buffer.subarray(0, newlineIndex).toString('utf8').trim();
            this.buffer = this.buffer.subarray(newlineIndex + 1);
            if (!line) {
                continue;
            }
            this.processResponseFrame(line);
        }
    }

    private processResponseFrame(frame: string): void {
        try {
            const response = JSON.parse(frame) as RpcResponse;
            if (response.id !== undefined && this.pendingRequests.has(response.id)) {
                const { resolve, reject } = this.pendingRequests.get(response.id)!;
                this.pendingRequests.delete(response.id);

                if (response.error) {
                    const rpcErr = response.error;
                    let details: string | undefined = undefined;
                    if (rpcErr.data !== undefined) {
                        if (typeof rpcErr.data === 'string') {
                            details = rpcErr.data;
                        } else if (typeof rpcErr.data === 'object' && rpcErr.data !== null) {
                            details = typeof rpcErr.data.details === 'string'
                                ? rpcErr.data.details
                                : JSON.stringify(rpcErr.data);
                        }
                    } else if (typeof rpcErr.details === 'string') {
                        details = rpcErr.details;
                    }

                    const queryError: QueryError = {
                        code: typeof rpcErr.code === 'number' ? rpcErr.code : -32603,
                        message: typeof rpcErr.message === 'string' ? rpcErr.message : 'Unknown error',
                        details
                    };
                    reject(queryError);
                } else {
                    resolve(response.result);
                }
            }
        } catch (e) {
            console.error(`Failed to parse daemon response: ${frame}`, e);
        }
    }

    public async sendRequest<T = any>(method: string, params?: any): Promise<T> {
        if (!this.process || !this.process.stdin) {
            throw new Error('QuiverSQL Daemon is not running');
        }

        const id = ++requestIdCounter;
        const request: RpcRequest = {
            jsonrpc: '2.0',
            method,
            params,
            id
        };

        const body = JSON.stringify(request);
        const reqStr = `Content-Length: ${Buffer.byteLength(body, 'utf8')}\r\n\r\n${body}`;
        
        return new Promise<T>((resolve, reject) => {
            this.pendingRequests.set(id, { resolve, reject });
            this.process!.stdin!.write(reqStr, (err) => {
                if (err) {
                    this.pendingRequests.delete(id);
                    const queryError: QueryError = {
                        code: -32603,
                        message: err.message,
                        details: err.stack
                    };
                    reject(queryError);
                }
            });
        });
    }

    public startQuery(sql: string, options: { pageSize?: number; timeoutMs?: number } = {}): Promise<QueryPage> {
        // Phase 9 — opt into Arrow IPC pages when the user has flipped the
        // setting. The daemon persists this choice on the streaming session
        // so subsequent `query_page` calls inherit it without re-passing.
        const resultFormat = vscode.workspace
            .getConfiguration('qsql')
            .get<string>('resultFormat', 'json');
        const request: QueryStartRequest = {
            sql,
            page_size: options.pageSize,
            timeout_ms: options.timeoutMs,
            ...(resultFormat && resultFormat !== 'json' ? { result_format: resultFormat } : {}),
        };
        return this.sendRequest<QueryPage>('query_start', request);
    }

    public getQueryPage(queryId: string, pageIndex?: number, pageSize?: number): Promise<QueryPage> {
        const request: QueryPageRequest = {
            query_id: queryId,
            page_index: pageIndex,
            page_size: pageSize
        };
        return this.sendRequest<QueryPage>('query_page', request);
    }

    public cancelQuery(queryId: string): Promise<QueryCancelResult> {
        return this.sendRequest<QueryCancelResult>('query_cancel', { query_id: queryId });
    }

    public stop() {
        if (this.process) {
            this.process.kill();
            this.process = undefined;
        }
    }

    private rejectPendingRequests(error: QueryError): void {
        for (const { reject } of this.pendingRequests.values()) {
            reject(error);
        }
        this.pendingRequests.clear();
    }

    public listSources(): Promise<CatalogSource[]> {
        return this.sendRequest<CatalogSource[]>('list_sources');
    }

    public listSourceTables(name: string, offset: number = 0, limit: number = 250): Promise<ListSourceTablesResult> {
        return this.sendRequest<ListSourceTablesResult>('list_source_tables', { name, offset, limit });
    }

    public removeSource(name: string): Promise<RemoveSourceResult> {
        const request: RemoveSourceRequest = { name };
        return this.sendRequest<RemoveSourceResult>('remove_source', request);
    }

    
    /**
     * Send an `explain_query` request to the daemon.
     *
     * Phase 10 — the optional second argument bundles `includeNative` (the
     * legacy boolean) and `analyze` (the new opt-in EXPLAIN ANALYZE flag).
     * Callers that don't pass anything get the existing behaviour:
     * `include_native = true`, `analyze = undefined` (planner-only).
     *
     * Passing `{ analyze: true }` makes the daemon execute the plan to
     * completion under the existing scan-guard envelope and stamp each
     * plan-graph node with runtime metrics (`actual_rows` /
     * `elapsed_compute_ms` / `mem_used_bytes`). Over-budget runs surface
     * the standard `-32100 Scan Budget Exceeded` error.
     */
    public explainQuery(
        sql: string,
        options: { includeNative?: boolean; analyze?: boolean } | boolean = {},
    ): Promise<ExplainQueryResult> {
        // Backwards-compat: the previous signature accepted a bare boolean
        // `includeNative` as the second arg.
        const opts: { includeNative?: boolean; analyze?: boolean } =
            typeof options === 'boolean' ? { includeNative: options } : options;
        const request: ExplainQueryRequest = {
            sql,
            include_native: opts.includeNative ?? true,
        };
        if (opts.analyze === true) {
            request.analyze = true;
        }
        return this.sendRequest<ExplainQueryResult>('explain_query', request);
    }

    public getSourceMetadata(name: string): Promise<CatalogSource> {
        const request: GetSourceMetadataRequest = { name };
        return this.sendRequest<CatalogSource>('get_source_metadata', request);
    }
}
