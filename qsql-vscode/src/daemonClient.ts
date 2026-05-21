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
    private buffer = '';

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

            this.process = cp.spawn(daemonPath);

            this.process.on('error', (err) => {
                vscode.window.showErrorMessage(`Failed to start QuiverSQL Daemon: ${err.message}`);
                reject(err);
            });

            this.process.stdout?.on('data', (data) => {
                this.buffer += data.toString();
                this.processBuffer();
            });

            this.process.stderr?.on('data', (data) => {
                console.error(`QuiverSQL Daemon stderr: ${data}`);
            });

            this.process.on('close', (code) => {
                console.log(`QuiverSQL Daemon exited with code ${code}`);
                this.process = undefined;
                this.rejectPendingRequests({
                    code: -32010,
                    message: `QuiverSQL Daemon exited with code ${code}`,
                    details: undefined
                });
            });

            this.process.on('spawn', () => {
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
        let newlineIndex;
        while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
            const line = this.buffer.slice(0, newlineIndex).trim();
            this.buffer = this.buffer.slice(newlineIndex + 1);

            if (!line) continue;

            try {
                const response = JSON.parse(line) as RpcResponse;
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
                console.error(`Failed to parse daemon response: ${line}`, e);
            }
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

        const reqStr = JSON.stringify(request) + '\n';
        
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
        const request: QueryStartRequest = {
            sql,
            page_size: options.pageSize,
            timeout_ms: options.timeoutMs
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

    public removeSource(name: string): Promise<RemoveSourceResult> {
        const request: RemoveSourceRequest = { name };
        return this.sendRequest<RemoveSourceResult>('remove_source', request);
    }

    
    public explainQuery(sql: string, includeNative: boolean = true): Promise<ExplainQueryResult> {
        const request: ExplainQueryRequest = {
            sql,
            include_native: includeNative
        };
        return this.sendRequest<ExplainQueryResult>('explain_query', request);
    }

    public getSourceMetadata(name: string): Promise<CatalogSource> {
        const request: GetSourceMetadataRequest = { name };
        return this.sendRequest<CatalogSource>('get_source_metadata', request);
    }
}
