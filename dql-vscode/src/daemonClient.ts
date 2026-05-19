import * as cp from 'child_process';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

let requestIdCounter = 0;

interface RpcRequest {
    jsonrpc: '2.0';
    method: string;
    params?: any;
    id?: number;
}

interface RpcResponse {
    jsonrpc: '2.0';
    result?: any;
    error?: any;
    id?: number;
}

export class DaemonClient {
    private process?: cp.ChildProcess;
    private pendingRequests = new Map<number, { resolve: (val: any) => void; reject: (err: any) => void }>();
    private buffer = '';

    constructor(private context: vscode.ExtensionContext) {}

    public async start(): Promise<void> {
        return new Promise((resolve, reject) => {
            const isWindows = os.platform() === 'win32';
            const binaryName = isWindows ? 'dql-daemon.exe' : 'dql-daemon';
            
            // For now, point to the debug build in the adjacent workspace
            const daemonPath = path.join(this.context.extensionPath, '..', 'dql-workspace', 'target', 'debug', binaryName);

            this.process = cp.spawn(daemonPath);

            this.process.on('error', (err) => {
                vscode.window.showErrorMessage(`Failed to start DQL Daemon: ${err.message}`);
                reject(err);
            });

            this.process.stdout?.on('data', (data) => {
                this.buffer += data.toString();
                this.processBuffer();
            });

            this.process.stderr?.on('data', (data) => {
                console.error(`DQL Daemon stderr: ${data}`);
            });

            this.process.on('close', (code) => {
                console.log(`DQL Daemon exited with code ${code}`);
                this.process = undefined;
            });

            this.process.on('spawn', () => {
                // Resolve as soon as the OS confirms the process has spawned
                resolve();
            });

            // Fallback for older Node versions
            setTimeout(resolve, 100);
        });
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
                        reject(response.error);
                    } else {
                        resolve(response.result);
                    }
                }
            } catch (e) {
                console.error(`Failed to parse daemon response: ${line}`, e);
            }
        }
    }

    public async sendRequest(method: string, params?: any): Promise<any> {
        if (!this.process || !this.process.stdin) {
            throw new Error('DQL Daemon is not running');
        }

        const id = ++requestIdCounter;
        const request: RpcRequest = {
            jsonrpc: '2.0',
            method,
            params,
            id
        };

        const reqStr = JSON.stringify(request) + '\n';
        
        return new Promise((resolve, reject) => {
            this.pendingRequests.set(id, { resolve, reject });
            this.process!.stdin!.write(reqStr, (err) => {
                if (err) {
                    this.pendingRequests.delete(id);
                    reject(err);
                }
            });
        });
    }

    public stop() {
        if (this.process) {
            this.process.kill();
            this.process = undefined;
        }
    }
}
