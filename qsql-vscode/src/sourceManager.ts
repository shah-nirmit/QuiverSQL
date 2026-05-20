import * as vscode from 'vscode';
import { DaemonClient } from './daemonClient';

export interface PersistentSourceProfile {
    name: string;
    kind: 'file' | 'sqlite' | 'postgres' | 'mysql' | 'mariadb';
    details: {
        path?: string;      // for file
        format?: string;    // for file
        dbPath?: string;    // for sqlite
        tableName?: string; // for sqlite table name
        schema?: string;    // for SQL database schema/database
    };
    secretKey?: string;
}

export class SourceManager {
    private static readonly SOURCES_KEY = 'qsql.sources';
    public replayErrors = new Map<string, string>();

    constructor(
        private context: vscode.ExtensionContext,
        private daemonClient: DaemonClient
    ) {}

    /**
     * Load all stored source profiles.
     */
    public getProfiles(): PersistentSourceProfile[] {
        return this.context.globalState.get<PersistentSourceProfile[]>(SourceManager.SOURCES_KEY) || [];
    }

    /**
     * Save all source profiles to globalState.
     */
    private async saveProfiles(profiles: PersistentSourceProfile[]): Promise<void> {
        await this.context.globalState.update(SourceManager.SOURCES_KEY, profiles);
    }

    /**
     * Add and register a new source profile, securing secrets if any.
     */
    public async addSource(
        name: string,
        kind: PersistentSourceProfile['kind'],
        details: PersistentSourceProfile['details'],
        secret?: string
    ): Promise<void> {
        const profiles = this.getProfiles();
        const filtered = profiles.filter(p => p.name !== name);

        const secretKey = secret ? `qsql.secret.${name}` : undefined;
        if (secret && secretKey) {
            await this.context.secrets.store(secretKey, secret);
        }

        const newProfile: PersistentSourceProfile = {
            name,
            kind,
            details,
            secretKey
        };

        filtered.push(newProfile);
        await this.saveProfiles(filtered);
    }

    /**
     * Remove a source profile and its associated secrets.
     */
    public async removeSource(name: string): Promise<void> {
        const profiles = this.getProfiles();
        const profile = profiles.find(p => p.name === name);
        if (profile?.secretKey) {
            await this.context.secrets.delete(profile.secretKey);
        }
        const filtered = profiles.filter(p => p.name !== name);
        await this.saveProfiles(filtered);
        this.replayErrors.delete(name);
    }

    /**
     * Replay all registered source profiles concurrently on activation.
     */
    public async replaySources(): Promise<void> {
        const profiles = this.getProfiles();
        if (profiles.length === 0) {
            return;
        }

        const replayPromises = profiles.map(async (profile) => {
            try {
                let _password = '';
                if (profile.secretKey) {
                    _password = (await this.context.secrets.get(profile.secretKey)) || '';
                }

                if (profile.kind === 'file') {
                    await this.daemonClient.sendRequest('register_file', {
                        table_name: profile.name,
                        path: profile.details.path,
                        format: profile.details.format
                    });
                } else if (profile.kind === 'sqlite') {
                    await this.daemonClient.sendRequest('register_sqlite', {
                        db_path: profile.details.dbPath,
                        table_name: profile.details.tableName,
                        alias: profile.name
                    });
                } else if (profile.kind === 'postgres') {
                    await this.daemonClient.sendRequest('register_postgres', {
                        connection_string: _password,
                        table_name: profile.details.tableName,
                        schema: profile.details.schema,
                        alias: profile.name
                    });
                } else if (profile.kind === 'mysql' || profile.kind === 'mariadb') {
                    await this.daemonClient.sendRequest(
                        profile.kind === 'mariadb' ? 'register_mariadb' : 'register_mysql',
                        {
                            connection_string: _password,
                            table_name: profile.details.tableName,
                            schema: profile.details.schema,
                            alias: profile.name
                        }
                    );
                }
                this.replayErrors.delete(profile.name);
            } catch (e: any) {
                console.error(`Failed to replay source ${profile.name}:`, e);
                this.replayErrors.set(profile.name, e.message || String(e));
            }
        });

        await Promise.all(replayPromises);
    }
}
