export type SourceKind =
    | 'csv'
    | 'parquet'
    | 'json'
    | 'ndjson'
    | 'sqlite'
    | 'fixed_width'
    | 'postgres'
    | 'mysql'
    | 'mariadb';

export interface SourceProfile {
    name: string;
    kind: SourceKind;
    connection_details: Record<string, any>;
}

export interface TableRef {
    source_name: string;
    table_name: string;
}

export interface ConnectorCapabilities {
    projection: boolean;
    filter: boolean;
    limit: boolean;
    sort: boolean;
    aggregate: boolean;
    joins: boolean;
    dialect_name: string;
}

export interface SchemaField {
    name: string;
    data_type: string;
    nullable: boolean;
}

export interface Schema {
    fields: SchemaField[];
}

export interface QueryPage {
    query_id: string;
    schema: Schema;
    page_index: number;
    page_size: number;
    is_last: boolean;
    /** Row payload when result_format === 'json' (the default). Mutually
     *  exclusive with data_ipc — exactly one is populated per page. */
    data: Record<string, any>[];
    /** Phase 9 — base64-encoded Arrow IPC stream payload when
     *  result_format === 'arrow_ipc'. */
    data_ipc?: string;
    /** Phase 9 — echoes the format the daemon used to encode this page. */
    result_format?: string;
    metrics: PerformanceMetrics;
    warning?: string;
}

export interface QueryHandle {
    query_id: string;
}

export interface QueryError {
    code: number;
    message: string;
    details?: string;
}

export const SCAN_GUARD_ERROR_CODE = -32100;

export function isScanGuardError(err: QueryError): boolean {
    return err.code === SCAN_GUARD_ERROR_CODE;
}

export interface ExplainResult {
    logical_plan: string;
    physical_plan: string;
}

export interface PerformanceMetrics {
    planning_time_ms: number;
    execution_time_ms: number;
    first_page_time_ms: number;
    rows_produced: number;
    rows_returned: number;
}

export interface QueryStartRequest {
    sql: string;
    page_size?: number;
    timeout_ms?: number;
    /** Phase 9 — 'json' (default) or 'arrow_ipc'. Persisted on the daemon
     *  session so subsequent query_page calls reuse the same format. */
    result_format?: string;
}

export interface QueryPageRequest {
    query_id: string;
    page_index?: number;
    page_size?: number;
    /** Phase 9 — per-page override for the daemon's session-level format
     *  default. Rarely set in practice; the VS Code client only sends it on
     *  query_start. */
    result_format?: string;
}

export interface QueryCancelRequest {
    query_id: string;
}

export interface QueryCancelResult {
    query_id: string;
    cancelled: boolean;
    message: string;
}


export interface CatalogSource {
    name: string;
    kind: SourceKind;
    connection_details: Record<string, any>;
    tables?: string[];
    schema?: Schema;
    capabilities?: ConnectorCapabilities;
    status: string;
    error?: string;
}

export interface RemoveSourceRequest {
    name: string;
}

export interface RemoveSourceResult {
    name: string;
    removed: boolean;
}

export interface GetSourceMetadataRequest {
    name: string;
}

export interface ListSourceTablesRequest {
    name: string;
    offset?: number;
    limit?: number;
}

export interface ListSourceTablesResult {
    name: string;
    tables: string[];
    offset: number;
    limit: number;
    total_known?: number;
    truncated: boolean;
}

export interface ExplainQueryRequest {
    sql: string;
    include_native?: boolean;
}

export type ProviderKind = SourceKind | 'unknown';

export interface SourcePlanEntry {
    provider_kind: string;
    native_sql: string;
    native_explain: any;
    dialect: string;
}

export interface ExplainQueryResult {
    sql: string;
    federated_plan: PlanGraph;
    source_plans: Record<string, SourcePlanEntry>;
    raw: string;
    warnings: string[];
    physical_plan_text?: string;
}

export interface PlanGraph {
    root_ids: string[];
    nodes: Record<string, PlanNode>;
    node_count: number;
    truncated: boolean;
}

export interface PlanNode {
    id: string;
    origin: string;
    node_type: string;
    label: string;
    children: string[];
    attributes: Record<string, string>;
    metrics: PlanMetrics;
    source_ref?: string;
    native_plan_ref?: string;
    provider_kind?: string;
    remote_sql?: string;
}

export interface PlanMetrics {
    estimated_rows?: number;
    estimated_bytes?: number;
    startup_cost?: number;
    total_cost?: number;
}
