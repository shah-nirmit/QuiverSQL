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
    data: Record<string, any>[];
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
}

export interface QueryPageRequest {
    query_id: string;
    page_index?: number;
    page_size?: number;
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

export interface ExplainQueryResult {
    sql: string;
    federated_plan: PlanGraph;
    source_plans: Record<string, any>;
    raw: string;
    warnings: string[];
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
}

export interface PlanMetrics {
    estimated_rows?: number;
    estimated_bytes?: number;
    startup_cost?: number;
    total_cost?: number;
}
