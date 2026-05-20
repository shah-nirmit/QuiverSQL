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
    page_index: number;
    is_last: boolean;
    data: Record<string, any>[];
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
    rows_produced: number;
}
