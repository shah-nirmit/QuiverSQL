/**
 * Phase 9 — client-side result-page decoder + renderable view model.
 *
 * The daemon ships paged query results in one of two transport formats
 * (selected by the user's `qsql.resultFormat` setting):
 *
 *   - JSON (default): rows live in `QueryPage.data` as `Record<string, any>[]`.
 *   - Arrow IPC: a base64-encoded Arrow IPC stream lives in
 *     `QueryPage.data_ipc`. Decoded here via `apache-arrow`.
 *
 * The webview never sees `QueryPage` directly anymore; it consumes the
 * `RenderablePage` discriminated union returned by `decodeResultPage` so the
 * cell renderer can branch on `kind` and feed each cell through the
 * type-aware `formatCellValue` overload that takes an Arrow `DataType`.
 *
 * Errors during base64 decode or IPC parse surface as a thrown `Error`; the
 * caller is expected to swap to a structured error UI rather than silently
 * fall back to JSON (per the Phase 9 plan's "no auto-fallback" rule).
 */

import * as arrow from 'apache-arrow';
import { QueryPage, Schema, PerformanceMetrics } from './models';

/** Common fields every renderable page carries, regardless of transport. */
interface RenderablePageBase {
    query_id: string;
    schema: Schema;
    page_index: number;
    page_size: number;
    is_last: boolean;
    metrics: PerformanceMetrics;
    warning?: string;
}

/** Discriminated union the grid renderer iterates. */
export type RenderablePage =
    | (RenderablePageBase & { kind: 'json'; rows: Record<string, any>[] })
    | (RenderablePageBase & { kind: 'arrow'; table: arrow.Table });

/**
 * Decodes a daemon `QueryPage` into a `RenderablePage`. Pure / sync — no
 * VS Code or network dependencies — so unit tests can exercise it directly.
 */
export function decodeResultPage(page: QueryPage): RenderablePage {
    const base: RenderablePageBase = {
        query_id: page.query_id,
        schema: page.schema,
        page_index: page.page_index,
        page_size: page.page_size,
        is_last: page.is_last,
        metrics: page.metrics,
        warning: page.warning,
    };

    if (page.result_format === 'arrow_ipc') {
        if (!page.data_ipc) {
            throw new Error(
                "QueryPage.result_format is 'arrow_ipc' but data_ipc is missing — the daemon violated the wire-shape contract."
            );
        }
        const bytes = base64ToUint8Array(page.data_ipc);
        let table: arrow.Table;
        try {
            table = arrow.tableFromIPC(bytes);
        } catch (e: any) {
            throw new Error(`Failed to decode Arrow IPC page: ${e?.message ?? String(e)}`, {
                cause: e,
            });
        }
        return { ...base, kind: 'arrow', table };
    }

    // JSON path (default / explicit) — pass rows through unchanged.
    return { ...base, kind: 'json', rows: page.data };
}

/**
 * Returns the column names to render, in the order they appear in the page
 * schema. Falls back to the JSON-row keys when the schema is empty (matches
 * the legacy `getQueryPageColumns` behaviour the grid relies on for
 * malformed-but-not-fatal pages).
 */
export function columnsForPage(page: RenderablePage): string[] {
    if (page.schema.fields.length > 0) {
        return page.schema.fields.map(f => f.name);
    }
    if (page.kind === 'json' && page.rows.length > 0) {
        return Object.keys(page.rows[0]);
    }
    if (page.kind === 'arrow') {
        return page.table.schema.fields.map(f => f.name);
    }
    return [];
}

/** Number of rows the page will render — uniform across kinds. */
export function rowCount(page: RenderablePage): number {
    return page.kind === 'json' ? page.rows.length : page.table.numRows;
}

/**
 * Returns the `arrow.DataType` for a given column index when the page came
 * over IPC, or `undefined` for the JSON path. The webview cell formatter
 * uses this to branch on type (int64 → exact string, timestamp → ISO,
 * decimal → string passthrough, null → muted span).
 */
export function arrowTypeForColumn(
    page: RenderablePage,
    columnName: string,
): arrow.DataType | undefined {
    if (page.kind !== 'arrow') {
        return undefined;
    }
    const field = page.table.schema.fields.find(f => f.name === columnName);
    return field?.type;
}

/**
 * Reads a single cell value from a `RenderablePage`. For the Arrow path, it
 * pulls from the typed Vector so int64 stays a `bigint`, decimals stay
 * strings, etc. — the webview's formatter coerces these to user-facing
 * strings type-aware.
 */
export function cellAt(
    page: RenderablePage,
    rowIndex: number,
    columnName: string,
): unknown {
    if (page.kind === 'json') {
        const row = page.rows[rowIndex];
        return row?.[columnName];
    }
    const vector = page.table.getChild(columnName);
    return vector ? vector.get(rowIndex) : undefined;
}

/**
 * Base64 decoder that works under both Node (extension host) and the
 * webview. The extension host has `Buffer`; the webview has `atob`. We pick
 * `Buffer` when available because it's faster and handles wide payloads
 * without UTF-16 round-tripping.
 */
function base64ToUint8Array(b64: string): Uint8Array {
    if (typeof Buffer !== 'undefined') {
        return Uint8Array.from(Buffer.from(b64, 'base64'));
    }
    // Webview fallback — atob exists in browser-like environments.
    // Skip any whitespace the daemon may have inserted; we never insert any,
    // but defence-in-depth never hurts.
    const stripped = b64.replace(/\s+/g, '');
    const binary = atob(stripped);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        out[i] = binary.charCodeAt(i);
    }
    return out;
}
