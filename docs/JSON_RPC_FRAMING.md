# QuiverSQL JSON-RPC Framing

QuiverSQL keeps JSON-RPC 2.0 over daemon stdin/stdout as the local transport.

## Request Framing

The daemon accepts two request frame formats:

- `Content-Length: <bytes>\r\n\r\n<body>` for robust framed requests. This is the preferred format for new clients because the JSON body can contain newlines and can be read without relying on line boundaries.
- One JSON request per newline for backward compatibility with existing scripts and tests.

## Response Framing

The daemon now emits every response as `Content-Length: <bytes>\r\n\r\n<body>`. Clients should parse the header, read the exact byte count, and then decode the JSON-RPC response body.

The VS Code client can still parse legacy newline-delimited responses so older daemon builds remain usable during alpha development, but new daemon responses no longer rely on newline framing.

## Guidance

Use `Content-Length` for large requests, pretty-printed JSON, or request bodies that may contain embedded newlines. Keep request ids stable and expect normal JSON-RPC `result` or `error` response objects. Response `Content-Length` counts UTF-8 bytes, not characters.

## Binary Result Format (Phase 9)

Paged query results have an opt-in transport format selected by the `result_format` field on `QueryStartRequest` (and, less commonly, per-page on `QueryPageRequest`). Accepted values:

- **`"json"`** — default. Rows materialise into `QueryPage.data` as a JSON array of `Record<string, any>`. Easy to inspect in any tool but lossy for `int64` (JS Number truncates above 2^53), decimals, and timestamps.
- **`"arrow_ipc"`** — opt-in. The page payload travels in `QueryPage.data_ipc` as a base64-encoded Arrow IPC stream. The daemon writes via `arrow::ipc::writer::StreamWriter`; clients decode via any Arrow IPC reader (e.g. `apache-arrow.tableFromIPC()` for the VS Code client). Preserves Arrow types end-to-end and is materially faster + smaller for big pages.

The daemon persists the chosen `result_format` on the streaming session, so subsequent `query_page` calls inherit it — clients normally only set the field on `query_start`. Unknown values yield a structured `-32602 Invalid params` error.

### Wire-shape invariant

For any given `QueryPage`:

- When `result_format == "json"` (or omitted): `data` carries the rows and `data_ipc` is omitted from the response.
- When `result_format == "arrow_ipc"`: `data_ipc` carries the base64 payload and `data` is the empty array `[]`. `result_format` is echoed back to the client.

Both `data_ipc` and `result_format` use `#[serde(skip_serializing_if = "Option::is_none")]` so legacy JSON clients see byte-identical responses on the default path.

### Example: opt-in `query_start`

```
Content-Length: 84

{"jsonrpc":"2.0","id":1,"method":"query_start","params":{"sql":"SELECT * FROM employees","result_format":"arrow_ipc"}}
```

The response shape (eliding metadata):

```jsonc
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "query_id": "q_1",
    "schema": { "fields": [ /* … */ ] },
    "page_index": 0,
    "page_size": 1000,
    "is_last": false,
    "data": [],                              // empty — IPC mode
    "data_ipc": "QVJSCkLAAA…",               // base64 Arrow IPC stream
    "result_format": "arrow_ipc",
    "metrics": { /* … */ }
  }
}
```

### When to use which

- **JSON** is the right default for ad-hoc / small-result workflows where eyeballing the payload matters and type fidelity is acceptable.
- **Arrow IPC** wins when results are large (>10K rows), contain `int64` outside the JS Number-safe range, contain decimals or timestamps where lossy ISO/float coercion would lose information, or the client has an Arrow consumer ready (a typed grid, a Polars/DuckDB ingest path, a notebook kernel).

The VS Code extension exposes this as a single setting — `qsql.resultFormat: "json" | "arrow_ipc"` (default `"json"`) — and threads it through `query_start` automatically.
