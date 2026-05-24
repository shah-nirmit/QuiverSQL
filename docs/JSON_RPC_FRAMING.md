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
