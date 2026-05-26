# QuiverSQL Versioning

QuiverSQL uses SemVer for public releases.

The current project version is `0.3.1-alpha.0`. Keep these files in sync when changing it:

- `VERSION`
- `qsql-workspace/qsql-core/Cargo.toml`
- `qsql-workspace/qsql-connectors/Cargo.toml`
- `qsql-workspace/qsql-daemon/Cargo.toml`
- `qsql-vscode/package.json`
- `qsql-vscode/package-lock.json`
- `qsql-workspace/Cargo.lock`
- `CHANGELOG.md`

## Alpha Rules

Alpha versions use prerelease labels such as `0.1.0-alpha.0`, `0.1.0-alpha.1`, and `0.1.0-alpha.2`.

Because QuiverSQL is still pre-1.0, breaking changes can happen in minor and prerelease versions. Breaking changes should still be intentional, documented in `CHANGELOG.md`, and called out clearly in pull requests.

## Runtime Version Surfaces

The daemon exposes a JSON-RPC `version` method:

```json
{
  "jsonrpc": "2.0",
  "method": "version",
  "id": 1
}
```

The response includes the product version, daemon crate version, core crate version, connector crate version, and RPC protocol version.

The VS Code extension contributes `QuiverSQL: Show Version`, which displays the extension version and daemon component versions.

## Suggested Release Checklist

- Update all version files and manifests listed above.
- Update `CHANGELOG.md`.
- Run Rust and VS Code verification commands from the README.
- Tag the release as `v<version>`, for example `v0.1.0-alpha.0`.
- Attach or document build artifacts only after the CI workflow is green.
