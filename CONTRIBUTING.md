# Contributing To QuiverSQL

Thanks for taking a look at QuiverSQL. This project is an alpha prototype of a developer-first, Arrow-native query virtualization layer for VS Code. Contributions are welcome, but the codebase is still early, so the best contributions are focused, well-tested, and clear about the current product direction.

QuiverSQL has two big constraints that should guide every contribution:

- Keep the developer workflow local-first and VS Code-first until the interactive MVP is solid.
- Treat public interfaces carefully: daemon RPC methods, source-profile shapes, connector contracts, sample data, and documented commands should not churn casually.

This guide is intentionally detailed. It follows the same spirit as Microsoft SQL tooling contribution docs: discuss large work early, keep branches small, build locally before opening a pull request, include tests, and make reviews easy.

## Ways To Contribute

You can contribute without writing core engine code. Useful contributions include:

- Filing reproducible bugs with SQL, sample data shape, platform, and expected result.
- Improving README, architecture docs, quickstart instructions, or sample queries.
- Adding tests for file registration, SQLite federation, query detection, lineage, and daemon behavior.
- Improving VS Code UX around source attachment, query execution, result display, explain, and lineage.
- Adding connector groundwork such as capability metadata, SQL emitter tests, or source-specific edge cases.
- Improving sample data if it remains fictional, small, and useful across formats.
- Reviewing pull requests for correctness, test coverage, and contributor ergonomics.

## Before You Start Work

Open an issue first when the change:

- Adds or changes daemon RPC methods.
- Changes result JSON shape, error behavior, or source registration behavior.
- Adds a new connector or file format.
- Touches the planner, optimizer, lineage model, or catalog direction.
- Introduces a dependency, build step, generated artifact, or packaging behavior.
- Could break documented quickstart steps.

Small documentation fixes, typo fixes, test-only additions, and narrow bug fixes can go straight to a pull request.

## Repository Layout

```text
qsql-workspace/       Rust workspace for the daemon, core engine, and connectors
qsql-vscode/          VS Code extension
samples/quickstart/  Small fictional sample data for manual testing
.github/             CI, Dependabot, issue templates, and PR template
```

Important Rust crates:

- `qsql-core`: DataFusion-backed execution, file registration, result conversion, and lineage.
- `qsql-connectors`: connector traits and SQLite provider.
- `qsql-daemon`: stdio JSON-RPC process used by the extension.

Important extension areas:

- `daemonClient.ts`: daemon process discovery and JSON-RPC client.
- `extension.ts`: commands, CodeLens, attach flows, explain flow, and activation.
- `webviewPanel.ts`: result grid rendering.
- `dataSourcesProvider.ts` and `lineageProvider.ts`: tree views.

## Development Prerequisites

Install:

- Git
- Rust toolchain with Cargo, rustfmt, and Clippy
- Node.js 18 or newer
- VS Code 1.85 or newer

Windows notes:

- PowerShell commands in this guide assume you are at the repository root unless stated otherwise.
- If VS Code cannot find the daemon, set `qsql.daemonPath` to the absolute path of `qsql-daemon.exe`.

macOS/Linux notes:

- Use the same commands without `.exe` in daemon paths.
- If your shell does not load Cargo automatically, make sure Cargo is on `PATH`.

## Fork, Clone, And Branch

Fork the repository in GitHub, then clone your fork:

```powershell
git clone https://github.com/<your-user>/<your-fork>.git
cd <your-fork>
```

Add the upstream repository:

```powershell
git remote add upstream https://github.com/<owner>/<repo>.git
git fetch upstream
```

Create a focused branch:

```powershell
git checkout -b feature/short-description
```

Branch guidance:

- Use one branch per bug fix, feature, or documentation change.
- Keep branches rebased or merged with upstream regularly.
- Do not commit directly to `master` in your fork.
- Avoid mixing formatting-only changes with behavior changes.

## Build The Daemon

From the repository root:

```powershell
cd qsql-workspace
cargo build -p qsql-daemon
```

The debug daemon is written to:

```text
qsql-workspace/target/debug/qsql-daemon.exe
```

On macOS/Linux the binary is:

```text
qsql-workspace/target/debug/qsql-daemon
```

## Build The VS Code Extension

From the repository root:

```powershell
cd qsql-vscode
npm ci
npm run compile
```

To launch locally:

1. Open the repository in VS Code.
2. Press `F5` to start an extension development host.
3. Open or create a `.sql` file.
4. Use **QuiverSQL: Connect Data Source** or the QuiverSQL Explorer to attach sample data.
5. Run a query with `Ctrl+Enter` on Windows/Linux or `Cmd+Enter` on macOS.

If the daemon is not found automatically, set this VS Code setting:

```json
{
  "qsql.daemonPath": "C:/absolute/path/to/qsql-daemon.exe"
}
```

Use the non-`.exe` binary path on macOS/Linux.

## Run The Quickstart Samples

The committed samples live in:

```text
samples/quickstart/
```

They include:

- `employees.csv`
- `departments.ndjson`
- `projects.json`
- `orders.parquet`
- `demo.sqlite`

Suggested aliases:

```text
employees      samples/quickstart/employees.csv
departments    samples/quickstart/departments.ndjson
projects       samples/quickstart/projects.json
orders         samples/quickstart/orders.parquet
compensation   samples/quickstart/demo.sqlite table: compensation
offices        samples/quickstart/demo.sqlite table: offices
```

Manual smoke queries:

```sql
SELECT name, role, salary
FROM employees
WHERE salary > 90000
ORDER BY salary DESC;
```

```sql
SELECT e.name, e.role, c.bonus, c.review_score
FROM employees e
JOIN compensation c ON e.id = c.employee_id
ORDER BY c.review_score DESC;
```

```sql
SELECT e.name, o.product, o.amount
FROM employees e
JOIN orders o ON e.id = o.employee_id
WHERE o.shipped = true
ORDER BY o.amount DESC;
```

Regenerate samples when the sample schema intentionally changes:

```powershell
cd qsql-workspace
cargo run -p qsql-connectors --example generate_quickstart_samples
```

Sample data rules:

- Keep samples fictional.
- Keep files small enough for source control.
- Update `samples/quickstart/README.md` and root `README.md` if aliases or query examples change.
- Add or update tests when a sample format demonstrates supported behavior.

## Required Checks

Run Rust checks:

```powershell
cd qsql-workspace
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

Run extension checks:

```powershell
cd qsql-vscode
npm ci
npm run typecheck
npm run lint
npm run test:scanner
```

Run whitespace checks before opening a pull request:

```powershell
git diff --check
```

## Testing Expectations

Use the smallest useful test for the risk of your change.

| Change Type | Expected Test Coverage |
| --- | --- |
| Rust engine behavior | Unit or integration tests under `qsql-workspace`. |
| Connector behavior | Connector tests plus at least one DataFusion registration/query path when relevant. |
| Daemon RPC behavior | Integration-style tests or a clear manual JSON-RPC smoke test. |
| VS Code query detection | Scanner tests under `qsql-vscode/src/test`. |
| VS Code UI behavior | Typecheck, lint, and a manual extension-host smoke test. |
| Documentation only | `git diff --check` and read the changed docs end to end. |
| Sample data | Regenerate samples and run the quickstart sample integration test. |

## Versioning Changes

The current project version lives in `VERSION` and is mirrored into the Rust crate manifests, VS Code package metadata, lockfiles, and changelog. See `docs/VERSIONING.md` for the full checklist.

When a pull request changes the version:

- Update every version surface in the versioning checklist.
- Update `CHANGELOG.md` with user-visible changes.
- Keep alpha versions in SemVer prerelease form, such as `0.1.0-alpha.0`.
- Verify the daemon `version` JSON-RPC method and the `QuiverSQL: Show Version` command still report the expected values.

## Coding Guidelines

General:

- Prefer existing patterns over new abstractions.
- Keep changes scoped to the module that owns the behavior.
- Avoid unrelated refactors in feature or bug-fix pull requests.
- Add comments only when they explain non-obvious behavior.
- Do not commit generated build outputs.

Rust:

- Run `cargo fmt` before submitting.
- Keep `clippy -D warnings` clean.
- Return useful errors instead of panicking in production code.
- Keep tests deterministic and use temporary files for generated test data.
- Keep `Cargo.lock` updated for reproducible daemon builds.

TypeScript:

- Keep `npm run typecheck` and `npm run lint` clean.
- Escape user or query data before rendering it in webviews.
- Keep VS Code command IDs stable once documented.
- Prefer typed helpers over ad hoc `any` where the data shape is stable.

## Connector Guidelines

Connector work should be explicit about capabilities. When adding or changing a connector, document:

- Source kind and connection method.
- Supported type mapping into Arrow/DataFusion.
- Whether filters, projections, limits, aggregates, joins, and functions are pushed down.
- How identifiers are quoted.
- How errors are redacted.
- What test database or fixture is required.

For early connectors, correctness and clear limitations matter more than broad feature coverage.

## Daemon RPC Guidelines

The daemon is the boundary between the VS Code extension and Rust runtime. Treat it as a public interface even during alpha.

When changing RPC behavior:

- Keep existing methods backward compatible when possible.
- Add a test or documented manual smoke command.
- Return structured errors where practical.
- Do not leak secrets, local credentials, or full connection strings in error messages.
- Update README and extension code together when a workflow changes.

Current methods include:

- `ping`
- `version`
- `execute`
- `execute_json`
- `register_file`
- `register_sqlite`
- `get_lineage`

## Documentation Guidelines

Documentation should match the alpha reality:

- Be clear about what works now, what is partial, and what is planned.
- Keep quickstart commands copy-pasteable.
- Use paths that exist in the repository.
- Avoid claims about production readiness, hosted services, marketplace packages, or unsupported connectors.
- Update support checklists when capabilities move from planned to partial or supported.

## Security And Data Guidelines

Do not commit:

- Credentials
- Connection strings
- Private database files
- Customer data
- Query result exports from real systems
- Local environment files

Use only fictional data in samples and tests. If a bug requires sensitive reproduction details, sanitize the schema and data before opening an issue.

## Commit Hygiene

Good commits make review easier.

- Use descriptive commit messages.
- Keep generated sample updates in the same commit as the generator/schema change.
- Keep formatting-only commits separate from behavior changes when possible.
- Do not include `target/`, `out/`, `node_modules/`, `.vsix`, logs, or local IDE files.

## Pull Request Process

Before opening a pull request:

1. Sync with upstream.
2. Re-run the checks relevant to your change.
3. Review your own diff.
4. Make sure docs and samples match behavior.
5. Fill out the pull request template.

A good pull request includes:

- A short summary of the change.
- Why the change is needed.
- Test commands run and their results.
- Screenshots for meaningful VS Code UI changes.
- Any limitations or follow-up work.
- Links to related issues.

Review expectations:

- Maintainers may ask for smaller scope, more tests, or docs changes.
- Breaking changes may be declined or deferred if they do not match the project roadmap.
- The project may favor simple, correct alpha behavior over broad but brittle support.

## Issue Guidelines

For bugs, include:

- QuiverSQL commit or version.
- Operating system.
- VS Code version.
- Rust and Node versions if build-related.
- Data source type.
- Minimal SQL query.
- Expected result and actual result.
- Relevant error text with secrets removed.

For features, include:

- The workflow you want to enable.
- Why current workarounds are insufficient.
- Whether this belongs in the VS Code extension, daemon, connector layer, planner, lineage, or docs.
- Any compatibility or security concerns.

## Public API And Compatibility

QuiverSQL is alpha, so some change is expected. Still, treat these as compatibility-sensitive:

- Documented commands and settings.
- Daemon RPC method names and result shapes.
- Sample aliases and quickstart flows.
- Connector trait behavior.
- Source profile and table reference models once introduced.

When in doubt, open an issue before implementing.

## License

By contributing, you agree that your contributions will be licensed under the Apache License, Version 2.0.
