# QuiverSQL

**Current version:** `0.3.1-alpha.0`<br>
**Release status:** alpha prototype

QuiverSQL is a developer-first, Arrow-native query virtualization layer: lighter than Dremio, Denodo, Trino, and Starburst; broader than DuckDB or Apache DataFusion alone; and focused first on interactive SQL over files plus heterogeneous databases from VS Code.

The project is currently an alpha prototype. The intended product direction is a Q3 2026 interactive launch for the VSIX and translation engine, followed by API and CLI hardening toward Q4 2027.

QuiverSQL is local-first today: a TypeScript VS Code extension talks to a Rust daemon over JSON-RPC, the daemon embeds DataFusion, and results move through Arrow/DataFusion into a VS Code result grid. This repository is not production ready yet, but it now contains the core shape of the original architecture: local SQL execution, file-backed virtual tables, SQLite and SQL database federation, paged result delivery, source replay, federated explain visualisation with per-table pushdown SQL, sort/broadcast pushdown badges, and basic lineage.

## Product Thesis

Current data workflows often force developers to convert files to Parquet, register external tables, use a heavier federated engine, materialize cross-source joins into staging tables, then inspect lineage afterward. QuiverSQL's opening is to make that flow interactive, local-first, and source-aware.

QuiverSQL is meant to sit between these existing solutions:

| Area | Existing Workaround | QuiverSQL Direction |
| --- | --- | --- |
| SQL over local files | DuckDB is excellent for CSV, JSON, Parquet, Excel, cloud/object storage, and local analytics. | Keep the file-query ergonomics, but add source-aware VS Code workflows, lineage, and federation. |
| Rust-native query execution | Apache DataFusion provides Arrow-native SQL/DataFrame execution and extensibility. | Use DataFusion as the execution foundation, while adding product UX, connectors, catalog, and planner behavior. |
| Federated SQL | Trino and Starburst solve large-scale federation with catalogs and connectors. | Offer a lighter local developer tool before cluster operations are needed. |
| Enterprise virtualization | Dremio and Denodo provide governance, caching, catalogs, and enterprise federation. | Stay smaller, embedded, open, and developer-first. |
| Cloud external queries | Athena, BigQuery, Snowflake, and Databricks reduce ETL inside cloud ecosystems. | Avoid cloud lock-in and fragmented SQL by making local interactive workflows pleasant first. |
| Translation and lineage components | SQLGlot, ADBC, Substrait, OpenLineage, and Marquez solve pieces of the stack. | Integrate translation, execution, explain, and lineage into one VS Code-centered workflow over time. |

## Target Architecture

```mermaid
flowchart TB
  subgraph UX["User Interfaces"]
    VSIX["VS Code Extension\nSQL editor, source explorer,\nresult grid, explain, lineage graph"]
    CLI["Future CLI / API\nQ4 2027"]
  end

  subgraph Control["Local QuiverSQL Control Plane"]
    Gateway["QuiverSQL Local Daemon\nJSON-RPC today\nArrow IPC / Flight later"]
    Secrets["Secrets + Auth\nVS Code SecretStorage / OS keychain"]
    Parser["QuiverSQL Parser + Validator\ncanonical SQL dialect"]
    Catalog["Virtual Catalog\nsources, schemas, stats, capabilities"]
    Planner["Logical Planner\nDataFusion logical plan"]
    Optimizer["Federated Optimizer\npushdown, join placement,\ntemp staging, cost rules"]
    Explain["Explain + Translation Output"]
    Lineage["Lineage Compiler\nquery, table, column graph"]
  end

  subgraph Runtime["Execution Runtime"]
    Exec["Execution Coordinator"]
    DF["Arrow/DataFusion Engine\nvectorized, streaming, spill"]
    Files["File Table Providers\nCSV, delimited, fixed width,\nJSON, Excel, Parquet"]
    Remote["Remote SQL Delegates\nADBC/native drivers/JDBC bridge"]
    Cache["Result + Metadata Cache\nArrow IPC / local DuckDB or SQLite"]
  end

  subgraph Sources["Data Sources"]
    FS["Local/Unix FS\nS3/Azure/GCS/HDFS"]
    DB["Postgres, MySQL, SQL Server,\nOracle, SQLite, Snowflake, etc."]
    LineageSink["OpenLineage-compatible sink\nplus local lineage store"]
  end

  VSIX --> Gateway
  CLI --> Gateway
  Gateway --> Secrets
  Gateway --> Parser
  Parser --> Catalog
  Catalog --> Planner
  Parser --> Planner
  Planner --> Optimizer
  Optimizer --> Exec
  Optimizer --> Explain
  Exec --> DF
  DF --> Files
  Exec --> Remote
  Exec --> Cache
  Files --> FS
  Remote --> DB
  Planner --> Lineage
  Exec --> Lineage
  Lineage --> LineageSink
  Explain --> VSIX
  Cache --> VSIX
```

The repository implements the early VSIX plus local daemon slice of this diagram. Many control-plane and optimizer components are still planned.

## Support Checklist

### Data Sources Supported

- [x] CSV
- [x] Parquet
- [x] NDJSON / newline-delimited JSON
- [x] SQLite
- [x] Postgres
- [x] MySQL / MariaDB
- [x] Fixed-width files
- [ ] Excel `.xlsx`
- [ ] SQL Server
- [ ] Oracle
- [ ] Snowflake
- [ ] S3 / Azure Blob / GCS / HDFS object or distributed storage

### Features Supported

- [x] VS Code extension
- [x] Local Rust daemon
- [x] JSON-RPC over stdio
- [x] DataFusion-backed SQL execution
- [x] Attach local files as virtual tables
- [x] Attach SQLite tables as virtual tables
- [x] Attach Postgres tables as virtual tables
- [x] Attach MySQL / MariaDB tables as virtual tables
- [x] Query editor command and CodeLens run action
- [x] JSON result delivery
- [x] Paged JSON result delivery
- [x] VS Code result grid
- [x] Federated `EXPLAIN` with per-table pushed-down SQL, Remote EXPLAIN, and DataFusion physical plan
- [x] Plan-graph badges for broadcast and sort pushdowns (multi-surface, evidence-driven)
- [x] Basic table/column lineage from resolved logical plans
- [x] Basic joins across registered file, SQLite, Postgres, and MySQL/MariaDB tables
- [x] Quickstart sample data
- [x] Runtime virtual catalog
- [x] Persisted VS Code source profiles
- [x] Source profiles with capability metadata
- [x] VS Code SecretStorage / OS keychain integration
- [x] Query cancellation and timeout support
- [x] Predicate pushdown for supported SQL connector filters
- [x] Projection pushdown for SQL connectors
- [x] Limit pushdown for SQL connectors
- [x] Sort / top-k pushdown for SQL connectors
- [x] Broadcast-join rewrite for small local input ⋈ remote fact tables
- [x] Provider-specific icons in the Data Sources explorer and plan graph
- [x] Federated plan visualization with per-table pushed-down SQL cards
- [ ] Aggregate pushdown
- [ ] Cost-aware federated optimizer
- [ ] Join placement strategy
- [ ] Connector-specific SQL translation output
- [ ] OpenLineage-compatible run events
- [x] Paged result grid
- [ ] Streaming result grid without full result collection
- [ ] Public CLI
- [ ] Public API
- [ ] Packaged VSIX and daemon installers

### Detailed Status

| Capability | Status | Notes |
| --- | --- | --- |
| VS Code extension | Supported now | QuiverSQL Explorer, SQL command, CodeLens, result grid, explain panel, lineage tree. |
| Local daemon | Supported now | JSON-RPC over stdio. Arrow IPC/Flight is planned. |
| DataFusion execution | Supported now | DataFusion is the current Rust-native execution engine. |
| CSV files | Supported now | Registered as virtual tables through DataFusion. |
| Parquet files | Supported now | Registered as virtual tables through DataFusion. |
| NDJSON / JSON files | Supported now | Uses DataFusion's NDJSON reader. `.json` samples are newline-delimited JSON. |
| SQLite tables | Supported now | SQLite tables register through a DataFusion `TableProvider` with projection/filter/limit SQL pushdown. |
| Postgres tables | Supported now | Tables can be registered through the daemon and VS Code wizard. Live tests are env-gated with `QSQL_POSTGRES_URL`. |
| MySQL / MariaDB tables | Supported now | Tables can be registered through a shared MySQL/MariaDB connector. Live tests are env-gated with `QSQL_MYSQL_URL`. |
| Fixed-width files | Supported now | Custom streaming `TableProvider` + `ExecutionPlan` driven by a JSON layout sidecar (column name, byte-offset, length, SQL type). Honours projection + limit pushdown; filter pushdown is `Unsupported` so DataFusion wraps the scan in a `FilterExec`. |
| Quickstart sample data | Supported now | CSV, NDJSON, JSON, Parquet, and SQLite samples live in `samples/quickstart/`. |
| Runtime catalog | Supported now | Daemon tracks source metadata, schema, status, and connector capabilities. |
| Persisted source replay | Supported now | VS Code stores source profiles and replays them on activation. Database connection strings are stored in SecretStorage. |
| Projection/filter/limit pushdown | Supported now | Implemented for SQLite, Postgres, MySQL, and MariaDB through `datafusion-federation`'s SQL unparser. |
| Sort / top-k pushdown | Supported now | `ORDER BY` (with optional `LIMIT`) is embedded into the remote SQL when the entire sort subtree is a single federated source. Cross-source sorts still execute locally in DataFusion. |
| Broadcast-join rewrite | Supported now | Small local input ⋈ remote fact tables: the rewrite materialises the local DISTINCT key set, injects it as an `IN (…)` filter on the remote scan, and re-optimises so federation embeds the predicate in the source-native SQL. |
| Query paging | Supported now | `query_start`, `query_page`, and `query_cancel` provide paged JSON result delivery and cancellation. |
| Federated joins | Partial | Local DataFusion can join registered file and SQL tables; broadcast rewrite covers the small-local + remote-fact case. Join placement and cross-source cost planning are early. |
| Explain plan | Supported now | Federated plan graph with provider-specific icons, per-table cards showing Native SQL → Remote EXPLAIN → Logical fragment, collapsible DataFusion physical plan, and evidence-driven broadcast / sort pushdown badges. |
| Lineage | Partial | Basic table/column lineage from resolved logical plans exists; OpenLineage-compatible events are planned. |
| Daemon discovery | Partial | `qsql.daemonPath` is configurable; packaged binaries and installers are planned. |
| Result grid | Partial | Current grid supports paged JSON results and cancellation; true streaming/Arrow IPC pages are planned. |
| Excel `.xlsx` | Planned | Part of the original file-provider goal. |
| Fixed-width files | Planned | Expected to require explicit schema/layout files first. |
| SQL Server connector | Planned | Connector trait and SQL emitter work needed first. |
| Aggregate pushdown and join placement | Planned | Needed for serious cross-source federation. |
| OpenLineage events | Planned | QuiverSQL-specific logical lineage exists first; OpenLineage sink comes later. |
| Public CLI / API | Planned | Designed early, hardened toward Q4 2027. |
| Marketplace VSIX / installers | Not started | The repo is currently source-first for contributors. |

## Current Repository Progress

The current implementation maps to the first alpha slice of the original delivery plan:

- `qsql-core`: DataFusion session, SQL execution, paged JSON result models, cancellation, file registration, table-provider registration, runtime catalog, and basic lineage.
- `qsql-connectors`: shared SQL pushdown provider layer plus SQLite, Postgres, and MySQL/MariaDB connectors.
- `qsql-daemon`: stdio JSON-RPC daemon with health/version, execution, paging/cancellation, source catalog, file/SQLite/Postgres/MySQL/MariaDB registration, and lineage.
- `qsql-vscode`: VS Code extension with data-source explorer, SecretStorage-backed source replay, connect wizard, query execution, paged result grid, explain panel, and lineage tree.
- `samples/quickstart`: fictional, committed sample data across the formats currently supported.

## Quickstart

### Prerequisites

- Rust toolchain with Cargo
- Node.js 18 or newer
- VS Code 1.85 or newer

### Build The Daemon

```powershell
cd qsql-workspace
cargo build -p qsql-daemon
```

The debug daemon will be written to:

```text
qsql-workspace/target/debug/qsql-daemon.exe
```

On macOS/Linux the binary name is `qsql-daemon`.

### Build The Extension

```powershell
cd qsql-vscode
npm ci
npm run compile
```

Open this repository in VS Code, press `F5`, and launch the extension host.

If the extension cannot find the daemon automatically, set `qsql.daemonPath` in VS Code settings to the absolute path of the daemon binary.

### Try The Sample Data

The quickstart samples live in `samples/quickstart/` and include:

- `employees.csv`
- `departments.ndjson`
- `projects.json` as newline-delimited JSON
- `orders.parquet`
- `demo.sqlite`

From the QuiverSQL Explorer, use **QuiverSQL: Connect Data Source** to attach the sample files and tables. Suggested aliases:

```text
employees      samples/quickstart/employees.csv
departments    samples/quickstart/departments.ndjson
projects       samples/quickstart/projects.json
orders         samples/quickstart/orders.parquet
compensation   samples/quickstart/demo.sqlite table: compensation
offices        samples/quickstart/demo.sqlite table: offices
```

Then run:

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

### Try SQL Database Connectors

Postgres, MySQL, and MariaDB support is available through the VS Code connect wizard and daemon RPC methods. The current SQL connector scope is intentionally narrow: table registration, schema introspection, `SELECT` scans, projection pushdown, supported basic filter pushdown, and limit pushdown.

Connection strings are stored by the VS Code extension in SecretStorage. Daemon catalog responses redact credentials.

Example local test URLs:

```text
QSQL_POSTGRES_URL=postgres://qsql_test:qsql_test@localhost:5432/qsql_test
QSQL_MYSQL_URL=mysql://qsql_test:qsql_test@localhost:3306/qsql_test
```

The optional live connector tests are ignored when those environment variables are missing and run automatically when the variables are set.

For Docker setup, shell-specific environment variable syntax, and live-test commands, see [SQL Connector Testing](docs/SQL_CONNECTOR_TESTING.md).

### Regenerate Samples

The committed sample files are small, fictional, and safe to keep in the repository. To regenerate them:

```powershell
cd qsql-workspace
cargo run -p qsql-connectors --example generate_quickstart_samples
```

## Development Commands

Rust:

```powershell
cd qsql-workspace
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

VS Code extension:

```powershell
cd qsql-vscode
npm ci
npm run typecheck
npm run lint
npm run test
```

## Versioning

QuiverSQL uses SemVer. The current alpha version is recorded in:

- `VERSION`
- `qsql-workspace/*/Cargo.toml`
- `qsql-vscode/package.json`
- `qsql-vscode/package-lock.json`
- `CHANGELOG.md`

During the alpha period, versions use prerelease labels such as `0.2.0-alpha.0`. Breaking changes can still happen before the first stable release, but version bumps should still describe the user-visible change clearly in the changelog.

The daemon exposes a JSON-RPC `version` method with product, daemon, core, connector, and RPC protocol versions. The VS Code extension also includes `QuiverSQL: Show Version`, which displays the extension version and daemon component versions when the daemon is available.

## Roadmap

| Phase | Status | Major Work |
| --- | --- | --- |
| Phase 0 | Complete | Baseline correctness, JSON-RPC tests, benchmark harness, quickstart fixtures, scanner/webview tests. |
| Phase 1 | Complete | Typed contracts, structured JSON-RPC errors, capability metadata, serde golden tests. |
| Phase 2 | Complete | `query_start`, `query_page`, `query_cancel`, first-page metadata, paged grid, cancellation/timeouts. |
| Phase 3 | Complete | Runtime catalog, persisted source profiles, activation replay, SecretStorage-backed credentials. |
| Phase 4 | Complete | SQL emission hooks, SQLite/Postgres/MySQL/MariaDB connectors, projection/filter/limit pushdown. |
| Phase 5 | Complete | Query plan visualization: federated plan graph, Tree/Table/Source tabs, native source plans, truncation safeguards. |
| Phase 6 | Complete | Database-level registration, architecture-review remediation (streaming runtime, scan guards, broadcast-join rewrite, schema cache, generation counters, credential redaction at the JSON-RPC boundary, LSP-style framing). |
| Phase 7 | Complete | Sort / top-k pushdown formalisation, scan-guard structured error code + UX, evidence-driven broadcast & sort badges, per-table pushed-down SQL cards in the Explain panel, provider-specific icons in the Data Sources explorer. |
| Phase 8 | Complete | Fixed-width file support: JSON layout sidecar, streaming `TableProvider`/`ExecutionPlan`, two-file VS Code wizard branch, source-profile persistence + replay, parity tests against the CSV equivalent, 100K-row medium-fixture smoke. |
| Phase 9 | Planned | Arrow IPC result pages with JSON fallback. |
| Phase 10 | Planned | Aggregate pushdown, cost-aware federated optimizer, lineage graph view. |
| Phase 11 | Planned | Packaging, release artifacts, benchmark report gates, and installer smoke tests. |

## Contributing

QuiverSQL welcomes issues, docs improvements, tests, connector work, VS Code UX improvements, sample data improvements, and code review. For small documentation fixes, a direct pull request is fine. For larger features, connector work, daemon RPC changes, or public behavior changes, open an issue first so the design can be discussed before implementation.

High-level flow:

1. Fork and clone the repository.
2. Create a focused branch for one bug, feature, or docs change.
3. Build the daemon and extension locally.
4. Run the quickstart samples or a relevant manual smoke test.
5. Run the required Rust and TypeScript checks.
6. Open a pull request with a clear summary, test results, and any known limitations.

The detailed contributor workflow is in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
