# QuiverSQL User Guide & Feature Demo

Welcome to **QuiverSQL**! This guide will walk you through setting up and demonstrating all the core features of the VS Code extension, including local file querying, remote database connections, federated joins, broadcast join optimizations, sort pushdowns, safety guards, and the revamped Explain Plan with per-table pushdown SQL.

---

## 1. Setup & Initialization

Before starting, ensure you have the required databases running and the extension compiled.

### A. Start the Demo Databases
QuiverSQL includes a `docker-compose.yml` file that provisions PostgreSQL and MySQL databases loaded with a realistic e-commerce and HR schema.

Open a terminal in the root workspace and run:
```bash
docker-compose down -v
docker-compose up -d
```
*Wait ~10 seconds for the databases to initialize.*

### B. Compile and Launch the VS Code Extension
1. Open a terminal in the root workspace and build the backend daemon:
   ```bash
   cd qsql-workspace
   cargo build -p qsql-daemon
   ```
2. Open another terminal and compile the VS Code client:
   ```bash
   cd qsql-vscode
   npm ci
   npm run compile
   ```
3. Press **F5** in VS Code (or select **Run > Start Debugging**) to open the **Extension Development Host**.

*(Note: Ensure `qsql.daemonPath` in the Host's Settings is pointing to your built `qsql-daemon` debug binary, e.g., `/absolute/path/to/qsql-workspace/target/debug/qsql-daemon`)*

---

## 2. Connecting Data Sources

In the **Extension Development Host**, open the QuiverSQL sidebar by clicking the Quiver icon in the Activity Bar. You will see the **Data Sources** view.

### A. Attaching Local Files & SQLite
1. Open the Command Palette (`Cmd+Shift+P` on Mac, `Ctrl+Shift+P` on Windows/Linux) and search for **QuiverSQL: Attach File as Table**.
2. Select `samples/quickstart/employees.csv` and name the table `employees`.
3. Select `samples/quickstart/departments.ndjson` and name the table `departments`.
4. Open the Command Palette and run **QuiverSQL: Attach SQLite Database**.
5. Select `samples/quickstart/demo.sqlite` and provide the alias `sqlite`.

### C. Attaching a Fixed-Width File

Fixed-width text files have no header row, so QuiverSQL needs a JSON **layout sidecar** that describes each column's byte-offset, length, type, and nullability. The repository ships a sample pair under `samples/quickstart/`:

- `employees_fwf.txt` — the data file, six 79-byte rows mirroring `employees.csv` row-for-row.
- `employees_fwf.layout.json` — the matching layout. Each entry binds a column name to a byte span and a SQL type:

  ```json
  {
    "fields": [
      { "name": "id",            "start": 0,  "length": 6,  "type": "INTEGER", "nullable": false },
      { "name": "name",          "start": 6,  "length": 24, "type": "VARCHAR", "nullable": false },
      { "name": "department_id", "start": 30, "length": 2,  "type": "INTEGER", "nullable": false },
      { "name": "role",          "start": 32, "length": 25, "type": "VARCHAR", "nullable": false },
      { "name": "salary",        "start": 57, "length": 6,  "type": "INTEGER", "nullable": false },
      { "name": "location",      "start": 63, "length": 16, "type": "VARCHAR", "nullable": false }
    ]
  }
  ```

  Type names accept any SQL spelling that QuiverSQL's connectors already understand — `INTEGER`, `BIGINT`, `VARCHAR`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`, etc. Strings are ASCII-trimmed by default; flip `"trim": false` per field if you need the literal padded value.

To attach the sample:

1. Open the Command Palette and run **QuiverSQL: Connect Data Source**.
2. Pick **Fixed-width File**.
3. Select `samples/quickstart/employees_fwf.txt` as the data file.
4. Select `samples/quickstart/employees_fwf.layout.json` as the layout JSON.
5. Confirm the table alias `employees_fwf`.

Smoke query (run from a new `.sql` buffer):

```sql
SELECT id, name, role, salary
FROM employees_fwf
WHERE salary > 90000
ORDER BY salary DESC;
```

Returns the same three high earners as the CSV-backed `employees` table — that's the row-for-row parity the daemon integration test asserts (`fixed_width_matches_csv_equivalent_row_for_row` in `qsql-daemon/tests/fixed_width_tests.rs`).

If the layout file has overlapping spans, an unknown type, a zero-length field, or an empty `fields` array, registration fails with a descriptive error pointing at the offending field — the wizard surfaces this through a standard error banner so you can fix the layout and retry without touching the daemon.

### B. Connecting Remote Databases
1. Click the **+** (Connect Data Source) icon in the Data Sources view header (or run **QuiverSQL: Connect Data Source**).
2. Connect PostgreSQL:
   - **Database Type**: PostgreSQL
   - **Connection URL**: `postgres://qsql_test:qsql_test@localhost:5432/qsql_test`
   - **Alias**: `pg`
3. Connect MySQL:
   - **Database Type**: MySQL
   - **Connection URL**: `mysql://qsql_test:qsql_test@localhost:3306/qsql_test`
   - **Alias**: `mysql`

You should now see all five sources (`employees`, `departments`, `sqlite`, `pg`, `mysql`) listed in the Data Sources tree view — each with its own **provider-specific icon** (PostgreSQL elephant for `pg`, MySQL dolphin for `mysql`, SQLite emblem for `sqlite`, grid for the CSV `employees`, braces for the NDJSON `departments`). Hover any source to confirm its provider label in the tooltip.

---

## 3. Querying Local Files

Create a new file called `demo.sql`. Let's run a simple local join between CSV and NDJSON.

```sql
SELECT
    e.name,
    e.role,
    e.salary,
    d.name as department
FROM employees e
JOIN departments d ON e.department_id = d.id;
```
*Highlight the query (or just place your cursor in it) and click the inline **▶ Run Query** CodeLens button. The results will appear instantly in the Result Grid panel.*

---

## 4. Federated Cross-Database Queries

QuiverSQL allows you to write standard SQL that transparently joins data across entirely different database systems.

Let's join our PostgreSQL identity data (`pg.customers`) with our MySQL operational data (`mysql.orders`).

```sql
SELECT 
    c.name as customer_name,
    c.region,
    cp.tier,
    o.order_total,
    p.name as product_name
FROM pg.customers c
JOIN pg.customer_profiles cp ON c.id = cp.customer_id
JOIN mysql.orders o ON c.id = o.customer_id
JOIN mysql.products p ON o.product_id = p.id
ORDER BY o.order_total DESC;
```
*Run this query. QuiverSQL's engine automatically extracts the relevant subsets of data from Postgres and MySQL concurrently and performs the final complex join locally.*

---

## 5. Visualizing Query Lineage & Explain Plans

### Query Lineage
While your cursor is inside the federated query above, look at the **Query Lineage** tree view in the QuiverSQL sidebar. You will see a visual representation of all the exact database tables being referenced (`pg.customers`, `pg.customer_profiles`, `mysql.orders`, `mysql.products`).

### Explain Plan & Sort Pushdowns
1. Above the federated query, click the **📊 Explain Query** CodeLens.
2. A beautiful SVG Plan Graph will open. The top of the **Tree** tab has a legend bar explaining badge colours (`Broadcast pushdown`, `Sort pushdown`, `TableScan`).
3. Locate the `Sort` nodes. You will notice that QuiverSQL recognized the `ORDER BY` clause and pushed it down to the native databases where applicable! Look for the **`Sort ↓ pushed`** badge on **both** the `Sort` node *and* the `TableScan` it feeds — the badge on the scan tells you which scan returns pre-sorted rows.
4. Click any `TableScan` to jump to its per-table card in the **Source** tab (see Section 8 below).

---

## 6. The Broadcast Join Optimization

QuiverSQL has a sophisticated query optimizer. When you join a massive remote table with a tiny local file, it automatically rewrites the plan to push down the local keys as an `IN (...)` filter to the remote database, saving massive network transfer costs.

```sql
SELECT 
    e.name as employee_name,
    e.role,
    cp.tier,
    c.name as managed_customer
FROM employees e
JOIN pg.customer_profiles cp ON e.name = cp.account_manager
JOIN pg.customers c ON cp.customer_id = c.id;
```

1. Click **📊 Explain Query** on the query above.
2. Look at the plan graph for the PostgreSQL `TableScan` nodes and the `Join` between `employees` and `pg.customer_profiles`.
3. You will see orange broadcast badges on **three** different surfaces — the rewrite stamps each so the badge is visible at any zoom level:
   - **`Broadcast IN ↓ N keys`** on the `pg.customer_profiles` `TableScan` (the side that received the IN-list pushdown).
   - **`Broadcast ⇆ N keys`** on the `Join` between `employees` and `pg.customer_profiles` (the join that was rewritten).
   - **`Broadcast keys ↑ N keys`** on the `employees` `TableScan` (the local side whose rows became the IN list).
4. This means QuiverSQL automatically pulled the small `employees.csv` local file, extracted the names, and rewrote the PostgreSQL query to only fetch customers managed by those specific employees.
5. Switch to the **Source** tab and find the `pg.customer_profiles` card — the `Native SQL` step shows the **actual** query QuiverSQL sent to Postgres, complete with the `IN ('Alice', 'Bob', …)` clause containing the broadcast keys. (Section 8 walks through this in detail.)

---

## 7. Dynamic Settings & The Scan Guard

QuiverSQL provides robust safety nets to prevent developers from accidentally running `SELECT * FROM giant_table` and crashing the extension or database. 

Let's test the dynamic **Scan Guard** feature.

1. Open VS Code **Settings** (`Cmd+,`) in the Extension Host.
2. Search for `qsql.remoteScanMaxRows`.
3. Change the limit from `1000000` to `2`.
4. Run the following query without a limit:
   ```sql
   SELECT * FROM mysql.products;
   ```
5. You will see a striking **Scan Budget Exceeded** error banner in the Results Grid! The engine intercepted the query execution immediately because the database reported the scan would fetch 4 rows, which exceeded your newly configured budget of 2 rows. 
6. (Change the setting back to `1000000` to restore normal behavior).

Notice that changing the VS Code settings hot-restarts the daemon invisibly in the background, applying your new guard limits instantly without requiring an editor reload!

---

## 8. Inspecting Pushdowns in the Explain Plan

QuiverSQL's Explain Plan is built to be a self-explanatory artefact: open it on any federated query and you can see exactly which SQL travels over the wire to each remote DBMS, what plan that DBMS will use, and how those rows feed back into the DataFusion logical plan.

Run the federated query from Section 4 again, then click **📊 Explain Query** to follow along.

### A. Tree tab — provider-specific icons & legend

- Each `TableScan` node now carries a **provider icon** at the top-left of the rectangle — PostgreSQL on `pg.customers` and `pg.customer_profiles`, MySQL on `mysql.orders` and `mysql.products`. CSV / NDJSON scans show their own glyphs too.
- Hover any node to see a native tooltip with the full attribute list (predicates, projection, sort keys) — no truncation.
- The legend bar above the canvas explains badge colours: orange = broadcast pushdown, blue = sort pushdown, focus border = TableScan.
- **Click a `TableScan` node**. The view auto-switches to the **Source** tab and scrolls to that table's card, briefly highlighting it.

### B. Source tab — three layers per remote table

Switch to the **Source** tab. The top has two collapsible sections:

1. **Federated Logical Plan** *(expanded)* — what DataFusion sees after the federation + broadcast rewrites.
2. **DataFusion Physical Plan** *(collapsed by default)* — the executed physical plan, including the `VirtualExecutionPlan` leaves that hold the actual remote SQL. Expand it if you want the raw firehose.

Below those, every remote table gets a **per-table card**, stacked in execution order:

```
┌─ [icon] pg.customers   PostgreSQL · postgresql ─────────────────┐
│  ① Native SQL                                         [Copy]    │
│     SELECT "id","name","region" FROM "public"."customers"        │
│     ORDER BY "id" ASC                                            │
│  ② Remote EXPLAIN                                     [Copy]    │
│     {"Plan": {"Node Type": "Index Scan", …}}                     │
│  ③ Logical plan fragment                              [Copy]    │
│     TableScan: pg.customers projection=[id, name, region] …     │
└──────────────────────────────────────────────────────────────────┘
```

The key win: **step ① is the SQL that actually ran**, not the placeholder `SELECT * FROM table` we used to send. So step ② is a Postgres / MySQL plan for the real query — projection-aware, filter-aware, sort-aware.

### C. Confirm each pushdown is captured

With the demo databases up:

1. **Projection pushdown.** Re-run the Section 4 federated query. In each card's `Native SQL`, confirm the projection lists exactly the columns the outer SELECT references (no `SELECT *`).
2. **Broadcast pushdown.** Run the Section 6 broadcast-join query. The orange badge fires on three surfaces — `Broadcast IN ↓ N keys` on the receiving `pg.customer_profiles` `TableScan`, `Broadcast ⇆ N keys` on the rewritten `Join`, and `Broadcast keys ↑ N keys` on the local `employees` `TableScan` — **and** the `pg.customer_profiles` card's `Native SQL` step now contains an `IN ( … )` clause with N values that QuiverSQL extracted from `employees.csv` and inlined. The three stamps make the badge resilient to downstream optimiser rearrangements; it will still surface even when the synthesised Filter node gets folded into the federation extension.
3. **Sort pushdown.** Federation can push `ORDER BY` to the remote DB only when the entire sort subtree is a single federated source — joining across Postgres + MySQL in Section 5 forces the join (and therefore the sort) to happen locally in DataFusion. To see the pushdown work, run a single-source sort instead, e.g.:
   ```sql
   SELECT * FROM mysql.orders ORDER BY order_total DESC LIMIT 50;
   ```
   The `Sort ↓ pushed` badge appears on both the `Sort` operator *and* the `mysql.orders` `TableScan`, and step ① of the `mysql.orders` card ends with `` ORDER BY `order_total` DESC LIMIT 50 ``. Re-run the Section 5 multi-source query and observe — correctly — that neither badge fires there, since the multi-DB join forces a local sort.
4. **Local-only queries.** Run `SELECT * FROM employees`. The Source tab shows the Federated Logical Plan section and a friendly message explaining that this query has no remote sources — no Native SQL cards.

### D. When things fail gracefully

If a remote DBMS is offline at explain time, a warning banner appears at the top: *"Physical plan unavailable (remote source may be offline); per-table Native SQL cards will be missing in the Source tab."* The Tree tab still renders, just without the icon-augmented TableScan provider labels or the per-table cards — you can fix the connection and re-run.

---

## 9. Running the tests

```powershell
# Rust unit tests (backend)
cd qsql-workspace
cargo test -p qsql-daemon --lib explain::
cargo test -p qsql-core

# TypeScript tests (VS Code extension)
cd qsql-vscode
npm run compile
node out/test/detectQueries.test.js
```

Specific suites worth running after Explain-related changes:

- `cargo test -p qsql-daemon --lib explain::tests::extract_final_sql` — verifies pushdown SQL parsing.
- `cargo test -p qsql-daemon --lib explain::tests::build_plan_graph_stamps_remote_sql` — verifies that captured SQL + provider kind reach the PlanNode.
- `node out/test/detectQueries.test.js` — runs `testPlanVisualizationRendersPerTableCards`, `testProviderIcons*`, plus the original Scanner suite.
