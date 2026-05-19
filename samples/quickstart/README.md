# Quickstart Samples

These files are small, fictional datasets for trying QuiverSQL quickly.

## Files

- `employees.csv`: employee attributes and salaries.
- `departments.ndjson`: newline-delimited department records.
- `projects.json`: newline-delimited JSON with a `.json` extension.
- `orders.parquet`: Arrow/Parquet order data.
- `demo.sqlite`: SQLite database with `compensation` and `offices` tables.

## Suggested Aliases

Use **QuiverSQL: Connect Data Source** and attach the files with these aliases:

```text
employees      employees.csv
departments    departments.ndjson
projects       projects.json
orders         orders.parquet
compensation   demo.sqlite table: compensation
offices        demo.sqlite table: offices
```

## Sample Queries

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

## Regenerate

From the Rust workspace:

```powershell
cargo run -p qsql-connectors --example generate_quickstart_samples
```
