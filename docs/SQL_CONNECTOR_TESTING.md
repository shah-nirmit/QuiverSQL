# SQL Connector Testing

This guide covers local Postgres and MySQL/MariaDB testing for QuiverSQL's SQL
connectors and Phase 4 pushdown support.

## What Is Being Tested

The live SQL connector tests verify that QuiverSQL can:

- connect to Postgres and MySQL/MariaDB;
- create and query a small test table;
- introspect table schema from `information_schema`;
- build dialect-specific SQL with projection, filter, and limit pushdown;
- execute the emitted SQL against the live database;
- register SQL sources through the daemon without exposing credentials.

The live database tests are conditionally ignored. They compile every time, but
normal test runs report them as ignored when the matching environment variable is
not set. After Docker is up and the environment variables are set in the same
shell that runs Cargo, the same normal test commands run the live database tests.

## Start The Databases

From the repository root:

```powershell
docker compose up -d
```

The compose file starts:

- Postgres on `localhost:5432`;
- MySQL on `localhost:3306`;
- one-shot init services that create and seed `customers` and `orders` tables.

Check container state:

```powershell
docker compose ps
```

Check init logs if the tables are missing:

```powershell
docker compose logs postgres-init mysql-init
```

Reset all database state and recreate the seeded tables:

```powershell
docker compose down -v
docker compose up -d
```

## Connection Strings

Use these in the VS Code connection wizard:

```text
Postgres: postgres://qsql_test:qsql_test@localhost:5432/qsql_test
MySQL:    mysql://qsql_test:qsql_test@localhost:3306/qsql_test
MariaDB:  mysql://qsql_test:qsql_test@localhost:3306/qsql_test
```

For Postgres, use schema `public` and table names such as `customers` or
`orders`.

For MySQL/MariaDB, the database is `qsql_test`. The schema field can be blank or
`qsql_test`, depending on the UI path being tested.

## Set Environment Variables

Set the variables in the same terminal session where you run Cargo.

PowerShell:

```powershell
$env:QSQL_POSTGRES_URL="postgres://qsql_test:qsql_test@localhost:5432/qsql_test"
$env:QSQL_MYSQL_URL="mysql://qsql_test:qsql_test@localhost:3306/qsql_test"
```

Verify them:

```powershell
echo $env:QSQL_POSTGRES_URL
echo $env:QSQL_MYSQL_URL
```

Windows `cmd.exe`:

```cmd
set QSQL_POSTGRES_URL=postgres://qsql_test:qsql_test@localhost:5432/qsql_test
set QSQL_MYSQL_URL=mysql://qsql_test:qsql_test@localhost:3306/qsql_test
```

Verify them:

```cmd
echo %QSQL_POSTGRES_URL%
echo %QSQL_MYSQL_URL%
```

Git Bash, WSL, macOS, or Linux:

```bash
export QSQL_POSTGRES_URL="postgres://qsql_test:qsql_test@localhost:5432/qsql_test"
export QSQL_MYSQL_URL="mysql://qsql_test:qsql_test@localhost:3306/qsql_test"
```

Important: PowerShell's `set QSQL_POSTGRES_URL=...` does not set an environment
variable that Cargo can inherit. It creates a PowerShell variable instead, so
the Rust tests will behave as if the database URL is missing.

Connector tests:

```powershell
cargo test --locked -p qsql-connectors
```

Daemon tests:

```powershell
cargo test --locked -p qsql-daemon
```

Full workspace test run:

```powershell
cargo test --locked --workspace
```

When the environment variables are missing, these commands report the live tests
as ignored. When the environment variables are present, the same commands run the
live tests.

## How To Know The Live Tests Ran

Without the environment variables, the output reports the live tests as ignored:

```text
test postgres::tests::postgres_live_select_requires_env ... ignored, requires a live Postgres database and QSQL_POSTGRES_URL
test mysql::tests::mysql_live_select_requires_env ... ignored, requires a live MySQL/MariaDB database and QSQL_MYSQL_URL
test optional_postgres_registration_redacts_credentials ... ignored, requires a live Postgres database and QSQL_POSTGRES_URL
test optional_mysql_registration_redacts_credentials ... ignored, requires a live MySQL database and QSQL_MYSQL_URL
```

With the environment variables set, those same tests report `ok`:

```text
test postgres::tests::postgres_live_select_requires_env ... ok
test mysql::tests::mysql_live_select_requires_env ... ok
test optional_postgres_registration_redacts_credentials ... ok
test optional_mysql_registration_redacts_credentials ... ok
```

You do not need `-- --ignored` for the normal live database workflow.

## Forcing Ignored Tests

If you intentionally want to run tests that are currently ignored, use Rust's
standard ignored-test flag:

```powershell
cargo test --locked -p qsql-connectors -- --ignored
```

If you force ignored live tests without setting the environment variables, they
fail with a clear message naming the missing variable.

## Quick Manual Database Checks

Postgres with `psql`:

```powershell
psql "postgres://qsql_test:qsql_test@localhost:5432/qsql_test" -c "SELECT id, name, region FROM customers ORDER BY id;"
```

MySQL with the MySQL client:

```powershell
mysql -h localhost -P 3306 -u qsql_test -pqsql_test qsql_test -e "SELECT id, name, region FROM customers ORDER BY id;"
```

If those commands fail but Docker containers are running, inspect the compose
logs first:

```powershell
docker compose logs postgres mysql postgres-init mysql-init
```

## Pushdown Smoke Queries

After registering `customers` through the VS Code wizard, these queries exercise
Phase 4 pushdown:

```sql
SELECT name, region
FROM customers;
```

```sql
SELECT id, name, revenue
FROM customers
WHERE region = 'emea'
LIMIT 2;
```

```sql
SELECT id, name
FROM customers
WHERE name LIKE 'Grace%';
```

Projection, supported filters, and `LIMIT` are pushed down. Aggregates, sort,
and joins are intentionally left to later phases.

## Common Problems

If tests say the URLs are not set, check that you used the syntax for your shell:

- PowerShell: `$env:QSQL_POSTGRES_URL="..."`
- `cmd.exe`: `set QSQL_POSTGRES_URL=...`
- Bash: `export QSQL_POSTGRES_URL="..."`

If a test hangs or cannot connect, confirm Docker is publishing ports:

```powershell
docker compose ps
```

If a live test fails after previous manual edits to the database, reset volumes:

```powershell
docker compose down -v
docker compose up -d
```
