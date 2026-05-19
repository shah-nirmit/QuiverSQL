//! Run with: cargo run -p dql-connectors --example create_sample_db
//!
//! Creates `dql-workspace/test.db` with two tables:
//!   - `orders`      (join with employees CSV on employee_id / employees.id)
//!   - `departments` (join with employees CSV on department name)

use rusqlite::{params, Connection};
use std::path::PathBuf;

fn main() {
    // Resolve path relative to the workspace root (two levels up from examples/)
    let db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()  // dql-workspace/
        .unwrap()
        .join("test.db");

    // Wipe and recreate so the script is idempotent
    if db_path.exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove old test.db");
    }

    let conn = Connection::open(&db_path).expect("Failed to create test.db");

    // ------------------------------------------------------------------ orders
    conn.execute_batch(
        "CREATE TABLE orders (
            id          INTEGER PRIMARY KEY,
            employee_id INTEGER NOT NULL,
            product     TEXT    NOT NULL,
            amount      REAL    NOT NULL,
            shipped     INTEGER NOT NULL  -- 0=pending, 1=shipped
        );",
    )
    .unwrap();

    let orders: &[(i64, &str, f64, i64)] = &[
        (1,  "Laptop Pro 16\"",    2499.99, 1),
        (2,  "Mechanical Keyboard", 149.50, 1),
        (1,  "USB-C Hub",            49.99, 0),
        (3,  "4K Monitor",          699.00, 1),
        (5,  "Laptop Pro 16\"",    2499.99, 0),
        (2,  "Wireless Mouse",       35.00, 1),
        (4,  "Standing Desk",       850.00, 1),
        (6,  "Noise Cancelling Headphones", 299.00, 0),
        (8,  "SSD 2TB",             129.99, 1),
        (10, "Ergonomic Chair",     499.00, 1),
        (11, "Webcam 4K",            89.99, 1),
        (3,  "Laptop Stand",         59.99, 0),
        (7,  "Whiteboard",          120.00, 1),
        (9,  "Cable Management Kit",  24.99, 1),
        (12, "Docking Station",     199.00, 0),
    ];

    let mut stmt = conn
        .prepare(
            "INSERT INTO orders (employee_id, product, amount, shipped) VALUES (?1, ?2, ?3, ?4)",
        )
        .unwrap();

    for (emp_id, product, amount, shipped) in orders {
        stmt.execute(params![emp_id, product, amount, shipped]).unwrap();
    }

    // --------------------------------------------------------------- departments
    conn.execute_batch(
        "CREATE TABLE departments (
            id          INTEGER PRIMARY KEY,
            name        TEXT    NOT NULL UNIQUE,
            budget      REAL    NOT NULL,
            headcount   INTEGER NOT NULL,
            office      TEXT    NOT NULL
        );",
    )
    .unwrap();

    let depts: &[(&str, f64, i64, &str)] = &[
        ("Engineering", 1_200_000.0, 5, "New York / Austin"),
        ("Sales",          450_000.0, 3, "Chicago / New York"),
        ("Marketing",      320_000.0, 2, "San Francisco"),
        ("HR",             180_000.0, 2, "Chicago / San Francisco"),
    ];

    let mut stmt = conn
        .prepare(
            "INSERT INTO departments (name, budget, headcount, office) VALUES (?1, ?2, ?3, ?4)",
        )
        .unwrap();

    for (name, budget, headcount, office) in depts {
        stmt.execute(params![name, budget, headcount, office]).unwrap();
    }

    println!("✅  Created: {}", db_path.display());
    println!("    Tables: orders ({} rows), departments ({} rows)", orders.len(), depts.len());
    println!();
    println!("Try these federated queries in DQL:");
    println!("  -- Employees with their shipped orders");
    println!("  SELECT e.name, e.department, o.product, o.amount");
    println!("  FROM employees e JOIN orders o ON e.id = o.employee_id");
    println!("  WHERE o.shipped = 1 ORDER BY o.amount DESC;");
    println!();
    println!("  -- Avg salary vs dept budget");
    println!("  SELECT d.name, d.budget, AVG(e.salary) as avg_salary, d.headcount");
    println!("  FROM departments d JOIN employees e ON d.name = e.department");
    println!("  GROUP BY d.name, d.budget, d.headcount ORDER BY d.budget DESC;");
}
