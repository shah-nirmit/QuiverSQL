//! Generate the fictional quickstart sample data under `samples/quickstart`.
//!
//! Run with:
//!   cargo run -p qsql-connectors --example generate_quickstart_samples

use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rusqlite::{params, Connection};
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn Error>> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("failed to resolve repository root")?
        .to_path_buf();
    let sample_dir = repo_root.join("samples").join("quickstart");

    println!("Writing quickstart samples to {}", sample_dir.display());
    fs::create_dir_all(&sample_dir)?;
    println!("  employees.csv");
    write_csv(&sample_dir)?;
    println!("  departments.ndjson and projects.json");
    write_json_samples(&sample_dir)?;
    println!("  demo.sqlite");
    write_sqlite(&sample_dir)?;
    println!("  orders.parquet");
    write_parquet(&sample_dir)?;

    println!("Generated quickstart samples in {}", sample_dir.display());
    Ok(())
}

fn write_csv(sample_dir: &Path) -> Result<(), Box<dyn Error>> {
    let employees = "\
id,name,department_id,role,salary,location
1,Alice Rao,10,Data Engineer,98000,New York
2,Bob Chen,20,Account Executive,72000,Chicago
3,Carla Mendes,10,Analytics Engineer,91000,Austin
4,Dev Patel,30,Marketing Manager,83000,San Francisco
5,Emma Brooks,40,People Partner,76000,Chicago
6,Finn Morgan,10,Platform Engineer,105000,Seattle
";

    fs::write(sample_dir.join("employees.csv"), employees)?;
    Ok(())
}

fn write_json_samples(sample_dir: &Path) -> Result<(), Box<dyn Error>> {
    let departments = [
        r#"{"id":10,"name":"Engineering","budget":1250000.0,"office":"New York"}"#,
        r#"{"id":20,"name":"Sales","budget":520000.0,"office":"Chicago"}"#,
        r#"{"id":30,"name":"Marketing","budget":410000.0,"office":"San Francisco"}"#,
        r#"{"id":40,"name":"People","budget":260000.0,"office":"Chicago"}"#,
    ]
    .join("\n");

    let projects = [
        r#"{"project_id":1001,"department_id":10,"project_name":"Atlas","status":"active","priority":1}"#,
        r#"{"project_id":1002,"department_id":10,"project_name":"Beacon","status":"planning","priority":2}"#,
        r#"{"project_id":2001,"department_id":20,"project_name":"Compass","status":"active","priority":2}"#,
        r#"{"project_id":3001,"department_id":30,"project_name":"Launchpad","status":"paused","priority":3}"#,
    ]
    .join("\n");

    fs::write(
        sample_dir.join("departments.ndjson"),
        format!("{departments}\n"),
    )?;
    fs::write(sample_dir.join("projects.json"), format!("{projects}\n"))?;
    Ok(())
}

fn write_sqlite(sample_dir: &Path) -> Result<(), Box<dyn Error>> {
    let db_path = sample_dir.join("demo.sqlite");
    if db_path.exists() {
        fs::remove_file(&db_path)?;
    }

    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        "CREATE TABLE compensation (
            employee_id INTEGER PRIMARY KEY,
            bonus REAL NOT NULL,
            review_score REAL NOT NULL,
            band TEXT NOT NULL
        );

        CREATE TABLE offices (
            city TEXT PRIMARY KEY,
            region TEXT NOT NULL,
            remote_friendly INTEGER NOT NULL
        );",
    )?;

    let compensation = [
        (1, 8500.0, 4.7, "Senior"),
        (2, 4200.0, 4.1, "Mid"),
        (3, 7600.0, 4.5, "Senior"),
        (4, 5000.0, 4.2, "Mid"),
        (5, 3900.0, 4.0, "Mid"),
        (6, 9500.0, 4.8, "Staff"),
    ];

    for (employee_id, bonus, review_score, band) in compensation {
        conn.execute(
            "INSERT INTO compensation (employee_id, bonus, review_score, band) VALUES (?1, ?2, ?3, ?4)",
            params![employee_id, bonus, review_score, band],
        )?;
    }

    let offices = [
        ("New York", "East", 1),
        ("Chicago", "Central", 1),
        ("Austin", "Central", 1),
        ("San Francisco", "West", 0),
        ("Seattle", "West", 1),
    ];

    for (city, region, remote_friendly) in offices {
        conn.execute(
            "INSERT INTO offices (city, region, remote_friendly) VALUES (?1, ?2, ?3)",
            params![city, region, remote_friendly],
        )?;
    }

    Ok(())
}

fn write_parquet(sample_dir: &Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("employee_id", DataType::Int64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("shipped", DataType::Boolean, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![
            10001, 10002, 10003, 10004, 10005, 10006,
        ])),
        Arc::new(Int64Array::from(vec![1, 3, 6, 2, 4, 1])),
        Arc::new(StringArray::from(vec![
            "Laptop Pro",
            "Data Warehouse Credits",
            "Observability Suite",
            "Sales Enablement Pack",
            "Campaign Design",
            "Docking Station",
        ])),
        Arc::new(Float64Array::from(vec![
            2499.99, 1800.00, 3200.50, 640.00, 1250.00, 199.00,
        ])),
        Arc::new(BooleanArray::from(vec![
            true, true, false, true, false, true,
        ])),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let file = File::create(sample_dir.join("orders.parquet"))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
