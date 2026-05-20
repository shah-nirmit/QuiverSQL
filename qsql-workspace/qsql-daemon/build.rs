fn main() {
    configure_live_test_cfg("QSQL_POSTGRES_URL", "qsql_live_postgres_tests");
    configure_live_test_cfg("QSQL_MYSQL_URL", "qsql_live_mysql_tests");
}

fn configure_live_test_cfg(env_var: &str, cfg_name: &str) {
    println!("cargo:rerun-if-env-changed={env_var}");
    println!("cargo:rustc-check-cfg=cfg({cfg_name})");

    if std::env::var(env_var)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
    {
        println!("cargo:rustc-cfg={cfg_name}");
    }
}
