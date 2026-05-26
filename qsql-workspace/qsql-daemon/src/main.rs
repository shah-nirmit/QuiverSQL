#[tokio::main]
async fn main() {
    qsql_daemon::run().await;
}
