use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceProfile {
    pub id: String,
    pub kind: String,
    pub uri_or_driver: String,
    pub secret_ref: Option<String>,
    // options and capabilities can be expanded later
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableRef {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub source: String,
    pub format: Option<String>,
}
