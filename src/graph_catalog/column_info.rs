#[derive(Debug, Clone, serde::Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

impl ColumnInfo {
    pub fn new(name: String, data_type: String) -> Self {
        Self { name, data_type }
    }
}

// Row deserialization handled by serde
