use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
    #[serde(default)]
    pub user: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorsConfiguration {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: u64,
}

impl Default for CorsConfiguration {
    fn default() -> Self {
        Self {
            allowed_origins: vec!["*".to_string()],
            allowed_methods: vec![
                "GET".to_string(),
                "POST".to_string(),
                "PUT".to_string(),
                "DELETE".to_string(),
                "HEAD".to_string(),
            ],
            allowed_headers: vec!["*".to_string()],
            expose_headers: vec!["ETag".to_string()],
            max_age_seconds: 3600,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BlockRef {
    pub id: i64,
    pub ordinal: i64,
    pub offset: i64,
    pub size: i64,
    pub chat_id: i64,
    pub message_id: i64,
    pub file_id: String,
    pub file_unique_id: String,
    pub message_date: i64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ObjectRecord {
    pub id: i64,
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub etag: String,
    pub metadata: ObjectMetadata,
    pub created_at: i64,
    pub modified_at: i64,
    pub blocks: Vec<BlockRef>,
}

#[derive(Clone, Debug)]
pub struct UploadRecord {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub metadata: ObjectMetadata,
    pub kind: String,
    pub created_at: i64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct PartRecord {
    pub upload_id: String,
    pub part_number: i32,
    pub size: i64,
    pub etag: String,
    pub blocks: Vec<BlockRef>,
}

#[derive(Clone, Debug)]
pub struct BucketRecord {
    pub name: String,
    pub created_at: i64,
}

#[derive(Clone, Debug)]
pub struct ListingRecord {
    pub key: String,
    pub size: i64,
    pub etag: String,
    pub modified_at: i64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct GarbageRecord {
    pub block_id: i64,
    pub chat_id: i64,
    pub message_id: i64,
    pub message_date: i64,
    pub attempts: i64,
    pub next_attempt: i64,
    pub last_error: Option<String>,
}
