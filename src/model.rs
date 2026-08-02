use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TelegramBackend {
    #[default]
    BotApi,
    Grammers,
}

impl TelegramBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BotApi => "bot_api",
            Self::Grammers => "grammers",
        }
    }
}

impl fmt::Display for TelegramBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for TelegramBackend {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "bot_api" | "bot-api" => Ok(Self::BotApi),
            "grammers" | "mtproto" => Ok(Self::Grammers),
            _ => Err("expected bot_api or grammers"),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectCondition {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
}

impl ObjectCondition {
    pub fn allows(&self, etag: Option<&str>) -> bool {
        if let Some(value) = &self.if_match
            && !etag_matches(value, etag)
        {
            return false;
        }
        if let Some(value) = &self.if_none_match
            && etag_matches(value, etag)
        {
            return false;
        }
        true
    }
}

pub fn normalize_etag(value: &str) -> String {
    value.trim().trim_matches('"').to_string()
}

fn etag_matches(value: &str, etag: Option<&str>) -> bool {
    let Some(etag) = etag else {
        return false;
    };
    value == "*"
        || value
            .split(',')
            .any(|candidate| normalize_etag(candidate) == etag)
}

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
    pub backend: TelegramBackend,
    pub document_id: Option<i64>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluates_etag_conditions() {
        let condition = ObjectCondition {
            if_match: Some("\"one\", \"two\"".to_string()),
            if_none_match: None,
        };
        assert!(condition.allows(Some("one")));
        assert!(!condition.allows(Some("three")));
        assert!(!condition.allows(None));

        let condition = ObjectCondition {
            if_match: None,
            if_none_match: Some("*".to_string()),
        };
        assert!(condition.allows(None));
        assert!(!condition.allows(Some("one")));
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct GarbageRecord {
    pub block_id: i64,
    pub chat_id: i64,
    pub message_id: i64,
    pub backend: TelegramBackend,
    pub document_id: Option<i64>,
    pub file_id: String,
    pub file_unique_id: String,
    pub message_date: i64,
    pub attempts: i64,
    pub next_attempt: i64,
    pub last_error: Option<String>,
}

impl GarbageRecord {
    pub fn as_block_ref(&self) -> BlockRef {
        BlockRef {
            id: self.block_id,
            ordinal: 0,
            offset: 0,
            size: 0,
            chat_id: self.chat_id,
            message_id: self.message_id,
            backend: self.backend,
            document_id: self.document_id,
            file_id: self.file_id.clone(),
            file_unique_id: self.file_unique_id.clone(),
            message_date: self.message_date,
        }
    }
}
