use super::xml::{
    BucketXml, Buckets, CommonPrefix, ListAllMyBuckets, ListBucketResult, ObjectXml, xml_response,
};
use super::{format_time, query_value, quoted};
use crate::engine::Engine;
use crate::model::key_successor;
use anyhow::{Result, anyhow, bail};
use axum::response::Response;
use base64::Engine as _;
use std::collections::HashSet;

pub(super) async fn list_buckets(engine: &Engine) -> Result<Response> {
    let buckets = engine.db.list_buckets().await?;
    let response = ListAllMyBuckets {
        buckets: Buckets {
            bucket: buckets
                .into_iter()
                .map(|bucket| BucketXml {
                    name: bucket.name,
                    creation_date: format_time(bucket.created_at),
                })
                .collect(),
        },
    };
    xml_response(response)
}

pub(super) async fn list_objects(
    engine: &Engine,
    bucket: &str,
    query: &[(String, String)],
) -> Result<Response> {
    if !engine.db.bucket_exists(bucket).await? {
        bail!("NoSuchBucket");
    }
    let prefix = query_value(query, "prefix").unwrap_or_default();
    let delimiter = query_value(query, "delimiter").unwrap_or_default();
    let max_keys = match query_value(query, "max-keys") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| anyhow!("InvalidArgument"))?
            .min(1000),
        None => 1000,
    };
    let is_v2 = query_value(query, "list-type").as_deref() == Some("2");
    let start_after = if is_v2 {
        query_value(query, "start-after")
    } else {
        None
    };
    let mut after = if is_v2 {
        start_after.clone().unwrap_or_default()
    } else {
        query_value(query, "marker").unwrap_or_default()
    };
    if is_v2 && let Some(token) = query_value(query, "continuation-token") {
        after = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(token)
                .map_err(|_| anyhow!("InvalidArgument"))?,
        )
        .map_err(|_| anyhow!("InvalidArgument"))?;
    }
    let mut contents = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut seen = HashSet::new();
    let mut truncated = false;
    let mut next_cursor: Option<String> = None;
    // The request marker: keys and prefix groups at or below it were already
    // returned on earlier pages, so a group whose prefix is <= floor is seeked
    // past without being emitted again.
    let floor = after.clone();
    let mut lower = after;
    let mut lower_inclusive = false;
    const SCAN_BATCH_SIZE: usize = 1000;

    if max_keys > 0 {
        'scan: loop {
            let entries = engine
                .db
                .list_objects(bucket, &prefix, &lower, lower_inclusive, SCAN_BATCH_SIZE)
                .await?;
            let page_len = entries.len();
            if page_len == 0 {
                break;
            }
            for entry in entries {
                // Skip entries already covered by an in-batch seek.
                let in_bound = if lower_inclusive {
                    entry.key >= lower
                } else {
                    entry.key > lower
                };
                if !in_bound {
                    continue;
                }
                if let Some(common) = common_prefix(&entry.key, &prefix, &delimiter) {
                    let emitted = common > floor && seen.insert(common.clone());
                    if emitted {
                        common_prefixes.push(CommonPrefix {
                            prefix: common.clone(),
                        });
                    }
                    // Seek past every key in this group instead of scanning
                    // them one by one.
                    match key_successor(&common) {
                        Some(bound) => {
                            lower = bound;
                            lower_inclusive = true;
                        }
                        None => {
                            lower = entry.key;
                            lower_inclusive = false;
                        }
                    }
                    if contents.len() + common_prefixes.len() >= max_keys {
                        truncated = !engine
                            .db
                            .list_objects(bucket, &prefix, &lower, lower_inclusive, 1)
                            .await?
                            .is_empty();
                        if truncated {
                            next_cursor = Some(common);
                        }
                        break 'scan;
                    }
                } else {
                    lower = entry.key.clone();
                    lower_inclusive = false;
                    contents.push(ObjectXml {
                        key: entry.key,
                        last_modified: format_time(entry.modified_at),
                        etag: quoted(&entry.etag),
                        size: entry.size,
                        storage_class: "STANDARD".to_string(),
                    });
                    if contents.len() + common_prefixes.len() >= max_keys {
                        truncated = !engine
                            .db
                            .list_objects(bucket, &prefix, &lower, false, 1)
                            .await?
                            .is_empty();
                        if truncated {
                            next_cursor = Some(lower.clone());
                        }
                        break 'scan;
                    }
                }
            }
            if page_len < SCAN_BATCH_SIZE {
                break;
            }
        }
    }
    let next = next_cursor
        .as_deref()
        .map(|cursor| base64::engine::general_purpose::STANDARD.encode(cursor.as_bytes()));
    xml_response(ListBucketResult {
        name: bucket.to_string(),
        prefix,
        delimiter: if delimiter.is_empty() {
            None
        } else {
            Some(delimiter)
        },
        max_keys: max_keys as i32,
        key_count: (contents.len() + common_prefixes.len()) as i32,
        is_truncated: truncated,
        marker: if is_v2 {
            None
        } else {
            query_value(query, "marker")
        },
        start_after,
        continuation_token: query_value(query, "continuation-token"),
        next_marker: if truncated && !is_v2 {
            next_cursor
        } else {
            None
        },
        next_continuation_token: if is_v2 { next } else { None },
        contents,
        common_prefixes,
    })
}

fn common_prefix(key: &str, prefix: &str, delimiter: &str) -> Option<String> {
    if delimiter.is_empty() {
        return None;
    }
    let rest = key.strip_prefix(prefix).unwrap_or(key);
    rest.find(delimiter)
        .map(|index| format!("{}{}", prefix, &rest[..index + delimiter.len()]))
}
