use crate::model::CorsConfiguration;
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use quick_xml::se::to_string;
use serde::{Deserialize, Serialize};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

pub(super) fn xml_response<T: Serialize>(value: T) -> Result<Response> {
    let xml = to_string(&value).map_err(|error| anyhow!("XML serialization: {error}"))?;
    let body = Body::from(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>{}",
        with_namespace(&xml)
    ));
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    Ok(response)
}

pub(super) fn with_namespace(xml: &str) -> String {
    if let Some(index) = xml.find('>') {
        format!("{} xmlns=\"{}\"{}", &xml[..index], XMLNS, &xml[index..])
    } else {
        xml.to_string()
    }
}

#[derive(Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
pub(super) struct ListAllMyBuckets {
    pub(super) buckets: Buckets,
}

#[derive(Serialize)]
pub(super) struct Buckets {
    #[serde(rename = "Bucket", default)]
    pub(super) bucket: Vec<BucketXml>,
}

#[derive(Serialize)]
pub(super) struct BucketXml {
    #[serde(rename = "Name")]
    pub(super) name: String,
    #[serde(rename = "CreationDate")]
    pub(super) creation_date: String,
}

#[derive(Serialize)]
pub(super) struct ListBucketResult {
    #[serde(rename = "Name")]
    pub(super) name: String,
    #[serde(rename = "Prefix")]
    pub(super) prefix: String,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub(super) delimiter: Option<String>,
    #[serde(rename = "MaxKeys")]
    pub(super) max_keys: i32,
    #[serde(rename = "KeyCount")]
    pub(super) key_count: i32,
    #[serde(rename = "IsTruncated")]
    pub(super) is_truncated: bool,
    #[serde(rename = "Marker", skip_serializing_if = "Option::is_none")]
    pub(super) marker: Option<String>,
    #[serde(rename = "StartAfter", skip_serializing_if = "Option::is_none")]
    pub(super) start_after: Option<String>,
    #[serde(rename = "ContinuationToken", skip_serializing_if = "Option::is_none")]
    pub(super) continuation_token: Option<String>,
    #[serde(rename = "NextMarker", skip_serializing_if = "Option::is_none")]
    pub(super) next_marker: Option<String>,
    #[serde(
        rename = "NextContinuationToken",
        skip_serializing_if = "Option::is_none"
    )]
    pub(super) next_continuation_token: Option<String>,
    #[serde(rename = "Contents", default)]
    pub(super) contents: Vec<ObjectXml>,
    #[serde(rename = "CommonPrefixes", default)]
    pub(super) common_prefixes: Vec<CommonPrefix>,
}

#[derive(Serialize)]
pub(super) struct ObjectXml {
    #[serde(rename = "Key")]
    pub(super) key: String,
    #[serde(rename = "LastModified")]
    pub(super) last_modified: String,
    #[serde(rename = "ETag")]
    pub(super) etag: String,
    #[serde(rename = "Size")]
    pub(super) size: i64,
    #[serde(rename = "StorageClass")]
    pub(super) storage_class: String,
}

#[derive(Serialize)]
pub(super) struct CommonPrefix {
    #[serde(rename = "Prefix")]
    pub(super) prefix: String,
}

#[derive(Serialize)]
#[serde(rename = "LocationConstraint")]
pub(super) struct BucketLocation {
    #[serde(rename = "$text")]
    pub(super) location: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "CORSConfiguration")]
pub(super) struct CorsConfigurationXml {
    #[serde(rename = "CORSRule", default)]
    pub(super) rules: Vec<CorsRuleXml>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct CorsRuleXml {
    #[serde(rename = "AllowedOrigin", default)]
    pub(super) allowed_origins: Vec<String>,
    #[serde(rename = "AllowedMethod", default)]
    pub(super) allowed_methods: Vec<String>,
    #[serde(rename = "AllowedHeader", default)]
    pub(super) allowed_headers: Vec<String>,
    #[serde(rename = "ExposeHeader", default)]
    pub(super) expose_headers: Vec<String>,
    #[serde(rename = "MaxAgeSeconds", default)]
    pub(super) max_age_seconds: Option<u64>,
}

impl CorsConfigurationXml {
    pub(super) fn from(configuration: &CorsConfiguration) -> Self {
        Self {
            rules: vec![CorsRuleXml {
                allowed_origins: configuration.allowed_origins.clone(),
                allowed_methods: configuration.allowed_methods.clone(),
                allowed_headers: configuration.allowed_headers.clone(),
                expose_headers: configuration.expose_headers.clone(),
                max_age_seconds: Some(configuration.max_age_seconds),
            }],
        }
    }

    pub(super) fn into_configuration(self) -> Result<CorsConfiguration> {
        if self.rules.is_empty() {
            bail!("InvalidRequest");
        }
        let mut configuration = CorsConfiguration {
            allowed_origins: Vec::new(),
            allowed_methods: Vec::new(),
            allowed_headers: Vec::new(),
            expose_headers: Vec::new(),
            max_age_seconds: 3600,
        };
        for rule in self.rules {
            configuration.allowed_origins.extend(rule.allowed_origins);
            configuration.allowed_methods.extend(
                rule.allowed_methods
                    .into_iter()
                    .map(|method| method.to_ascii_uppercase()),
            );
            configuration.allowed_headers.extend(rule.allowed_headers);
            configuration.expose_headers.extend(rule.expose_headers);
            if let Some(max_age) = rule.max_age_seconds {
                configuration.max_age_seconds = max_age;
            }
        }
        for values in [
            &mut configuration.allowed_origins,
            &mut configuration.allowed_methods,
            &mut configuration.allowed_headers,
            &mut configuration.expose_headers,
        ] {
            values.sort_unstable();
            values.dedup();
        }
        if configuration.allowed_origins.is_empty()
            || configuration.allowed_methods.is_empty()
            || configuration.allowed_headers.is_empty()
        {
            bail!("InvalidRequest");
        }
        if configuration
            .allowed_origins
            .iter()
            .any(|origin| origin == "*")
            && configuration.allowed_origins.len() > 1
        {
            bail!("InvalidRequest");
        }
        Ok(configuration)
    }
}

#[derive(Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
pub(super) struct InitiateMultipartUpload {
    #[serde(rename = "Bucket")]
    pub(super) bucket: String,
    #[serde(rename = "Key")]
    pub(super) key: String,
    #[serde(rename = "UploadId")]
    pub(super) upload_id: String,
}

#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
pub(super) struct CompleteMultipartResult {
    #[serde(rename = "Location")]
    pub(super) location: String,
    #[serde(rename = "Bucket")]
    pub(super) bucket: String,
    #[serde(rename = "Key")]
    pub(super) key: String,
    #[serde(rename = "ETag")]
    pub(super) etag: String,
}

#[derive(Serialize)]
pub(super) struct CopyObjectResult {
    #[serde(rename = "ETag")]
    pub(super) etag: String,
    #[serde(rename = "LastModified")]
    pub(super) last_modified: String,
}

#[derive(Serialize)]
pub(super) struct ListPartsResult {
    #[serde(rename = "Bucket")]
    pub(super) bucket: String,
    #[serde(rename = "Key")]
    pub(super) key: String,
    #[serde(rename = "UploadId")]
    pub(super) upload_id: String,
    #[serde(rename = "Part", default)]
    pub(super) parts: Vec<PartXml>,
}

#[derive(Serialize)]
pub(super) struct PartXml {
    #[serde(rename = "PartNumber")]
    pub(super) part_number: i32,
    #[serde(rename = "LastModified")]
    pub(super) last_modified: String,
    #[serde(rename = "ETag")]
    pub(super) etag: String,
    #[serde(rename = "Size")]
    pub(super) size: i64,
}

#[derive(Serialize)]
pub(super) struct ListMultipartUploadsResult {
    #[serde(rename = "Bucket")]
    pub(super) bucket: String,
    #[serde(rename = "Upload", default)]
    pub(super) uploads: Vec<UploadXml>,
}

#[derive(Serialize)]
pub(super) struct UploadXml {
    #[serde(rename = "Key")]
    pub(super) key: String,
    #[serde(rename = "UploadId")]
    pub(super) upload_id: String,
    #[serde(rename = "Initiated")]
    pub(super) initiated: String,
}

#[derive(Serialize)]
pub(super) struct DeleteResult {
    #[serde(rename = "Deleted", skip_serializing_if = "Option::is_none")]
    pub(super) deleted: Option<Vec<DeletedXml>>,
    #[serde(rename = "Error", skip_serializing_if = "Vec::is_empty")]
    pub(super) errors: Vec<DeleteErrorXml>,
}

#[derive(Serialize)]
pub(super) struct DeletedXml {
    #[serde(rename = "Key")]
    pub(super) key: String,
}

#[derive(Serialize)]
pub(super) struct DeleteErrorXml {
    #[serde(rename = "Key")]
    pub(super) key: String,
    #[serde(rename = "Code")]
    pub(super) code: String,
    #[serde(rename = "Message")]
    pub(super) message: String,
}

#[derive(Serialize)]
#[serde(rename = "Error")]
pub(super) struct ErrorXml {
    #[serde(rename = "Code")]
    pub(super) code: String,
    #[serde(rename = "Message")]
    pub(super) message: String,
    #[serde(rename = "Resource")]
    pub(super) resource: String,
    #[serde(rename = "RequestId")]
    pub(super) request_id: String,
}

#[derive(Deserialize)]
pub(super) struct CompleteMultipartRequest {
    #[serde(rename = "Part", default)]
    pub(super) parts: Vec<CompletedPart>,
}

#[derive(Deserialize)]
pub(super) struct CompletedPart {
    #[serde(rename = "PartNumber")]
    pub(super) part_number: i32,
    #[serde(rename = "ETag")]
    pub(super) etag: String,
}

#[derive(Deserialize)]
pub(super) struct DeleteRequest {
    #[serde(rename = "Object", default)]
    pub(super) objects: Vec<DeleteObject>,
    #[serde(rename = "Quiet", default)]
    pub(super) quiet: bool,
}

#[derive(Deserialize)]
pub(super) struct DeleteObject {
    #[serde(rename = "Key")]
    pub(super) key: String,
}
