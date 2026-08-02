use crate::auth::SigV4;
use crate::engine::Engine;
use crate::model::{
    CorsConfiguration, ObjectCondition, ObjectMetadata, ObjectRecord, normalize_etag,
};
use anyhow::{Result, anyhow, bail};
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::get;
use base64::Engine as _;
use quick_xml::{de::from_str, se::to_string};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;
use uuid::Uuid;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(Clone)]
pub struct AppState {
    pub engine: Engine,
    pub auth: SigV4,
    pub public_host: Option<String>,
    pub cors: CorsConfiguration,
}

pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .fallback(handle)
        .with_state(Arc::new(state))
}

async fn healthz() -> Response {
    empty_response(StatusCode::OK)
}

async fn readyz(State(state): State<Arc<AppState>>) -> Response {
    match state.engine.db.integrity_check().await {
        Ok(result) if result == "ok" => empty_response(StatusCode::OK),
        Ok(result) => Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from(format!("SQLite integrity check: {result}")))
            .unwrap_or_else(|_| empty_response(StatusCode::SERVICE_UNAVAILABLE)),
        Err(error) => Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from(format!("database unavailable: {error}")))
            .unwrap_or_else(|_| empty_response(StatusCode::SERVICE_UNAVAILABLE)),
    }
}

async fn handle(State(state): State<Arc<AppState>>, request: Request<Body>) -> Response {
    let (parts, mut body) = request.into_parts();
    let request_id = Uuid::new_v4().simple().to_string();
    let request_uri = parts.uri.clone();
    let request_headers = parts.headers.clone();
    let query_keys = query_keys(&parts.uri);
    let is_cors_put = parts.method == Method::PUT
        && form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
            .any(|(key, _)| key.eq_ignore_ascii_case("cors"));
    let payload_hash = if is_cors_put && !parts.headers.contains_key("x-amz-content-sha256") {
        match to_bytes(body, 1024 * 1024).await {
            Ok(bytes) => {
                let hash = hex::encode(Sha256::digest(&bytes));
                body = Body::from(bytes);
                Some(hash)
            }
            Err(error) => {
                let response = error_response(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest".to_string(),
                    &format!("read CORS request body: {error}"),
                    &request_uri,
                    &request_id,
                );
                return apply_cors(&state, &request_uri, &request_headers, response).await;
            }
        }
    } else {
        None
    };
    if parts.method != Method::OPTIONS
        && let Err(error) = state.auth.verify_with_payload_hash(
            &parts.method,
            &parts.uri,
            &parts.headers,
            payload_hash.as_deref(),
        )
    {
        tracing::warn!(
            request_id = %request_id,
            method = %parts.method,
            path = %parts.uri.path(),
            query_keys = ?query_keys,
            host = ?parts.headers.get(header::HOST).and_then(|value| value.to_str().ok()),
            signed_headers = ?parts
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split("SignedHeaders=").nth(1))
                .and_then(|value| value.split(',').next()),
            content_sha256 = ?parts
                .headers
                .get("x-amz-content-sha256")
                .and_then(|value| value.to_str().ok()),
            computed_payload_sha256 = ?payload_hash,
            error = %error,
            "S3 request authentication failed"
        );
        return apply_cors(
            &state,
            &request_uri,
            &request_headers,
            error_response(
                StatusCode::FORBIDDEN,
                error_code(&error),
                &error.to_string(),
                &parts.uri,
                &request_id,
            ),
        )
        .await;
    }
    tracing::info!(
        request_id = %request_id,
        method = %parts.method,
        path = %request_uri.path(),
        query_keys = ?query_keys,
        host = ?request_headers.get(header::HOST).and_then(|value| value.to_str().ok()),
        "S3 request received"
    );
    let request_method = parts.method.clone();
    let response = match dispatch(
        &state,
        parts.method,
        parts.uri,
        parts.headers,
        body,
        &request_id,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            let status = status_for(&error);
            let code = error_code(&error);
            let message = public_error_message(&code, &error);
            log_request_failure(
                &request_id,
                &request_method,
                request_uri.path(),
                status,
                &code,
                &error,
            );
            error_response(status, code, &message, &request_uri, &request_id)
        }
    };
    tracing::info!(
        request_id = %request_id,
        status = %response.status(),
        "S3 request completed"
    );
    apply_cors(&state, &request_uri, &request_headers, response).await
}

async fn dispatch(
    state: &AppState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
    request_id: &str,
) -> Result<Response> {
    let target = Target::parse(&uri, &headers, state.public_host.as_deref())?;
    let query: Vec<(String, String)> =
        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    if method == Method::OPTIONS {
        return Ok(empty_response(StatusCode::NO_CONTENT));
    }
    if target.bucket.is_none() {
        return match method {
            Method::GET => list_buckets(&state.engine).await,
            _ => Err(anyhow!("MethodNotAllowed")),
        };
    }
    let bucket = target.bucket.as_ref().unwrap();
    if target.key.is_none() {
        return dispatch_bucket(state, method, bucket, &query, body, request_id).await;
    }
    let key = target.key.as_ref().unwrap();
    dispatch_object(
        state, method, bucket, key, &query, &headers, body, request_id,
    )
    .await
}

async fn dispatch_bucket(
    state: &AppState,
    method: Method,
    bucket: &str,
    query: &[(String, String)],
    body: Body,
    _request_id: &str,
) -> Result<Response> {
    if query_has(query, "cors") {
        return match method {
            Method::GET => get_bucket_cors(state, bucket).await,
            Method::PUT => put_bucket_cors(state, bucket, body).await,
            Method::DELETE => delete_bucket_cors(state, bucket).await,
            _ => Err(anyhow!("MethodNotAllowed")),
        };
    }
    if query_has(query, "uploads") {
        return match method {
            Method::GET => list_multipart_uploads(&state.engine, bucket).await,
            _ => Err(anyhow!("MethodNotAllowed")),
        };
    }
    if query_has(query, "location") {
        return if method == Method::GET {
            xml_response(BucketLocation {
                location: if state.engine.config.region == "us-east-1" {
                    String::new()
                } else {
                    state.engine.config.region.clone()
                },
            })
        } else {
            Err(anyhow!("MethodNotAllowed"))
        };
    }
    if query_has(query, "delete") {
        return if method == Method::POST {
            multi_delete(&state.engine, bucket, body).await
        } else {
            Err(anyhow!("MethodNotAllowed"))
        };
    }
    if query_value(query, "list-type").as_deref() == Some("2")
        || query_has(query, "prefix")
        || query_has(query, "delimiter")
        || query_has(query, "max-keys")
    {
        return if method == Method::GET {
            list_objects(&state.engine, bucket, query).await
        } else {
            Err(anyhow!("MethodNotAllowed"))
        };
    }
    match method {
        Method::PUT => {
            validate_bucket(bucket)?;
            state.engine.db.create_bucket(bucket).await?;
            let mut response = empty_response(StatusCode::OK);
            response.headers_mut().insert(
                header::LOCATION,
                HeaderValue::from_str(&format!("/{bucket}"))?,
            );
            Ok(response)
        }
        Method::HEAD => {
            if state.engine.db.bucket_exists(bucket).await? {
                Ok(empty_response(StatusCode::OK))
            } else {
                Err(anyhow!("NoSuchBucket"))
            }
        }
        Method::DELETE => match state.engine.db.delete_bucket(bucket).await? {
            None => Err(anyhow!("NoSuchBucket")),
            Some(false) => Err(anyhow!("BucketNotEmpty")),
            Some(true) => Ok(empty_response(StatusCode::NO_CONTENT)),
        },
        Method::GET => list_objects(&state.engine, bucket, query).await,
        _ => Err(anyhow!("MethodNotAllowed")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_object(
    state: &AppState,
    method: Method,
    bucket: &str,
    key: &str,
    query: &[(String, String)],
    headers: &HeaderMap,
    body: Body,
    _request_id: &str,
) -> Result<Response> {
    if !state.engine.db.bucket_exists(bucket).await? {
        bail!("NoSuchBucket");
    }
    if let Some(upload_id) = query_value(query, "uploadId") {
        let upload = state
            .engine
            .db
            .get_upload(&upload_id)
            .await?
            .ok_or_else(|| anyhow!("NoSuchUpload"))?;
        if upload.bucket != bucket || upload.key != key {
            bail!("NoSuchUpload");
        }
        return match method {
            Method::GET => list_parts(&state.engine, upload_id).await,
            Method::PUT => {
                let part_number = query_value(query, "partNumber")
                    .ok_or_else(|| anyhow!("InvalidArgument"))?
                    .parse::<i32>()
                    .map_err(|_| anyhow!("InvalidArgument"))?;
                let part = state
                    .engine
                    .upload_part(&upload_id, part_number, body)
                    .await?;
                let mut response = empty_response(StatusCode::OK);
                response
                    .headers_mut()
                    .insert(header::ETAG, HeaderValue::from_str(&quoted(&part.etag))?);
                Ok(response)
            }
            Method::POST => complete_multipart(&state.engine, &upload_id, body).await,
            Method::DELETE => {
                if !state.engine.abort_multipart(&upload_id).await? {
                    bail!("NoSuchUpload");
                }
                Ok(empty_response(StatusCode::NO_CONTENT))
            }
            _ => Err(anyhow!("MethodNotAllowed")),
        };
    }
    if method == Method::PUT {
        if let Some(source) = headers
            .get("x-amz-copy-source")
            .and_then(|value| value.to_str().ok())
        {
            let condition = object_condition(headers);
            check_write_conditions(state.engine.db.get_object(bucket, key).await?, &condition)?;
            return copy_object(state, bucket, key, source, headers, &condition).await;
        }
        let condition = object_condition(headers);
        check_write_conditions(state.engine.db.get_object(bucket, key).await?, &condition)?;
        let metadata = metadata_from_headers(headers);
        let length = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<i64>().ok());
        let object = state
            .engine
            .put_object(bucket, key, body, metadata, length, condition)
            .await?;
        let mut response = empty_response(StatusCode::OK);
        response
            .headers_mut()
            .insert(header::ETAG, HeaderValue::from_str(&quoted(&object.etag))?);
        Ok(response)
    } else if method == Method::POST && query_has(query, "uploads") {
        let upload_id = state
            .engine
            .create_multipart(bucket, key, metadata_from_headers(headers))
            .await?;
        xml_response(InitiateMultipartUpload {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id,
        })
    } else if method == Method::POST && query_has(query, "delete") {
        multi_delete(&state.engine, bucket, body).await
    } else if method == Method::POST {
        Err(anyhow!("NotImplemented"))
    } else if method == Method::GET || method == Method::HEAD {
        get_object(&state.engine, bucket, key, headers, method == Method::HEAD).await
    } else if method == Method::DELETE {
        let condition = object_condition(headers);
        check_write_conditions(state.engine.db.get_object(bucket, key).await?, &condition)?;
        let _ = state.engine.delete_object(bucket, key, &condition).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    } else {
        Err(anyhow!("MethodNotAllowed"))
    }
}

async fn effective_cors(state: &AppState, uri: &Uri, headers: &HeaderMap) -> CorsConfiguration {
    let Ok(target) = Target::parse(uri, headers, state.public_host.as_deref()) else {
        return state.cors.clone();
    };
    let Some(bucket) = target.bucket else {
        return state.cors.clone();
    };
    state
        .engine
        .db
        .get_bucket_cors(&bucket)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| state.cors.clone())
}

async fn apply_cors(
    state: &AppState,
    uri: &Uri,
    request_headers: &HeaderMap,
    mut response: Response,
) -> Response {
    let Some(origin) = request_headers
        .get("origin")
        .and_then(|value| value.to_str().ok())
    else {
        return response;
    };
    let cors = effective_cors(state, uri, request_headers).await;
    let origin_allowed = cors
        .allowed_origins
        .iter()
        .any(|allowed| allowed == "*" || allowed.eq_ignore_ascii_case(origin));
    if !origin_allowed {
        return response;
    }
    let allow_origin = if cors.allowed_origins.iter().any(|allowed| allowed == "*") {
        "*"
    } else {
        origin
    };
    insert_header(
        response.headers_mut(),
        "Access-Control-Allow-Origin",
        allow_origin,
    );
    insert_header(
        response.headers_mut(),
        "Access-Control-Allow-Methods",
        &allow_methods(&cors).join(", "),
    );
    insert_header(
        response.headers_mut(),
        "Access-Control-Allow-Headers",
        &cors.allowed_headers.join(", "),
    );
    insert_header(
        response.headers_mut(),
        "Access-Control-Expose-Headers",
        &cors.expose_headers.join(", "),
    );
    insert_header(
        response.headers_mut(),
        "Access-Control-Max-Age",
        &cors.max_age_seconds.to_string(),
    );
    insert_header(response.headers_mut(), header::VARY, "Origin");
    response
}

fn allow_methods(cors: &CorsConfiguration) -> Vec<String> {
    let mut methods = cors.allowed_methods.clone();
    if !methods
        .iter()
        .any(|method| method.eq_ignore_ascii_case("OPTIONS"))
    {
        methods.push("OPTIONS".to_string());
    }
    methods
}

async fn get_bucket_cors(state: &AppState, bucket: &str) -> Result<Response> {
    if !state.engine.db.bucket_exists(bucket).await? {
        bail!("NoSuchBucket");
    }
    let configuration = effective_bucket_cors(state, bucket).await;
    xml_response(CorsConfigurationXml::from(&configuration))
}

async fn put_bucket_cors(state: &AppState, bucket: &str, body: Body) -> Result<Response> {
    if !state.engine.db.bucket_exists(bucket).await? {
        bail!("NoSuchBucket");
    }
    let bytes = to_bytes(body, 1024 * 1024).await?;
    let xml: CorsConfigurationXml =
        from_str(std::str::from_utf8(&bytes).map_err(|_| anyhow!("MalformedXML"))?)
            .map_err(|_| anyhow!("MalformedXML"))?;
    let configuration = xml.into_configuration()?;
    state
        .engine
        .db
        .set_bucket_cors(bucket, &configuration)
        .await?;
    Ok(empty_response(StatusCode::OK))
}

async fn delete_bucket_cors(state: &AppState, bucket: &str) -> Result<Response> {
    if !state.engine.db.bucket_exists(bucket).await? {
        bail!("NoSuchBucket");
    }
    state.engine.db.delete_bucket_cors(bucket).await?;
    Ok(empty_response(StatusCode::NO_CONTENT))
}

async fn effective_bucket_cors(state: &AppState, bucket: &str) -> CorsConfiguration {
    state
        .engine
        .db
        .get_bucket_cors(bucket)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| state.cors.clone())
}

async fn list_buckets(engine: &Engine) -> Result<Response> {
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

async fn list_objects(
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
    let mut next_cursor = None;
    let mut limit_prefix: Option<String> = None;
    const SCAN_BATCH_SIZE: usize = 1000;

    if max_keys > 0 {
        'scan: loop {
            let entries = engine
                .db
                .list_objects(bucket, &prefix, &after, SCAN_BATCH_SIZE)
                .await?;
            let page_len = entries.len();
            if page_len == 0 {
                break;
            }
            for entry in entries {
                let entry_prefix = common_prefix(&entry.key, &prefix, &delimiter);
                if let Some(limit_prefix) = &limit_prefix {
                    if entry_prefix.as_deref() == Some(limit_prefix.as_str()) {
                        after = entry.key;
                        continue;
                    }
                    truncated = true;
                    next_cursor = Some(after.clone());
                    break 'scan;
                }

                after = entry.key.clone();
                if let Some(common) = entry_prefix {
                    let is_new = seen.insert(common.clone());
                    if is_new {
                        common_prefixes.push(CommonPrefix {
                            prefix: common.clone(),
                        });
                    }
                    if is_new && contents.len() + common_prefixes.len() >= max_keys {
                        limit_prefix = Some(common);
                    }
                } else {
                    contents.push(ObjectXml {
                        key: entry.key.clone(),
                        last_modified: format_time(entry.modified_at),
                        etag: quoted(&entry.etag),
                        size: entry.size,
                        storage_class: "STANDARD".to_string(),
                    });
                    if contents.len() + common_prefixes.len() >= max_keys {
                        let has_more = !engine
                            .db
                            .list_objects(bucket, &prefix, &after, 1)
                            .await?
                            .is_empty();
                        truncated = has_more;
                        if has_more {
                            next_cursor = Some(after.clone());
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

async fn get_object(
    engine: &Engine,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    head_only: bool,
) -> Result<Response> {
    let object = engine.get_object(bucket, key).await?;
    check_conditions(&object, headers)?;
    let (start, end, partial) = parse_range(
        headers
            .get(header::RANGE)
            .and_then(|value| value.to_str().ok()),
        object.size,
    )?;
    let mut response_headers = object_headers(
        &object,
        end.saturating_sub(start).saturating_add(1),
        start,
        end,
        partial,
    );
    if object.size == 0 {
        response_headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    }
    let body = if head_only || object.size == 0 {
        Body::empty()
    } else {
        Body::from_stream(engine.range_stream(&object, start, end))
    };
    Ok((
        if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        },
        response_headers,
        body,
    )
        .into_response())
}

async fn copy_object(
    state: &AppState,
    bucket: &str,
    key: &str,
    source: &str,
    headers: &HeaderMap,
    condition: &ObjectCondition,
) -> Result<Response> {
    let (source_bucket, source_key) = parse_copy_source(source)?;
    let source_object = state.engine.get_object(&source_bucket, &source_key).await?;
    check_copy_source_conditions(&source_object, headers)?;
    let metadata = if headers
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false)
    {
        metadata_from_headers(headers)
    } else {
        source_object.metadata.clone()
    };
    let object = state
        .engine
        .copy_object(
            &source_bucket,
            &source_key,
            bucket,
            key,
            &metadata,
            condition,
        )
        .await?;
    xml_response(CopyObjectResult {
        etag: quoted(&object.etag),
        last_modified: format_time(object.modified_at),
    })
}

async fn complete_multipart(engine: &Engine, upload_id: &str, body: Body) -> Result<Response> {
    let bytes = to_bytes(body, 2 * 1024 * 1024).await?;
    let request: CompleteMultipartRequest =
        from_str(std::str::from_utf8(&bytes).map_err(|_| anyhow!("InvalidRequest"))?)
            .map_err(|_| anyhow!("InvalidRequest"))?;
    let requested = request
        .parts
        .into_iter()
        .map(|part| (part.part_number, normalize_etag(&part.etag)))
        .collect::<Vec<_>>();
    let object = engine.complete_multipart(upload_id, &requested).await?;
    let location = format!("/{}/{}", object.bucket, object.key);
    xml_response(CompleteMultipartResult {
        bucket: object.bucket,
        key: object.key,
        etag: quoted(&object.etag),
        location,
    })
}

async fn list_parts(engine: &Engine, upload_id: String) -> Result<Response> {
    let parts = engine.list_parts(&upload_id).await?;
    let upload = engine
        .db
        .get_upload(&upload_id)
        .await?
        .ok_or_else(|| anyhow!("NoSuchUpload"))?;
    xml_response(ListPartsResult {
        bucket: upload.bucket,
        key: upload.key,
        upload_id,
        parts: parts
            .into_iter()
            .map(|part| PartXml {
                part_number: part.part_number,
                etag: quoted(&part.etag),
                size: part.size,
                last_modified: format_time(upload.created_at),
            })
            .collect(),
    })
}

async fn list_multipart_uploads(engine: &Engine, bucket: &str) -> Result<Response> {
    if !engine.db.bucket_exists(bucket).await? {
        bail!("NoSuchBucket");
    }
    let uploads = engine
        .list_uploads(bucket, None)
        .await?
        .into_iter()
        .filter(|upload| upload.kind == "multipart")
        .collect::<Vec<_>>();
    xml_response(ListMultipartUploadsResult {
        bucket: bucket.to_string(),
        uploads: uploads
            .into_iter()
            .map(|upload| UploadXml {
                key: upload.key,
                upload_id: upload.upload_id,
                initiated: format_time(upload.created_at),
            })
            .collect(),
    })
}

async fn multi_delete(engine: &Engine, bucket: &str, body: Body) -> Result<Response> {
    if !engine.db.bucket_exists(bucket).await? {
        bail!("NoSuchBucket");
    }
    let bytes = to_bytes(body, 4 * 1024 * 1024).await?;
    let request: DeleteRequest =
        from_str(std::str::from_utf8(&bytes).map_err(|_| anyhow!("MalformedXML"))?)
            .map_err(|_| anyhow!("MalformedXML"))?;
    let DeleteRequest { objects, quiet } = request;
    if objects.is_empty() || objects.len() > 1000 {
        bail!("InvalidRequest");
    }
    let mut deleted = Vec::new();
    let condition = ObjectCondition::default();
    for object in objects {
        let key = object.key;
        let _ = engine.delete_object(bucket, &key, &condition).await?;
        deleted.push(DeletedXml { key });
    }
    xml_response(DeleteResult {
        deleted: (!quiet).then_some(deleted),
    })
}

fn parse_copy_source(source: &str) -> Result<(String, String)> {
    let source = source.trim_start_matches('/');
    let mut parts = source.splitn(2, '/');
    let bucket = percent_encoding::percent_decode_str(parts.next().unwrap_or_default())
        .decode_utf8()
        .map_err(|_| anyhow!("InvalidRequest"))?
        .to_string();
    let key = percent_encoding::percent_decode_str(parts.next().unwrap_or_default())
        .decode_utf8()
        .map_err(|_| anyhow!("InvalidRequest"))?
        .to_string();
    if bucket.is_empty() || key.is_empty() {
        bail!("InvalidRequest");
    }
    Ok((bucket, key))
}

fn metadata_from_headers(headers: &HeaderMap) -> ObjectMetadata {
    let text = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    };
    let mut user = BTreeMap::new();
    for (name, value) in headers {
        if let Some(name) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(value) = value.to_str()
        {
            user.insert(name.to_string(), value.to_string());
        }
    }
    ObjectMetadata {
        content_type: text("content-type"),
        content_disposition: text("content-disposition"),
        content_encoding: text("content-encoding"),
        cache_control: text("cache-control"),
        expires: text("expires"),
        user,
    }
}

fn object_headers(
    object: &ObjectRecord,
    length: i64,
    start: i64,
    end: i64,
    partial: bool,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, header::ETAG, &quoted(&object.etag));
    insert_header(
        &mut headers,
        header::LAST_MODIFIED,
        &httpdate::fmt_http_date(
            std::time::UNIX_EPOCH
                + std::time::Duration::from_secs(object.modified_at.max(0) as u64),
        ),
    );
    insert_header(&mut headers, header::CONTENT_LENGTH, &length.to_string());
    insert_header(&mut headers, header::ACCEPT_RANGES, "bytes");
    insert_header(
        &mut headers,
        header::CONTENT_TYPE,
        object
            .metadata
            .content_type
            .as_deref()
            .unwrap_or("application/octet-stream"),
    );
    if let Some(value) = &object.metadata.content_disposition {
        insert_header(&mut headers, header::CONTENT_DISPOSITION, value);
    }
    if let Some(value) = &object.metadata.content_encoding {
        insert_header(&mut headers, header::CONTENT_ENCODING, value);
    }
    if let Some(value) = &object.metadata.cache_control {
        insert_header(&mut headers, header::CACHE_CONTROL, value);
    }
    if let Some(value) = &object.metadata.expires {
        insert_header(&mut headers, header::EXPIRES, value);
    }
    for (key, value) in &object.metadata.user {
        let name = format!("x-amz-meta-{key}");
        insert_dynamic_header(&mut headers, &name, value);
    }
    if partial {
        insert_header(
            &mut headers,
            header::CONTENT_RANGE,
            &format!("bytes {start}-{end}/{}", object.size),
        );
    }
    headers
}

fn check_conditions(object: &ObjectRecord, headers: &HeaderMap) -> Result<()> {
    let has_if_match = headers.contains_key(header::IF_MATCH);
    if let Some(value) = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        && value != "*"
        && !value
            .split(',')
            .any(|candidate| normalize_etag(candidate) == object.etag)
    {
        bail!("PreconditionFailed");
    }
    let has_if_none_match = headers.contains_key(header::IF_NONE_MATCH);
    if let Some(value) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        && (value == "*"
            || value
                .split(',')
                .any(|candidate| normalize_etag(candidate) == object.etag))
    {
        bail!("NotModified");
    }
    if !has_if_match
        && let Some(value) = headers
            .get(header::IF_UNMODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
        && let Ok(timestamp) = httpdate::parse_http_date(value)
    {
        let modified = std::time::UNIX_EPOCH
            + std::time::Duration::from_secs(object.modified_at.max(0) as u64);
        if modified > timestamp {
            bail!("PreconditionFailed");
        }
    }
    if !has_if_none_match
        && let Some(value) = headers
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
        && let Ok(timestamp) = httpdate::parse_http_date(value)
    {
        let modified = std::time::UNIX_EPOCH
            + std::time::Duration::from_secs(object.modified_at.max(0) as u64);
        if modified <= timestamp {
            bail!("NotModified");
        }
    }
    Ok(())
}

fn object_condition(headers: &HeaderMap) -> ObjectCondition {
    ObjectCondition {
        if_match: headers
            .get(header::IF_MATCH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        if_none_match: headers
            .get(header::IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
    }
}

fn check_write_conditions(object: Option<ObjectRecord>, condition: &ObjectCondition) -> Result<()> {
    if !condition.allows(object.as_ref().map(|object| object.etag.as_str())) {
        bail!("PreconditionFailed");
    }
    Ok(())
}

fn check_copy_source_conditions(object: &ObjectRecord, headers: &HeaderMap) -> Result<()> {
    if let Some(value) = headers
        .get("x-amz-copy-source-if-match")
        .and_then(|value| value.to_str().ok())
        && value != "*"
        && !value
            .split(',')
            .any(|candidate| normalize_etag(candidate) == object.etag)
    {
        bail!("PreconditionFailed");
    }
    if let Some(value) = headers
        .get("x-amz-copy-source-if-none-match")
        .and_then(|value| value.to_str().ok())
        && (value == "*"
            || value
                .split(',')
                .any(|candidate| normalize_etag(candidate) == object.etag))
    {
        bail!("PreconditionFailed");
    }
    Ok(())
}

fn parse_range(value: Option<&str>, size: i64) -> Result<(i64, i64, bool)> {
    if size == 0 {
        return Ok((0, -1, false));
    }
    let Some(value) = value else {
        return Ok((0, size - 1, false));
    };
    let value = value
        .strip_prefix("bytes=")
        .ok_or_else(|| anyhow!("InvalidRange"))?;
    if value.contains(',') {
        bail!("InvalidRange");
    }
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| anyhow!("InvalidRange"))?;
    if start.is_empty() {
        let suffix = end.parse::<i64>().map_err(|_| anyhow!("InvalidRange"))?;
        if suffix <= 0 {
            bail!("InvalidRange");
        }
        return Ok((size.saturating_sub(suffix), size - 1, true));
    }
    let start = start.parse::<i64>().map_err(|_| anyhow!("InvalidRange"))?;
    if start < 0 || start >= size {
        bail!("InvalidRange");
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<i64>()
            .map_err(|_| anyhow!("InvalidRange"))?
            .min(size - 1)
    };
    if end < start {
        bail!("InvalidRange");
    }
    Ok((start, end, true))
}

fn validate_bucket(bucket: &str) -> Result<()> {
    if bucket.is_empty()
        || bucket.len() > 63
        || !bucket
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
        || !bucket.as_bytes()[0].is_ascii_alphanumeric()
        || !bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
    {
        bail!("InvalidBucketName");
    }
    Ok(())
}

fn query_has(query: &[(String, String)], name: &str) -> bool {
    query.iter().any(|(key, _)| key == name)
}

fn query_keys(uri: &Uri) -> Vec<String> {
    form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .map(|(key, _)| key.into_owned())
        .collect()
}

fn query_value(query: &[(String, String)], name: &str) -> Option<String> {
    query
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}
fn quoted(value: &str) -> String {
    format!("\"{value}\"")
}
fn format_time(timestamp: i64) -> String {
    OffsetDateTime::from_unix_timestamp(timestamp.max(0))
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}
fn insert_header(headers: &mut HeaderMap, name: impl header::IntoHeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}
fn insert_dynamic_header(headers: &mut HeaderMap, name: &str, value: &str) {
    if let (Ok(name), Ok(value)) = (
        HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_str(value),
    ) {
        headers.insert(name, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_suffix_and_open_ranges() -> Result<()> {
        assert_eq!(parse_range(None, 10)?, (0, 9, false));
        assert_eq!(parse_range(Some("bytes=2-5"), 10)?, (2, 5, true));
        assert_eq!(parse_range(Some("bytes=7-"), 10)?, (7, 9, true));
        assert_eq!(parse_range(Some("bytes=-3"), 10)?, (7, 9, true));
        assert!(parse_range(Some("bytes=10-"), 10).is_err());
        Ok(())
    }

    #[test]
    fn etag_conditions_take_precedence_over_date_conditions() -> Result<()> {
        let object = ObjectRecord {
            id: 1,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            size: 0,
            etag: "current".to_string(),
            metadata: ObjectMetadata::default(),
            created_at: 100,
            modified_at: 100,
            blocks: Vec::new(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("\"old\""));
        headers.insert(
            header::IF_MODIFIED_SINCE,
            HeaderValue::from_str(&httpdate::fmt_http_date(
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(200),
            ))?,
        );
        check_conditions(&object, &headers)?;

        let mut headers = HeaderMap::new();
        headers.insert(header::IF_MATCH, HeaderValue::from_static("\"current\""));
        headers.insert(
            header::IF_UNMODIFIED_SINCE,
            HeaderValue::from_str(&httpdate::fmt_http_date(
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(50),
            ))?,
        );
        check_conditions(&object, &headers)?;
        Ok(())
    }

    #[test]
    fn parses_path_style_and_validates_bucket_names() -> Result<()> {
        let uri: Uri = "/my-bucket/folder%2Ffile.txt".parse()?;
        let target = Target::parse(&uri, &HeaderMap::new(), None)?;
        assert_eq!(target.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(target.key.as_deref(), Some("folder/file.txt"));
        assert!(validate_bucket("my-bucket").is_ok());
        assert!(validate_bucket("bad_bucket").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn serializes_standard_s3_error_root() -> Result<()> {
        let uri: Uri = "/A".parse()?;
        let response = error_response(
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch".to_string(),
            "signature mismatch",
            &uri,
            "request-1",
        );
        let body = to_bytes(response.into_body(), 1024 * 1024).await?;
        let body = std::str::from_utf8(&body)?;
        assert!(body.contains("<Error xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"));
        assert!(!body.contains("<ErrorXml"));
        Ok(())
    }

    #[test]
    fn serializes_s3_multipart_and_location_roots() -> Result<()> {
        let initiate = to_string(&InitiateMultipartUpload {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            upload_id: "upload".to_string(),
        })?;
        assert!(initiate.starts_with("<InitiateMultipartUploadResult>"));

        let complete = to_string(&CompleteMultipartResult {
            location: "/bucket/key".to_string(),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            etag: "etag".to_string(),
        })?;
        assert!(complete.starts_with("<CompleteMultipartUploadResult>"));

        let location = to_string(&BucketLocation {
            location: "us-east-1".to_string(),
        })?;
        assert_eq!(
            location,
            "<LocationConstraint>us-east-1</LocationConstraint>"
        );
        Ok(())
    }

    #[test]
    fn maps_s3_error_codes_to_http_statuses() {
        assert_eq!(
            status_for(&anyhow!("InvalidRequest")),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(status_for(&anyhow!("BucketNotEmpty")), StatusCode::CONFLICT);
        assert_eq!(
            status_for(&anyhow!("MethodNotAllowed")),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            status_for(&anyhow!("NotImplemented")),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            public_error_message("InternalError", &anyhow!("database path: /secret")),
            "Internal server error"
        );
    }

    #[test]
    fn rejects_invalid_utf8_paths_as_client_errors() -> Result<()> {
        let uri: Uri = "/bucket/%FF".parse()?;
        let error = Target::parse(&uri, &HeaderMap::new(), None)
            .err()
            .ok_or_else(|| anyhow!("expected invalid path error"))?;
        assert_eq!(status_for(&error), StatusCode::BAD_REQUEST);
        let error = parse_copy_source("/bucket/%FF")
            .err()
            .ok_or_else(|| anyhow!("expected invalid copy source error"))?;
        assert_eq!(status_for(&error), StatusCode::BAD_REQUEST);
        Ok(())
    }
}

fn xml_response<T: Serialize>(value: T) -> Result<Response> {
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

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn status_for(error: &anyhow::Error) -> StatusCode {
    match error_code(error).as_str() {
        "NoSuchKey" | "NoSuchBucket" | "NoSuchUpload" => StatusCode::NOT_FOUND,
        "PreconditionFailed" => StatusCode::PRECONDITION_FAILED,
        "NotModified" => StatusCode::NOT_MODIFIED,
        "BucketNotEmpty" => StatusCode::CONFLICT,
        "InvalidRequest" | "EntityTooSmall" | "InvalidPart" | "InvalidPartOrder"
        | "InvalidArgument" | "InvalidBucketName" | "MalformedXML" => StatusCode::BAD_REQUEST,
        "MethodNotAllowed" => StatusCode::METHOD_NOT_ALLOWED,
        "NotImplemented" => StatusCode::NOT_IMPLEMENTED,
        "InvalidRange" => StatusCode::RANGE_NOT_SATISFIABLE,
        "AccessDenied" | "SignatureDoesNotMatch" | "RequestTimeTooSkewed" => StatusCode::FORBIDDEN,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_code(error: &anyhow::Error) -> String {
    let text = error.to_string();
    for code in [
        "NoSuchKey",
        "NoSuchBucket",
        "NoSuchUpload",
        "PreconditionFailed",
        "NotModified",
        "BucketNotEmpty",
        "EntityTooSmall",
        "InvalidPartOrder",
        "InvalidPart",
        "InvalidRequest",
        "InvalidRange",
        "InvalidArgument",
        "InvalidBucketName",
        "MalformedXML",
        "NotImplemented",
        "MethodNotAllowed",
        "AccessDenied",
        "SignatureDoesNotMatch",
        "RequestTimeTooSkewed",
    ] {
        if text.contains(code) {
            return code.to_string();
        }
    }
    "InternalError".to_string()
}

fn public_error_message(code: &str, error: &anyhow::Error) -> String {
    if code == "InternalError" {
        "Internal server error".to_string()
    } else {
        error.to_string()
    }
}

fn log_request_failure(
    request_id: &str,
    method: &Method,
    path: &str,
    status: StatusCode,
    code: &str,
    error: &anyhow::Error,
) {
    if matches!(code, "NoSuchKey" | "NoSuchUpload") {
        tracing::debug!(
            request_id,
            %method,
            path,
            %status,
            code,
            error = %error,
            "S3 request failed"
        );
    } else {
        tracing::error!(
            request_id,
            %method,
            path,
            %status,
            code,
            error = %error,
            "S3 request failed"
        );
    }
}

fn error_response(
    status: StatusCode,
    code: String,
    message: &str,
    uri: &Uri,
    request_id: &str,
) -> Response {
    let response = ErrorXml {
        code,
        message: message.to_string(),
        resource: uri.path().to_string(),
        request_id: request_id.to_string(),
    };
    let xml = to_string(&response)
        .unwrap_or_else(|_| "<Error><Code>InternalError</Code></Error>".to_string());
    let mut response = Response::new(Body::from(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>{}",
        with_namespace(&xml)
    )));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    insert_header(response.headers_mut(), "x-amz-request-id", request_id);
    response
}

fn with_namespace(xml: &str) -> String {
    if let Some(index) = xml.find('>') {
        format!("{} xmlns=\"{}\"{}", &xml[..index], XMLNS, &xml[index..])
    } else {
        xml.to_string()
    }
}

struct Target {
    bucket: Option<String>,
    key: Option<String>,
}

impl Target {
    fn parse(uri: &Uri, headers: &HeaderMap, public_host: Option<&str>) -> Result<Self> {
        let host = headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let path = uri.path().trim_start_matches('/');
        if let Some(public_host) = public_host {
            let public_host = public_host.trim_end_matches('/');
            if host != public_host {
                let bucket = host
                    .strip_suffix(public_host)
                    .and_then(|prefix| prefix.strip_suffix('.'))
                    .unwrap_or_default();
                if !bucket.is_empty() {
                    return Ok(Self {
                        bucket: Some(bucket.to_string()),
                        key: if path.is_empty() {
                            None
                        } else {
                            Some(decode_path(path)?)
                        },
                    });
                }
            }
        }
        if path.is_empty() {
            return Ok(Self {
                bucket: None,
                key: None,
            });
        }
        let (bucket, key) = match path.split_once('/') {
            Some((bucket, key)) => (bucket, Some(key)),
            None => (path, None),
        };
        Ok(Self {
            bucket: Some(decode_path(bucket)?),
            key: key.map(decode_path).transpose()?,
        })
    }
}

fn decode_path(path: &str) -> Result<String> {
    Ok(percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| anyhow!("InvalidRequest"))?
        .to_string())
}

#[derive(Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
struct ListAllMyBuckets {
    buckets: Buckets,
}
#[derive(Serialize)]
struct Buckets {
    #[serde(rename = "Bucket", default)]
    bucket: Vec<BucketXml>,
}
#[derive(Serialize)]
struct BucketXml {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "CreationDate")]
    creation_date: String,
}
#[derive(Serialize)]
struct ListBucketResult {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    delimiter: Option<String>,
    #[serde(rename = "MaxKeys")]
    max_keys: i32,
    #[serde(rename = "KeyCount")]
    key_count: i32,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Marker", skip_serializing_if = "Option::is_none")]
    marker: Option<String>,
    #[serde(rename = "StartAfter", skip_serializing_if = "Option::is_none")]
    start_after: Option<String>,
    #[serde(rename = "ContinuationToken", skip_serializing_if = "Option::is_none")]
    continuation_token: Option<String>,
    #[serde(rename = "NextMarker", skip_serializing_if = "Option::is_none")]
    next_marker: Option<String>,
    #[serde(
        rename = "NextContinuationToken",
        skip_serializing_if = "Option::is_none"
    )]
    next_continuation_token: Option<String>,
    #[serde(rename = "Contents", default)]
    contents: Vec<ObjectXml>,
    #[serde(rename = "CommonPrefixes", default)]
    common_prefixes: Vec<CommonPrefix>,
}
#[derive(Serialize)]
struct ObjectXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "StorageClass")]
    storage_class: String,
}
#[derive(Serialize)]
struct CommonPrefix {
    #[serde(rename = "Prefix")]
    prefix: String,
}
#[derive(Serialize)]
#[serde(rename = "LocationConstraint")]
struct BucketLocation {
    #[serde(rename = "$text")]
    location: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "CORSConfiguration")]
struct CorsConfigurationXml {
    #[serde(rename = "CORSRule", default)]
    rules: Vec<CorsRuleXml>,
}

#[derive(Serialize, Deserialize)]
struct CorsRuleXml {
    #[serde(rename = "AllowedOrigin", default)]
    allowed_origins: Vec<String>,
    #[serde(rename = "AllowedMethod", default)]
    allowed_methods: Vec<String>,
    #[serde(rename = "AllowedHeader", default)]
    allowed_headers: Vec<String>,
    #[serde(rename = "ExposeHeader", default)]
    expose_headers: Vec<String>,
    #[serde(rename = "MaxAgeSeconds", default)]
    max_age_seconds: Option<u64>,
}

impl CorsConfigurationXml {
    fn from(configuration: &CorsConfiguration) -> Self {
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

    fn into_configuration(self) -> Result<CorsConfiguration> {
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
struct InitiateMultipartUpload {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
}
#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
struct CompleteMultipartResult {
    #[serde(rename = "Location")]
    location: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "ETag")]
    etag: String,
}
#[derive(Serialize)]
struct CopyObjectResult {
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "LastModified")]
    last_modified: String,
}
#[derive(Serialize)]
struct ListPartsResult {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
    #[serde(rename = "Part", default)]
    parts: Vec<PartXml>,
}
#[derive(Serialize)]
struct PartXml {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "Size")]
    size: i64,
}
#[derive(Serialize)]
struct ListMultipartUploadsResult {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Upload", default)]
    uploads: Vec<UploadXml>,
}
#[derive(Serialize)]
struct UploadXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
    #[serde(rename = "Initiated")]
    initiated: String,
}
#[derive(Serialize)]
struct DeleteResult {
    #[serde(rename = "Deleted", skip_serializing_if = "Option::is_none")]
    deleted: Option<Vec<DeletedXml>>,
}
#[derive(Serialize)]
struct DeletedXml {
    #[serde(rename = "Key")]
    key: String,
}
#[derive(Serialize)]
#[serde(rename = "Error")]
struct ErrorXml {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "Resource")]
    resource: String,
    #[serde(rename = "RequestId")]
    request_id: String,
}

#[derive(Deserialize)]
struct CompleteMultipartRequest {
    #[serde(rename = "Part", default)]
    parts: Vec<CompletedPart>,
}
#[derive(Deserialize)]
struct CompletedPart {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "ETag")]
    etag: String,
}
#[derive(Deserialize)]
struct DeleteRequest {
    #[serde(rename = "Object", default)]
    objects: Vec<DeleteObject>,
    #[serde(rename = "Quiet", default)]
    quiet: bool,
}
#[derive(Deserialize)]
struct DeleteObject {
    #[serde(rename = "Key")]
    key: String,
}

trait IntoResponseExt {
    fn into_response(self) -> Response;
}
impl IntoResponseExt for (StatusCode, HeaderMap, Body) {
    fn into_response(self) -> Response {
        let (status, headers, body) = self;
        let mut response = Response::new(body);
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response
    }
}
