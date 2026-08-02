use self::target::{Target, decode_path};
use self::xml::{
    BucketLocation, CorsConfigurationXml, ErrorXml, InitiateMultipartUpload, with_namespace,
    xml_response,
};
use crate::auth::SigV4;
use crate::engine::Engine;
use crate::limits::check_size;
use crate::model::{
    CorsConfiguration, ObjectCondition, ObjectMetadata, ObjectRecord, normalize_etag,
};
use anyhow::{Result, anyhow, bail};
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::get;
use quick_xml::{de::from_str, se::to_string};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;
use uuid::Uuid;

mod list;
mod multipart;
mod object;
mod target;
#[cfg(test)]
mod tests;
mod xml;

use self::list::{list_buckets, list_objects};
use self::multipart::{complete_multipart, list_multipart_uploads, list_parts, multi_delete};
use self::object::{copy_object, get_object};

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
    match state.engine.db.ping().await {
        Ok(()) => empty_response(StatusCode::OK),
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
    if parts.method != Method::OPTIONS && is_streaming_payload(&parts.headers) {
        // aws-chunked bodies carry chunk framing and chunk signatures; storing
        // them verbatim would corrupt the object, so reject them up front.
        let response = error_response(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented".to_string(),
            "aws-chunked streaming payloads are not supported; retry with a non-streaming payload",
            &request_uri,
            &request_id,
        );
        return apply_cors(&state, &request_uri, &request_headers, response).await;
    }
    let request_method = parts.method.clone();
    let response = match dispatch(&state, parts.method, parts.uri, parts.headers, body).await {
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
    let Some(bucket) = target.bucket.as_deref() else {
        return Err(anyhow!("InvalidRequest"));
    };
    let Some(key) = target.key.as_deref() else {
        return dispatch_bucket(state, method, bucket, &query, body).await;
    };
    dispatch_object(ObjectRequest {
        state,
        method,
        bucket,
        key,
        query: &query,
        headers: &headers,
        body,
    })
    .await
}

struct ObjectRequest<'a> {
    state: &'a AppState,
    method: Method,
    bucket: &'a str,
    key: &'a str,
    query: &'a [(String, String)],
    headers: &'a HeaderMap,
    body: Body,
}

async fn dispatch_bucket(
    state: &AppState,
    method: Method,
    bucket: &str,
    query: &[(String, String)],
    body: Body,
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

async fn dispatch_object(request: ObjectRequest<'_>) -> Result<Response> {
    let ObjectRequest {
        state,
        method,
        bucket,
        key,
        query,
        headers,
        body,
    } = request;
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
                if headers.contains_key("x-amz-copy-source") {
                    // UploadPartCopy has no request body; treating it as a
                    // regular part upload would store an empty part.
                    return Err(anyhow!("NotImplemented"));
                }
                let part_number = query_value(query, "partNumber")
                    .ok_or_else(|| anyhow!("InvalidArgument"))?
                    .parse::<i32>()
                    .map_err(|_| anyhow!("InvalidArgument"))?;
                let length = request_content_length(headers)?;
                check_size(length, state.engine.limits.max_object_size)?;
                let _permit = state.engine.limits.admission.acquire().await?;
                let part = state
                    .engine
                    .upload_part(&upload_id, part_number, body, length)
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
        let length = request_content_length(headers)?;
        check_size(length, state.engine.limits.max_object_size)?;
        let _permit = state.engine.limits.admission.acquire().await?;
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

fn parse_copy_source(source: &str) -> Result<(String, String)> {
    let source = source.trim_start_matches('/');
    let mut parts = source.splitn(2, '/');
    let bucket = decode_path(parts.next().unwrap_or_default())?;
    let key = decode_path(parts.next().unwrap_or_default())?;
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

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

/// One row per S3 error code: substring matched against error text and the HTTP
/// status it maps to. A code that is a substring of another (InvalidPart vs
/// InvalidPartOrder) must be listed after the longer one.
const S3_ERRORS: &[(&str, StatusCode)] = &[
    ("NoSuchKey", StatusCode::NOT_FOUND),
    ("NoSuchBucket", StatusCode::NOT_FOUND),
    ("NoSuchUpload", StatusCode::NOT_FOUND),
    ("EntityTooLarge", StatusCode::PAYLOAD_TOO_LARGE),
    ("SlowDown", StatusCode::SERVICE_UNAVAILABLE),
    ("PreconditionFailed", StatusCode::PRECONDITION_FAILED),
    ("NotModified", StatusCode::NOT_MODIFIED),
    ("BucketNotEmpty", StatusCode::CONFLICT),
    ("EntityTooSmall", StatusCode::BAD_REQUEST),
    ("IncompleteBody", StatusCode::BAD_REQUEST),
    ("InvalidPartOrder", StatusCode::BAD_REQUEST),
    ("InvalidPart", StatusCode::BAD_REQUEST),
    ("InvalidRequest", StatusCode::BAD_REQUEST),
    ("InvalidRange", StatusCode::RANGE_NOT_SATISFIABLE),
    ("InvalidArgument", StatusCode::BAD_REQUEST),
    ("InvalidBucketName", StatusCode::BAD_REQUEST),
    ("MalformedXML", StatusCode::BAD_REQUEST),
    ("NotImplemented", StatusCode::NOT_IMPLEMENTED),
    ("MethodNotAllowed", StatusCode::METHOD_NOT_ALLOWED),
    ("AccessDenied", StatusCode::FORBIDDEN),
    ("SignatureDoesNotMatch", StatusCode::FORBIDDEN),
    ("RequestTimeTooSkewed", StatusCode::FORBIDDEN),
];

fn status_for(error: &anyhow::Error) -> StatusCode {
    let text = error.to_string();
    S3_ERRORS
        .iter()
        .find(|(code, _)| text.contains(code))
        .map(|(_, status)| *status)
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

fn error_code(error: &anyhow::Error) -> String {
    let text = error.to_string();
    S3_ERRORS
        .iter()
        .find(|(code, _)| text.contains(code))
        .map(|(code, _)| (*code).to_string())
        .unwrap_or_else(|| "InternalError".to_string())
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
    if status == StatusCode::NOT_MODIFIED {
        // 304 responses must not carry a body.
        let mut response = empty_response(status);
        insert_header(response.headers_mut(), "x-amz-request-id", request_id);
        return response;
    }
    let retryable = code == "SlowDown";
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
    if retryable {
        insert_header(response.headers_mut(), "retry-after", "5");
    }
    response
}

fn is_streaming_payload(headers: &HeaderMap) -> bool {
    headers
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("STREAMING-"))
        || headers
            .get(header::CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("aws-chunked"))
            })
}

fn request_content_length(headers: &HeaderMap) -> Result<Option<i64>> {
    let Some(value) = headers.get(header::CONTENT_LENGTH) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| anyhow!("InvalidRequest"))?
        .parse::<i64>()
        .map_err(|_| anyhow!("InvalidRequest"))?;
    if value < 0 {
        bail!("InvalidRequest");
    }
    Ok(Some(value))
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
