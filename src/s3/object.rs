use super::xml::{CopyObjectResult, xml_response};
use super::{
    AppState, IntoResponseExt, check_conditions, check_copy_source_conditions, format_time,
    metadata_from_headers, object_headers, parse_copy_source, parse_range, quoted,
};
use crate::engine::Engine;
use crate::model::ObjectCondition;
use anyhow::Result;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;

pub(super) async fn get_object(
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
    let admission_permit = if !head_only && object.size > 0 {
        Some(engine.limits.admission.acquire().await?)
    } else {
        None
    };
    let body = if head_only || object.size == 0 {
        Body::empty()
    } else {
        Body::from_stream(engine.range_stream(&object, start, end, admission_permit))
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

pub(super) async fn copy_object(
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
