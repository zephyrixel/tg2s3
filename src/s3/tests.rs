use super::xml::CompleteMultipartResult;
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
