use super::xml::{
    CompleteMultipartRequest, CompleteMultipartResult, DeleteErrorXml, DeleteRequest, DeleteResult,
    DeletedXml, ListMultipartUploadsResult, ListPartsResult, PartXml, UploadXml, xml_response,
};
use super::{format_time, quoted};
use crate::engine::Engine;
use crate::model::{ObjectCondition, normalize_etag};
use anyhow::{Result, anyhow, bail};
use axum::body::{Body, to_bytes};
use axum::response::Response;
use quick_xml::de::from_str;

pub(super) async fn complete_multipart(
    engine: &Engine,
    upload_id: &str,
    body: Body,
) -> Result<Response> {
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

pub(super) async fn list_parts(engine: &Engine, upload_id: String) -> Result<Response> {
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

pub(super) async fn list_multipart_uploads(engine: &Engine, bucket: &str) -> Result<Response> {
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

pub(super) async fn multi_delete(engine: &Engine, bucket: &str, body: Body) -> Result<Response> {
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
    let mut errors = Vec::new();
    let condition = ObjectCondition::default();
    for object in objects {
        let key = object.key;
        match engine.delete_object(bucket, &key, &condition).await {
            Ok(_) => deleted.push(DeletedXml { key }),
            Err(error) => {
                let code = super::error_code(&error);
                let message = super::public_error_message(&code, &error);
                errors.push(DeleteErrorXml { key, code, message });
            }
        }
    }
    xml_response(DeleteResult {
        deleted: (!quiet).then_some(deleted),
        errors,
    })
}
