use super::Engine;
use crate::model::PartRecord;
use anyhow::Result;
use axum::body::Body;

impl Engine {
    pub(super) async fn upload_stream(
        &self,
        upload_id: &str,
        part_number: i32,
        body: Body,
        expected_length: Option<i64>,
    ) -> Result<(PartRecord, i64)> {
        let context = crate::transfer::UploadContext::new(
            &self.db,
            &self.telegram,
            self.config.clone(),
            self.limits.clone(),
            self.upload_slots.clone(),
        );
        crate::transfer::upload(context, upload_id, part_number, body, expected_length).await
    }
}
