use super::Engine;
use crate::limits::AdmissionPermit;
use crate::model::ObjectRecord;
use anyhow::Result;
use bytes::Bytes;
use futures_util::Stream;

impl Engine {
    pub fn range_stream(
        &self,
        object: &ObjectRecord,
        start: i64,
        end: i64,
        admission_permit: Option<AdmissionPermit>,
    ) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
        crate::transfer::range_stream(
            self.telegram.clone(),
            self.download_slots.clone(),
            object.blocks.clone(),
            start,
            end,
            admission_permit,
            self.limits.clone(),
        )
    }
}
