use super::task::{UploadTasks, start_file_task};
use super::{BlockSpec, UploadContext, UploadedBlock, block_size, drain_tasks, reap_one};
use crate::config::Config;
use crate::limits::{TransferDirection, TransferLimits};
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use futures_util::StreamExt;
use md5::{Digest, Md5};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub(super) async fn cleanup_stale_spools(data_dir: &Path) -> Result<usize> {
    let directory = data_dir.join("upload-spool");
    let mut entries = match fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(24 * 60 * 60))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if !metadata.is_file() || metadata.modified().unwrap_or(SystemTime::now()) > cutoff {
            continue;
        }
        match fs::remove_file(entry.path()).await {
            Ok(()) => removed += 1,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(path = %entry.path().display(), error = %error, "failed to remove stale upload spool");
            }
        }
    }
    Ok(removed)
}

pub(super) async fn upload_unknown_length(
    context: &UploadContext,
    upload_id: &str,
    part_number: i32,
    body: Body,
) -> Result<(Vec<UploadedBlock>, i64, Md5)> {
    let spool = spool_body(
        &context.config,
        &context.limits,
        upload_id,
        part_number,
        body,
    )
    .await?;
    tracing::info!(
        upload_id,
        part_number,
        bytes = spool.size,
        path = %spool.path.display(),
        "using disk spool for upload without Content-Length"
    );
    let result = upload_spooled(context, upload_id, part_number, &spool).await;
    spool.remove().await;
    result
}

pub(super) struct Spool {
    pub(super) path: PathBuf,
    pub(super) size: i64,
    pub(super) digest: Md5,
    cleanup_on_drop: bool,
}

impl Spool {
    async fn remove(mut self) {
        self.cleanup_on_drop = false;
        remove_spool_file(&self.path).await;
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }
        let path = self.path.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            remove_spool_file(&path).await;
        });
    }
}

async fn remove_spool_file(path: &Path) {
    if let Err(error) = fs::remove_file(path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), error = %error, "failed to remove upload spool");
    }
}

async fn spool_body(
    config: &Config,
    limits: &TransferLimits,
    upload_id: &str,
    part_number: i32,
    body: Body,
) -> Result<Spool> {
    let directory = config.data_dir.join("upload-spool");
    fs::create_dir_all(&directory).await?;
    let path = directory.join(format!(
        "{}-{}-{}.part",
        upload_id,
        part_number,
        Uuid::new_v4()
    ));
    let mut spool = Spool {
        path,
        size: 0,
        digest: Md5::new(),
        cleanup_on_drop: true,
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&spool.path)
        .await?;
    let mut source = body.into_data_stream();
    let result = async {
        while let Some(frame) = source.next().await {
            let data = frame.map_err(anyhow::Error::from)?;
            let next_size = spool
                .size
                .checked_add(data.len() as i64)
                .ok_or_else(|| anyhow!("EntityTooLarge"))?;
            if next_size > config.max_object_size {
                bail!("EntityTooLarge");
            }
            limits
                .bandwidth
                .throttle(TransferDirection::Upload, data.len(), true)
                .await?;
            file.write_all(&data).await?;
            spool.digest.update(&data);
            spool.size = next_size;
        }
        file.flush().await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    drop(file);
    if let Err(error) = result {
        spool.remove().await;
        return Err(error);
    }
    Ok(spool)
}

async fn upload_spooled(
    context: &UploadContext,
    upload_id: &str,
    part_number: i32,
    spool: &Spool,
) -> Result<(Vec<UploadedBlock>, i64, Md5)> {
    let mut tasks = UploadTasks::new();
    let mut uploaded = Vec::new();
    let mut failure = None;
    let mut ordinal = 0_i64;
    let mut offset = 0_i64;
    while offset < spool.size {
        let size = block_size(context.config.chunk_size, offset, spool.size)?;
        let task = start_file_task(
            context,
            BlockSpec {
                upload_id: upload_id.to_string(),
                part_number,
                ordinal,
                offset,
                size,
            },
            spool.path.clone(),
        );
        tasks.push(task);
        ordinal += 1;
        offset = match offset.checked_add(size as i64) {
            Some(offset) => offset,
            None => {
                failure = Some(anyhow!("EntityTooLarge"));
                break;
            }
        };
        if tasks.len() >= context.config.upload_concurrency
            && let Err(error) = reap_one(&mut tasks, &mut uploaded).await
        {
            failure = Some(error);
            break;
        }
    }
    drain_tasks(&mut tasks, &mut uploaded, &mut failure).await;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok((uploaded, spool.size, spool.digest.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn removes_a_spool_dropped_during_cancellation() -> Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("cancelled.part");
        tokio::fs::File::create(&path).await?;
        drop(Spool {
            path: path.clone(),
            size: 0,
            digest: Md5::new(),
            cleanup_on_drop: true,
        });

        for _ in 0..20 {
            if !path.exists() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        bail!("cancelled upload spool was not removed")
    }
}
