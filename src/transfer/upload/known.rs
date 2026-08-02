use super::task::{UploadTasks, finish_pipe_task, start_pipe_task};
use super::{BlockSpec, UploadContext, UploadedBlock, block_size, drain_tasks, reap_one};
use crate::limits::TransferDirection;
use anyhow::{Result, anyhow, bail};
use axum::body::Body;
use futures_util::StreamExt;
use md5::{Digest, Md5};
use tokio::io::AsyncWriteExt;

pub(super) async fn upload_known_length(
    context: &UploadContext,
    upload_id: &str,
    part_number: i32,
    body: Body,
    expected_length: i64,
) -> Result<(Vec<UploadedBlock>, i64, Md5)> {
    if expected_length < 0 {
        bail!("InvalidRequest");
    }
    let mut source = body.into_data_stream();
    let mut tasks = UploadTasks::new();
    let mut uploaded = Vec::new();
    let mut current = None;
    let mut digest = Md5::new();
    let mut total = 0_i64;
    let mut ordinal = 0_i64;
    let mut offset = 0_i64;
    let mut failure = None;

    'source: while let Some(frame) = source.next().await {
        let data = match frame {
            Ok(data) => data,
            Err(error) => {
                failure = Some(anyhow!(error));
                break;
            }
        };
        let next_total = match total.checked_add(data.len() as i64) {
            Some(total) => total,
            None => {
                failure = Some(anyhow!("EntityTooLarge"));
                break;
            }
        };
        if next_total > context.config.max_object_size {
            failure = Some(anyhow!("EntityTooLarge"));
            break;
        }
        if next_total > expected_length {
            failure = Some(anyhow!(
                "IncompleteBody: request Content-Length {expected_length} does not match body length {next_total}"
            ));
            break;
        }
        if let Err(error) = context
            .limits
            .bandwidth
            .throttle(TransferDirection::Upload, data.len(), true)
            .await
        {
            failure = Some(error);
            break;
        }
        digest.update(&data);
        total = next_total;

        let mut position = 0_usize;
        while position < data.len() {
            if current.is_none() {
                let block_size =
                    match block_size(context.config.chunk_size, offset, expected_length) {
                        Ok(size) => size,
                        Err(error) => {
                            failure = Some(error);
                            break 'source;
                        }
                    };
                current = Some(start_pipe_task(
                    context,
                    BlockSpec {
                        upload_id: upload_id.to_string(),
                        part_number,
                        ordinal,
                        offset,
                        size: block_size,
                    },
                ));
                ordinal += 1;
            }
            let (amount, block_size, complete) = {
                let block = current.as_mut().expect("current upload block exists");
                let remaining = block.size - block.written;
                let amount = remaining.min((data.len() - position) as u64) as usize;
                if let Err(error) = block
                    .writer
                    .write_all(&data[position..position + amount])
                    .await
                {
                    failure = Some(error.into());
                    break 'source;
                }
                block.written += amount as u64;
                (amount, block.size, block.written == block.size)
            };
            position += amount;
            if complete {
                let finished = current.take().expect("current upload block exists");
                if let Err(error) = finish_pipe_task(finished, &mut tasks).await {
                    failure = Some(error);
                    break 'source;
                }
                if tasks.len() >= context.config.upload_concurrency
                    && let Err(error) = reap_one(&mut tasks, &mut uploaded).await
                {
                    failure = Some(error);
                    break 'source;
                }
                offset = match offset.checked_add(block_size as i64) {
                    Some(offset) => offset,
                    None => {
                        failure = Some(anyhow!("EntityTooLarge"));
                        break 'source;
                    }
                };
            }
        }
    }

    if let Some(block) = current.take()
        && let Err(error) = finish_pipe_task(block, &mut tasks).await
    {
        failure.get_or_insert(error);
    }
    drain_tasks(&mut tasks, &mut uploaded, &mut failure).await;
    if let Some(error) = failure {
        return Err(error);
    }
    if total != expected_length {
        bail!(
            "IncompleteBody: request Content-Length {expected_length} does not match body length {total}"
        );
    }
    Ok((uploaded, total, digest))
}
