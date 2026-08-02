use crate::limits::{AdmissionPermit, TransferDirection, TransferLimits};
use crate::model::BlockRef;
use crate::telegram::TelegramClient;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use std::sync::Arc;
use tokio::sync::Semaphore;

pub(crate) fn range_stream(
    telegram: TelegramClient,
    download_slots: Arc<Semaphore>,
    blocks: Vec<BlockRef>,
    start: i64,
    end: i64,
    admission_permit: Option<AdmissionPermit>,
    limits: Arc<TransferLimits>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    stream::unfold(
        RangeState {
            telegram,
            download_slots,
            blocks,
            index: 0,
            start,
            end,
            _admission_permit: admission_permit,
            limits,
            download_permit: None,
            current: None,
            terminated: false,
        },
        |mut state| async move {
            if state.terminated {
                return None;
            }
            loop {
                if let Some(mut current) = state.current.take() {
                    match current.next().await {
                        Some(Ok(bytes)) if bytes.is_empty() => {
                            state.current = Some(current);
                            continue;
                        }
                        Some(Ok(bytes)) => {
                            state.current = Some(current);
                            if let Err(error) = state
                                .limits
                                .bandwidth
                                .throttle(TransferDirection::Download, bytes.len(), false)
                                .await
                            {
                                state.terminated = true;
                                state.download_permit.take();
                                return Some((Err(std::io::Error::other(error)), state));
                            }
                            return Some((Ok(bytes), state));
                        }
                        Some(Err(error)) => {
                            state.terminated = true;
                            state.download_permit.take();
                            return Some((Err(std::io::Error::other(error)), state));
                        }
                        None => {
                            state.download_permit.take();
                        }
                    }
                }

                let block = state.blocks.get(state.index).cloned()?;
                state.index += 1;
                let block_end = block.offset + block.size - 1;
                if block_end < state.start || block.offset > state.end {
                    continue;
                }
                let read_start = state.start.max(block.offset) - block.offset;
                let read_end = state.end.min(block_end) - block.offset;
                let permit = match state.download_slots.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(error) => {
                        state.terminated = true;
                        return Some((Err(std::io::Error::other(error)), state));
                    }
                };
                match state
                    .telegram
                    .download_stream(&block, read_start, read_end)
                    .await
                {
                    Ok(stream) => {
                        state.download_permit = Some(permit);
                        state.current = Some(stream);
                    }
                    Err(error) => {
                        state.terminated = true;
                        return Some((Err(std::io::Error::other(error)), state));
                    }
                }
            }
        },
    )
}

struct RangeState {
    telegram: TelegramClient,
    download_slots: Arc<Semaphore>,
    blocks: Vec<BlockRef>,
    index: usize,
    start: i64,
    end: i64,
    _admission_permit: Option<AdmissionPermit>,
    limits: Arc<TransferLimits>,
    download_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    current: Option<crate::telegram::DownloadStream>,
    terminated: bool,
}
