use super::DownloadStream;
use anyhow::anyhow;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};

pub(crate) fn exact_range_stream<S>(source: S, skip: u64, length: u64) -> DownloadStream
where
    S: Stream<Item = anyhow::Result<Bytes>> + Send + 'static,
{
    Box::pin(stream::unfold(
        RangeState {
            source: Box::pin(source),
            skip,
            remaining: length,
            terminated: false,
        },
        |mut state| async move {
            if state.terminated || state.remaining == 0 {
                return None;
            }
            loop {
                match state.source.next().await {
                    Some(Ok(chunk)) if chunk.is_empty() => continue,
                    Some(Ok(chunk)) => {
                        if state.skip >= chunk.len() as u64 {
                            state.skip -= chunk.len() as u64;
                            continue;
                        }
                        if state.skip > 0 {
                            let skip = state.skip as usize;
                            state.skip = 0;
                            let chunk = chunk.slice(skip..);
                            return Some((Ok(state.take(chunk)), state));
                        }
                        return Some((Ok(state.take(chunk)), state));
                    }
                    Some(Err(error)) => {
                        state.terminated = true;
                        return Some((Err(error), state));
                    }
                    None => {
                        state.terminated = true;
                        return Some((
                            Err(anyhow!(
                                "Telegram returned a short stream: {} bytes remain",
                                state.remaining
                            )),
                            state,
                        ));
                    }
                }
            }
        },
    ))
}

struct RangeState {
    source: std::pin::Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send>>,
    skip: u64,
    remaining: u64,
    terminated: bool,
}

impl RangeState {
    fn take(&mut self, chunk: Bytes) -> Bytes {
        let length = self.remaining.min(chunk.len() as u64) as usize;
        self.remaining -= length as u64;
        if length == chunk.len() {
            chunk
        } else {
            chunk.slice(..length)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    #[tokio::test]
    async fn emits_only_the_requested_range_without_joining_chunks() {
        let source = stream::iter(vec![
            Ok(Bytes::from_static(b"012")),
            Ok(Bytes::from_static(b"3456")),
            Ok(Bytes::from_static(b"789")),
        ]);
        let mut output = exact_range_stream(source, 2, 5);
        let mut result = Vec::new();
        while let Some(chunk) = output.next().await {
            result.extend_from_slice(&chunk.expect("range should be valid"));
        }
        assert_eq!(result, b"23456");
    }

    #[tokio::test]
    async fn reports_short_sources() {
        let source = stream::iter(vec![Ok(Bytes::from_static(b"0123"))]);
        let mut output = exact_range_stream(source, 0, 5);
        assert_eq!(
            output.next().await.unwrap().unwrap(),
            Bytes::from_static(b"0123")
        );
        let error = output.next().await.unwrap().expect_err("source is short");
        assert!(error.to_string().contains("short stream"));
    }
}
