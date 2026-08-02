use anyhow::{Result, anyhow};
use axum::http::{HeaderMap, Uri, header};

pub(super) struct Target {
    pub(super) bucket: Option<String>,
    pub(super) key: Option<String>,
}

impl Target {
    pub(super) fn parse(uri: &Uri, headers: &HeaderMap, public_host: Option<&str>) -> Result<Self> {
        let host = headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let path = uri.path().trim_start_matches('/');
        if let Some(public_host) = public_host {
            let public_host = public_host.trim_end_matches('/');
            if host != public_host {
                let bucket = host
                    .strip_suffix(public_host)
                    .and_then(|prefix| prefix.strip_suffix('.'))
                    .unwrap_or_default();
                if !bucket.is_empty() {
                    return Ok(Self {
                        bucket: Some(bucket.to_string()),
                        key: if path.is_empty() {
                            None
                        } else {
                            Some(decode_path(path)?)
                        },
                    });
                }
            }
        }
        if path.is_empty() {
            return Ok(Self {
                bucket: None,
                key: None,
            });
        }
        let (bucket, key) = match path.split_once('/') {
            Some((bucket, key)) => (bucket, Some(key)),
            None => (path, None),
        };
        Ok(Self {
            bucket: Some(decode_path(bucket)?),
            key: key.map(decode_path).transpose()?,
        })
    }
}

pub(super) fn decode_path(path: &str) -> Result<String> {
    Ok(percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| anyhow!("InvalidRequest"))?
        .to_string())
}
