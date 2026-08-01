use anyhow::{Result, anyhow, bail};
use base64::Engine as _;
use hmac::{Hmac, Mac};
use http::{HeaderMap, Method, Uri};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use subtle::ConstantTimeEq;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset, format_description::FormatItem};
use url::form_urlencoded;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct SigV4 {
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub region: String,
    pub allow_anonymous: bool,
}

impl SigV4 {
    pub fn verify(&self, method: &Method, uri: &Uri, headers: &HeaderMap) -> Result<()> {
        let Some(access_key) = &self.access_key else {
            if self.allow_anonymous {
                return Ok(());
            }
            bail!("AccessDenied");
        };
        let secret_key = self
            .secret_key
            .as_ref()
            .ok_or_else(|| anyhow!("AccessDenied"))?;
        let query: Vec<(String, String)> =
            form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        let presigned = query
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case("X-Amz-Signature"));
        let (credential, signed_headers, signature, amz_date, expires) = if presigned {
            let value = |name: &str| {
                query
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(name))
                    .map(|(_, value)| value.clone())
            };
            let credential = value("X-Amz-Credential").ok_or_else(|| anyhow!("AccessDenied"))?;
            let signed_headers =
                value("X-Amz-SignedHeaders").ok_or_else(|| anyhow!("AccessDenied"))?;
            let signature = value("X-Amz-Signature").ok_or_else(|| anyhow!("AccessDenied"))?;
            let amz_date = value("X-Amz-Date").ok_or_else(|| anyhow!("AccessDenied"))?;
            let expires = value("X-Amz-Expires")
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or_else(|| anyhow!("AccessDenied"))?;
            if !(0..=604_800).contains(&expires) {
                bail!("AccessDenied");
            }
            (
                credential,
                signed_headers,
                signature,
                amz_date,
                Some(expires),
            )
        } else {
            let authorization = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| anyhow!("AccessDenied"))?;
            let (algorithm, fields) = authorization
                .split_once(' ')
                .ok_or_else(|| anyhow!("AccessDenied"))?;
            if algorithm != "AWS4-HMAC-SHA256" {
                bail!("AccessDenied");
            }
            let mut map = BTreeMap::new();
            for field in fields.split(',') {
                let (key, value) = field
                    .trim()
                    .split_once('=')
                    .ok_or_else(|| anyhow!("AccessDenied"))?;
                map.insert(key, value);
            }
            let credential = map
                .get("Credential")
                .ok_or_else(|| anyhow!("AccessDenied"))?
                .to_string();
            let signed_headers = map
                .get("SignedHeaders")
                .ok_or_else(|| anyhow!("AccessDenied"))?
                .to_string();
            let signature = map
                .get("Signature")
                .ok_or_else(|| anyhow!("AccessDenied"))?
                .to_string();
            let amz_date = headers
                .get("x-amz-date")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| anyhow!("AccessDenied"))?
                .to_string();
            (credential, signed_headers, signature, amz_date, None)
        };

        let credential_parts: Vec<&str> = credential.split('/').collect();
        if credential_parts.len() != 5
            || credential_parts[0] != access_key
            || credential_parts[2] != self.region
            || credential_parts[3] != "s3"
            || credential_parts[4] != "aws4_request"
        {
            bail!("AccessDenied");
        }
        let date = credential_parts[1];
        if !amz_date.starts_with(date) {
            bail!("AccessDenied");
        }
        let request_time = parse_amz_date(&amz_date)?;
        let now = OffsetDateTime::now_utc();
        if let Some(expires) = expires {
            if now < request_time || (now - request_time).whole_seconds() > expires {
                bail!("AccessDenied");
            }
        } else if (now - request_time).whole_seconds().abs() > 900 {
            bail!("RequestTimeTooSkewed");
        }

        let payload_hash = headers
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_else(|| {
                if *method == Method::GET || *method == Method::HEAD {
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                } else {
                    "UNSIGNED-PAYLOAD"
                }
            });
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n\n{}\n{}",
            method,
            canonical_uri(uri.path()),
            canonical_query(&query, presigned),
            canonical_headers(headers, &signed_headers)?,
            signed_headers,
            payload_hash,
        );
        let scope = format!("{}/{}/{}/aws4_request", date, self.region, "s3");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = signing_key(secret_key, date, &self.region, "s3")?;
        let expected = hex::encode(hmac_bytes(&signing_key, string_to_sign.as_bytes())?);
        if expected.len() != signature.len()
            || expected.as_bytes().ct_eq(signature.as_bytes()).unwrap_u8() != 1
        {
            bail!("SignatureDoesNotMatch");
        }
        Ok(())
    }
}

fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        aws_encode_path(path)
    }
}

fn canonical_query(query: &[(String, String)], omit_signature: bool) -> String {
    let mut values: Vec<(String, String)> = query
        .iter()
        .filter(|(key, _)| !(omit_signature && key.eq_ignore_ascii_case("X-Amz-Signature")))
        .map(|(key, value)| (aws_encode(key), aws_encode(value)))
        .collect();
    values.sort();
    values
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &HeaderMap, signed_headers: &str) -> Result<String> {
    let mut result = String::new();
    for name in signed_headers.split(';') {
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty() {
            continue;
        }
        let value = headers
            .get(name.as_str())
            .ok_or_else(|| anyhow!("Signed header missing"))?
            .to_str()
            .map_err(|_| anyhow!("Invalid signed header"))?;
        result.push_str(&name);
        result.push(':');
        result.push_str(&collapse_spaces(value));
        result.push('\n');
    }
    Ok(result)
}

fn collapse_spaces(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn aws_encode(value: &str) -> String {
    let mut output = String::new();
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(*byte as char);
        } else {
            output.push('%');
            output.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789ABCDEF"[(byte & 0x0f) as usize]));
        }
    }
    output
}

fn aws_encode_path(value: &str) -> String {
    value
        .split('/')
        .map(aws_encode)
        .collect::<Vec<_>>()
        .join("/")
}

fn parse_amz_date(value: &str) -> Result<OffsetDateTime> {
    static FORMAT: &[FormatItem<'static>] =
        time::macros::format_description!("[year][month][day]T[hour][minute][second]");
    Ok(
        PrimitiveDateTime::parse(value.trim_end_matches('Z'), FORMAT)?
            .assume_utc()
            .to_offset(UtcOffset::UTC),
    )
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Result<Vec<u8>> {
    let date_key = hmac_bytes(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region_key = hmac_bytes(&date_key, region.as_bytes())?;
    let service_key = hmac_bytes(&region_key, service.as_bytes())?;
    hmac_bytes(&service_key, b"aws4_request")
}

fn hmac_bytes(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| anyhow!("invalid HMAC key"))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

#[allow(dead_code)]
fn decode_base64(value: &str) -> Result<Vec<u8>> {
    Ok(base64::engine::general_purpose::STANDARD.decode(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Request};

    #[test]
    fn verifies_header_signature_and_rejects_tampering() -> Result<()> {
        let method = Method::GET;
        let uri: Uri = "/bucket/key?prefix=foo&empty=".parse()?;
        let mut request = Request::builder()
            .method(method.clone())
            .uri(uri.clone())
            .header("host", "localhost:9000")
            .header(
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
            .body(())?;
        let now = OffsetDateTime::now_utc();
        let format =
            time::macros::format_description!("[year][month][day]T[hour][minute][second]Z");
        let amz_date = now.format(format)?;
        request
            .headers_mut()
            .insert("x-amz-date", HeaderValue::from_str(&amz_date)?);
        let date = &amz_date[..8];
        let credential = format!("AKIDEXAMPLE/{date}/us-east-1/s3/aws4_request");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let payload = request
            .headers()
            .get("x-amz-content-sha256")
            .unwrap()
            .to_str()?;
        let canonical_request = format!(
            "GET\n{}\n{}\n{}\n\n{}\n{}",
            canonical_uri(uri.path()),
            canonical_query(
                &form_urlencoded::parse(uri.query().unwrap().as_bytes())
                    .into_owned()
                    .collect::<Vec<_>>(),
                false
            ),
            canonical_headers(request.headers(), signed_headers)?,
            signed_headers,
            payload,
        );
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let key = signing_key("secret", date, "us-east-1", "s3")?;
        let signature = hex::encode(hmac_bytes(&key, string_to_sign.as_bytes())?);
        request.headers_mut().insert("authorization", HeaderValue::from_str(&format!("AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"))?);

        let verifier = SigV4 {
            access_key: Some("AKIDEXAMPLE".to_string()),
            secret_key: Some("secret".to_string()),
            region: "us-east-1".to_string(),
            allow_anonymous: false,
        };
        verifier.verify(request.method(), request.uri(), request.headers())?;
        request
            .headers_mut()
            .insert("x-amz-date", HeaderValue::from_static("20000101T000000Z"));
        assert!(
            verifier
                .verify(request.method(), request.uri(), request.headers())
                .is_err()
        );
        Ok(())
    }
}
