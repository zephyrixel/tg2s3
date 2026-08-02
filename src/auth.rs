use anyhow::{Result, anyhow, bail};
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
        self.verify_with_payload_hash(method, uri, headers, None)
    }

    pub fn verify_with_payload_hash(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        payload_hash_override: Option<&str>,
    ) -> Result<()> {
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
            if value("X-Amz-Algorithm").as_deref() != Some("AWS4-HMAC-SHA256") {
                bail!("AccessDenied");
            }
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
        if date.len() != 8
            || !date.bytes().all(|byte| byte.is_ascii_digit())
            || !amz_date.is_ascii()
            || amz_date.len() != 16
            || !amz_date.starts_with(date)
        {
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
            .or(payload_hash_override)
            .unwrap_or_else(|| {
                if presigned {
                    "UNSIGNED-PAYLOAD"
                } else if *method == Method::GET || *method == Method::HEAD {
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                } else {
                    "UNSIGNED-PAYLOAD"
                }
            });
        let scope = format!("{}/{}/{}/aws4_request", date, self.region, "s3");
        let signing_key = signing_key(secret_key, date, &self.region, "s3")?;
        let signature_matches = |payload_hash: &str| -> Result<bool> {
            let canonical_request = format!(
                "{}\n{}\n{}\n{}\n\n{}\n{}",
                method,
                canonical_uri(uri.path()),
                canonical_query(&query, presigned),
                canonical_headers(headers, &signed_headers)?,
                signed_headers,
                payload_hash,
            );
            let string_to_sign = format!(
                "AWS4-HMAC-SHA256\n{}\n{}\n{}",
                amz_date,
                scope,
                hex::encode(Sha256::digest(canonical_request.as_bytes()))
            );
            let expected = hex::encode(hmac_bytes(&signing_key, string_to_sign.as_bytes())?);
            Ok(expected.len() == signature.len()
                && expected.as_bytes().ct_eq(signature.as_bytes()).unwrap_u8() == 1)
        };
        let matches = signature_matches(payload_hash)?;
        let matches_unsigned = !matches
            && payload_hash_override.is_some()
            && !headers.contains_key("x-amz-content-sha256")
            && signature_matches("UNSIGNED-PAYLOAD")?;
        if !matches && !matches_unsigned {
            bail!("SignatureDoesNotMatch");
        }
        Ok(())
    }
}

fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        // S3's AWS SDK signer already uses the escaped request path and
        // disables an additional URI escaping pass.
        path.to_string()
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
    let mut result = Vec::new();
    for name in signed_headers.split(';') {
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty() {
            continue;
        }
        // SigV4 joins repeated header values with commas.
        let mut values = Vec::new();
        for value in headers.get_all(name.as_str()) {
            values.push(collapse_spaces(
                value
                    .to_str()
                    .map_err(|_| anyhow!("Invalid signed header"))?,
            ));
        }
        if values.is_empty() {
            return Err(anyhow!("Signed header missing"));
        }
        result.push(format!("{}:{}", name, values.join(",")));
    }
    Ok(result.join("\n"))
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

    #[test]
    fn verifies_put_signature_with_payload_hash_override() -> Result<()> {
        let method = Method::PUT;
        let uri: Uri = "/A?cors".parse()?;
        let body = br#"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>"#;
        let payload_hash = hex::encode(Sha256::digest(body));
        let mut request = Request::builder()
            .method(method.clone())
            .uri(uri.clone())
            .header("host", "localhost:9000")
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
        let signed_headers = "host;x-amz-date";
        let canonical_request = format!(
            "PUT\n{}\n{}\n{}\n\n{}\n{}",
            canonical_uri(uri.path()),
            canonical_query(
                &form_urlencoded::parse(uri.query().unwrap().as_bytes())
                    .into_owned()
                    .collect::<Vec<_>>(),
                false
            ),
            canonical_headers(request.headers(), signed_headers)?,
            signed_headers,
            payload_hash,
        );
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let key = signing_key("secret", date, "us-east-1", "s3")?;
        let signature = hex::encode(hmac_bytes(&key, string_to_sign.as_bytes())?);
        request.headers_mut().insert(
            "authorization",
            HeaderValue::from_str(&format!(
                "AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"
            ))?,
        );

        let verifier = SigV4 {
            access_key: Some("AKIDEXAMPLE".to_string()),
            secret_key: Some("secret".to_string()),
            region: "us-east-1".to_string(),
            allow_anonymous: false,
        };
        verifier.verify_with_payload_hash(
            request.method(),
            request.uri(),
            request.headers(),
            Some(&payload_hash),
        )?;
        Ok(())
    }

    #[test]
    fn preserves_preescaped_s3_paths() {
        assert_eq!(
            canonical_uri("/A/folder%2Fname%20with%20space"),
            "/A/folder%2Fname%20with%20space"
        );
    }

    #[test]
    fn canonical_request_matches_aws_sdk_v1_layout() -> Result<()> {
        let method = Method::GET;
        let uri: Uri =
            "/A/uploads/1/69371da6-64aa-49d3-9a69-89e15f3361b2_vidu-video-3399804535125746.mp4"
                .parse()?;
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("tgs3.example.test"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
        );
        headers.insert("x-amz-date", HeaderValue::from_static("20260801T162829Z"));
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "{}\n{}\n\n{}\n\n{}\n{}",
            method,
            canonical_uri(uri.path()),
            canonical_headers(&headers, signed_headers)?,
            signed_headers,
            headers.get("x-amz-content-sha256").unwrap().to_str()?,
        );
        assert_eq!(
            hex::encode(Sha256::digest(canonical_request.as_bytes())),
            "6a5cbd56a212306383d0d9e59ec8d8a9e422fd84d445c7f14403d757c3f25b40"
        );
        Ok(())
    }

    #[test]
    fn verifies_s3_presigned_get_with_unsigned_payload() -> Result<()> {
        let method = Method::GET;
        let format =
            time::macros::format_description!("[year][month][day]T[hour][minute][second]Z");
        let amz_date = OffsetDateTime::now_utc().format(format)?;
        let date = &amz_date[..8];
        let credential = format!("AKIDEXAMPLE/{date}/us-east-1/s3/aws4_request");
        let uri: Uri = format!(
            "/A/object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={amz_date}&X-Amz-Expires=3599&X-Amz-SignedHeaders=host&X-Amz-Signature=placeholder",
            aws_encode(&credential),
        )
        .parse()?;
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("localhost:9000"));
        let verifier = SigV4 {
            access_key: Some("AKIDEXAMPLE".to_string()),
            secret_key: Some("secret".to_string()),
            region: "us-east-1".to_string(),
            allow_anonymous: false,
        };
        let query: Vec<(String, String)> =
            form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n\n{}\n{}",
            method,
            canonical_uri(uri.path()),
            canonical_query(&query, true),
            canonical_headers(&headers, "host")?,
            "host",
            "UNSIGNED-PAYLOAD",
        );
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let key = signing_key("secret", date, "us-east-1", "s3")?;
        let signature = hex::encode(hmac_bytes(&key, string_to_sign.as_bytes())?);
        let uri = uri.to_string().replace("placeholder", &signature).parse()?;
        verifier.verify(&method, &uri, &headers)?;
        Ok(())
    }
}
