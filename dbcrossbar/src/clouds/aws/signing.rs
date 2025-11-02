//! S3 URL signing.

#[cfg(test)]
mod tests {
    use base64::{prelude::BASE64_STANDARD, Engine};
    use chrono::{DateTime, Utc};
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    use crate::common::*;

    /// Credentials used to access S3.
    struct AwsCredentials {
        /// The value of `AWS_ACCESS_KEY_ID`.
        access_key_id: String,
        /// The value of `AWS_SECRET_ACCESS_KEY`.
        secret_access_key: String,
        /// The value of `AWS_SESSION_TOKEN`.
        session_token: Option<String>,
    }

    /// Sign an `s3://` URL for use with AWS. Returns the signed URL and an optional
    /// value for the `x-amz-security-token` header.
    fn sign_s3_url<'creds>(
        credentials: &'creds AwsCredentials,
        method: &str,
        expires: DateTime<Utc>,
        url: &Url,
    ) -> Result<(Url, &'creds Option<String>)> {
        if url.scheme() != "s3" {
            return Err(format_err!("can't sign non-S3 URL {}", url));
        }
        let host = url
            .host()
            .ok_or_else(|| format_err!("no host in URL {}", url))?;

        let mut mac =
            Hmac::<Sha1>::new_from_slice(credentials.secret_access_key.as_bytes())
                .map_err(|err| format_err!("cannot compute signature: {}", err))?;
        let full_path = format!("/{}{}", host, url.path());
        let payload = format!("{}\n\n\n{}\n{}", method, expires.timestamp(), full_path,);
        mac.update(payload.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());
        let mut signed: Url = format!("https://s3.amazonaws.com{}", full_path).parse()?;
        signed
            .query_pairs_mut()
            .append_pair("AWSAccessKeyId", &credentials.access_key_id)
            .append_pair("Expires", &format!("{}", expires.timestamp()))
            .append_pair("Signature", &signature);
        Ok((signed, &credentials.session_token))
    }

    #[test]
    fn signatures_are_valid() {
        // Example is taken from
        // https://s3.amazonaws.com/doc/s3-developer-guide/RESTAuthentication.html.
        let creds = AwsCredentials {
            access_key_id: "44CF9590006BF252F707".to_owned(),
            secret_access_key: "OtxrzxIsfpFjA7SwPzILwy8Bw21TLhquhboDYROV".to_owned(),
            session_token: None,
        };
        let (signed_url, _x_amz_security_token) = sign_s3_url(
            &creds,
            "GET",
            DateTime::from_timestamp(1_141_889_120, 0).unwrap(),
            &"s3://quotes/nelson".parse().unwrap(),
        )
        .unwrap();
        let expected: Url =
            "https://s3.amazonaws.com/quotes/nelson?AWSAccessKeyId=44CF9590006BF252F707&Expires=1141889120&Signature=vjbyPxybdZaNmGa%2ByT272YEAiv4%3D".parse().unwrap();
        assert_eq!(signed_url, expected);
    }
}
