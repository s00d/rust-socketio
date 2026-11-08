//! Keeps secrets in the query string (tokens, api keys) out of logs.

use url::Url;

/// Query keys whose values are protocol plumbing, never secrets.
const PLAIN_KEYS: [&str; 5] = ["EIO", "transport", "sid", "t", "b64"];

/// `url` for logs: every query value except the engine.io ones replaced by `***`.
pub(crate) fn url(url: &Url, enabled: bool) -> String {
    if !enabled || url.query().is_none() {
        return url.to_string();
    }
    let mut out = url.clone();
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| {
            let v = if PLAIN_KEYS.contains(&k.as_ref()) {
                v.into_owned()
            } else {
                "***".to_owned()
            };
            (k.into_owned(), v)
        })
        .collect();
    out.query_pairs_mut().clear().extend_pairs(pairs);
    // `***` is percent-encoded by the serializer; show it plainly.
    out.to_string().replace("%2A%2A%2A", "***")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hides_every_custom_query_value() {
        let u =
            Url::parse("http://h:1/socket.io/?token=SECRET&version=2.0.0&EIO=4&transport=polling")
                .unwrap();
        let shown = url(&u, true);
        assert_eq!(
            shown,
            "http://h:1/socket.io/?token=***&version=***&EIO=4&transport=polling"
        );
        assert!(url(&u, false).contains("SECRET"));
        let plain = Url::parse("http://h/").unwrap();
        assert_eq!(url(&plain, true), "http://h/");
    }
}
