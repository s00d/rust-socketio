use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::time::{SystemTime, UNIX_EPOCH};

use url::Url;

/// A random `u64` from the std hasher's per-process random keys (no RNG dependency).
pub(crate) fn random_u64() -> u64 {
    RandomState::new().hash_one(SystemTime::now())
}

fn base36(mut n: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    loop {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
        if n == 0 {
            break;
        }
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// The cache-busting `t` value of engine.io-client's `randomString()`: the millisecond clock in
/// base 36 without its first three digits, plus three random base-36 digits.
pub(crate) fn timestamp_param() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();
    let clock = base36(millis);
    let random = base36(36 * 36 * 36 + random_u64() % (36 * 36 * 36));
    format!("{}{}", clock.get(3..).unwrap_or(""), &random[1..])
}

/// `url` with a fresh `t` query parameter.
pub(crate) fn with_timestamp(mut url: Url) -> Url {
    url.query_pairs_mut().append_pair("t", &timestamp_param());
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base36_matches_js_to_string() {
        assert_eq!(base36(0), "0");
        assert_eq!(base36(35), "z");
        assert_eq!(base36(36), "10");
        // (1700000000000).toString(36) in node
        assert_eq!(base36(1_700_000_000_000), "loyw3v28");
    }

    #[test]
    fn timestamp_param_shape() {
        let t = timestamp_param();
        // 8 clock digits today minus 3, plus 3 random digits
        assert_eq!(t.len(), 8);
        assert!(
            t.bytes()
                .all(|b| b.is_ascii_alphanumeric() && !b.is_ascii_uppercase())
        );
        assert_ne!(timestamp_param(), timestamp_param() + "x");
    }

    #[test]
    fn with_timestamp_appends_t() {
        let url = with_timestamp(Url::parse("http://h/engine.io/?EIO=4").unwrap());
        assert!(url.query().unwrap().starts_with("EIO=4&t="));
    }
}
