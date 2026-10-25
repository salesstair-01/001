//! Stored key layout.
//!
//! SlateDB compares keys bytewise, so the user key is escaped and terminated
//! the way `internal/encoding.AppendBytes` escapes strings (0x00 becomes
//! 00 FF, the end is 00 01): every user key's versions then form one
//! contiguous run that sorts in user-key order. The inverted commit_ts
//! follows, so newer versions come first, as in the Go owner.
//!
//! The owner's own records (epoch, highest commit_ts, committed transaction
//! ids) live under 00 00, which no escaped user key starts with.

pub const META: &[u8] = &[0x00, 0x00];

/// Smallest stored user-data key.
pub const DATA_START: &[u8] = &[0x00, 0x01];

pub fn meta_key(name: &[u8]) -> Vec<u8> {
    let mut k = META.to_vec();
    k.extend_from_slice(name);
    k
}

fn escape(uk: &[u8], out: &mut Vec<u8>) {
    for &c in uk {
        if c == 0 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(c);
        }
    }
}

/// The start of every version of `uk`.
pub fn versions_start(uk: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(uk.len() + 12);
    escape(uk, &mut k);
    k.extend_from_slice(&[0x00, 0x01]);
    k
}

/// The first key past every version of `uk`.
pub fn versions_end(uk: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(uk.len() + 4);
    escape(uk, &mut k);
    k.extend_from_slice(&[0x00, 0x02]);
    k
}

/// The stored key of `uk` at commit_ts `ts`.
pub fn version_key(uk: &[u8], ts: u64) -> Vec<u8> {
    let mut k = versions_start(uk);
    k.extend_from_slice(&(!ts).to_be_bytes());
    k
}

/// Splits a stored key into its user key and commit_ts.
pub fn split(key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let mut uk = Vec::with_capacity(key.len());
    let mut i = 0;
    while i + 1 < key.len() {
        match (key[i], key[i + 1]) {
            (0x00, 0xFF) => {
                uk.push(0);
                i += 2;
            }
            (0x00, 0x01) => {
                let ts = key.get(i + 2..i + 10)?;
                if key.len() != i + 10 {
                    return None;
                }
                return Some((uk, !u64::from_be_bytes(ts.try_into().ok()?)));
            }
            (0x00, _) => return None,
            (c, _) => {
                uk.push(c);
                i += 1;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_order() {
        let keys: Vec<&[u8]> = vec![
            b"",
            b"\x00",
            b"\x00\x00",
            b"\x00\x01",
            b"a",
            b"a\x00",
            b"a\x00b",
            b"ab",
            b"\xff\xff",
        ];
        for w in keys.windows(2) {
            assert!(w[0] < w[1]);
            // every version of the smaller key sorts before every version of the larger
            assert!(versions_end(w[0]) <= versions_start(w[1]), "{:?} {:?}", w[0], w[1]);
        }
        for k in &keys {
            for ts in [0u64, 1, 7, u64::MAX] {
                let v = version_key(k, ts);
                assert_eq!(split(&v), Some((k.to_vec(), ts)));
                assert!(v.as_slice() >= DATA_START);
                assert!(v.as_slice() > META && !v.starts_with(META));
                assert!(v >= versions_start(k) && v < versions_end(k));
            }
            assert!(version_key(k, 9) < version_key(k, 8), "newer versions first");
        }
    }
}
