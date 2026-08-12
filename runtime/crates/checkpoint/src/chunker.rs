//! Split a state blob into fixed-size chunks for parallel upload.

use bytes::Bytes;

/// Split `data` into chunks of `chunk_bytes` (last chunk may be shorter).
/// Zero-copy: each chunk is a ref-counted slice of the input.
pub fn split(data: Bytes, chunk_bytes: u64) -> Vec<Bytes> {
    let chunk_bytes = chunk_bytes.max(1) as usize;
    let mut chunks = Vec::with_capacity(data.len().div_ceil(chunk_bytes));
    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + chunk_bytes).min(data.len());
        chunks.push(data.slice(offset..end));
        offset = end;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_with_short_tail() {
        let data = Bytes::from(vec![7u8; 10]);
        let chunks = split(data, 4);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 4);
        assert_eq!(chunks[1].len(), 4);
        assert_eq!(chunks[2].len(), 2);
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        assert!(split(Bytes::new(), 4).is_empty());
    }

    #[test]
    fn zero_chunk_size_clamped() {
        let chunks = split(Bytes::from_static(b"ab"), 0);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn reassembly_matches_original() {
        let data = Bytes::from((0..=255u8).collect::<Vec<u8>>());
        let chunks = split(data.clone(), 7);
        let mut joined = Vec::new();
        for c in &chunks {
            joined.extend_from_slice(c);
        }
        assert_eq!(&joined[..], &data[..]);
    }
}
