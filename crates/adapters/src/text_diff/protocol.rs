//! Private version-one framing: magic (including version), BE status/lengths, raw payloads.
use super::compute;
use openlegal_application::text_diff::{ComputedDiff, MAX_PATCH_BYTES};
use openlegal_domain::text_diff::{MAX_TEXT_BYTES, TextDiffError};
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};

const REQUEST_MAGIC: &[u8; 8] = b"OLDIFF01";
const RESPONSE_MAGIC: &[u8; 8] = b"OLDIFR01";
const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn request_header(before: usize, after: usize) -> Result<[u8; 16], TextDiffError> {
    if before > MAX_TEXT_BYTES || after > MAX_TEXT_BYTES {
        return Err(TextDiffError::InvalidInput);
    }
    let mut header = [0; 16];
    header[..8].copy_from_slice(REQUEST_MAGIC);
    header[8..12].copy_from_slice(&(before as u32).to_be_bytes());
    header[12..16].copy_from_slice(&(after as u32).to_be_bytes());
    Ok(header)
}
fn read_u32(reader: &mut impl Read) -> Result<usize, TextDiffError> {
    let mut bytes = [0; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| TextDiffError::InvalidInput)?;
    Ok(u32::from_be_bytes(bytes) as usize)
}
fn request(reader: &mut impl Read) -> Result<(String, String), TextDiffError> {
    let mut magic = [0; 8];
    reader
        .read_exact(&mut magic)
        .map_err(|_| TextDiffError::InvalidInput)?;
    if &magic != REQUEST_MAGIC {
        return Err(TextDiffError::InvalidInput);
    }
    let before_len = read_u32(reader)?;
    let after_len = read_u32(reader)?;
    if before_len > MAX_TEXT_BYTES || after_len > MAX_TEXT_BYTES {
        return Err(TextDiffError::InvalidInput);
    }
    let mut before = vec![0; before_len];
    let mut after = vec![0; after_len];
    reader
        .read_exact(&mut before)
        .map_err(|_| TextDiffError::InvalidInput)?;
    reader
        .read_exact(&mut after)
        .map_err(|_| TextDiffError::InvalidInput)?;
    if reader
        .read(&mut [0; 1])
        .map_err(|_| TextDiffError::InvalidInput)?
        != 0
    {
        return Err(TextDiffError::InvalidInput);
    }
    Ok((
        String::from_utf8(before).map_err(|_| TextDiffError::InvalidInput)?,
        String::from_utf8(after).map_err(|_| TextDiffError::InvalidInput)?,
    ))
}

struct CappedMetadata(Vec<u8>);
impl Write for CappedMetadata {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(buf.len()) > MAX_METADATA_BYTES {
            return Err(std::io::Error::other("metadata limit"));
        }
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn run(mut reader: impl Read, writer: impl Write) -> Result<(), TextDiffError> {
    let result = request(&mut reader).and_then(|(before, after)| compute::compare(&before, &after));
    write_response(result, writer)
}

fn write_response(
    result: Result<ComputedDiff, TextDiffError>,
    mut writer: impl Write,
) -> Result<(), TextDiffError> {
    let result = result.and_then(|diff| {
        let mut metadata = CappedMetadata(Vec::new());
        serde_json::to_writer(&mut metadata, &diff.inline_changes)
            .map_err(|_| TextDiffError::ResourceLimit)?;
        Ok((diff.patch, metadata.0))
    });
    let (status, patch, metadata) = match result {
        Ok((patch, metadata)) => (0u32, patch, metadata),
        Err(TextDiffError::InvalidInput) => (1, String::new(), Vec::new()),
        Err(TextDiffError::ResourceLimit) => (2, String::new(), Vec::new()),
        Err(_) => (3, String::new(), Vec::new()),
    };
    writer
        .write_all(RESPONSE_MAGIC)
        .and_then(|()| writer.write_all(&status.to_be_bytes()))
        .and_then(|()| writer.write_all(&(patch.len() as u32).to_be_bytes()))
        .and_then(|()| writer.write_all(&(metadata.len() as u32).to_be_bytes()))
        .and_then(|()| writer.write_all(patch.as_bytes()))
        .and_then(|()| writer.write_all(&metadata))
        .and_then(|()| writer.flush())
        .map_err(|_| TextDiffError::Unavailable)
}

pub(super) async fn read_response(
    mut reader: impl AsyncRead + Unpin,
) -> Result<ComputedDiff, TextDiffError> {
    let mut header = [0; 20];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|_| TextDiffError::Unavailable)?;
    if &header[..8] != RESPONSE_MAGIC {
        return Err(TextDiffError::Unavailable);
    }
    let status = u32::from_be_bytes(
        header[8..12]
            .try_into()
            .map_err(|_| TextDiffError::Internal)?,
    );
    let patch_len = u32::from_be_bytes(
        header[12..16]
            .try_into()
            .map_err(|_| TextDiffError::Internal)?,
    ) as usize;
    let metadata_len = u32::from_be_bytes(
        header[16..20]
            .try_into()
            .map_err(|_| TextDiffError::Internal)?,
    ) as usize;
    if patch_len > MAX_PATCH_BYTES || metadata_len > MAX_METADATA_BYTES {
        return Err(TextDiffError::ResourceLimit);
    }
    if status > 3 || (status != 0 && (patch_len != 0 || metadata_len != 0)) {
        return Err(TextDiffError::Unavailable);
    }
    let mut patch = vec![0; patch_len];
    let mut metadata = vec![0; metadata_len];
    reader
        .read_exact(&mut patch)
        .await
        .map_err(|_| TextDiffError::Unavailable)?;
    reader
        .read_exact(&mut metadata)
        .await
        .map_err(|_| TextDiffError::Unavailable)?;
    if reader
        .read(&mut [0; 1])
        .await
        .map_err(|_| TextDiffError::Unavailable)?
        != 0
    {
        return Err(TextDiffError::Unavailable);
    }
    match status {
        0 => Ok(ComputedDiff {
            patch: String::from_utf8(patch).map_err(|_| TextDiffError::Unavailable)?,
            inline_changes: serde_json::from_slice(&metadata)
                .map_err(|_| TextDiffError::Unavailable)?,
        }),
        1 => Err(TextDiffError::InvalidInput),
        2 => Err(TextDiffError::ResourceLimit),
        _ => Err(TextDiffError::Internal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn exchange(before: &str, after: &str) -> Vec<u8> {
        let mut request = request_header(before.len(), after.len()).unwrap().to_vec();
        request.extend_from_slice(before.as_bytes());
        request.extend_from_slice(after.as_bytes());
        let mut response = Vec::new();
        run(request.as_slice(), &mut response).unwrap();
        response
    }
    #[tokio::test]
    async fn roundtrip_and_complete_framing() {
        let response = exchange("韓\r\n", "한\n");
        let decoded = read_response(response.as_slice()).await.unwrap();
        assert!(decoded.patch.contains("-韓\r\n+한\n"));
        assert_eq!(decoded.inline_changes.len(), 2);
        let mut trailing = response.clone();
        trailing.push(0);
        assert!(read_response(trailing.as_slice()).await.is_err());
        assert!(
            read_response(&response[..response.len() - 1])
                .await
                .is_err()
        );
        let mut oversized = response.clone();
        oversized[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            read_response(oversized.as_slice()).await,
            Err(TextDiffError::ResourceLimit)
        ));
        let mut wrong = response;
        wrong[7] = b'2';
        assert!(read_response(wrong.as_slice()).await.is_err());
    }
    #[tokio::test]
    async fn malformed_requests_and_invalid_text_return_typed_errors() {
        let mut too_large = request_header(0, 0).unwrap().to_vec();
        too_large[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut trailing = request_header(0, 0).unwrap().to_vec();
        trailing.push(1);
        for request in [Vec::new(), too_large, trailing] {
            let mut response = Vec::new();
            run(request.as_slice(), &mut response).unwrap();
            assert!(matches!(
                read_response(response.as_slice()).await,
                Err(TextDiffError::InvalidInput)
            ));
        }
        assert!(matches!(
            read_response(exchange("\0", "").as_slice()).await,
            Err(TextDiffError::InvalidInput)
        ));
    }
    #[test]
    fn metadata_writer_accepts_exact_limit_without_partial_overflow() {
        let mut writer = CappedMetadata(Vec::new());
        writer.write_all(&vec![b'x'; MAX_METADATA_BYTES]).unwrap();
        assert_eq!(writer.0.len(), MAX_METADATA_BYTES);
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.0.len(), MAX_METADATA_BYTES);
    }

    #[tokio::test]
    async fn metadata_overflow_emits_only_a_typed_resource_failure() {
        use openlegal_application::text_diff::{DiffSide, SourceLineHighlights};
        // Synthetic engine result tests the serialization boundary independently
        // of the computation's separate range and input guards.
        let oversized = ComputedDiff {
            patch: "this must never be emitted".into(),
            inline_changes: vec![
                SourceLineHighlights {
                    side: DiffSide::Before,
                    line_index: 0,
                    ranges: Vec::new(),
                };
                200_000
            ],
        };
        let mut response = Vec::new();
        write_response(Ok(oversized), &mut response).unwrap();
        assert_eq!(response.len(), 20);
        assert_eq!(&response[..8], RESPONSE_MAGIC);
        assert_eq!(&response[8..12], &2u32.to_be_bytes());
        assert_eq!(&response[12..], &[0; 8]);
        assert!(matches!(
            read_response(response.as_slice()).await,
            Err(TextDiffError::ResourceLimit)
        ));
    }
}
