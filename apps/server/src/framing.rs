//! Bounded newline-delimited JSON framing for the custom reliable-byte-stream binding.

use std::{io, sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout, timeout_at},
};

/// A complete frame keeps its allocation charged until its owner finishes using it.
pub struct Frame {
    pub bytes: Vec<u8>,
    pub(crate) permit: OwnedSemaphorePermit,
}

/// Retains partial input and its deadline when the SDK cancels a receive future.
pub struct FrameReader<R> {
    reader: BufReader<R>,
    bytes: Vec<u8>,
    permit: Option<OwnedSemaphorePermit>,
    deadline: Option<Instant>,
    max: usize,
    budget: Arc<Semaphore>,
    idle: Duration,
    completion: Duration,
    read_buffer_permit: Option<OwnedSemaphorePermit>,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(
        reader: R,
        max: usize,
        budget: Arc<Semaphore>,
        idle: Duration,
        completion: Duration,
    ) -> Self {
        let capacity = 8192.min(max.max(1));
        let read_buffer_permit = budget.clone().try_acquire_many_owned(capacity as u32).ok();
        let capacity = if read_buffer_permit.is_some() {
            capacity
        } else {
            0
        };
        Self {
            reader: BufReader::with_capacity(capacity, reader),
            bytes: Vec::new(),
            permit: None,
            deadline: None,
            max,
            budget,
            idle,
            completion,
            read_buffer_permit,
        }
    }

    /// Reads one LF-terminated message. An unterminated trailing frame is an error.
    /// The completion timeout is absolute from the first received byte.
    pub async fn read(&mut self) -> io::Result<Option<Frame>> {
        if self.read_buffer_permit.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "read buffer budget exhausted",
            ));
        }
        loop {
            let deadline = *self
                .deadline
                .get_or_insert_with(|| Instant::now() + self.idle);
            let available = timeout_at(deadline, self.reader.fill_buf())
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "frame deadline exceeded")
                })??;
            if available.is_empty() {
                return if self.bytes.is_empty() {
                    Ok(None)
                } else {
                    Err(invalid("unterminated frame"))
                };
            }
            if self.permit.is_none() {
                let count = u32::try_from(self.max).map_err(|_| invalid("invalid frame limit"))?;
                self.permit = Some(self.budget.clone().try_acquire_many_owned(count).map_err(
                    |_| io::Error::new(io::ErrorKind::OutOfMemory, "frame budget exhausted"),
                )?);
                self.bytes.reserve_exact(self.max);
                self.deadline = Some(Instant::now() + self.completion);
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let length = newline.unwrap_or(available.len());
            if length > self.max.saturating_sub(self.bytes.len()) {
                return Err(invalid("frame exceeds byte limit"));
            }
            self.bytes.extend_from_slice(&available[..length]);
            self.reader.consume(length + usize::from(newline.is_some()));
            if newline.is_some() {
                let mut bytes = std::mem::take(&mut self.bytes);
                if bytes.last() == Some(&b'\r') {
                    bytes.pop();
                }
                self.deadline = None;
                let permit = self
                    .permit
                    .take()
                    .ok_or_else(|| invalid("missing frame budget"))?;
                if bytes.is_empty() {
                    return Err(invalid("empty frame"));
                }
                return Ok(Some(Frame { bytes, permit }));
            }
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct CappedBuffer {
    bytes: Vec<u8>,
    max: usize,
}
impl io::Write for CappedBuffer {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.len() > self.max.saturating_sub(self.bytes.len()) {
            return Err(invalid("output exceeds byte limit"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Serializes under a byte reservation; serialization and socket writes are bounded.
pub async fn write_json<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    max: usize,
    budget: &Arc<Semaphore>,
    deadline: Duration,
) -> io::Result<()> {
    let count = u32::try_from(max).map_err(|_| invalid("invalid output limit"))?;
    let _permit = budget
        .clone()
        .try_acquire_many_owned(count)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "output budget exhausted"))?;
    let mut buffer = CappedBuffer {
        bytes: Vec::with_capacity(max),
        max,
    };
    serde_json::to_writer(&mut buffer, value)
        .map_err(|_| invalid("invalid or oversized output"))?;
    timeout(deadline, async {
        writer.write_all(&buffer.bytes).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "output deadline exceeded"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn fragmentation_coalescing_and_budget_release() {
        let (mut tx, rx) = tokio::io::duplex(32);
        let budget = Arc::new(Semaphore::new(96));
        let mut reader = FrameReader::new(
            rx,
            32,
            budget.clone(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        tx.write_all(b"{\"a\":").await.unwrap();
        assert!(
            timeout(Duration::from_millis(5), reader.read())
                .await
                .is_err()
        );
        tx.write_all(b"1}\n{}\r\n").await.unwrap();
        let first = reader.read().await.unwrap().unwrap();
        assert_eq!(first.bytes, b"{\"a\":1}");
        assert_eq!(budget.available_permits(), 32);
        drop(first);
        assert_eq!(reader.read().await.unwrap().unwrap().bytes, b"{}");
        assert_eq!(budget.available_permits(), 64);
    }

    #[tokio::test]
    async fn rejects_oversize_and_incomplete_eof() {
        for input in [b"12345".as_slice(), b"{}".as_slice()] {
            let mut reader = FrameReader::new(
                input,
                4,
                Arc::new(Semaphore::new(8)),
                Duration::from_secs(1),
                Duration::from_secs(1),
            );
            assert_eq!(
                reader.read().await.err().unwrap().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[tokio::test]
    async fn output_limit_writes_nothing() {
        let mut output = Vec::new();
        assert!(
            write_json(
                &mut output,
                &"oversized",
                4,
                &Arc::new(Semaphore::new(8)),
                Duration::from_secs(1)
            )
            .await
            .is_err()
        );
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn partial_frame_keeps_its_absolute_deadline() {
        let (mut tx, rx) = tokio::io::duplex(32);
        let mut reader = FrameReader::new(
            rx,
            32,
            Arc::new(Semaphore::new(96)),
            Duration::from_secs(1),
            Duration::from_millis(20),
        );
        tx.write_all(b"{").await.unwrap();
        assert!(
            timeout(Duration::from_millis(5), reader.read())
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            reader.read().await.err().unwrap().kind(),
            io::ErrorKind::TimedOut
        );
    }
}
