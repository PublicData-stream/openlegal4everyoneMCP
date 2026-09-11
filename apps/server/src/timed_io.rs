//! Socket progress deadlines continue to run while a downstream writer is stalled.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Sleep,
};
use tokio_util::sync::CancellationToken;

/// Each private upstream TCP connection has independent idle and response deadlines.
pub(crate) struct ConnectionState {
    pub cancel: CancellationToken,
    active: AtomicUsize,
    last_activity: Mutex<tokio::time::Instant>,
}

impl ConnectionState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel: CancellationToken::new(),
            active: AtomicUsize::new(0),
            last_activity: Mutex::new(tokio::time::Instant::now()),
        })
    }

    pub fn request(self: &Arc<Self>, duration: Duration) -> RequestDeadline {
        self.active.fetch_add(1, Ordering::AcqRel);
        let cancel = self.cancel.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            cancel.cancel();
        });
        RequestDeadline {
            state: self.clone(),
            task,
        }
    }

    pub async fn idle(&self, duration: Duration) {
        loop {
            tokio::time::sleep(duration).await;
            let elapsed = self
                .last_activity
                .lock()
                .map(|last| last.elapsed())
                .unwrap_or(duration);
            if self.active.load(Ordering::Acquire) == 0 && elapsed >= duration {
                return;
            }
        }
    }
}

pub(crate) struct RequestDeadline {
    state: Arc<ConnectionState>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for RequestDeadline {
    fn drop(&mut self) {
        self.task.abort();
        if let Ok(mut last) = self.state.last_activity.lock() {
            *last = tokio::time::Instant::now();
        }
        self.state.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct TimedIo<T> {
    inner: T,
    write_timeout: Duration,
    write_timer: Option<Pin<Box<Sleep>>>,
}

impl<T> TimedIo<T> {
    pub fn new(inner: T, write_timeout: Duration) -> Self {
        Self {
            inner,
            write_timeout,
            write_timer: None,
        }
    }

    fn write_progress<R>(
        &mut self,
        result: Poll<io::Result<R>>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<R>> {
        use std::future::Future;
        if result.is_ready() {
            self.write_timer = None;
            return result;
        }
        let timer = self
            .write_timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(self.write_timeout)));
        if timer.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "downstream write deadline exceeded",
            )))
        } else {
            Poll::Pending
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for TimedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for TimedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.write_progress(result, cx)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        self.write_progress(result, cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_shutdown(cx);
        self.write_progress(result, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    #[tokio::test(start_paused = true)]
    async fn stalled_writer_deadline_releases_owned_work() {
        let (writer, _reader) = tokio::io::duplex(1);
        let budget = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = budget.clone().acquire_owned().await.unwrap();
        let task = tokio::spawn(async move {
            let _permit = permit;
            TimedIo::new(writer, Duration::from_secs(1))
                .write_all(b"blocked")
                .await
        });
        assert_eq!(
            task.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(budget.available_permits(), 1);
    }
}
