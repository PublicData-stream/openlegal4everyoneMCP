//! Memory-only attachments share comparison admission accounting and immutable input leases.
use super::*;
use std::sync::atomic::AtomicUsize;

const MAX_ATTACHMENTS: usize = 64;
const ENTRY_OVERHEAD: usize = 1024;

pub(super) struct AttachmentData {
    text: String,
    charge: usize,
    ledger: Arc<AtomicUsize>,
}
impl Drop for AttachmentData {
    fn drop(&mut self) {
        self.ledger.fetch_sub(self.charge, Ordering::Relaxed);
    }
}
pub(super) struct StoredAttachment {
    data: Arc<AttachmentData>,
    summary: AttachmentSummary,
    pub(super) expires: Instant,
}
// Own publication until the awaiting caller actually receives it. Dropping a buffered
// oneshot success after cancellation must release its otherwise undisclosed bearer.
struct AttachmentDelivery {
    service: Arc<TextDiffService>,
    result: Option<ApplyPatchResult>,
}
impl Drop for AttachmentDelivery {
    fn drop(&mut self) {
        if let Some(result) = &self.result {
            let _ = self.service.delete_attachment(&result.result.attachment_id);
        }
    }
}
pub(super) enum ResolvedText {
    Inline(String),
    Attachment(Arc<AttachmentData>),
}
impl AsRef<str> for ResolvedText {
    fn as_ref(&self) -> &str {
        match self {
            Self::Inline(s) => s,
            Self::Attachment(a) => &a.text,
        }
    }
}
impl std::ops::Deref for ResolvedText {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_ref()
    }
}
pub(super) struct ResolvedCompare {
    pub before: ResolvedText,
    pub after: ResolvedText,
    pub before_label: Option<String>,
    pub after_label: Option<String>,
}
impl From<CompareInput> for ResolvedCompare {
    fn from(input: CompareInput) -> Self {
        Self {
            before: ResolvedText::Inline(input.before),
            after: ResolvedText::Inline(input.after),
            before_label: input.before_label,
            after_label: input.after_label,
        }
    }
}
fn used(state: &State) -> usize {
    state.entries.values().map(|e| e.bytes).sum::<usize>()
        + state.reserved
        + state.attachment_bytes.load(Ordering::Relaxed)
}
fn maximum(kind: AttachmentKind) -> usize {
    match kind {
        AttachmentKind::Text => MAX_TEXT_BYTES,
        AttachmentKind::Patch => MAX_PATCH_INPUT_BYTES,
    }
}
fn validate(kind: AttachmentKind, text: &str) -> Result<(), TextDiffError> {
    if text.len() > maximum(kind) || text.contains('\0') {
        return Err(TextDiffError::InvalidInput);
    }
    if kind == AttachmentKind::Text {
        text_info(text, "Text")?;
    }
    Ok(())
}
impl TextDiffService {
    fn fresh_attachment_id(&self, state: &State) -> Result<String, TextDiffError> {
        for _ in 0..4 {
            let bytes = self.handles.generate()?;
            let id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            if !state.attachments.contains_key(&id) && !state.entries.contains_key(&id) {
                return Ok(id);
            }
        }
        Err(TextDiffError::Internal)
    }
    fn resolve(
        &self,
        input: TextSource,
        kind: AttachmentKind,
    ) -> Result<ResolvedText, TextDiffError> {
        let result = match input {
            TextSource::Inline(text) => ResolvedText::Inline(text),
            TextSource::Attachment(handle) => {
                valid_handle(&handle.attachment_id)?;
                let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
                expire(&mut state);
                let entry = state
                    .attachments
                    .get(&handle.attachment_id)
                    .ok_or(TextDiffError::NotFound)?;
                if !entry.summary.sealed || entry.summary.kind != kind {
                    return Err(TextDiffError::InvalidInput);
                }
                ResolvedText::Attachment(entry.data.clone())
            }
        };
        validate(kind, &result)?;
        Ok(result)
    }
    /// Append sequential UTF-8 chunks. Exact replays succeed without extending expiry.
    pub fn upload_attachment(
        &self,
        request: AttachmentUpload,
    ) -> Result<AttachmentSummary, TextDiffError> {
        if request.chunk.len() > MAX_ATTACHMENT_CHUNK_BYTES || request.chunk.contains('\0') {
            return Err(TextDiffError::InvalidInput);
        }
        let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
        expire(&mut state);
        if state.stopping {
            return Err(TextDiffError::Unavailable);
        }
        if let Some(id) = request.attachment_id {
            valid_handle(&id)?;
            if request.kind.is_some() || request.total_bytes.is_some() {
                return Err(TextDiffError::InvalidInput);
            }
            let entry = state
                .attachments
                .get_mut(&id)
                .ok_or(TextDiffError::NotFound)?;
            let end = request
                .offset
                .checked_add(request.chunk.len())
                .ok_or(TextDiffError::InvalidInput)?;
            if end > entry.summary.total_bytes
                || (request.complete && end != entry.summary.total_bytes)
            {
                return Err(TextDiffError::InvalidInput);
            }
            if request.offset < entry.data.text.len() || entry.summary.sealed {
                if entry.data.text.get(request.offset..end) != Some(request.chunk.as_str())
                    || (request.complete && !entry.summary.sealed)
                {
                    return Err(TextDiffError::InvalidInput);
                }
                return Ok(entry.summary.clone());
            }
            if request.offset != entry.data.text.len()
                || (request.chunk.is_empty() && !request.complete)
            {
                return Err(TextDiffError::InvalidInput);
            }
            let data = Arc::get_mut(&mut entry.data).ok_or(TextDiffError::Internal)?;
            // Capacity was reserved at creation; this append cannot grow the allocation.
            data.text.push_str(&request.chunk);
            if request.complete
                && let Err(error) = validate(entry.summary.kind, &data.text)
            {
                data.text.truncate(request.offset);
                return Err(error);
            }
            entry.summary.committed_bytes = end;
            entry.summary.sealed = request.complete;
            return Ok(entry.summary.clone());
        }
        let kind = request.kind.ok_or(TextDiffError::InvalidInput)?;
        let total = request.total_bytes.ok_or(TextDiffError::InvalidInput)?;
        if request.offset != 0
            || total > maximum(kind)
            || request.chunk.len() > total
            || (request.complete && request.chunk.len() != total)
            || (!request.complete && request.chunk.is_empty())
        {
            return Err(TextDiffError::InvalidInput);
        }
        if request.complete {
            validate(kind, &request.chunk)?;
        }
        if state.attachments.len() >= MAX_ATTACHMENTS
            || used(&state).saturating_add(total + ENTRY_OVERHEAD) > MAX_STORE_BYTES
        {
            return Err(TextDiffError::Busy);
        }
        let id = self.fresh_attachment_id(&state)?;
        let mut text = String::new();
        text.try_reserve_exact(total)
            .map_err(|_| TextDiffError::ResourceLimit)?;
        let charge = text.capacity() + ENTRY_OVERHEAD;
        if used(&state).saturating_add(charge) > MAX_STORE_BYTES {
            return Err(TextDiffError::Busy);
        }
        text.push_str(&request.chunk);
        let summary = AttachmentSummary {
            schema_version: 1,
            attachment_id: id.clone(),
            kind,
            total_bytes: total,
            committed_bytes: text.len(),
            sealed: request.complete,
            expires_at: self.clock.now().saturating_add(600),
        };
        state.attachment_bytes.fetch_add(charge, Ordering::Relaxed);
        let data = Arc::new(AttachmentData {
            text,
            charge,
            ledger: state.attachment_bytes.clone(),
        });
        state.attachments.insert(
            id,
            StoredAttachment {
                data,
                summary: summary.clone(),
                expires: Instant::now() + RETENTION,
            },
        );
        Ok(summary)
    }
    pub fn read_attachment(
        &self,
        request: AttachmentRead,
    ) -> Result<AttachmentPage, TextDiffError> {
        valid_handle(&request.attachment_id)?;
        let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
        expire(&mut state);
        let entry = state
            .attachments
            .get(&request.attachment_id)
            .ok_or(TextDiffError::NotFound)?;
        if !entry.summary.sealed || !entry.data.text.is_char_boundary(request.offset) {
            return Err(TextDiffError::InvalidInput);
        }
        let mut end = request
            .offset
            .saturating_add(MAX_ATTACHMENT_CHUNK_BYTES)
            .min(entry.data.text.len());
        while !entry.data.text.is_char_boundary(end) {
            end -= 1;
        }
        Ok(AttachmentPage {
            schema_version: 1,
            attachment: entry.summary.clone(),
            offset: request.offset,
            next_offset: end,
            complete: end == entry.data.text.len(),
            text: entry.data.text[request.offset..end].to_owned(),
        })
    }
    pub fn delete_attachment(&self, id: &str) -> Result<(), TextDiffError> {
        valid_handle(id)?;
        self.state
            .lock()
            .map_err(|_| TextDiffError::Internal)?
            .attachments
            .remove(id);
        Ok(())
    }
    fn publish_attachment(
        &self,
        text: String,
        kind: AttachmentKind,
    ) -> Result<AttachmentSummary, TextDiffError> {
        validate(kind, &text)?;
        let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
        expire(&mut state);
        let charge = text.capacity() + ENTRY_OVERHEAD;
        if state.stopping {
            return Err(TextDiffError::Unavailable);
        }
        if state.attachments.len() >= MAX_ATTACHMENTS
            || used(&state).saturating_add(charge) > MAX_STORE_BYTES
        {
            return Err(TextDiffError::Busy);
        }
        let id = self.fresh_attachment_id(&state)?;
        let summary = AttachmentSummary {
            schema_version: 1,
            attachment_id: id.clone(),
            kind,
            total_bytes: text.len(),
            committed_bytes: text.len(),
            sealed: true,
            expires_at: self.clock.now().saturating_add(600),
        };
        state.attachment_bytes.fetch_add(charge, Ordering::Relaxed);
        let data = Arc::new(AttachmentData {
            text,
            charge,
            ledger: state.attachment_bytes.clone(),
        });
        state.attachments.insert(
            id,
            StoredAttachment {
                data,
                summary: summary.clone(),
                expires: Instant::now() + RETENTION,
            },
        );
        Ok(summary)
    }
    /// Canonical comparison includes one complete exportable patch, independently deletable.
    pub async fn compare_sources(
        self: &Arc<Self>,
        input: DiffInput,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<DiffResult, TextDiffError> {
        let input = ResolvedCompare {
            before: self.resolve(input.before, AttachmentKind::Text)?,
            after: self.resolve(input.after, AttachmentKind::Text)?,
            before_label: input.before_label,
            after_label: input.after_label,
        };
        let comparison = self
            .compare_inner(input, None, cancellation, deadline)
            .await?;
        let exported = (|| {
            let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
            expire(&mut state);
            let stored = state
                .entries
                .get(&comparison.comparison_id)
                .ok_or(TextDiffError::NotFound)?;
            let charge = stored.patch.len() + ENTRY_OVERHEAD;
            if state.attachments.len() >= MAX_ATTACHMENTS
                || used(&state).saturating_add(charge) > MAX_STORE_BYTES
            {
                return Err(TextDiffError::Busy);
            }
            let id = self.fresh_attachment_id(&state)?;
            // Hold the admission lock and charge the immutable copy before allocating it.
            state.attachment_bytes.fetch_add(charge, Ordering::Relaxed);
            let text = stored.patch.clone();
            let summary = AttachmentSummary {
                schema_version: 1,
                attachment_id: id.clone(),
                kind: AttachmentKind::Patch,
                total_bytes: text.len(),
                committed_bytes: text.len(),
                sealed: true,
                expires_at: comparison.expires_at,
            };
            let expires = stored.expires;
            let data = Arc::new(AttachmentData {
                text,
                charge,
                ledger: state.attachment_bytes.clone(),
            });
            state.attachments.insert(
                id,
                StoredAttachment {
                    data,
                    summary: summary.clone(),
                    expires,
                },
            );
            Ok(summary)
        })();
        match exported {
            Ok(patch) => Ok(DiffResult { schema_version: 1, comparison, patch, explanation: "Rust similar uses Myers over LF-inclusive lines with three context lines, then Myers over Unicode scalar values in complete replacement blocks. Whitespace, BOM, CR/LF and final-newline differences are preserved without normalization. Unified output is Git-style; alignment need not match Git and does not establish legal equivalence.".into() }),
            Err(error) => { self.delete(&comparison.comparison_id)?; Err(error) }
        }
    }
    /// Apply in the supervised worker pool and publish only the complete validated result.
    pub async fn apply_patch(
        self: &Arc<Self>,
        input: ApplyPatchInput,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<ApplyPatchResult, TextDiffError> {
        let target = self.resolve(input.target, AttachmentKind::Text)?;
        let patch = self.resolve(input.patch, AttachmentKind::Patch)?;
        if cancellation.is_cancelled() || deadline <= Instant::now() {
            return Err(TextDiffError::Cancelled);
        }
        let (receive, guard, deadline) = {
            let permit = self
                .admission
                .clone()
                .try_acquire_owned()
                .map_err(|_| TextDiffError::Busy)?;
            let mut jobs = self.jobs.lock().map_err(|_| TextDiffError::Internal)?;
            while let Some(result) = jobs.try_join_next() {
                if result.is_err() {
                    self.worker_failed.store(true, Ordering::Relaxed);
                    self.shutdown.cancel();
                    return Err(TextDiffError::Internal);
                }
            }
            {
                let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
                expire(&mut state);
                if state.stopping || self.shutdown.is_cancelled() {
                    return Err(TextDiffError::Unavailable);
                }
                if state.entries.len() + state.reserved_slots >= MAX_ENTRIES
                    || state.attachments.len() >= MAX_ATTACHMENTS
                    || used(&state).saturating_add(JOB_RESERVATION) > MAX_STORE_BYTES
                {
                    return Err(TextDiffError::Busy);
                }
                state.reserved += JOB_RESERVATION;
                state.reserved_slots += 1;
            }
            let reservation = Reservation(self.clone());
            let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
            let token = CancellationToken::new();
            let guard = token.clone().drop_guard();
            let service = self.clone();
            let (send, receive) = oneshot::channel();
            jobs.spawn(async move {
                let _permit = permit;
                let _reservation = reservation;
                let target: Arc<str> = Arc::from(target.as_ref());
                let patch: Arc<str> = Arc::from(patch.as_ref());
                let work = service
                    .engine
                    .apply_patch(target, patch, token.clone(), deadline);
                tokio::pin!(work);
                let result = tokio::select! {
                    biased;
                    _ = service.shutdown.cancelled() => { token.cancel(); work.await }
                    result = &mut work => result,
                }
                .and_then(|text| {
                    if token.is_cancelled()
                        || service.shutdown.is_cancelled()
                        || Instant::now() >= deadline
                    {
                        return Err(TextDiffError::Cancelled);
                    }
                    let info = text_info(&text, "Patched text")?;
                    let result = service.publish_attachment(text, AttachmentKind::Text)?;
                    Ok(AttachmentDelivery {
                        service: service.clone(),
                        result: Some(ApplyPatchResult {
                            schema_version: 1,
                            result,
                            info,
                        }),
                    })
                });
                let _ = send.send(result);
            });
            (receive, guard, deadline)
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(TextDiffError::Cancelled),
            _ = self.shutdown.cancelled() => Err(TextDiffError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => Err(TextDiffError::Cancelled),
            result = receive => result.map_err(|_| TextDiffError::Internal)?,
        };
        if result.is_ok() {
            guard.disarm();
        } else {
            drop(guard);
        }
        result.and_then(|mut delivery| delivery.result.take().ok_or(TextDiffError::Internal))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    struct Handles(AtomicU64);
    impl HandleGenerator for Handles {
        fn generate(&self) -> Result<[u8; 32], TextDiffError> {
            let mut b = [0; 32];
            b[..8].copy_from_slice(&self.0.fetch_add(1, Ordering::Relaxed).to_be_bytes());
            Ok(b)
        }
    }
    struct Engine;
    impl DiffEngine for Engine {
        fn diff(
            &self,
            _: Arc<str>,
            _: Arc<str>,
            _: CancellationToken,
            _: Instant,
        ) -> BoxFuture<'static, Result<ComputedDiff, TextDiffError>> {
            Box::pin(async { Err(TextDiffError::Unavailable) })
        }
        fn apply_patch(
            &self,
            _: Arc<str>,
            _: Arc<str>,
            _: CancellationToken,
            _: Instant,
        ) -> BoxFuture<'static, Result<String, TextDiffError>> {
            Box::pin(async { Ok("한\r\n".into()) })
        }
    }
    fn service() -> Arc<TextDiffService> {
        TextDiffService::new(Arc::new(Engine), Arc::new(Handles(AtomicU64::new(1))))
    }
    fn upload(chunk: &str, total: usize, complete: bool) -> AttachmentUpload {
        AttachmentUpload {
            attachment_id: None,
            kind: Some(AttachmentKind::Text),
            total_bytes: Some(total),
            offset: 0,
            chunk: chunk.into(),
            complete,
        }
    }
    #[test]
    fn sequential_upload_replays_seals_and_preserves_utf8() {
        let s = service();
        let a = s.upload_attachment(upload("한", 5, false)).unwrap();
        assert!(
            s.read_attachment(AttachmentRead {
                attachment_id: a.attachment_id.clone(),
                offset: 0
            })
            .is_err()
        );
        let continuation = AttachmentUpload {
            attachment_id: Some(a.attachment_id.clone()),
            kind: None,
            total_bytes: None,
            offset: 3,
            chunk: "\r\n".into(),
            complete: true,
        };
        let sealed = s.upload_attachment(continuation.clone()).unwrap();
        assert_eq!(
            s.upload_attachment(continuation).unwrap().expires_at,
            a.expires_at
        );
        assert!(sealed.sealed);
        assert_eq!(
            s.read_attachment(AttachmentRead {
                attachment_id: a.attachment_id.clone(),
                offset: 0
            })
            .unwrap()
            .text,
            "한\r\n"
        );
        assert!(
            s.read_attachment(AttachmentRead {
                attachment_id: a.attachment_id.clone(),
                offset: 1
            })
            .is_err()
        );
        let conflict = AttachmentUpload {
            attachment_id: Some(a.attachment_id.clone()),
            kind: None,
            total_bytes: None,
            offset: 3,
            chunk: "!!".into(),
            complete: true,
        };
        assert!(s.upload_attachment(conflict).is_err());
        s.delete_attachment(&a.attachment_id).unwrap();
        s.delete_attachment(&a.attachment_id).unwrap();
        assert!(
            s.read_attachment(AttachmentRead {
                attachment_id: a.attachment_id,
                offset: 0
            })
            .is_err()
        );
    }
    #[test]
    fn leases_remain_charged_after_delete_and_expiry_is_fixed() {
        let s = service();
        let a = s.upload_attachment(upload("x", 1, true)).unwrap();
        let lease = s
            .resolve(
                TextSource::Attachment(AttachmentHandle {
                    attachment_id: a.attachment_id.clone(),
                }),
                AttachmentKind::Text,
            )
            .unwrap();
        s.delete_attachment(&a.attachment_id).unwrap();
        assert!(
            s.state
                .lock()
                .unwrap()
                .attachment_bytes
                .load(Ordering::Relaxed)
                > 0
        );
        drop(lease);
        assert_eq!(
            s.state
                .lock()
                .unwrap()
                .attachment_bytes
                .load(Ordering::Relaxed),
            0
        );
        let a = s.upload_attachment(upload("", 0, true)).unwrap();
        s.state
            .lock()
            .unwrap()
            .attachments
            .get_mut(&a.attachment_id)
            .unwrap()
            .expires = Instant::now();
        assert!(matches!(
            s.read_attachment(AttachmentRead {
                attachment_id: a.attachment_id,
                offset: 0
            }),
            Err(TextDiffError::NotFound)
        ));
    }
    #[test]
    fn resource_and_kind_limits_precede_consumption() {
        let s = service();
        assert!(
            s.upload_attachment(upload("x", MAX_TEXT_BYTES + 1, false))
                .is_err()
        );
        assert!(s.upload_attachment(upload("x", 2, true)).is_err());
        for _ in 0..MAX_ATTACHMENTS {
            s.upload_attachment(upload("", 0, true)).unwrap();
        }
        assert!(matches!(
            s.upload_attachment(upload("", 0, true)),
            Err(TextDiffError::Busy)
        ));
    }
    #[tokio::test]
    async fn application_returns_sealed_complete_result() {
        let s = service();
        let result = s
            .apply_patch(
                ApplyPatchInput {
                    target: TextSource::Inline("a".into()),
                    patch: TextSource::Inline("".into()),
                },
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(result.info.bytes, 5);
        assert_eq!(
            s.read_attachment(AttachmentRead {
                attachment_id: result.result.attachment_id,
                offset: 0
            })
            .unwrap()
            .text,
            "한\r\n"
        );
    }
    #[tokio::test]
    async fn cancelled_or_dropped_buffered_patch_success_releases_attachment() {
        use std::task::{Context, Poll};
        for drop_call in [false, true] {
            let s = service();
            let cancel = CancellationToken::new();
            let mut call = Box::pin(s.apply_patch(
                ApplyPatchInput {
                    target: TextSource::Inline("a".into()),
                    patch: TextSource::Inline("".into()),
                },
                cancel.clone(),
                Instant::now() + Duration::from_secs(5),
            ));
            let mut context = Context::from_waker(futures::task::noop_waker_ref());
            assert!(matches!(call.as_mut().poll(&mut context), Poll::Pending));
            tokio::task::yield_now().await;
            assert!(!s.state.lock().unwrap().attachments.is_empty());
            if drop_call {
                drop(call);
            } else {
                cancel.cancel();
                assert!(matches!(call.await, Err(TextDiffError::Cancelled)));
            }
            assert!(s.state.lock().unwrap().attachments.is_empty());
            assert_eq!(
                s.state
                    .lock()
                    .unwrap()
                    .attachment_bytes
                    .load(Ordering::Relaxed),
                0
            );
        }
    }
}
