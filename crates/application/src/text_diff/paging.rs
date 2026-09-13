//! Pure parsing/paging of the fixed Git unified-patch dialect.
use openlegal_domain::text_diff::{DiffFragment, TextDiffError};

const PAGE_BYTES: usize = 240 * 1024; // reserve wire envelope and JSON field overhead

pub(super) fn chunks(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return vec![""];
    }
    let mut result = Vec::new();
    let mut offset = 0;
    while offset < text.len() {
        let mut end = (offset + 32 * 1024).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        result.push(&text[offset..end]);
        offset = end;
    }
    result
}

struct Part {
    before: usize,
    after: usize,
    before_count: usize,
    after_count: usize,
    body: String,
    rows: usize,
    encoded: usize,
}
impl Part {
    fn new(before: usize, after: usize) -> Self {
        Self {
            before,
            after,
            before_count: 0,
            after_count: 0,
            body: String::new(),
            rows: 0,
            encoded: 0,
        }
    }
    fn finish(self) -> DiffFragment {
        // Git's empty-side start identifies the preceding line, not the next row.
        let before_start = if self.before_count == 0 {
            self.before.saturating_sub(1)
        } else {
            self.before
        };
        let after_start = if self.after_count == 0 {
            self.after.saturating_sub(1)
        } else {
            self.after
        };
        DiffFragment {
            patch: format!(
                "--- before\n+++ after\n@@ -{before_start},{} +{after_start},{} @@\n{}",
                self.before_count, self.after_count, self.body
            ),
            before_start,
            after_start,
            before_count: self.before_count,
            after_count: self.after_count,
        }
    }
}

pub(super) fn changes(
    patch: &str,
) -> Result<(Vec<Vec<DiffFragment>>, usize, usize), TextDiffError> {
    let mut pages = Vec::new();
    let mut page = Vec::new();
    let (mut page_rows, mut page_bytes) = (0, 0);
    let mut part: Option<Part> = None;
    let (mut before, mut after, mut old_remaining, mut new_remaining) =
        (0usize, 0usize, 0usize, 0usize);
    let (mut additions, mut deletions) = (0, 0);
    let mut previous_row = false;
    let mut finish = |part: Part| -> Result<(), TextDiffError> {
        if part.rows == 0 {
            return Ok(());
        }
        let rows = part.rows;
        let fragment = part.finish();
        let bytes = serde_json::to_vec(&fragment)
            .map_err(|_| TextDiffError::Internal)?
            .len();
        if bytes > PAGE_BYTES {
            return Err(TextDiffError::ResourceLimit);
        }
        if !page.is_empty() && (page_rows + rows > 400 || page_bytes + bytes > PAGE_BYTES) {
            pages.push(std::mem::take(&mut page));
            page_rows = 0;
            page_bytes = 0;
        }
        page_rows += rows;
        page_bytes += bytes;
        page.push(fragment);
        Ok(())
    };
    for line in patch.split_inclusive('\n') {
        if line.starts_with("@@ ") {
            if old_remaining != 0 || new_remaining != 0 {
                return Err(TextDiffError::Internal);
            }
            if let Some(part) = part.take() {
                finish(part)?;
            }
            let mut tokens = line.split_ascii_whitespace();
            if tokens.next() != Some("@@") {
                return Err(TextDiffError::Internal);
            }
            let (old, old_count) = range(tokens.next(), '-')?;
            let (new, new_count) = range(tokens.next(), '+')?;
            if tokens.next() != Some("@@") {
                return Err(TextDiffError::Internal);
            }
            before = old + usize::from(old_count == 0);
            after = new + usize::from(new_count == 0);
            old_remaining = old_count;
            new_remaining = new_count;
            part = Some(Part::new(before, after));
            previous_row = false;
            continue;
        }
        if part.is_none() {
            if line.starts_with("diff --git ")
                || line.starts_with("index ")
                || line.starts_with("--- ")
                || line.starts_with("+++ ")
            {
                continue;
            }
            return Err(TextDiffError::Internal);
        }
        if line == "\\ No newline at end of file\n" {
            if !previous_row {
                return Err(TextDiffError::Internal);
            }
            let current = part.as_mut().ok_or(TextDiffError::Internal)?;
            current.body.push_str(line);
            current.encoded += line.len() + 2;
            previous_row = false;
            continue;
        }
        let prefix = line
            .as_bytes()
            .first()
            .copied()
            .ok_or(TextDiffError::Internal)?;
        if !matches!(prefix, b' ' | b'+' | b'-') {
            return Err(TextDiffError::Internal);
        }
        if line.len() > openlegal_domain::text_diff::MAX_LINE_BYTES + 2 {
            return Err(TextDiffError::ResourceLimit);
        }
        let encoded = serde_json::to_string(line)
            .map_err(|_| TextDiffError::Internal)?
            .len();
        if part
            .as_ref()
            .is_some_and(|part| part.rows >= 400 || part.encoded + encoded + 1024 > PAGE_BYTES)
        {
            finish(part.take().ok_or(TextDiffError::Internal)?)?;
            part = Some(Part::new(before, after));
        }
        let current = part.as_mut().ok_or(TextDiffError::Internal)?;
        if prefix != b'+' {
            old_remaining = old_remaining
                .checked_sub(1)
                .ok_or(TextDiffError::Internal)?;
            before += 1;
            current.before_count += 1;
        }
        if prefix != b'-' {
            new_remaining = new_remaining
                .checked_sub(1)
                .ok_or(TextDiffError::Internal)?;
            after += 1;
            current.after_count += 1;
        }
        additions += usize::from(prefix == b'+');
        deletions += usize::from(prefix == b'-');
        current.body.push_str(line);
        current.rows += 1;
        current.encoded += encoded;
        previous_row = true;
    }
    if old_remaining != 0 || new_remaining != 0 {
        return Err(TextDiffError::Internal);
    }
    if let Some(part) = part {
        finish(part)?;
    }
    if !page.is_empty() {
        pages.push(page);
    }
    Ok((pages, additions, deletions))
}

fn range(token: Option<&str>, prefix: char) -> Result<(usize, usize), TextDiffError> {
    let token = token
        .and_then(|value| value.strip_prefix(prefix))
        .ok_or(TextDiffError::Internal)?;
    let (start, count) = token.split_once(',').unwrap_or((token, "1"));
    let start = start
        .parse::<usize>()
        .map_err(|_| TextDiffError::Internal)?;
    let count = count
        .parse::<usize>()
        .map_err(|_| TextDiffError::Internal)?;
    if start > 100_001 || count > 100_001 {
        return Err(TextDiffError::Internal);
    }
    Ok((start, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insertion_coordinates_and_no_newline_are_retained() {
        let (pages, added, removed) =
            changes("--- a\n+++ b\n@@ -0,0 +1,1 @@\n+한글\r\n\\ No newline at end of file\n")
                .unwrap();
        let fragment = &pages[0][0];
        assert_eq!(
            (added, removed, fragment.before_start, fragment.after_start),
            (1, 0, 0, 1)
        );
        assert!(
            fragment
                .patch
                .ends_with("+한글\r\n\\ No newline at end of file\n")
        );
    }
    #[test]
    fn page_rows_and_absolute_coordinates_are_bounded() {
        let patch = format!("@@ -0,0 +1,900 @@\n{}", "+x\n".repeat(900));
        let (pages, added, _) = changes(&patch).unwrap();
        assert_eq!(pages.len(), 3);
        assert_eq!(added, 900);
        assert_eq!(pages[1][0].after_start, 401);
    }
    #[test]
    fn malformed_counts_fail_and_utf8_chunks_round_trip() {
        assert!(changes("@@ -0,0 +1,2 @@\n+x\n").is_err());
        let text = "한".repeat(20000);
        let chunks = chunks(&text);
        assert!(chunks.iter().all(|chunk| chunk.len() <= 32768));
        assert_eq!(chunks.concat(), text);
    }
}
