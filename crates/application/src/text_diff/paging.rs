//! Validate exact unified patches and page their server-computed scalar highlights.
use super::{ComputedDiff, DiffSide};
use openlegal_domain::text_diff::{
    DiffFragment, InlineChange, MAX_INLINE_RANGES, MAX_LINES, MAX_PAGE_INLINE_RANGES, ScalarRange,
    TextDiffError,
};
use std::collections::BTreeMap;

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
    range_count: usize,
    inline_changes: Vec<InlineChange>,
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
            range_count: 0,
            inline_changes: Vec::new(),
        }
    }
    fn finish(self) -> DiffFragment {
        // An empty-side start identifies the preceding line, not the next row.
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
            inline_changes: self.inline_changes,
        }
    }
}

/// Check worker output against both originals before publishing any part of it.
/// This also verifies unchanged gaps, so a well-formed but incomplete patch fails.
pub(super) fn changes(
    computed: &ComputedDiff,
    old_text: &str,
    new_text: &str,
) -> Result<(Vec<Vec<DiffFragment>>, usize, usize), TextDiffError> {
    if old_text == new_text {
        return if computed.patch.is_empty() && computed.inline_changes.is_empty() {
            Ok((Vec::new(), 0, 0))
        } else {
            Err(TextDiffError::Internal)
        };
    }
    let old_lines: Vec<_> = old_text.split_inclusive('\n').collect();
    let new_lines: Vec<_> = new_text.split_inclusive('\n').collect();
    if computed.inline_changes.len() > MAX_LINES * 2 {
        return Err(TextDiffError::ResourceLimit);
    }
    let mut annotations = BTreeMap::new();
    let mut total_ranges = 0usize;
    for change in &computed.inline_changes {
        total_ranges = total_ranges
            .checked_add(change.ranges.len())
            .ok_or(TextDiffError::ResourceLimit)?;
        if total_ranges > MAX_INLINE_RANGES || change.ranges.len() > MAX_PAGE_INLINE_RANGES {
            return Err(TextDiffError::ResourceLimit);
        }
        let source = match change.side {
            DiffSide::Before => &old_lines,
            DiffSide::After => &new_lines,
        };
        let line = source
            .get(change.line_index as usize)
            .ok_or(TextDiffError::Internal)?;
        let scalar_count = line.chars().count();
        let mut end = 0;
        for &[start, next] in &change.ranges {
            if start < end || start >= next || next as usize > scalar_count {
                return Err(TextDiffError::Internal);
            }
            end = next;
        }
        if annotations
            .insert((change.side, change.line_index as usize), &change.ranges)
            .is_some()
        {
            return Err(TextDiffError::Internal);
        }
    }
    let mut pages = Vec::new();
    let mut page = Vec::new();
    let (mut page_rows, mut page_bytes, mut page_ranges) = (0, 0, 0);
    let mut part: Option<Part> = None;
    let (mut before, mut after, mut old_remaining, mut new_remaining) =
        (0usize, 0usize, 0usize, 0usize);
    let (mut old_cursor, mut new_cursor) = (0usize, 0usize);
    let (mut additions, mut deletions) = (0, 0);
    let mut finish = |part: Part| -> Result<(), TextDiffError> {
        if part.rows == 0 {
            return Err(TextDiffError::Internal);
        }
        let rows = part.rows;
        let ranges = part.range_count;
        let fragment = part.finish();
        let bytes = serde_json::to_vec(&fragment)
            .map_err(|_| TextDiffError::Internal)?
            .len();
        if bytes > PAGE_BYTES || ranges > MAX_PAGE_INLINE_RANGES {
            return Err(TextDiffError::ResourceLimit);
        }
        if !page.is_empty()
            && (page_rows + rows > 400
                || page_bytes + bytes > PAGE_BYTES
                || page_ranges + ranges > MAX_PAGE_INLINE_RANGES)
        {
            pages.push(std::mem::take(&mut page));
            page_rows = 0;
            page_bytes = 0;
            page_ranges = 0;
        }
        page_rows += rows;
        page_bytes += bytes;
        page_ranges += ranges;
        page.push(fragment);
        Ok(())
    };
    let mut lines = computed.patch.split_inclusive('\n').peekable();
    if lines.next() != Some("--- before\n") || lines.next() != Some("+++ after\n") {
        return Err(TextDiffError::Internal);
    }
    let mut saw_hunk = false;
    let mut hunk_changed = false;
    while let Some(line) = lines.next() {
        if line.starts_with("@@ ") {
            if (saw_hunk && !hunk_changed) || old_remaining != 0 || new_remaining != 0 {
                return Err(TextDiffError::Internal);
            }
            if let Some(part) = part.take() {
                finish(part)?;
            }
            saw_hunk = true;
            hunk_changed = false;
            let mut tokens = line.split_ascii_whitespace();
            if tokens.next() != Some("@@") {
                return Err(TextDiffError::Internal);
            }
            let (old, old_count) = range(tokens.next(), '-')?;
            let (new, new_count) = range(tokens.next(), '+')?;
            if tokens.next() != Some("@@") || tokens.next().is_some() {
                return Err(TextDiffError::Internal);
            }
            before = old + usize::from(old_count == 0);
            after = new + usize::from(new_count == 0);
            let old_offset = before.checked_sub(1).ok_or(TextDiffError::Internal)?;
            let new_offset = after.checked_sub(1).ok_or(TextDiffError::Internal)?;
            if old_lines
                .get(old_cursor..old_offset)
                .ok_or(TextDiffError::Internal)?
                != new_lines
                    .get(new_cursor..new_offset)
                    .ok_or(TextDiffError::Internal)?
            {
                return Err(TextDiffError::Internal);
            }
            old_cursor = old_offset;
            new_cursor = new_offset;
            old_remaining = old_count;
            new_remaining = new_count;
            part = Some(Part::new(before, after));
            continue;
        }
        if part.is_none() {
            return Err(TextDiffError::Internal);
        }
        let prefix = line
            .as_bytes()
            .first()
            .copied()
            .ok_or(TextDiffError::Internal)?;
        if !matches!(prefix, b' ' | b'+' | b'-') || !line.ends_with('\n') {
            return Err(TextDiffError::Internal);
        }
        let no_newline = lines
            .peek()
            .is_some_and(|line| *line == "\\ No newline at end of file\n");
        let source_line = if no_newline {
            &line[1..line.len() - 1]
        } else {
            &line[1..]
        };
        if no_newline {
            lines.next();
        }
        if prefix != b'+' && old_lines.get(old_cursor).copied() != Some(source_line) {
            return Err(TextDiffError::Internal);
        }
        if prefix != b'-' && new_lines.get(new_cursor).copied() != Some(source_line) {
            return Err(TextDiffError::Internal);
        }
        let ranges = match prefix {
            b'-' => Some(
                annotations
                    .remove(&(DiffSide::Before, old_cursor))
                    .ok_or(TextDiffError::Internal)?,
            ),
            b'+' => Some(
                annotations
                    .remove(&(DiffSide::After, new_cursor))
                    .ok_or(TextDiffError::Internal)?,
            ),
            _ => None,
        };
        let range_count = ranges.map_or(0, Vec::len);
        // Include all metadata in the conservative packing estimate, then verify
        // the complete serialized fragment in finish(). A marker stays with its row.
        let encoded = serde_json::to_string(line)
            .map_err(|_| TextDiffError::Internal)?
            .len()
            + ranges.map_or(0, |ranges| serialized_highlight_size(ranges))
            + usize::from(no_newline) * 40;
        if part.as_ref().is_some_and(|part| {
            part.rows >= 400
                || part.encoded + encoded + 1024 > PAGE_BYTES
                || part.range_count + range_count > MAX_PAGE_INLINE_RANGES
        }) {
            finish(part.take().ok_or(TextDiffError::Internal)?)?;
            part = Some(Part::new(before, after));
        }
        let current = part.as_mut().ok_or(TextDiffError::Internal)?;
        if let Some(ranges) = ranges {
            current.inline_changes.push(InlineChange {
                row_index: current.rows as u32,
                ranges: ranges.clone(),
            });
        }
        if prefix != b'+' {
            old_remaining = old_remaining
                .checked_sub(1)
                .ok_or(TextDiffError::Internal)?;
            before += 1;
            old_cursor += 1;
            current.before_count += 1;
        }
        if prefix != b'-' {
            new_remaining = new_remaining
                .checked_sub(1)
                .ok_or(TextDiffError::Internal)?;
            after += 1;
            new_cursor += 1;
            current.after_count += 1;
        }
        hunk_changed |= prefix != b' ';
        additions += usize::from(prefix == b'+');
        deletions += usize::from(prefix == b'-');
        current.body.push_str(line);
        if no_newline {
            current.body.push_str("\\ No newline at end of file\n");
        }
        current.rows += 1;
        current.encoded += encoded;
        current.range_count += range_count;
    }
    if !saw_hunk
        || !hunk_changed
        || old_remaining != 0
        || new_remaining != 0
        || !annotations.is_empty()
        || old_lines.get(old_cursor..).ok_or(TextDiffError::Internal)?
            != new_lines.get(new_cursor..).ok_or(TextDiffError::Internal)?
    {
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

fn serialized_highlight_size(ranges: &[ScalarRange]) -> usize {
    // A u32 consumes at most ten decimal bytes. Count punctuation and the row key.
    64 + ranges.len() * 24
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
    if start > MAX_LINES + 1 || count > MAX_LINES {
        return Err(TextDiffError::Internal);
    }
    Ok((start, count))
}

#[cfg(test)]
mod tests {
    use super::super::SourceLineHighlights;
    use super::*;
    fn insertion(text: &str, ranges: Vec<ScalarRange>) -> ComputedDiff {
        let raw: Vec<_> = text.split_inclusive('\n').collect();
        ComputedDiff {
            patch: format!(
                "--- before\n+++ after\n@@ -0,0 +1,{} @@\n{}",
                raw.len(),
                raw.iter()
                    .map(|line| format!(
                        "+{line}{}",
                        if line.ends_with('\n') {
                            ""
                        } else {
                            "\n\\ No newline at end of file\n"
                        }
                    ))
                    .collect::<String>()
            ),
            inline_changes: raw
                .iter()
                .enumerate()
                .map(|(line_index, _)| SourceLineHighlights {
                    side: DiffSide::After,
                    line_index: line_index as u32,
                    ranges: ranges.clone(),
                })
                .collect(),
        }
    }
    #[test]
    fn scalar_offsets_and_missing_newline_are_retained() {
        let text = "한😀\r";
        let computed = insertion(text, vec![[0, 3]]);
        let (pages, added, removed) = changes(&computed, "", text).unwrap();
        let fragment = &pages[0][0];
        assert_eq!(
            (added, removed, fragment.before_start, fragment.after_start),
            (1, 0, 0, 1)
        );
        assert!(
            fragment
                .patch
                .ends_with("+한😀\r\n\\ No newline at end of file\n")
        );
        assert_eq!(fragment.inline_changes[0].ranges, vec![[0, 3]]);
    }
    #[test]
    fn page_rows_and_absolute_coordinates_are_bounded() {
        let text = "x\n".repeat(900);
        let (pages, added, _) = changes(&insertion(&text, vec![[0, 2]]), "", &text).unwrap();
        assert_eq!(pages.len(), 3);
        assert_eq!(added, 900);
        assert_eq!(pages[1][0].after_start, 401);
        assert_eq!(pages[1][0].inline_changes[0].row_index, 0);
    }
    #[test]
    fn malformed_annotations_and_incomplete_patches_fail() {
        let mut computed = insertion("한😀\n", vec![[0, 4]]);
        assert!(changes(&computed, "", "한😀\n").is_err());
        computed.inline_changes[0].ranges = vec![[0, 2], [1, 3]];
        assert!(changes(&computed, "", "한😀\n").is_err());
        computed.inline_changes[0].ranges = vec![[0, 3]];
        computed
            .inline_changes
            .push(computed.inline_changes[0].clone());
        assert!(changes(&computed, "", "한😀\n").is_err());
        computed.inline_changes.clear();
        assert!(changes(&computed, "", "한😀\n").is_err());
        assert!(changes(&ComputedDiff::default(), "a", "b").is_err());
        let text = "한".repeat(20000);
        assert_eq!(chunks(&text).concat(), text);
        assert!(chunks(&text).iter().all(|chunk| chunk.len() <= 32768));
    }
    #[test]
    fn range_limits_split_pages_and_reject_single_rows() {
        let line = "ab".repeat(MAX_PAGE_INLINE_RANGES) + "\n";
        let ranges: Vec<_> = (0..MAX_PAGE_INLINE_RANGES as u32)
            .map(|n| [n * 2, n * 2 + 1])
            .collect();
        let text = line.repeat(2);
        let (pages, _, _) = changes(&insertion(&text, ranges.clone()), "", &text).unwrap();
        assert_eq!(pages.len(), 2);
        let mut excessive = ranges;
        excessive.push([8192, 8193]);
        assert_eq!(
            changes(&insertion(&line, excessive), "", &line).unwrap_err(),
            TextDiffError::ResourceLimit
        );
    }
    #[test]
    fn global_range_limit_and_encoded_pages_include_annotations() {
        let line = "ab".repeat(128) + "\n";
        let ranges: Vec<_> = (0..128).map(|n| [n * 2, n * 2 + 1]).collect();
        let text = line.repeat(512);
        let computed = insertion(&text, ranges.clone());
        let (pages, _, _) = changes(&computed, "", &text).unwrap();
        let mut count = 0;
        for page in pages {
            let n: usize = page
                .iter()
                .flat_map(|f| &f.inline_changes)
                .map(|c| c.ranges.len())
                .sum();
            assert!(n <= MAX_PAGE_INLINE_RANGES);
            assert!(serde_json::to_vec(&page).unwrap().len() <= PAGE_BYTES);
            count += n;
        }
        assert_eq!(count, MAX_INLINE_RANGES);
        let text = text + "x\n";
        let mut computed = insertion(&text, ranges);
        computed.inline_changes.last_mut().unwrap().ranges = vec![[0, 1]];
        assert_eq!(
            changes(&computed, "", &text).unwrap_err(),
            TextDiffError::ResourceLimit
        );
    }
    #[test]
    fn ordered_headers_and_change_bearing_hunks_are_required() {
        let good = insertion("x\n", vec![[0, 2]]);
        for patch in [
            good.patch.replacen("--- before\n", "", 1),
            good.patch
                .replacen("--- before\n+++ after\n", "+++ after\n--- before\n", 1),
            good.patch
                .replacen("--- before\n", "--- before\n--- before\n", 1),
            good.patch.replacen("@@ -0,0", "+++ after\n@@ -0,0", 1),
        ] {
            assert!(
                changes(
                    &ComputedDiff {
                        patch,
                        inline_changes: good.inline_changes.clone()
                    },
                    "",
                    "x\n"
                )
                .is_err()
            );
        }
        let context = ComputedDiff {
            patch: "--- before\n+++ after\n@@ -1,1 +1,1 @@\n x\n".into(),
            inline_changes: Vec::new(),
        };
        assert!(changes(&context, "x\n", "x\n").is_err());
        let extra_context = ComputedDiff {
            patch: "--- before\n+++ after\n@@ -1,1 +1,1 @@\n a\n@@ -2,1 +2,1 @@\n-b\n+c\n".into(),
            inline_changes: vec![
                SourceLineHighlights {
                    side: DiffSide::Before,
                    line_index: 1,
                    ranges: vec![[0, 1]],
                },
                SourceLineHighlights {
                    side: DiffSide::After,
                    line_index: 1,
                    ranges: vec![[0, 1]],
                },
            ],
        };
        assert!(changes(&extra_context, "a\nb\n", "a\nc\n").is_err());
    }
}
