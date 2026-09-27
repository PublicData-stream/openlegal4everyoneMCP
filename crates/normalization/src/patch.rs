//! Strict, atomic single-text unified patch application. Headers never become paths.
use openlegal_domain::text_diff::{MAX_LINE_BYTES, MAX_LINES, MAX_TEXT_BYTES, TextDiffError};

const MAX_PATCH: usize = 8 * 1024 * 1024;
// A successful patch can consume at most MAX_LINES source lines and produce at
// most MAX_LINES result lines; every patch row contributes to at least one.
const MAX_PATCH_ROWS: usize = 2 * MAX_LINES;
const MARKER: &str = "\\ No newline at end of file\n";

fn invalid<T>() -> Result<T, TextDiffError> {
    Err(TextDiffError::InvalidInput)
}
fn append(out: &mut String, text: &str) -> Result<(), TextDiffError> {
    if out.len().saturating_add(text.len()) > MAX_TEXT_BYTES {
        return Err(TextDiffError::ResourceLimit);
    }
    out.push_str(text);
    Ok(())
}
fn validate_text(text: &str) -> Result<(), TextDiffError> {
    if text.len() > MAX_TEXT_BYTES
        || text.contains('\0')
        || text.split_inclusive('\n').count() > MAX_LINES
        || text
            .split_inclusive('\n')
            .any(|line| line.strip_suffix('\n').unwrap_or(line).len() > MAX_LINE_BYTES)
    {
        return invalid();
    }
    Ok(())
}
fn coordinate(token: &str, prefix: char) -> Result<(usize, usize), TextDiffError> {
    let token = token
        .strip_prefix(prefix)
        .ok_or(TextDiffError::InvalidInput)?;
    let (start, count) = token.split_once(',').unwrap_or((token, "1"));
    if start.is_empty()
        || count.is_empty()
        || !start.bytes().all(|b| b.is_ascii_digit())
        || !count.bytes().all(|b| b.is_ascii_digit())
    {
        return invalid();
    }
    let start: usize = start.parse().map_err(|_| TextDiffError::InvalidInput)?;
    let count: usize = count.parse().map_err(|_| TextDiffError::InvalidInput)?;
    if count > MAX_LINES || start > MAX_LINES || (count != 0 && start == 0) {
        return invalid();
    }
    Ok((if count == 0 { start } else { start - 1 }, count))
}

struct PatchRow<'a> {
    prefix: u8,
    text: &'a str,
}

struct PatchHunk<'a> {
    old_start: usize,
    rows: Vec<PatchRow<'a>>,
}

// Validate the entire patch before comparing any row with the target. Otherwise
// an early context mismatch can hide malformed syntax in a later hunk.
fn parse_patch(patch: &str) -> Result<Vec<PatchHunk<'_>>, TextDiffError> {
    let mut lines = patch.split_inclusive('\n').peekable();
    if lines
        .peek()
        .is_some_and(|line| line.starts_with("diff --git "))
    {
        let header = lines.next().ok_or(TextDiffError::InvalidInput)?;
        if !header.ends_with('\n') {
            return invalid();
        }
        if lines.peek().is_some_and(|line| line.starts_with("index ")) {
            lines.next();
        }
    }
    for prefix in ["--- ", "+++ "] {
        let header = lines.next().ok_or(TextDiffError::InvalidInput)?;
        if !header.starts_with(prefix)
            || !header.ends_with('\n')
            || header.len() <= prefix.len() + 1
            || header.len() > 4096
        {
            return invalid();
        }
    }
    let mut hunks = Vec::new();
    let mut old_cursor = 0;
    let mut new_cursor: usize = 0;
    let mut unterminated_output = false;
    let mut total_rows = 0;
    while let Some(header) = lines.next() {
        if hunks.len() >= MAX_LINES || !header.ends_with('\n') {
            return invalid();
        }
        let inner = header
            .strip_prefix("@@ ")
            .ok_or(TextDiffError::InvalidInput)?;
        let (coordinates, suffix) = inner.split_once(" @@").ok_or(TextDiffError::InvalidInput)?;
        if suffix != "\n" && !suffix.starts_with(' ') {
            return invalid();
        }
        let (old, new) = coordinates
            .split_once(' ')
            .ok_or(TextDiffError::InvalidInput)?;
        let (old_start, old_count) = coordinate(old, '-')?;
        let (new_start, new_count) = coordinate(new, '+')?;
        if old_start < old_cursor {
            return invalid();
        }
        let gap = old_start - old_cursor;
        if new_cursor.checked_add(gap) != Some(new_start) || (gap > 0 && unterminated_output) {
            return invalid();
        }
        let (mut consumed, mut produced, mut changed) = (0, 0, false);
        let mut rows = Vec::new();
        while consumed < old_count || produced < new_count {
            if total_rows >= MAX_PATCH_ROWS {
                return Err(TextDiffError::ResourceLimit);
            }
            let row = lines.next().ok_or(TextDiffError::InvalidInput)?;
            let prefix = *row.as_bytes().first().ok_or(TextDiffError::InvalidInput)?;
            if !matches!(prefix, b' ' | b'-' | b'+') || !row.ends_with('\n') {
                return invalid();
            }
            let mut text = &row[1..];
            if lines.peek() == Some(&MARKER) {
                lines.next();
                text = text.strip_suffix('\n').ok_or(TextDiffError::InvalidInput)?;
                if text.is_empty() {
                    return invalid();
                }
            }
            if prefix != b'+' {
                if consumed >= old_count {
                    return invalid();
                }
                consumed += 1;
            }
            if prefix != b'-' {
                if produced >= new_count || unterminated_output {
                    return invalid();
                }
                produced += 1;
                unterminated_output = !text.ends_with('\n');
            }
            changed |= prefix != b' ';
            rows.push(PatchRow { prefix, text });
            total_rows += 1;
        }
        if !changed {
            return invalid();
        }
        old_cursor = old_start + old_count;
        new_cursor = new_start + new_count;
        hunks.push(PatchHunk { old_start, rows });
    }
    if hunks.is_empty() {
        return invalid();
    }
    Ok(hunks)
}

/// Apply exactly at declared positions, preserving every source byte. Empty patches are no-ops.
/// Accept a single optional `diff --git`/`index` preamble and ordinary ---/+++ headers.
/// Renames, modes, binary/combined patches, offsets, fuzz and multi-file patches are unsupported.
pub fn apply_patch(target: &str, patch: &str) -> Result<String, TextDiffError> {
    validate_text(target)?;
    if patch.len() > MAX_PATCH || patch.contains('\0') {
        return invalid();
    }
    if patch.is_empty() {
        return Ok(target.to_owned());
    }
    let hunks = parse_patch(patch)?;
    let source: Vec<&str> = target.split_inclusive('\n').collect();
    let mut out = String::new();
    let mut old_cursor = 0;
    for hunk in hunks {
        let gap = source
            .get(old_cursor..hunk.old_start)
            .ok_or(TextDiffError::PatchConflict)?;
        if !gap.is_empty() && !out.is_empty() && !out.ends_with('\n') {
            return Err(TextDiffError::PatchConflict);
        }
        for text in gap {
            append(&mut out, text)?;
        }
        old_cursor = hunk.old_start;
        for row in hunk.rows {
            if row.prefix != b'+' {
                if source.get(old_cursor).copied() != Some(row.text) {
                    return Err(TextDiffError::PatchConflict);
                }
                old_cursor += 1;
            }
            if row.prefix != b'-' {
                if !out.is_empty() && !out.ends_with('\n') {
                    return Err(TextDiffError::PatchConflict);
                }
                append(&mut out, row.text)?;
            }
        }
    }
    if old_cursor < source.len() && !out.is_empty() && !out.ends_with('\n') {
        return Err(TextDiffError::PatchConflict);
    }
    for text in &source[old_cursor..] {
        append(&mut out, text)?;
    }
    validate_text(&out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_utf8_and_line_endings() {
        let patch = "diff --git a/a b/a\nindex 123..456 100644\n--- a/a\n+++ b/a\n@@ -1,2 +1,2 @@\n \u{feff}한\r\n-끝\n\\ No newline at end of file\n+끝!\n\\ No newline at end of file\n";
        assert_eq!(
            apply_patch("\u{feff}한\r\n끝", patch).unwrap(),
            "\u{feff}한\r\n끝!"
        );
        assert_eq!(
            apply_patch("", "--- before\n+++ after\n@@ -0,0 +1 @@\n+한\n").unwrap(),
            "한\n"
        );
        assert_eq!(
            apply_patch("a\n", "--- before\n+++ after\n@@ -1 +0,0 @@\n-a\n").unwrap(),
            ""
        );
        assert_eq!(apply_patch("x\r", "").unwrap(), "x\r");
    }
    #[test]
    fn distinguishes_target_conflicts_from_patch_syntax() {
        let patch = "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+x\n";
        assert_eq!(
            apply_patch("wrong\n", patch),
            Err(TextDiffError::PatchConflict)
        );
        assert_eq!(apply_patch("", patch), Err(TextDiffError::PatchConflict));
        let later_malformed = "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+x\n@@ -2 +2 @@\n-b\n";
        assert_eq!(
            apply_patch("wrong\n", later_malformed),
            Err(TextDiffError::InvalidInput)
        );
    }
    #[test]
    fn rejects_unsupported_or_incomplete_patches_as_invalid_input() {
        for patch in [
            "--- a\n+++ b\n@@ -1,2 +1 @@\n-a\n+x\n",
            "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+x",
            "--- a\n+++ b\n@@ -1 +1 @@\n a\n",
            "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+\n\\ No newline at end of file\n",
            "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+x\n--- c\n+++ d\n",
            "diff --git a/a b/b\nnew file mode 100644\n--- a\n+++ b\n",
            "--- a\n+++ b\n@@ -18446744073709551616 +1 @@\n-a\n+x\n",
        ] {
            assert_eq!(
                apply_patch("a\n", patch),
                Err(TextDiffError::InvalidInput),
                "{patch}"
            );
        }
    }
    #[test]
    fn preflight_bounds_retained_patch_rows() {
        let mut patch = String::from("--- a\n+++ b\n@@ -1,99999 +1,99999 @@\n");
        patch.push_str(&"-a\n".repeat(99_999));
        patch.push_str(&"+b\n".repeat(99_999));
        patch.push_str("@@ -100000,2 +100000,2 @@\n-a\n-a\n+b\n+b\n");
        assert_eq!(apply_patch("", &patch), Err(TextDiffError::ResourceLimit));
    }
}
