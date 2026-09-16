//! Strict, atomic single-text unified patch application. Headers never become paths.
use openlegal_domain::text_diff::{MAX_LINE_BYTES, MAX_LINES, MAX_TEXT_BYTES, TextDiffError};

const MAX_PATCH: usize = 8 * 1024 * 1024;
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
    let source: Vec<&str> = target.split_inclusive('\n').collect();
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
    let mut out = String::new();
    let mut old_cursor = 0;
    let mut new_cursor: usize = 0;
    let mut hunks = 0;
    while let Some(header) = lines.next() {
        hunks += 1;
        if hunks > MAX_LINES || !header.ends_with('\n') {
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
        if old_start < old_cursor || old_start > source.len() {
            return invalid();
        }
        let gap = old_start - old_cursor;
        if new_cursor.checked_add(gap) != Some(new_start) {
            return invalid();
        }
        if gap > 0 && !out.is_empty() && !out.ends_with('\n') {
            return invalid();
        }
        for text in &source[old_cursor..old_start] {
            append(&mut out, text)?;
        }
        old_cursor = old_start;
        new_cursor = new_start;
        let (mut consumed, mut produced, mut changed) = (0, 0, false);
        while consumed < old_count || produced < new_count {
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
                if consumed >= old_count || source.get(old_cursor).copied() != Some(text) {
                    return invalid();
                }
                old_cursor += 1;
                consumed += 1;
            }
            if prefix != b'-' {
                if produced >= new_count || (!out.is_empty() && !out.ends_with('\n')) {
                    return invalid();
                }
                append(&mut out, text)?;
                new_cursor += 1;
                produced += 1;
            }
            changed |= prefix != b' ';
        }
        if !changed {
            return invalid();
        }
    }
    if hunks == 0 {
        return invalid();
    }
    if old_cursor < source.len() && !out.is_empty() && !out.ends_with('\n') {
        return invalid();
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
    fn rejects_conflicts_and_unsupported_or_incomplete_patches() {
        for patch in [
            "--- a\n+++ b\n@@ -1 +1 @@\n-wrong\n+x\n",
            "--- a\n+++ b\n@@ -2 +1 @@\n-a\n+x\n",
            "--- a\n+++ b\n@@ -1,2 +1 @@\n-a\n+x\n",
            "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+x",
            "--- a\n+++ b\n@@ -1 +1 @@\n a\n",
            "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+\n\\ No newline at end of file\n",
            "--- a\n+++ b\n@@ -1 +1 @@\n-a\n+x\n--- c\n+++ d\n",
            "diff --git a/a b/b\nnew file mode 100644\n--- a\n+++ b\n",
            "--- a\n+++ b\n@@ -18446744073709551616 +1 @@\n-a\n+x\n",
        ] {
            assert!(apply_patch("a\n", patch).is_err(), "{patch}");
        }
    }
}
