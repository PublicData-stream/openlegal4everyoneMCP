use openlegal_application::text_diff::{
    ComputedDiff, DiffSide, MAX_PATCH_BYTES, SourceLineHighlights, text_info,
};
use openlegal_domain::text_diff::{MAX_INLINE_RANGES, TextDiffError};
use similar::{Algorithm, ChangeTag, DiffTag, TextDiff};

pub(super) fn compare(before: &str, after: &str) -> Result<ComputedDiff, TextDiffError> {
    text_info(before, "Before")?;
    text_info(after, "After")?;
    let before_lines: Vec<&str> = before.split_inclusive('\n').collect();
    let after_lines: Vec<&str> = after.split_inclusive('\n').collect();
    let diff = TextDiff::configure()
        .algorithm(Algorithm::Myers)
        .diff_slices(&before_lines, &after_lines);
    let mut result = ComputedDiff::default();
    let groups = diff.grouped_ops(3);
    if groups.is_empty() {
        return Ok(result);
    }
    append(&mut result.patch, "--- before\n+++ after\n")?;
    for group in groups {
        let first = group.first().ok_or(TextDiffError::Internal)?;
        let last = group.last().ok_or(TextDiffError::Internal)?;
        let old_start = first.old_range().start;
        let new_start = first.new_range().start;
        let old_len = last.old_range().end - old_start;
        let new_len = last.new_range().end - new_start;
        append(
            &mut result.patch,
            &format!(
                "@@ -{} +{} @@\n",
                coordinates(old_start, old_len),
                coordinates(new_start, new_len)
            ),
        )?;
        for op in &group {
            for change in diff.iter_changes(op) {
                let prefix = match change.tag() {
                    ChangeTag::Equal => " ",
                    ChangeTag::Delete => "-",
                    ChangeTag::Insert => "+",
                };
                append(&mut result.patch, prefix)?;
                let value = change.value();
                append(&mut result.patch, value)?;
                if !value.ends_with('\n') {
                    append(&mut result.patch, "\n\\ No newline at end of file\n")?;
                }
            }
        }
    }
    // Derive annotations from complete replacement blocks, before hunk/page splitting.
    let mut index = 0;
    let mut range_count = 0;
    let ops = diff.ops();
    while index < ops.len() {
        if ops[index].tag() == DiffTag::Equal {
            index += 1;
            continue;
        }
        let old_start = ops[index].old_range().start;
        let new_start = ops[index].new_range().start;
        let mut old_end = old_start;
        let mut new_end = new_start;
        while index < ops.len() && ops[index].tag() != DiffTag::Equal {
            old_end = ops[index].old_range().end;
            new_end = ops[index].new_range().end;
            index += 1;
        }
        let old = before_lines[old_start..old_end].concat();
        let new = after_lines[new_start..new_end].concat();
        let chars = TextDiff::configure()
            .algorithm(Algorithm::Myers)
            .diff_chars(&old, &new);
        let mut old_ranges = Vec::new();
        let mut new_ranges = Vec::new();
        for op in chars.ops() {
            if op.tag() != DiffTag::Equal {
                if !op.old_range().is_empty() {
                    old_ranges.push(op.old_range());
                }
                if !op.new_range().is_empty() {
                    new_ranges.push(op.new_range());
                }
            }
        }
        annotate(
            &mut result.inline_changes,
            DiffSide::Before,
            old_start,
            &before_lines[old_start..old_end],
            &old_ranges,
            &mut range_count,
        )?;
        annotate(
            &mut result.inline_changes,
            DiffSide::After,
            new_start,
            &after_lines[new_start..new_end],
            &new_ranges,
            &mut range_count,
        )?;
    }
    Ok(result)
}

fn annotate(
    output: &mut Vec<SourceLineHighlights>,
    side: DiffSide,
    start: usize,
    lines: &[&str],
    changes: &[std::ops::Range<usize>],
    count: &mut usize,
) -> Result<(), TextDiffError> {
    let mut offset = 0;
    let mut range_index = 0;
    for (line_index, line) in lines.iter().enumerate() {
        let end = offset + line.chars().count();
        while range_index < changes.len() && changes[range_index].end <= offset {
            range_index += 1;
        }
        let mut ranges: Vec<[u32; 2]> = Vec::new();
        for change in &changes[range_index..] {
            if change.start >= end {
                break;
            }
            let a = change.start.max(offset) - offset;
            let b = change.end.min(end) - offset;
            if a < b {
                // Adjacent non-equal operations may have touching ranges.
                if let Some(last) = ranges.last_mut().filter(|last| last[1] == a as u32) {
                    last[1] = b as u32;
                } else {
                    *count += 1;
                    if *count > MAX_INLINE_RANGES {
                        return Err(TextDiffError::ResourceLimit);
                    }
                    ranges.push([a as u32, b as u32]);
                }
            }
        }
        output.push(SourceLineHighlights {
            side,
            line_index: (start + line_index) as u32,
            ranges,
        });
        offset = end;
    }
    Ok(())
}
fn coordinates(start: usize, len: usize) -> String {
    let start = if len == 0 { start } else { start + 1 };
    if len == 1 {
        start.to_string()
    } else {
        format!("{start},{len}")
    }
}
fn append(output: &mut String, value: &str) -> Result<(), TextDiffError> {
    if output.len().saturating_add(value.len()) > MAX_PATCH_BYTES {
        return Err(TextDiffError::ResourceLimit);
    }
    output.push_str(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn range_budget_accepts_last_range_and_rejects_the_next() {
        let mut ranges = Vec::new();
        let mut count = MAX_INLINE_RANGES - 1;
        annotate(
            &mut ranges,
            DiffSide::Before,
            0,
            &["a\n"],
            std::slice::from_ref(&(0..2)),
            &mut count,
        )
        .unwrap();
        assert_eq!(count, MAX_INLINE_RANGES);
        assert!(matches!(
            annotate(
                &mut ranges,
                DiffSide::Before,
                1,
                &["b\n"],
                std::slice::from_ref(&(0..2)),
                &mut count
            ),
            Err(TextDiffError::ResourceLimit)
        ));
    }
}
