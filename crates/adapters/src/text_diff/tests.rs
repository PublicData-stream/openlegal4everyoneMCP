use super::*;
use openlegal_application::text_diff::DiffSide;

#[test]
fn exact_line_endings_and_scalar_ranges() {
    for (old, new, expected_old, expected_new) in [
        ("abc\n", "axc\n", vec![[1, 2]], vec![[1, 2]]),
        ("한글\n", "한자\n", vec![[1, 2]], vec![[1, 2]]),
        ("中文日本語\n", "中華日本語\n", vec![[1, 2]], vec![[1, 2]]),
        ("a😀b\n", "a𠀀b\n", vec![[1, 2]], vec![[1, 2]]),
        ("e\u{301}\n", "é\n", vec![[0, 2]], vec![[0, 1]]),
        ("한\r\n", "한\n", vec![[1, 2]], vec![]),
        ("끝", "끝\n", vec![], vec![[1, 2]]),
        ("\u{feff}a\n", "a\n", vec![[0, 1]], vec![]),
        (
            "abcde\n",
            "aXcYe\n",
            vec![[1, 2], [3, 4]],
            vec![[1, 2], [3, 4]],
        ),
    ] {
        let result = compute::compare(old, new).unwrap();
        assert_eq!(result.inline_changes.len(), 2, "{old:?}");
        assert_eq!(result.inline_changes[0].ranges, expected_old, "{old:?}");
        assert_eq!(result.inline_changes[1].ranges, expected_new, "{new:?}");
        assert_eq!(result.inline_changes[0].side, DiffSide::Before);
        assert_eq!(result.inline_changes[1].side, DiffSide::After);
    }
    let result = compute::compare("a\rb", "a\rc").unwrap();
    assert!(
        result
            .patch
            .contains("-a\rb\n\\ No newline at end of file\n+a\rc\n")
    );
    assert_eq!(result.inline_changes[0].ranges, vec![[2, 3]]);
}

#[test]
fn empty_equal_insert_delete_and_context_coordinates() {
    for text in ["", "a", "한\r\n\r", "\u{feff}same\n"] {
        let result = compute::compare(text, text).unwrap();
        assert!(result.patch.is_empty());
        assert!(result.inline_changes.is_empty());
    }
    let insert = compute::compare("", "one\ntwo").unwrap();
    assert!(insert.patch.contains("@@ -0,0 +1,2 @@\n+one\n+two\n"));
    assert_eq!(insert.inline_changes[0].ranges, vec![[0, 4]]);
    assert_eq!(insert.inline_changes[1].ranges, vec![[0, 3]]);
    let delete = compute::compare("one\n", "").unwrap();
    assert!(delete.patch.contains("@@ -1 +0,0 @@\n-one\n"));
    assert_eq!(delete.inline_changes[0].ranges, vec![[0, 4]]);
    let result =
        compute::compare("0\n1\n2\n3\n4\n5\n6\n7\n8\n", "0\n1\n2\n3\nX\n5\n6\n7\n8\n").unwrap();
    assert!(result.patch.contains("@@ -2,7 +2,7 @@"));
    assert!(!result.patch.contains(" 0\n"));
    assert!(!result.patch.contains(" 8\n"));
    assert_eq!(result.inline_changes[0].line_index, 4);
}

#[test]
fn annotations_follow_whole_replacement_across_line_boundaries() {
    let result = compute::compare("ab\ncd\n", "abcd\n").unwrap();
    assert_eq!(result.inline_changes.len(), 3);
    assert_eq!(result.inline_changes[0].ranges, vec![[2, 3]]);
    assert!(result.inline_changes[1].ranges.is_empty());
    assert!(result.inline_changes[2].ranges.is_empty());
}

#[tokio::test]
async fn missing_executable_and_randomness_are_checked() {
    let _fixture_gate = crate::persistent::PROCESS_FIXTURE_GATE.lock().await;
    assert!(
        SimilarDiffEngine::new(Path::new("/nonexistent/openlegal-worker"))
            .await
            .is_err()
    );
    assert_ne!(
        OsHandleGenerator.generate().unwrap(),
        OsHandleGenerator.generate().unwrap()
    );
}

#[cfg(unix)]
fn fake_worker(directory: &Path, command: &str) -> SimilarDiffEngine {
    fake_worker_script(directory, &format!("exec {command}\n"))
}

#[cfg(unix)]
fn fake_worker_script(directory: &Path, body: &str) -> SimilarDiffEngine {
    use std::os::unix::fs::PermissionsExt;
    let executable = directory.join("worker-fixture");
    let script = format!("#!/bin/sh\necho $$ > '{}'/pid\n{body}", directory.display());
    std::fs::write(&executable, script).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    SimilarDiffEngine { executable }
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_and_deadline_reap_worker() {
    let _fixture_gate = crate::persistent::PROCESS_FIXTURE_GATE.lock().await;
    for cancel in [true, false] {
        let fixture = tempfile::tempdir().unwrap();
        let engine = fake_worker(fixture.path(), "/bin/sleep 60");
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let deadline = Instant::now()
            + if cancel {
                Duration::from_secs(5)
            } else {
                Duration::from_millis(150)
            };
        let task = tokio::spawn(async move {
            engine
                .diff(Arc::from("old"), Arc::from("new"), token, deadline)
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !fixture.path().join("pid").exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let pid = std::fs::read_to_string(fixture.path().join("pid")).unwrap();
        if cancel {
            cancellation.cancel();
        }
        assert!(matches!(task.await.unwrap(), Err(TextDiffError::Cancelled)));
        assert!(!Path::new("/proc").join(pid.trim()).exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_output_stderr_overflow_and_crash_reap_worker() {
    let _fixture_gate = crate::persistent::PROCESS_FIXTURE_GATE.lock().await;
    for (command, expected) in [
        ("/usr/bin/yes x", TextDiffError::Unavailable),
        ("/usr/bin/yes x 3>&1 >&2", TextDiffError::ResourceLimit),
        ("/bin/false", TextDiffError::Unavailable),
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let engine = fake_worker(fixture.path(), command);
        let result = engine
            .diff(
                Arc::from("old"),
                Arc::from("new"),
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
            .await;
        assert!(
            matches!(result, Err(error) if error == expected),
            "{command}: {result:?}, expected {expected:?}"
        );
        let pid = std::fs::read_to_string(fixture.path().join("pid")).unwrap();
        assert!(!Path::new("/proc").join(pid.trim()).exists());
    }
}

#[test]
fn repeated_lines_and_multihunk_source_indexes_are_stable() {
    let before = "same\na\nsame\nb\nsame\n1\n2\n3\n4\n5\n6\n7\nsame\nc\n";
    let after = "same\nA\nsame\nb\nsame\n1\n2\n3\n4\n5\n6\n7\nsame\nC\n";
    let first = compute::compare(before, after).unwrap();
    let second = compute::compare(before, after).unwrap();
    assert_eq!(first.patch, second.patch);
    assert_eq!(
        first
            .patch
            .lines()
            .filter(|line| line.starts_with("@@ "))
            .count(),
        2
    );
    assert_eq!(
        first
            .inline_changes
            .iter()
            .map(|line| line.line_index)
            .collect::<Vec<_>>(),
        vec![1, 1, 13, 13]
    );
    assert!(
        first
            .inline_changes
            .iter()
            .all(|line| line.ranges == [[0, 1]])
    );
}

#[cfg(unix)]
#[tokio::test]
async fn oversized_framed_sections_kill_and_reap_running_workers() {
    let _fixture_gate = crate::persistent::PROCESS_FIXTURE_GATE.lock().await;
    for length_offset in [12, 16] {
        // Declare one byte beyond a section's 8 MiB cap, then remain alive with
        // both pipes open. Rejection must use the header without reading a body.
        let fixture = tempfile::tempdir().unwrap();
        let mut header = [0u8; 20];
        header[..8].copy_from_slice(b"OLDIFR01");
        header[length_offset..length_offset + 4]
            .copy_from_slice(&(8u32 * 1024 * 1024 + 1).to_be_bytes());
        let escaped: String = header.iter().map(|byte| format!("\\{byte:03o}")).collect();
        let body = format!("printf '{escaped}'\nexec /bin/sleep 60\n");
        let engine = fake_worker_script(fixture.path(), &body);
        let result = engine
            .diff(
                Arc::from("old"),
                Arc::from("new"),
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
            .await;
        assert!(
            matches!(result, Err(TextDiffError::ResourceLimit)),
            "section at {length_offset}: {result:?}"
        );
        let pid = std::fs::read_to_string(fixture.path().join("pid")).unwrap();
        assert!(!Path::new("/proc").join(pid.trim()).exists());
    }
}
