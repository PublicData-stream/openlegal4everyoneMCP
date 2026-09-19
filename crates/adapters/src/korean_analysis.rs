//! Bounded dual-engine Korean surface analysis; no runtime dictionary discovery.
use crate::korean_dictionary;
use lindera::{
    dictionary::{DictionaryKind, load_embedded_dictionary},
    mode::Mode,
    segmenter::Segmenter,
};
use mecab_ko::Tokenizer;
use openlegal_domain::legal::DatabaseError as E;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    path::Path,
    sync::{Arc, Mutex, TryLockError},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use unicode_normalization::UnicodeNormalization;

pub const MAX_LINE_BYTES: usize = 65536;
pub const MAX_LINE_SCALARS: usize = 4096;
pub const MAX_RUN_SCALARS: usize = 128;
pub const MAX_FIELD_TOKENS: usize = 262144;
const SLOTS: usize = 4;
const SEMANTICS: &str =
    "ko_lindera5.0.1_ko-dic5.3.0_mecab0.7.2_surface_nfc_ascii_union_not-veto_line4096_run128_v2";

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct AnalyzedText {
    pub lindera: Vec<String>,
    pub mecab: Vec<String>,
}
impl AnalyzedText {
    pub fn clear(&mut self) {
        self.lindera.clear();
        self.mecab.clear();
    }
}

enum Engine {
    Mecab(Box<Tokenizer>),
    #[cfg(test)]
    Fixture,
}

pub struct KoreanAnalyzer {
    lindera: Segmenter,
    slots: Vec<Mutex<Engine>>,
    identity: String,
}

fn budget(deadline: Instant, cancel: &CancellationToken) -> Result<(), E> {
    if cancel.is_cancelled() {
        return Err(E::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(E::Capacity);
    }
    Ok(())
}

/// MeCab's grouped unknown candidates have superlinear work. Its lattice retains
/// whitespace boundaries and the unknown handler stops groups at those boundaries.
/// Reject large runs rather than changing segmentation by splitting the text.
fn validate_line(text: &str) -> Result<(), E> {
    let mut scalars = 0;
    let mut run = 0;
    for c in text.chars() {
        scalars += 1;
        run = if c.is_whitespace() { 0 } else { run + 1 };
        if scalars > MAX_LINE_SCALARS || run > MAX_RUN_SCALARS {
            return Err(E::Capacity);
        }
    }
    Ok(())
}

impl KoreanAnalyzer {
    pub fn open(path: &Path) -> Result<Arc<Self>, E> {
        let digest = korean_dictionary::admit(path)?;
        let mut slots = Vec::with_capacity(SLOTS);
        for _ in 0..SLOTS {
            slots.push(Mutex::new(Engine::Mecab(Box::new(
                Tokenizer::with_dict(path).map_err(|_| E::StorageCorrupt)?,
            ))));
        }
        // Reject changes between admission and loading the independent eager copies.
        if korean_dictionary::admit(path)? != digest {
            return Err(E::StorageCorrupt);
        }
        let dictionary =
            load_embedded_dictionary(DictionaryKind::KoDic).map_err(|_| E::StorageCorrupt)?;
        Ok(Arc::new(Self {
            lindera: Segmenter::new(Mode::Normal, dictionary, None),
            slots,
            identity: format!("{SEMANTICS}:{}:{digest}", korean_dictionary::SOURCE_SHA256),
        }))
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn analyze(
        &self,
        text: &str,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<AnalyzedText, E> {
        budget(deadline, cancel)?;
        let mut slot = None;
        for candidate in &self.slots {
            match candidate.try_lock() {
                Ok(guard) => {
                    slot = Some(guard);
                    break;
                }
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Poisoned(_)) => return Err(E::StorageCorrupt),
            }
        }
        let mut slot = slot.ok_or(E::Capacity)?;
        let mut result = AnalyzedText::default();
        for line in text.split_inclusive('\n') {
            budget(deadline, cancel)?;
            if line.len() > MAX_LINE_BYTES {
                return Err(E::Capacity);
            }
            let normalized = line.nfc().collect::<String>().to_ascii_lowercase();
            if normalized.len() > MAX_LINE_BYTES {
                return Err(E::Capacity);
            }
            validate_line(&normalized)?;
            if normalized.trim().is_empty() {
                continue;
            }
            let lindera: Vec<String> = self
                .lindera
                .segment(Cow::Borrowed(&normalized))
                .map_err(|_| E::StorageCorrupt)?
                .into_iter()
                .filter_map(|t| (!t.surface.trim().is_empty()).then(|| t.surface.into_owned()))
                .collect();
            budget(deadline, cancel)?;
            let mecab: Vec<String> = match &mut *slot {
                Engine::Mecab(tokenizer) => tokenizer
                    .tokenize(&normalized)
                    .into_iter()
                    .filter_map(|t| (!t.surface.trim().is_empty()).then_some(t.surface))
                    .collect(),
                #[cfg(test)]
                Engine::Fixture => lindera.clone(),
            };
            if lindera.is_empty() || mecab.is_empty() {
                return Err(E::StorageCorrupt);
            }
            if result
                .lindera
                .len()
                .saturating_add(result.mecab.len())
                .saturating_add(lindera.len())
                .saturating_add(mecab.len())
                > MAX_FIELD_TOKENS
            {
                return Err(E::Capacity);
            }
            result.lindera.extend(lindera);
            result.mecab.extend(mecab);
            budget(deadline, cancel)?;
        }
        Ok(result)
    }

    /// Fictional paired analyzer for service unit tests. Does not test MeCab morphology.
    #[cfg(test)]
    pub fn fixture() -> Arc<Self> {
        let dictionary =
            load_embedded_dictionary(DictionaryKind::KoDic).expect("embedded fixture dictionary");
        Arc::new(Self {
            lindera: Segmenter::new(Mode::Normal, dictionary, None),
            slots: (0..SLOTS).map(|_| Mutex::new(Engine::Fixture)).collect(),
            identity: format!("{SEMANTICS}:fictional-test-fixture"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn bounds_normalization_and_nonblocking_admission() {
        let analyzer = KoreanAnalyzer::fixture();
        let cancel = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let a = analyzer.analyze("가 ABC", deadline, &cancel).unwrap();
        let b = analyzer.analyze("가 abc", deadline, &cancel).unwrap();
        assert_eq!(a.lindera, b.lindera);
        assert_eq!(a.mecab, b.mecab);
        assert!(
            analyzer
                .analyze(" \n\t", deadline, &cancel)
                .unwrap()
                .mecab
                .is_empty()
        );
        assert_eq!(
            analyzer
                .analyze(&"a".repeat(MAX_LINE_BYTES + 1), deadline, &cancel)
                .unwrap_err(),
            E::Capacity
        );
        let guards: Vec<_> = analyzer.slots.iter().map(|s| s.lock().unwrap()).collect();
        assert_eq!(
            analyzer.analyze("가", deadline, &cancel).unwrap_err(),
            E::Capacity
        );
        drop(guards);
        cancel.cancel();
        assert_eq!(
            analyzer.analyze("가", deadline, &cancel).unwrap_err(),
            E::Cancelled
        );
    }

    #[test]
    #[ignore = "requires the explicitly provisioned full Korean dictionary"]
    fn full_dictionary() {
        let path = std::env::var_os("OPENLEGAL_TEST_MECAB_DICTIONARY")
            .expect("OPENLEGAL_TEST_MECAB_DICTIONARY must name the full artifact");
        let text = "이 법은 국민의 권리와 의무를 정한다.\n".repeat(100);
        let dictionary = load_embedded_dictionary(DictionaryKind::KoDic).unwrap();
        let baseline = Segmenter::new(Mode::Normal, dictionary, None);
        let started = Instant::now();
        let mut baseline_count = 0;
        for _ in 0..10 {
            for line in text.split_inclusive('\n') {
                baseline_count += baseline.segment(Cow::Borrowed(line)).unwrap().len();
            }
        }
        eprintln!(
            "Lindera baseline 1000 fictional lines: {:?}, {baseline_count} tokens",
            started.elapsed()
        );
        for line in std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .filter(|l| l.starts_with("VmRSS:") || l.starts_with("VmHWM:"))
        {
            eprintln!("baseline {line}");
        }
        drop(baseline);
        let started = Instant::now();
        let analyzer = KoreanAnalyzer::open(Path::new(&path)).expect("full dictionary admission");
        eprintln!(
            "four-slot analyzer startup: {:?}; identity {}",
            started.elapsed(),
            analyzer.identity()
        );
        let cancel = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        for text in [
            "대한민국 헌법",
            "개인정보 보호법",
            "제123조의2 제1항",
            "가나다라XYZ 2026",
            "\t국민의 권리\n",
            "헌법 ABC",
        ] {
            let analyzed = analyzer
                .analyze(text, deadline, &cancel)
                .expect("both engines analyze");
            assert!(!analyzed.lindera.is_empty());
            assert!(!analyzed.mecab.is_empty());
            eprintln!("{text:?}: {analyzed:?}");
        }
        let compound = analyzer
            .analyze("개인정보 보호법", deadline, &cancel)
            .unwrap();
        assert_eq!(compound.lindera, ["개인", "정보", "보호법"]);
        assert_eq!(compound.mecab, ["개인정보", "보호법"]);
        let started = Instant::now();
        let boundary = format!(
            "{} {}",
            "a".repeat(MAX_RUN_SCALARS),
            "a".repeat(MAX_RUN_SCALARS)
        );
        let bounded = analyzer.analyze(&boundary, deadline, &cancel).unwrap();
        assert!(!bounded.mecab.is_empty());
        eprintln!(
            "two separated maximum unknown runs: {:?}",
            started.elapsed()
        );
        let started = Instant::now();
        let mut combined_count = 0;
        for _ in 0..10 {
            let analyzed = analyzer.analyze(&text, deadline, &cancel).unwrap();
            combined_count += analyzed.lindera.len() + analyzed.mecab.len();
        }
        eprintln!(
            "dual-engine 1000 fictional lines: {:?}, {combined_count} tokens",
            started.elapsed()
        );
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        for line in status
            .lines()
            .filter(|l| l.starts_with("VmRSS:") || l.starts_with("VmHWM:"))
        {
            eprintln!("{line}");
        }
    }

    #[test]
    fn aggregate_token_budget_is_shared_by_engines() {
        let analyzer = KoreanAnalyzer::fixture();
        let text = "가\n".repeat(MAX_FIELD_TOKENS / 2 + 1);
        assert_eq!(
            analyzer
                .analyze(
                    &text,
                    Instant::now() + Duration::from_secs(60),
                    &CancellationToken::new()
                )
                .unwrap_err(),
            E::Capacity
        );
        assert_eq!(
            analyzer
                .analyze("가", Instant::now(), &CancellationToken::new())
                .unwrap_err(),
            E::Capacity
        );
    }

    #[test]
    fn adversarial_runs_are_rejected_before_engine_work() {
        let analyzer = KoreanAnalyzer::fixture();
        let deadline = Instant::now() + Duration::from_secs(30);
        let cancel = CancellationToken::new();
        for unit in ["a", "1", "!", "가", "😀"] {
            assert_eq!(
                analyzer
                    .analyze(&unit.repeat(MAX_RUN_SCALARS + 1), deadline, &cancel)
                    .unwrap_err(),
                E::Capacity
            );
        }
        assert!(
            analyzer
                .analyze(&"a".repeat(MAX_RUN_SCALARS), deadline, &cancel)
                .is_ok()
        );
        assert!(
            analyzer
                .analyze(
                    &format!(
                        "{} {}",
                        "a".repeat(MAX_RUN_SCALARS),
                        "a".repeat(MAX_RUN_SCALARS)
                    ),
                    deadline,
                    &cancel
                )
                .is_ok()
        );
        assert!(validate_line(&" ".repeat(MAX_LINE_SCALARS)).is_ok());
        assert_eq!(
            validate_line(&" ".repeat(MAX_LINE_SCALARS + 1)).unwrap_err(),
            E::Capacity
        );
    }
}
