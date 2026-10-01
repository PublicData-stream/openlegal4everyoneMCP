//! Jurisdictions with a legal-reference profile, named by ISO 3166-1 alpha-3 code.
//! Corpus object identities keep their own provider-chosen `jurisdiction` value; this
//! type maps between the two and never changes stored identities.
use schemars::JsonSchema;
use serde::Serialize;

/// A legal system whose law-name and citation rules are implemented.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, JsonSchema)]
pub enum Jurisdiction {
    /// Republic of Korea.
    #[serde(rename = "KOR")]
    Kor,
}

/// Why a supplied jurisdiction code was not accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JurisdictionError {
    /// Not three ASCII letters.
    Malformed,
    /// A well-formed code without an implemented profile.
    Unsupported,
}

impl Jurisdiction {
    /// Every jurisdiction with an implemented profile.
    pub const SUPPORTED: &'static [Self] = &[Self::Kor];
    /// Used when a caller names no jurisdiction.
    pub const DEFAULT: Self = Self::Kor;

    /// The ISO 3166-1 alpha-3 code.
    pub fn alpha3(self) -> &'static str {
        match self {
            Self::Kor => "KOR",
        }
    }

    /// The `ObjectId::jurisdiction` value of this jurisdiction's corpus records.
    pub fn corpus_code(self) -> &'static str {
        match self {
            Self::Kor => "kr",
        }
    }

    /// The IANA time zone whose calendar date is "today" when a caller gives none.
    pub fn default_timezone(self) -> &'static str {
        match self {
            Self::Kor => "Asia/Seoul",
        }
    }

    /// Parse an alpha-3 code, ignoring case and surrounding whitespace.
    pub fn parse(code: &str) -> Result<Self, JurisdictionError> {
        let code = code.trim();
        if code.len() != 3 || !code.bytes().all(|b| b.is_ascii_alphabetic()) {
            return Err(JurisdictionError::Malformed);
        }
        Self::SUPPORTED
            .iter()
            .copied()
            .find(|j| j.alpha3().eq_ignore_ascii_case(code))
            .ok_or(JurisdictionError::Unsupported)
    }

    /// The profile whose corpus records carry this `ObjectId::jurisdiction` value.
    pub fn from_corpus_code(code: &str) -> Option<Self> {
        Self::SUPPORTED
            .iter()
            .copied()
            .find(|j| j.corpus_code() == code)
    }

    /// Supported alpha-3 codes, for error reports.
    pub fn supported_codes() -> Vec<&'static str> {
        Self::SUPPORTED.iter().map(|j| j.alpha3()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Jurisdiction, JurisdictionError};

    #[test]
    fn alpha3_codes_parse_case_insensitively_and_map_to_corpus_codes() {
        assert_eq!(Jurisdiction::parse(" kor "), Ok(Jurisdiction::Kor));
        assert_eq!(
            Jurisdiction::parse("USA"),
            Err(JurisdictionError::Unsupported)
        );
        for malformed in ["KR", "KORE", "K0R", "", "한국"] {
            assert_eq!(
                Jurisdiction::parse(malformed),
                Err(JurisdictionError::Malformed)
            );
        }
        assert_eq!(Jurisdiction::Kor.corpus_code(), "kr");
        assert_eq!(
            Jurisdiction::from_corpus_code("kr"),
            Some(Jurisdiction::Kor)
        );
        assert_eq!(Jurisdiction::from_corpus_code("us"), None);
        assert_eq!(Jurisdiction::supported_codes(), ["KOR"]);
    }
}
