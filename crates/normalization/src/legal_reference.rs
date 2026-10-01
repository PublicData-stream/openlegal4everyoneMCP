//! Jurisdiction-independent citation model and the per-jurisdiction reference
//! profiles that parse law names, citations and article locators.
//!
//! Each supported ISO 3166-1 alpha-3 jurisdiction has one profile implementing
//! [`ReferenceProfile`]; [`profile`] selects it. Profiles are pure and bounded: they
//! read supplied text and sections and never consult a provider. Adding a
//! jurisdiction means adding a `Jurisdiction` variant and a module implementing the
//! trait, without changing the tools that call it.
use openlegal_domain::{
    jurisdiction::Jurisdiction,
    legal::{Dataset, LegalSection},
    legal_reference::{ArticleNumber, LawNameResolution},
};

/// Largest citation-check input, in bytes, for every profile.
pub const MAX_CITATION_TEXT_BYTES: usize = 50_000;
/// Largest law name accepted for resolution, in bytes, for every profile.
pub const MAX_LAW_NAME_BYTES: usize = 512;

/// Law-name, citation and article rules of one jurisdiction.
pub trait ReferenceProfile: Send + Sync {
    fn jurisdiction(&self) -> Jurisdiction;
    /// Normalize a supplied name and expand any known abbreviation.
    fn resolve_law_name(&self, input: &str) -> LawNameResolution;
    /// The comparison key of a title; equal keys name the same title.
    fn name_key(&self, value: &str) -> String;
    /// An anchored `database.rg` pattern matching titles with this name's key.
    fn title_pattern(&self, name: &str) -> String;
    /// The parent law's name of a subordinate instrument's name, or the name itself.
    fn base_law_name<'a>(&self, name: &'a str) -> &'a str;
    /// Parse a supplied article locator.
    fn parse_article_number(&self, value: &str) -> Option<ArticleNumber>;
    /// Format an article locator in this jurisdiction's citation style.
    fn format_article(&self, article: ArticleNumber) -> String;
    fn locate_article<'a>(
        &self,
        sections: &'a [LegalSection],
        wanted: ArticleNumber,
    ) -> ArticleLookup<'a>;
    fn has_subparagraph(
        &self,
        article: &LocatedArticle<'_>,
        paragraph: Option<u32>,
        wanted: u32,
    ) -> bool;
    /// Similarity (0 to 100) of a cited article title to the retained one.
    fn title_similarity(&self, cited: &str, retained: &str) -> Option<u8>;
    /// Statute citations and case numbers in supplied text, bounded per call.
    fn extract_citations(&self, text: &str) -> Extraction;
    /// Datasets searched for statute names by default.
    fn statute_datasets(&self) -> &'static [Dataset];
    /// Datasets whose `case_number` sections hold this jurisdiction's case numbers.
    fn case_datasets(&self) -> &'static [Dataset];
}

/// The profile implementing `jurisdiction`.
pub fn profile(jurisdiction: Jurisdiction) -> &'static dyn ReferenceProfile {
    match jurisdiction {
        Jurisdiction::Kor => &crate::kr_legal_reference::KOREA,
    }
}

/// An article found in a capture's provider sections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocatedArticle<'a> {
    pub section_id: &'a str,
    pub number: ArticleNumber,
    pub title: String,
    pub text: &'a str,
    pub deleted: bool,
    /// Paragraph numbers in order of appearance; empty when unnumbered.
    pub paragraphs: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArticleLookup<'a> {
    Found(LocatedArticle<'a>),
    NotFound {
        first: Option<ArticleNumber>,
        last: Option<ArticleNumber>,
    },
}

/// How an extracted citation names its law.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LawReference {
    /// Candidate names ending at the article, longest first, with their byte starts.
    Named {
        candidates: Vec<(String, usize)>,
        /// The word before the article is a generic word (in Korean, `법` or `시행령`).
        generic_tail: bool,
    },
    /// A reference to the previous citation's law (in Korean, `같은 법` or `동법`),
    /// optionally naming a subordinate instrument of it.
    Same {
        suffix: Option<&'static str>,
        start: usize,
    },
    /// A bare article joined to the previous citation by a connector.
    Continued,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractedStatute {
    pub law: LawReference,
    pub article: ArticleNumber,
    pub article_start: usize,
    pub byte_end: usize,
    pub paragraph: Option<u32>,
    pub subparagraph: Option<u32>,
    pub cited_title: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractedCase {
    pub byte_start: usize,
    pub byte_end: usize,
    pub case_number: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Extraction {
    pub statutes: Vec<ExtractedStatute>,
    pub cases: Vec<ExtractedCase>,
    pub truncated: bool,
}

/// Escape a literal for the Rust regex syntax used by `database.rg`.
pub fn push_escaped(out: &mut String, c: char) {
    if "\\.+*?()|[]{}^$#&-~".contains(c) {
        out.push('\\');
    }
    out.push(c);
}

#[cfg(test)]
mod tests {
    use super::profile;
    use openlegal_domain::jurisdiction::Jurisdiction;

    #[test]
    fn every_supported_jurisdiction_has_its_own_profile() {
        for &jurisdiction in Jurisdiction::SUPPORTED {
            assert_eq!(profile(jurisdiction).jurisdiction(), jurisdiction);
        }
        let korea = profile(Jurisdiction::Kor);
        assert_eq!(korea.resolve_law_name("산안법").resolved, "산업안전보건법");
        let article = korea.parse_article_number("44의2").unwrap();
        assert_eq!(korea.format_article(article), "제44조의2");
    }
}
