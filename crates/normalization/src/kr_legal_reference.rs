//! Korean law-name aliases, citation extraction and article location.
//!
//! These functions are pure and bounded: they read supplied text and sections and
//! never consult a provider. Callers decide what a parsed reference resolves to.
//! The alias table lists widely used abbreviations of official titles; it was
//! compiled for this repository after reviewing the feature set of korean-law-mcp
//! (MIT), without copying that project's table or code.
use openlegal_domain::{
    legal::LegalSection,
    legal_reference::{ArticleNumber, LawNameResolution},
};

/// Largest citation-check input, in bytes.
pub const MAX_CITATION_TEXT_BYTES: usize = 50_000;
/// Statute citations checked per call; later ones are reported as truncated.
pub const MAX_STATUTE_CITATIONS: usize = 50;
/// Distinct case numbers checked per call.
pub const MAX_CASE_CITATIONS: usize = 30;
/// Largest law name accepted for resolution, in bytes.
pub const MAX_LAW_NAME_BYTES: usize = 512;

const MIDDLE_DOTS: [char; 7] = ['·', 'ㆍ', '‧', '•', '・', '･', '∙'];
const OFFICIAL_DOT: char = 'ㆍ';
const SUFFIXES: [&str; 2] = ["시행규칙", "시행령"];
const MAX_NAME_WORDS: usize = 8;

/// Official title and common abbreviations. Titles use the provider's spacing and `ㆍ`.
const LAW_ALIASES: &[(&str, &[&str])] = &[
    ("대한민국헌법", &["헌법"]),
    ("민사소송법", &["민소법"]),
    ("형사소송법", &["형소법"]),
    ("민사집행법", &["민집법"]),
    ("행정소송법", &["행소법"]),
    (
        "채무자 회생 및 파산에 관한 법률",
        &["채무자회생법", "통합도산법"],
    ),
    ("근로기준법", &["근기법"]),
    ("산업안전보건법", &["산안법"]),
    (
        "산업안전보건기준에 관한 규칙",
        &["산안기준규칙", "안전보건규칙", "산업안전보건기준규칙"],
    ),
    (
        "중대재해 처벌 등에 관한 법률",
        &["중대재해처벌법", "중처법", "중대재해법"],
    ),
    ("산업재해보상보험법", &["산재보험법", "산재법"]),
    ("고용보험법", &["고보법"]),
    ("근로자퇴직급여 보장법", &["퇴직급여법", "근퇴법"]),
    (
        "노동조합 및 노동관계조정법",
        &["노동조합법", "노조법", "노조법"],
    ),
    (
        "파견근로자 보호 등에 관한 법률",
        &["파견법", "파견근로자법"],
    ),
    ("기간제 및 단시간근로자 보호 등에 관한 법률", &["기간제법"]),
    (
        "남녀고용평등과 일ㆍ가정 양립 지원에 관한 법률",
        &["남녀고용평등법", "고평법"],
    ),
    (
        "개인정보 보호법",
        &["개인정보보호법", "개보법", "개인정보법"],
    ),
    (
        "정보통신망 이용촉진 및 정보보호 등에 관한 법률",
        &["정보통신망법", "정통망법"],
    ),
    (
        "신용정보의 이용 및 보호에 관한 법률",
        &["신용정보법", "신정법"],
    ),
    ("위치정보의 보호 및 이용 등에 관한 법률", &["위치정보법"]),
    (
        "인공지능 발전과 신뢰 기반 조성 등에 관한 기본법",
        &["인공지능기본법", "AI기본법"],
    ),
    ("공공기관의 정보공개에 관한 법률", &["정보공개법"]),
    (
        "부정청탁 및 금품등 수수의 금지에 관한 법률",
        &["청탁금지법", "김영란법"],
    ),
    ("공직자의 이해충돌 방지법", &["이해충돌방지법"]),
    ("국가를 당사자로 하는 계약에 관한 법률", &["국가계약법"]),
    (
        "지방자치단체를 당사자로 하는 계약에 관한 법률",
        &["지방계약법"],
    ),
    ("공공기관의 운영에 관한 법률", &["공공기관운영법"]),
    ("독점규제 및 공정거래에 관한 법률", &["공정거래법"]),
    ("하도급거래 공정화에 관한 법률", &["하도급법"]),
    ("약관의 규제에 관한 법률", &["약관법", "약관규제법"]),
    ("표시ㆍ광고의 공정화에 관한 법률", &["표시광고법"]),
    ("가맹사업거래의 공정화에 관한 법률", &["가맹사업법"]),
    (
        "전자상거래 등에서의 소비자보호에 관한 법률",
        &["전자상거래법"],
    ),
    (
        "부정경쟁방지 및 영업비밀보호에 관한 법률",
        &["부정경쟁방지법"],
    ),
    ("자본시장과 금융투자업에 관한 법률", &["자본시장법"]),
    (
        "금융소비자 보호에 관한 법률",
        &["금융소비자보호법", "금소법"],
    ),
    (
        "특정 금융거래정보의 보고 및 이용 등에 관한 법률",
        &["특정금융정보법", "특금법"],
    ),
    ("전자금융거래법", &["전금법"]),
    ("주택임대차보호법", &["주임법"]),
    ("상가건물 임대차보호법", &["상가임대차법", "상임법"]),
    ("집합건물의 소유 및 관리에 관한 법률", &["집합건물법"]),
    ("부동산 거래신고 등에 관한 법률", &["부동산거래신고법"]),
    ("국토의 계획 및 이용에 관한 법률", &["국토계획법"]),
    ("도시 및 주거환경정비법", &["도시정비법", "도정법"]),
    (
        "공익사업을 위한 토지 등의 취득 및 보상에 관한 법률",
        &["토지보상법"],
    ),
    ("건설산업기본법", &["건산법"]),
    ("소방시설 설치 및 관리에 관한 법률", &["소방시설법"]),
    ("화학물질관리법", &["화관법"]),
    ("화학물질의 등록 및 평가 등에 관한 법률", &["화평법"]),
    ("감염병의 예방 및 관리에 관한 법률", &["감염병예방법"]),
    ("마약류 관리에 관한 법률", &["마약류관리법"]),
    ("국민건강보험법", &["건보법"]),
    ("국민기초생활 보장법", &["기초생활보장법"]),
    (
        "장애인차별금지 및 권리구제 등에 관한 법률",
        &["장애인차별금지법"],
    ),
    ("국세기본법", &["국기법"]),
    ("부가가치세법", &["부가세법"]),
    ("조세특례제한법", &["조특법"]),
    ("지방세특례제한법", &["지특법"]),
    (
        "자유무역협정의 이행을 위한 관세법의 특례에 관한 법률",
        &["FTA관세특례법", "FTA특례법"],
    ),
    ("도로교통법", &["도교법"]),
    ("교통사고처리 특례법", &["교특법"]),
    ("자동차손해배상 보장법", &["자배법"]),
    ("여객자동차 운수사업법", &["여객자동차법"]),
    ("화물자동차 운수사업법", &["화물자동차법"]),
    (
        "특정범죄 가중처벌 등에 관한 법률",
        &["특정범죄가중법", "특가법"],
    ),
    (
        "특정경제범죄 가중처벌 등에 관한 법률",
        &["특정경제범죄법", "특경법"],
    ),
    ("성폭력범죄의 처벌 등에 관한 특례법", &["성폭력처벌법"]),
    (
        "아동ㆍ청소년의 성보호에 관한 법률",
        &["청소년성보호법", "아청법"],
    ),
    ("아동학대범죄의 처벌 등에 관한 특례법", &["아동학대처벌법"]),
    ("가정폭력범죄의 처벌 등에 관한 특례법", &["가정폭력처벌법"]),
    ("스토킹범죄의 처벌 등에 관한 법률", &["스토킹처벌법"]),
    ("성매매알선 등 행위의 처벌에 관한 법률", &["성매매처벌법"]),
    (
        "폭력행위 등 처벌에 관한 법률",
        &["폭력행위처벌법", "폭처법"],
    ),
    (
        "학교폭력예방 및 대책에 관한 법률",
        &["학교폭력예방법", "학폭법"],
    ),
    (
        "범죄수익은닉의 규제 및 처벌 등에 관한 법률",
        &["범죄수익은닉규제법"],
    ),
    ("형의 집행 및 수용자의 처우에 관한 법률", &["형집행법"]),
    ("가족관계의 등록 등에 관한 법률", &["가족관계등록법"]),
];

/// Bracket, whitespace and middle-dot normalization. The result keeps word spacing.
pub fn normalize_law_name(input: &str) -> String {
    let trimmed = input
        .trim()
        .trim_matches(|c: char| "「」『』\"“”'‘’".contains(c))
        .trim();
    let mut out = String::with_capacity(trimmed.len());
    let mut space = false;
    for c in trimmed.chars() {
        if c.is_whitespace() {
            space = !out.is_empty();
            continue;
        }
        if space {
            out.push(' ');
            space = false;
        }
        out.push(if MIDDLE_DOTS.contains(&c) {
            OFFICIAL_DOT
        } else {
            c
        });
    }
    out
}

/// Comparison key that ignores spacing, middle dots and ASCII case.
pub fn name_key(value: &str) -> String {
    value
        .chars()
        .filter(|c| !c.is_whitespace() && !MIDDLE_DOTS.contains(c))
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

fn alias_title(key: &str) -> Option<(&'static str, Option<&'static str>)> {
    for (title, aliases) in LAW_ALIASES {
        if name_key(title) == key {
            return Some((title, None));
        }
        if let Some(alias) = aliases.iter().find(|alias| name_key(alias) == key) {
            return Some((title, Some(alias)));
        }
    }
    None
}

/// Expand a known abbreviation, including `약칭 시행령` and `약칭 시행규칙` forms.
/// Unknown names are returned normalized but otherwise unchanged.
pub fn resolve_law_name(input: &str) -> LawNameResolution {
    let normalized = normalize_law_name(input);
    let key = name_key(&normalized);
    let expanded = alias_title(&key)
        .map(|(title, alias)| (title.to_string(), alias))
        .or_else(|| {
            SUFFIXES.iter().find_map(|suffix| {
                let base = key.strip_suffix(suffix).filter(|base| !base.is_empty())?;
                let (title, alias) = alias_title(base)?;
                Some((format!("{title} {suffix}"), alias))
            })
        });
    match expanded {
        Some((resolved, alias)) => LawNameResolution {
            input: input.to_string(),
            normalized,
            resolved,
            matched_alias: alias.map(str::to_string),
        },
        None => LawNameResolution {
            input: input.to_string(),
            resolved: normalized.clone(),
            normalized,
            matched_alias: None,
        },
    }
}

/// A regular-expression fragment matching `name` with optional spaces between
/// characters and any middle-dot variant where the name has one. Callers anchor it.
pub fn title_pattern(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars().filter(|c| !c.is_whitespace()) {
        if !out.is_empty() {
            out.push_str(" ?");
        }
        if MIDDLE_DOTS.contains(&c) {
            out.push('[');
            out.extend(MIDDLE_DOTS);
            out.push(']');
        } else {
            push_escaped(&mut out, c);
        }
    }
    out
}

/// Escape a literal for the Rust regex syntax used by `database.rg`.
pub fn push_escaped(out: &mut String, c: char) {
    if "\\.+*?()|[]{}^$#&-~".contains(c) {
        out.push('\\');
    }
    out.push(c);
}

/// The law name without a trailing `시행령` or `시행규칙`.
pub fn base_law_name(name: &str) -> &str {
    for suffix in SUFFIXES {
        if let Some(base) = name.strip_suffix(suffix) {
            let base = base.trim_end();
            if !base.is_empty() {
                return base;
            }
        }
    }
    name
}

fn digits(text: &str, at: usize, max: usize) -> Option<(u32, usize)> {
    let bytes = text.as_bytes();
    let mut end = at;
    while end < bytes.len() && bytes[end].is_ascii_digit() && end - at < max {
        end += 1;
    }
    if end == at || end < bytes.len() && bytes[end].is_ascii_digit() {
        return None;
    }
    text[at..end].parse().ok().map(|n| (n, end))
}

fn skip_spaces(text: &str, mut at: usize, max: usize) -> usize {
    let start = at;
    while at < text.len() && text.as_bytes()[at] == b' ' && at - start < max {
        at += 1;
    }
    at
}

/// Parse `제{n}{unit}` at `at`, allowing single spaces around the number.
fn unit_number(text: &str, at: usize, unit: char) -> Option<(u32, usize)> {
    let rest = text.get(at..)?;
    if !rest.starts_with('제') {
        return None;
    }
    let at = skip_spaces(text, at + '제'.len_utf8(), 1);
    let (n, end) = digits(text, at, 5)?;
    let end = skip_spaces(text, end, 1);
    if n == 0 || !text[end..].starts_with(unit) {
        return None;
    }
    Some((n, end + unit.len_utf8()))
}

/// Parse a leading `제{n}조` or `제{n}조의{m}` and return the byte after it.
fn article_at(text: &str, at: usize) -> Option<(ArticleNumber, usize)> {
    let (number, mut end) = unit_number(text, at, '조')?;
    let mut branch = None;
    if text[end..].starts_with('의')
        && let Some((m, after)) = digits(text, end + '의'.len_utf8(), 3)
        && m > 0
    {
        branch = Some(m);
        end = after;
    }
    Some((ArticleNumber { number, branch }, end))
}

/// Accepts `제44조의2`, `44조의2`, `44의2`, `44-2` and `44`.
pub fn parse_article_number(value: &str) -> Option<ArticleNumber> {
    let compact: String = value.chars().filter(|c| !c.is_whitespace()).collect();
    let body = compact.strip_prefix('제').unwrap_or(&compact);
    let (number, end) = digits(body, 0, 5)?;
    let mut rest = &body[end..];
    rest = rest.strip_prefix('조').unwrap_or(rest);
    let branch = if rest.is_empty() {
        None
    } else {
        let tail = rest.strip_prefix('의').or_else(|| rest.strip_prefix('-'))?;
        let (m, end) = digits(tail, 0, 3)?;
        if end != tail.len() || m == 0 {
            return None;
        }
        Some(m)
    };
    (number > 0).then_some(ArticleNumber { number, branch })
}

fn circled_number(c: char) -> Option<u32> {
    let v = c as u32;
    match v {
        0x2460..=0x2473 => Some(v - 0x2460 + 1),
        0x3251..=0x325F => Some(v - 0x3251 + 21),
        0x32B1..=0x32BF => Some(v - 0x32B1 + 36),
        _ => None,
    }
}

/// An article found in a capture's provider article sections.
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

fn parenthetical(text: &str, at: usize) -> Option<(&str, usize)> {
    let rest = text.get(at..)?;
    let open = if rest.starts_with('(') {
        '('
    } else if rest.starts_with('（') {
        '（'
    } else {
        return None;
    };
    let inner_start = at + open.len_utf8();
    let window = &text[inner_start..];
    let close = window
        .char_indices()
        .take_while(|(i, c)| *i < 200 && *c != '\n')
        .find(|(_, c)| *c == ')' || *c == '）')?;
    Some((
        &window[..close.0],
        inner_start + close.0 + close.1.len_utf8(),
    ))
}

fn parse_article_section(section: &LegalSection) -> Option<LocatedArticle<'_>> {
    if !section.id.starts_with("article:") {
        return None;
    }
    let text = section.text.trim_start();
    let (number, mut end) = article_at(text, 0)?;
    let mut title = section.title.trim().to_string();
    if let Some((inner, after)) = parenthetical(text, end) {
        if title.is_empty() {
            title = inner.trim().to_string();
        }
        end = after;
    }
    let deleted = text[end..].trim_start().starts_with("삭제");
    let paragraphs = text
        .lines()
        .filter_map(|line| line.trim_start().chars().next().and_then(circled_number))
        .collect();
    Some(LocatedArticle {
        section_id: &section.id,
        number,
        title,
        text,
        deleted,
        paragraphs,
    })
}

/// Find an article among `article:` provider sections by its leading locator.
pub fn locate_article(sections: &[LegalSection], wanted: ArticleNumber) -> ArticleLookup<'_> {
    let mut first = None;
    let mut last = None;
    for article in sections.iter().filter_map(parse_article_section) {
        if article.number == wanted {
            return ArticleLookup::Found(article);
        }
        first = first.min(Some(article.number)).or(Some(article.number));
        last = last.max(Some(article.number));
    }
    ArticleLookup::NotFound { first, last }
}

/// Whether a numbered subparagraph (`1.`, `2.`) appears in the article, inside the
/// cited paragraph when paragraphs are numbered.
pub fn has_subparagraph(article: &LocatedArticle<'_>, paragraph: Option<u32>, wanted: u32) -> bool {
    let mut inside = paragraph.is_none() || article.paragraphs.is_empty();
    for line in article.text.lines() {
        let line = line.trim_start();
        if let Some(n) = line.chars().next().and_then(circled_number) {
            inside = paragraph.is_none_or(|p| p == n);
            continue;
        }
        if inside
            && let Some((n, end)) = digits(line, 0, 3)
            && n == wanted
            && line[end..].starts_with('.')
        {
            return true;
        }
    }
    false
}

/// Character-bigram similarity (0-100) of two article titles after removing spacing
/// and punctuation; containment counts as 100. None when either title is empty.
pub fn title_similarity(cited: &str, retained: &str) -> Option<u8> {
    let clean = |s: &str| -> Vec<char> { s.chars().filter(|c| c.is_alphanumeric()).collect() };
    let (a, b) = (clean(cited), clean(retained));
    if a.is_empty() || b.is_empty() {
        return None;
    }
    let (sa, sb): (String, String) = (a.iter().collect(), b.iter().collect());
    if sa.contains(&sb) || sb.contains(&sa) {
        return Some(100);
    }
    let grams = |v: &[char]| -> std::collections::BTreeSet<(char, char)> {
        v.windows(2).map(|w| (w[0], w[1])).collect()
    };
    let (ga, gb) = (grams(&a), grams(&b));
    let union = ga.union(&gb).count();
    if union == 0 {
        return Some(0);
    }
    let shared = ga.intersection(&gb).count();
    Some(((shared * 100 + union / 2) / union) as u8)
}

/// How an extracted citation names its law.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LawReference {
    /// Candidate names ending at the article, longest first, with their byte starts.
    Named {
        candidates: Vec<(String, usize)>,
        /// The word before the article is a generic word such as `법` or `시행령`.
        generic_tail: bool,
    },
    /// `같은 법`, `동법`, optionally followed by `시행령`/`시행규칙`.
    Same {
        suffix: Option<&'static str>,
        start: usize,
    },
    /// A bare article joined to the previous citation by a connector (`및`, `,`).
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

fn is_boundary(c: char) -> bool {
    c == '\n' || ".,;:!?()[]{}\"'“”‘’<>《》〈〉「」『』（）、。".contains(c)
}

fn ends_like_law_name(word: &str) -> bool {
    [
        "법",
        "법률",
        "령",
        "규칙",
        "규정",
        "헌법",
        "특례법",
        "기본법",
    ]
    .iter()
    .any(|suffix| word.ends_with(suffix))
}

fn generic_law_word(word: &str) -> bool {
    matches!(
        word,
        "법" | "법률" | "영" | "령" | "규칙" | "규정" | "시행령" | "시행규칙"
    )
}

fn same_law_reference(prefix: &str) -> Option<(Option<&'static str>, usize)> {
    const FORMS: [(&str, Option<&str>); 9] = [
        ("같은법시행규칙", Some("시행규칙")),
        ("같은법시행령", Some("시행령")),
        ("동법시행규칙", Some("시행규칙")),
        ("동법시행령", Some("시행령")),
        ("같은법", None),
        ("동법", None),
        ("같은영", None),
        ("같은규칙", None),
        ("동령", None),
    ];
    let words: Vec<(usize, &str)> = word_tail(prefix, 3);
    for take in (1..=words.len()).rev() {
        let slice = &words[words.len() - take..];
        let joined: String = slice.iter().map(|(_, w)| *w).collect();
        if let Some((_, suffix)) = FORMS.iter().find(|(form, _)| *form == joined) {
            return Some((*suffix, slice[0].0));
        }
    }
    None
}

/// Up to `max` whitespace-separated words ending `prefix`, stopping at punctuation.
fn word_tail(prefix: &str, max: usize) -> Vec<(usize, &str)> {
    let mut words = Vec::new();
    let mut end = prefix.trim_end().len();
    while words.len() < max && end > 0 {
        let head = &prefix[..end];
        let start = head
            .char_indices()
            .rev()
            .find(|(_, c)| c.is_whitespace() || is_boundary(*c))
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        if start == end {
            break;
        }
        words.push((start, &prefix[start..end]));
        let before = &prefix[..start];
        let trimmed = before.trim_end_matches(|c: char| c.is_whitespace() && c != '\n');
        if trimmed.len() == before.len() {
            break;
        }
        end = trimmed.len();
    }
    words.reverse();
    words
}

fn bracketed_name(prefix: &str) -> Option<(String, usize)> {
    let trimmed = prefix.trim_end();
    let (body, suffix) = SUFFIXES
        .iter()
        .find_map(|s| {
            trimmed
                .strip_suffix(s)
                .map(|b| (b.trim_end(), Some(*s)))
                .filter(|(b, _)| b.ends_with('」') || b.ends_with('』'))
        })
        .unwrap_or((trimmed, None));
    let close = body.chars().last()?;
    let open = match close {
        '」' => '「',
        '』' => '『',
        _ => return None,
    };
    let start = body.rfind(open)?;
    let inner = &body[start + open.len_utf8()..body.len() - close.len_utf8()];
    if inner.is_empty() || inner.len() > MAX_LAW_NAME_BYTES || inner.contains('\n') {
        return None;
    }
    let name = match suffix {
        Some(s) => format!("{} {s}", normalize_law_name(inner)),
        None => normalize_law_name(inner),
    };
    Some((name, start))
}

fn connector_only(gap: &str) -> bool {
    let rest = gap
        .replace("또는", "")
        .replace("내지", "")
        .replace("부터", "")
        .replace("까지", "");
    gap.len() <= 24
        && rest
            .chars()
            .all(|c| c.is_whitespace() || ",·ㆍ、~및와과".contains(c))
}

fn law_reference(text: &str, start: usize, previous_end: Option<usize>) -> Option<LawReference> {
    let prefix = &text[..start];
    if let Some((name, at)) = bracketed_name(prefix) {
        return Some(LawReference::Named {
            candidates: vec![(name, at)],
            generic_tail: false,
        });
    }
    if let Some((suffix, at)) = same_law_reference(prefix) {
        return Some(LawReference::Same { suffix, start: at });
    }
    let words = word_tail(prefix, MAX_NAME_WORDS);
    if let Some((_, last)) = words.last()
        && ends_like_law_name(last)
    {
        let mut candidates = Vec::new();
        for take in (1..=words.len()).rev() {
            let slice = &words[words.len() - take..];
            let joined = slice.iter().map(|(_, w)| *w).collect::<Vec<_>>().join(" ");
            if joined.len() <= MAX_LAW_NAME_BYTES {
                candidates.push((normalize_law_name(&joined), slice[0].0));
            }
        }
        return Some(LawReference::Named {
            candidates,
            generic_tail: generic_law_word(last),
        });
    }
    previous_end
        .filter(|end| *end <= start && connector_only(&text[*end..start]))
        .map(|_| LawReference::Continued)
}

fn usable_title(inner: &str) -> Option<String> {
    let inner = inner.trim();
    let skip = [
        "이하", "구 ", "개정", "신설", "삭제", "현행", "종전", "단서", "본문",
    ];
    if inner.is_empty()
        || inner.contains("라 한다")
        || inner
            .chars()
            .all(|c| c.is_ascii_digit() || c.is_whitespace() || c == '.')
        || skip.iter().any(|s| inner.starts_with(s))
    {
        return None;
    }
    Some(inner.to_string())
}

fn statute_at(
    text: &str,
    at: usize,
    previous_end: Option<usize>,
) -> Option<Result<ExtractedStatute, usize>> {
    let (article, mut end) = article_at(text, at)?;
    let mut cited_title = None;
    if let Some((inner, after)) = parenthetical(text, end) {
        cited_title = usable_title(inner);
        if cited_title.is_some() {
            end = after;
        }
    }
    let mut paragraph = None;
    let mut subparagraph = None;
    let next = skip_spaces(text, end, 1);
    if let Some((p, after)) = unit_number(text, next, '항') {
        paragraph = Some(p);
        end = after;
    }
    let next = skip_spaces(text, end, 1);
    if let Some((s, after)) = unit_number(text, next, '호') {
        subparagraph = Some(s);
        end = after;
    }
    Some(match law_reference(text, at, previous_end) {
        Some(law) => Ok(ExtractedStatute {
            law,
            article,
            article_start: at,
            byte_end: end,
            paragraph,
            subparagraph,
            cited_title,
        }),
        None => Err(end),
    })
}

const CASE_TYPES: &[&str] = &[
    "가합", "가단", "가소", "나", "다", "라", "마", "그", "머", "카", "타", "차", "드", "르", "므",
    "브", "스", "느단", "느합", "고합", "고단", "고정", "고약", "노", "도", "로", "모", "보", "오",
    "초", "감도", "구합", "구단", "구", "누", "두", "무", "허", "후", "헌가", "헌나", "헌다",
    "헌라", "헌마", "헌바", "헌사", "헌아", "재다", "재두", "재도",
];

fn case_at(text: &str, at: usize) -> Option<ExtractedCase> {
    if text[..at]
        .chars()
        .next_back()
        .is_some_and(|c| c.is_alphanumeric())
    {
        return None;
    }
    let (_, year_end) = digits(text, at, 4)?;
    let year = &text[at..year_end];
    if !(year.len() == 2 || year.len() == 4 && (year.starts_with("19") || year.starts_with("20"))) {
        return None;
    }
    let mut kind_end = year_end;
    for (count, (i, c)) in text[year_end..].char_indices().enumerate() {
        if count == 3 || !('가'..='힣').contains(&c) {
            break;
        }
        kind_end = year_end + i + c.len_utf8();
    }
    let kind = &text[year_end..kind_end];
    if !CASE_TYPES.contains(&kind) {
        return None;
    }
    let (_, end) = digits(text, kind_end, 7)?;
    if text[end..]
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() && !('가'..='힣').contains(&c))
    {
        return None;
    }
    Some(ExtractedCase {
        byte_start: at,
        byte_end: end,
        case_number: text[at..end].to_string(),
    })
}

/// Extract statute citations and court case numbers in input order. Input longer
/// than [`MAX_CITATION_TEXT_BYTES`] is rejected by callers before extraction.
pub fn extract_citations(text: &str) -> Extraction {
    let mut out = Extraction::default();
    let mut previous_end = None;
    let mut seen_cases = std::collections::BTreeSet::new();
    let mut i = 0;
    while i < text.len() {
        let Some(c) = text[i..].chars().next() else {
            break;
        };
        if c == '제' {
            match statute_at(text, i, previous_end) {
                Some(Ok(citation)) => {
                    if out.statutes.len() == MAX_STATUTE_CITATIONS {
                        out.truncated = true;
                    } else {
                        i = citation.byte_end;
                        previous_end = Some(citation.byte_end);
                        out.statutes.push(citation);
                        continue;
                    }
                }
                Some(Err(end)) => {
                    previous_end = None;
                    i = end;
                    continue;
                }
                None => {}
            }
        } else if c.is_ascii_digit()
            && let Some(case) = case_at(text, i)
        {
            i = case.byte_end;
            if seen_cases.insert(case.case_number.clone()) {
                if seen_cases.len() > MAX_CASE_CITATIONS {
                    out.truncated = true;
                } else {
                    out.cases.push(case);
                }
            }
            continue;
        } else if c == '\n' && text[i + 1..].starts_with('\n') {
            // A blank line ends a paragraph; `같은 법` and continuations do not cross it.
            previous_end = None;
        }
        i += c.len_utf8();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_domain::legal::SectionKind;

    fn section(id: &str, title: &str, text: &str) -> LegalSection {
        LegalSection {
            id: id.into(),
            title: title.into(),
            text: text.into(),
            kind: SectionKind::ProviderText,
            source_document_sha256: None,
            page: None,
        }
    }
    fn art(number: u32, branch: Option<u32>) -> ArticleNumber {
        ArticleNumber { number, branch }
    }

    #[test]
    fn aliases_expand_with_suffixes_and_ignore_spacing_and_dots() {
        let r = resolve_law_name(" 산안법 시행령 ");
        assert_eq!(r.resolved, "산업안전보건법 시행령");
        assert_eq!(r.matched_alias.as_deref(), Some("산안법"));
        assert_eq!(
            resolve_law_name("중처법").resolved,
            "중대재해 처벌 등에 관한 법률"
        );
        assert_eq!(
            resolve_law_name("개인정보보호법").resolved,
            "개인정보 보호법"
        );
        let dotted = resolve_law_name("「표시·광고의 공정화에 관한 법률」");
        assert_eq!(dotted.resolved, "표시ㆍ광고의 공정화에 관한 법률");
        assert_eq!(dotted.matched_alias, None);
        let unknown = resolve_law_name("가상의  시험법");
        assert_eq!(unknown.resolved, "가상의 시험법");
        assert_eq!(unknown.matched_alias, None);
        assert_eq!(
            resolve_law_name("ai기본법").matched_alias.as_deref(),
            Some("AI기본법")
        );
    }

    #[test]
    fn alias_table_has_unique_keys_and_well_formed_titles() {
        let mut keys = std::collections::BTreeMap::new();
        for (title, aliases) in LAW_ALIASES {
            assert_eq!(normalize_law_name(title), *title);
            assert!(!title.contains('·'));
            for key in std::iter::once(*title).chain(aliases.iter().copied()) {
                if let Some(previous) = keys.insert(name_key(key), *title) {
                    assert_eq!(previous, *title, "{key} maps to two titles");
                }
            }
        }
    }

    #[test]
    fn title_patterns_escape_and_accept_spacing_variants() {
        assert_eq!(title_pattern("민법"), "민 ?법");
        assert_eq!(title_pattern("a.b(c)"), "a ?\\. ?b ?\\( ?c ?\\)");
        assert!(title_pattern("일ㆍ가정").contains("[·ㆍ"));
        assert_eq!(base_law_name("산업안전보건법 시행령"), "산업안전보건법");
        assert_eq!(base_law_name("시행령"), "시행령");
    }

    #[test]
    fn article_numbers_accept_common_forms() {
        assert_eq!(parse_article_number("제44조의2"), Some(art(44, Some(2))));
        assert_eq!(parse_article_number("44의2"), Some(art(44, Some(2))));
        assert_eq!(parse_article_number("44-2"), Some(art(44, Some(2))));
        assert_eq!(parse_article_number(" 제 44 조 "), Some(art(44, None)));
        for bad in ["", "제0조", "제조", "44의", "44의0", "제44항", "123456"] {
            assert_eq!(parse_article_number(bad), None, "{bad}");
        }
        assert_eq!(art(2, Some(3)).to_string(), "제2조의3");
    }

    #[test]
    fn locates_articles_paragraphs_deletions_and_ranges() {
        let sections = vec![
            section("article:0001000", "", "제1장 총칙"),
            section("article:0001001", "목적", "제1조(목적) 이 법은 시험한다."),
            section(
                "article:0002001",
                "정의",
                "제2조(정의)\n① 첫째 항\n1. 첫째 호\n2. 둘째 호\n② 둘째 항\n1. 다른 호",
            ),
            section("article:0002002", "", "제2조의2 삭제 <2020. 1. 1.>"),
            section("source_ordinal:9", "부칙내용", "제1조(시행일) 부칙"),
            section("article:0010001", "벌칙", "제10조(벌칙) 처벌한다."),
        ];
        let ArticleLookup::Found(found) = locate_article(&sections, art(2, None)) else {
            panic!("article 2");
        };
        assert_eq!(found.title, "정의");
        assert_eq!(found.paragraphs, vec![1, 2]);
        assert!(has_subparagraph(&found, Some(1), 2));
        assert!(!has_subparagraph(&found, Some(2), 2));
        assert!(has_subparagraph(&found, None, 2));
        let ArticleLookup::Found(deleted) = locate_article(&sections, art(2, Some(2))) else {
            panic!("article 2-2");
        };
        assert!(deleted.deleted);
        assert_eq!(
            locate_article(&sections, art(11, None)),
            ArticleLookup::NotFound {
                first: Some(art(1, None)),
                last: Some(art(10, None))
            }
        );
        let ArticleLookup::Found(first) = locate_article(&sections, art(1, None)) else {
            panic!("article 1");
        };
        assert_eq!(first.section_id, "article:0001001");
        assert!(first.paragraphs.is_empty());
    }

    #[test]
    fn title_similarity_handles_containment_and_disjoint_titles() {
        assert_eq!(
            title_similarity("불법행위의 내용", "불법행위의내용"),
            Some(100)
        );
        assert_eq!(title_similarity("정의", "용어의 정의"), Some(100));
        assert_eq!(title_similarity("계약해제", "불법행위의 내용"), Some(0));
        assert_eq!(title_similarity("", "목적"), None);
        assert!(title_similarity("연차 유급휴가", "연차유급휴가의 사용").unwrap() >= 50);
    }

    fn named(c: &ExtractedStatute) -> Vec<&str> {
        match &c.law {
            LawReference::Named { candidates, .. } => {
                candidates.iter().map(|(n, _)| n.as_str()).collect()
            }
            other => panic!("not named: {other:?}"),
        }
    }

    #[test]
    fn extracts_bracketed_plain_inherited_and_continued_citations() {
        let text = "「노인장기요양보험법」 제38조제1항 및 같은 법 시행규칙 제30조에 따르고, \
                    절도죄는 형법 제329조(절도) 및 제330조, 민법 제750조(계약해제)를 본다.";
        let found = extract_citations(text);
        assert_eq!(found.statutes.len(), 5);
        let first = &found.statutes[0];
        assert_eq!(named(first), vec!["노인장기요양보험법"]);
        assert_eq!(first.article, art(38, None));
        assert_eq!(first.paragraph, Some(1));
        assert_eq!(
            found.statutes[1].law,
            LawReference::Same {
                suffix: Some("시행규칙"),
                start: text.find("같은").unwrap()
            }
        );
        let theft = &found.statutes[2];
        assert_eq!(named(theft), vec!["절도죄는 형법", "형법"]);
        assert_eq!(theft.cited_title.as_deref(), Some("절도"));
        assert_eq!(found.statutes[3].law, LawReference::Continued);
        assert_eq!(found.statutes[4].article, art(750, None));
        assert_eq!(found.statutes[4].cited_title.as_deref(), Some("계약해제"));
        assert_eq!(
            &text[found.statutes[4].article_start..found.statutes[4].byte_end],
            "제750조(계약해제)"
        );
        assert!(!found.truncated);
    }

    #[test]
    fn extraction_handles_spacing_suffixes_generic_words_and_blank_lines() {
        let text = "중대재해 처벌 등에 관한 법률 제4조, 산안법 시행령 제5조의2 제2항 제3호\n\n\
                    및 제7조. 이 법 제3조(이하 \"법\"이라 한다)";
        let found = extract_citations(text);
        assert_eq!(found.statutes.len(), 3, "{found:?}");
        assert!(named(&found.statutes[0]).contains(&"중대재해 처벌 등에 관한 법률"));
        let second = &found.statutes[1];
        assert_eq!(named(second)[0], "산안법 시행령");
        assert_eq!(second.article, art(5, Some(2)));
        assert_eq!((second.paragraph, second.subparagraph), (Some(2), Some(3)));
        let LawReference::Named { generic_tail, .. } = &found.statutes[2].law else {
            panic!("generic");
        };
        assert!(generic_tail);
        assert_eq!(found.statutes[2].cited_title, None);
    }

    #[test]
    fn extracts_distinct_case_numbers_without_dates() {
        let text = "대법원 2007. 5. 31. 선고 2007다27670 판결, 2018도14262, 2016헌마123, \
                    91누1234 및 2007다27670. 2020년3월 12345다1 x2018도1 2018도14262a";
        let found = extract_citations(text);
        let numbers: Vec<&str> = found.cases.iter().map(|c| c.case_number.as_str()).collect();
        assert_eq!(
            numbers,
            ["2007다27670", "2018도14262", "2016헌마123", "91누1234"]
        );
        assert_eq!(
            &text[found.cases[0].byte_start..found.cases[0].byte_end],
            "2007다27670"
        );
    }

    #[test]
    fn extraction_bounds_statute_count() {
        let text = "민법 제1조 ".repeat(MAX_STATUTE_CITATIONS + 3);
        let found = extract_citations(&text);
        assert_eq!(found.statutes.len(), MAX_STATUTE_CITATIONS);
        assert!(found.truncated);
    }
}
