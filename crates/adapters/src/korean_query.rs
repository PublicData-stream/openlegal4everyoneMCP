//! Engine-coherent positive matching and shared exclusions. No source rewriting.
use crate::korean_analysis::{AnalyzedText, KoreanAnalyzer};
use openlegal_domain::{
    legal::DatabaseError as E,
    search_query::{Expr, ExprKind},
};
use std::{collections::HashSet, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub enum CompiledQuery {
    All,
    Term { tokens: AnalyzedText, prefix: bool },
    Exact(String),
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Field { field: QueryField, child: Box<Self> },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum QueryField {
    Title,
    Body,
    CaseNumber,
}

impl CompiledQuery {
    pub fn needs_tokens(&self) -> bool {
        match self {
            Self::Term { .. } => true,
            Self::Not(child) | Self::Field { child, .. } => child.needs_tokens(),
            Self::And(children) | Self::Or(children) => children.iter().any(Self::needs_tokens),
            Self::All | Self::Exact(_) => false,
        }
    }

    pub fn only_title(&self) -> bool {
        match self {
            Self::Field { field, .. } => *field == QueryField::Title,
            Self::Not(child) => child.only_title(),
            Self::And(children) | Self::Or(children) => children.iter().all(Self::only_title),
            Self::All | Self::Term { .. } | Self::Exact(_) => false,
        }
    }

    pub fn needs_case_tokens(&self) -> bool {
        match self {
            Self::Term { .. } => true,
            Self::Field { field, child } => {
                *field == QueryField::CaseNumber && child.needs_tokens()
            }
            Self::Not(child) => child.needs_case_tokens(),
            Self::And(children) | Self::Or(children) => {
                children.iter().any(Self::needs_case_tokens)
            }
            Self::All | Self::Exact(_) => false,
        }
    }

    pub fn compile(
        expr: &Expr,
        analyzer: &KoreanAnalyzer,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<Self, E> {
        if cancel.is_cancelled() {
            return Err(E::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(E::BudgetExhausted);
        }
        let compile = |e: &Expr| Self::compile(e, analyzer, deadline, cancel);
        Ok(match &expr.kind {
            ExprKind::MatchAll => Self::All,
            ExprKind::Term(text) | ExprKind::Prefix(text) => {
                let tokens = analyzer.analyze(text, deadline, cancel)?;
                if tokens.lindera.is_empty() || tokens.mecab.is_empty() {
                    return Err(E::InvalidInput);
                }
                Self::Term {
                    tokens,
                    prefix: matches!(expr.kind, ExprKind::Prefix(_)),
                }
            }
            ExprKind::Exact(text) => Self::Exact(text.clone()),
            ExprKind::Not(child) => Self::Not(Box::new(compile(child)?)),
            ExprKind::And(children) => {
                Self::And(children.iter().map(compile).collect::<Result<_, _>>()?)
            }
            ExprKind::Or(children) => {
                Self::Or(children.iter().map(compile).collect::<Result<_, _>>()?)
            }
            ExprKind::Field { name, expression } => Self::Field {
                field: match name.as_str() {
                    "title" => QueryField::Title,
                    "body" => QueryField::Body,
                    "case_number" => QueryField::CaseNumber,
                    _ => return Err(E::InvalidInput),
                },
                child: Box::new(compile(expression)?),
            },
            ExprKind::Group { expression, .. } => compile(expression)?,
        })
    }

    pub fn matches(
        &self,
        title: &str,
        title_tokens: &AnalyzedText,
        body: &str,
        body_tokens: &AnalyzedText,
        case_number: &str,
        case_number_tokens: &AnalyzedText,
    ) -> bool {
        let fields = [
            Field::new(QueryField::Title, title, title_tokens),
            Field::new(QueryField::Body, body, body_tokens),
            Field::new(QueryField::CaseNumber, case_number, case_number_tokens),
        ];
        let [lindera, mecab] = self.evaluate(&fields);
        lindera || mecab
    }

    fn evaluate(&self, fields: &[Field<'_>]) -> [bool; 2] {
        match self {
            Self::All => [true; 2],
            Self::Term { tokens, prefix } => {
                let query = [&tokens.lindera, &tokens.mecab];
                std::array::from_fn(|engine| {
                    fields.iter().any(|field| {
                        query[engine].iter().enumerate().all(|(i, token)| {
                            if *prefix && i + 1 == query[engine].len() {
                                field.tokens[engine]
                                    .iter()
                                    .any(|value| value.starts_with(token))
                            } else {
                                field.tokens[engine].contains(token.as_str())
                            }
                        })
                    })
                })
            }
            Self::Exact(text) => [fields.iter().any(|field| field.text.contains(text)); 2],
            Self::Not(child) => {
                let [a, b] = child.evaluate(fields);
                [!(a || b); 2]
            }
            Self::And(children) => children.iter().fold([true; 2], |a, child| {
                let b = child.evaluate(fields);
                [a[0] && b[0], a[1] && b[1]]
            }),
            Self::Or(children) => children.iter().fold([false; 2], |a, child| {
                let b = child.evaluate(fields);
                [a[0] || b[0], a[1] || b[1]]
            }),
            Self::Field { field, child } => {
                // The parser prohibits nested field scopes; preserve field identity nonetheless.
                let selected: Vec<_> = fields
                    .iter()
                    .filter(|candidate| candidate.kind == *field)
                    .cloned()
                    .collect();
                child.evaluate(&selected)
            }
        }
    }
}

#[derive(Clone)]
struct Field<'a> {
    kind: QueryField,
    text: &'a str,
    tokens: [HashSet<&'a str>; 2],
}
impl<'a> Field<'a> {
    fn new(kind: QueryField, text: &'a str, tokens: &'a AnalyzedText) -> Self {
        Self {
            kind,
            text,
            tokens: [
                tokens.lindera.iter().map(String::as_str).collect(),
                tokens.mecab.iter().map(String::as_str).collect(),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn term(text: &str) -> CompiledQuery {
        CompiledQuery::Term {
            tokens: AnalyzedText {
                lindera: vec![text.into()],
                mecab: vec![text.into()],
            },
            prefix: false,
        }
    }
    fn test(query: CompiledQuery) -> bool {
        query.matches(
            "ABC",
            &AnalyzedText::default(),
            "body",
            &AnalyzedText {
                lindera: vec!["a".into()],
                mecab: vec!["b".into()],
            },
            "",
            &AnalyzedText::default(),
        )
    }
    #[test]
    fn positive_conjunction_never_splices_engines_and_either_engine_vetoes() {
        assert!(test(term("a")));
        assert!(test(term("b")));
        assert!(!test(CompiledQuery::And(vec![term("a"), term("b")])));
        assert!(test(CompiledQuery::Or(vec![term("a"), term("b")])));
        assert!(!test(CompiledQuery::And(vec![
            term("a"),
            CompiledQuery::Not(Box::new(term("b")))
        ])));
        assert!(test(CompiledQuery::Not(Box::new(CompiledQuery::And(
            vec![term("a"), term("b")]
        )))));
        assert!(test(CompiledQuery::Not(Box::new(CompiledQuery::Not(
            Box::new(term("b"))
        )))));
        assert!(test(CompiledQuery::Exact("ABC".into())));
        assert!(!test(CompiledQuery::Exact("abc".into())));
    }
    #[test]
    fn field_prefix_and_segmentation_alternatives_remain_separate() {
        let tokens = AnalyzedText {
            lindera: vec!["대한".into(), "민국".into()],
            mecab: vec!["대한민국".into()],
        };
        let exact = CompiledQuery::Term {
            tokens: tokens.clone(),
            prefix: false,
        };
        assert!(exact.matches(
            "",
            &AnalyzedText::default(),
            "대한민국",
            &tokens,
            "",
            &AnalyzedText::default()
        ));
        // Neither engine may borrow the other's document tokens.
        let swapped = AnalyzedText {
            lindera: tokens.mecab.clone(),
            mecab: tokens.lindera.clone(),
        };
        assert!(!exact.matches(
            "",
            &AnalyzedText::default(),
            "대한민국",
            &swapped,
            "",
            &AnalyzedText::default()
        ));
        let prefix = CompiledQuery::Term {
            tokens: AnalyzedText {
                lindera: vec!["대한".into(), "민".into()],
                mecab: vec!["대한민".into()],
            },
            prefix: true,
        };
        assert!(prefix.matches(
            "",
            &AnalyzedText::default(),
            "대한민국",
            &tokens,
            "",
            &AnalyzedText::default()
        ));
        let scoped = CompiledQuery::Field {
            field: QueryField::Title,
            child: Box::new(exact.clone()),
        };
        assert!(!scoped.matches(
            "",
            &AnalyzedText::default(),
            "대한민국",
            &tokens,
            "",
            &AnalyzedText::default()
        ));
        assert!(scoped.matches(
            "대한민국",
            &tokens,
            "",
            &AnalyzedText::default(),
            "",
            &AnalyzedText::default()
        ));
        let excluded_body = CompiledQuery::Not(Box::new(CompiledQuery::Field {
            field: QueryField::Body,
            child: Box::new(exact),
        }));
        assert!(!excluded_body.matches(
            "",
            &AnalyzedText::default(),
            "대한민국",
            &tokens,
            "",
            &AnalyzedText::default()
        ));
    }
    #[test]
    fn case_number_exact_and_scoped_terms_use_metadata_field() {
        let tokens = AnalyzedText {
            lindera: vec!["2018".into(), "도".into(), "14262".into()],
            mecab: vec!["2018".into(), "도".into(), "14262".into()],
        };
        let matches = |query: CompiledQuery| {
            query.matches(
                "unrelated title",
                &AnalyzedText::default(),
                "unrelated body",
                &AnalyzedText::default(),
                "2018도14262",
                &tokens,
            )
        };
        assert!(matches(CompiledQuery::Exact("2018도14262".into())));
        assert!(matches(CompiledQuery::Field {
            field: QueryField::CaseNumber,
            child: Box::new(term("2018")),
        }));
        assert!(!matches(CompiledQuery::Field {
            field: QueryField::Body,
            child: Box::new(CompiledQuery::Exact("2018도14262".into())),
        }));
    }
}
