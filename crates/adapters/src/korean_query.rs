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
    Field { title: bool, child: Box<Self> },
}

impl CompiledQuery {
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
            return Err(E::Capacity);
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
                title: match name.as_str() {
                    "title" => true,
                    "body" => false,
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
    ) -> bool {
        let mut fields = [
            Field::new(title, title_tokens),
            Field::new(body, body_tokens),
        ];
        fields[0].title = true;
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
            Self::Field { title, child } => {
                // The parser prohibits nested field scopes; preserve field identity nonetheless.
                let selected: Vec<_> = fields
                    .iter()
                    .filter(|field| field.title == *title)
                    .cloned()
                    .collect();
                child.evaluate(&selected)
            }
        }
    }
}

#[derive(Clone)]
struct Field<'a> {
    title: bool,
    text: &'a str,
    tokens: [HashSet<&'a str>; 2],
}
impl<'a> Field<'a> {
    fn new(text: &'a str, tokens: &'a AnalyzedText) -> Self {
        Self {
            title: false,
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
        assert!(exact.matches("", &AnalyzedText::default(), "대한민국", &tokens));
        // Neither engine may borrow the other's document tokens.
        let swapped = AnalyzedText {
            lindera: tokens.mecab.clone(),
            mecab: tokens.lindera.clone(),
        };
        assert!(!exact.matches("", &AnalyzedText::default(), "대한민국", &swapped));
        let prefix = CompiledQuery::Term {
            tokens: AnalyzedText {
                lindera: vec!["대한".into(), "민".into()],
                mecab: vec!["대한민".into()],
            },
            prefix: true,
        };
        assert!(prefix.matches("", &AnalyzedText::default(), "대한민국", &tokens));
        let scoped = CompiledQuery::Field {
            title: true,
            child: Box::new(exact.clone()),
        };
        assert!(!scoped.matches("", &AnalyzedText::default(), "대한민국", &tokens));
        assert!(scoped.matches("대한민국", &tokens, "", &AnalyzedText::default()));
        let excluded_body = CompiledQuery::Not(Box::new(CompiledQuery::Field {
            title: false,
            child: Box::new(exact),
        }));
        assert!(!excluded_body.matches("", &AnalyzedText::default(), "대한민국", &tokens));
    }
}
