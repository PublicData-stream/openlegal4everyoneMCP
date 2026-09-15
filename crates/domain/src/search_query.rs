//! Syntax contracts for supplied search text; these types do not execute searches.

use std::fmt;

pub const MAX_QUERY_BYTES: usize = 4096;
pub const MAX_QUERY_NODES: usize = 256;
pub const MAX_QUERY_NESTING: usize = 32;
pub const MAX_QUERY_FIELDS: usize = 64;
pub const MAX_QUERY_FIELD_BYTES: usize = 64;

/// Half-open UTF-8 byte offsets into the original query, on character boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteSpan {
    pub start: usize,
    pub end: usize,
}

/// Original source and its decoded syntax. No normalization or execution is implied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedQuery {
    pub source: String,
    pub expression: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expr {
    pub span: ByteSpan,
    pub kind: ExprKind,
}

/// Matching intent, independent of any provider or search analyzer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExprKind {
    MatchAll,
    Term(String),
    /// Literal sequence, including case, punctuation, spacing, and order.
    Exact(String),
    /// Prefix of a term; the trailing operator is omitted from this value.
    Prefix(String),
    Not(Box<Expr>),
    /// Nonempty associative chains, in source order (no rewriting or deduplication).
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Field {
        name: String,
        expression: Box<Expr>,
    },
    Group {
        origin: GroupOrigin,
        expression: Box<Expr>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupOrigin {
    Parentheses,
    SingleQuotes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseErrorKind {
    InputTooLong,
    TooManyNodes,
    NestingTooDeep,
    ForbiddenControl,
    ExpectedOperand,
    UnexpectedToken,
    MissingSeparator,
    UnclosedParenthesis,
    UnclosedQuote,
    EmptyGroup,
    EmptyPhrase,
    TrailingEscape,
    InvalidWildcard,
    InvalidFieldSyntax,
    UnknownField,
    NestedField,
}

/// A bounded diagnostic. It deliberately contains no caller query text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseError {
    pub kind: ParseErrorKind,
    pub span: ByteSpan,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "search query {:?} at bytes {}..{}",
            self.kind, self.span.start, self.span.end
        )
    }
}

impl std::error::Error for ParseError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryConfigError {
    TooManyFields,
    InvalidFieldName,
    DuplicateField,
}

impl fmt::Display for QueryConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "search query configuration: {self:?}")
    }
}

impl std::error::Error for QueryConfigError {}
