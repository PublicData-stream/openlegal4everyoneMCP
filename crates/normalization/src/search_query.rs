//! Pure search-query parsing. See `docs/search-query.md` for the language contract.
//!
//! ```
//! use openlegal_normalization::search_query::SearchQueryProcessor;
//! let processor = SearchQueryProcessor::new(&["title", "body"])?;
//! let query = processor.parse("in:title:(license OR permit) -'properly installed'")?;
//! assert_eq!(query.expression.span.end, query.source.len());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use openlegal_domain::search_query::{
    ByteSpan, Expr, ExprKind, GroupOrigin, MAX_QUERY_BYTES, MAX_QUERY_FIELD_BYTES,
    MAX_QUERY_FIELDS, MAX_QUERY_NESTING, MAX_QUERY_NODES, ParseError, ParseErrorKind, ParsedQuery,
    QueryConfigError,
};

/// Reusable parser with a validated, caller-owned vocabulary of field names.
/// Construction copies at most 64 bounded names; parsing has no side effects.
#[derive(Debug)]
pub struct SearchQueryProcessor {
    fields: Vec<String>,
}

impl SearchQueryProcessor {
    pub fn new(allowed_fields: &[&str]) -> Result<Self, QueryConfigError> {
        if allowed_fields.len() > MAX_QUERY_FIELDS {
            return Err(QueryConfigError::TooManyFields);
        }
        let mut fields = Vec::with_capacity(allowed_fields.len());
        for &name in allowed_fields {
            if !valid_field(name) {
                return Err(QueryConfigError::InvalidFieldName);
            }
            if fields.iter().any(|field| field == name) {
                return Err(QueryConfigError::DuplicateField);
            }
            fields.push(name.to_owned());
        }
        Ok(Self { fields })
    }

    /// Parse one bounded UTF-8 query. Errors carry source byte spans, never text.
    /// The returned AST is syntax intent, not a provider query or executable plan.
    pub fn parse(&self, input: &str) -> Result<ParsedQuery, ParseError> {
        if input.len() > MAX_QUERY_BYTES {
            return Err(error(ParseErrorKind::InputTooLong, span(0, input.len())));
        }
        if let Some((offset, ch)) = input.char_indices().find(|(_, ch)| ch.is_control()) {
            return Err(error(
                ParseErrorKind::ForbiddenControl,
                span(offset, offset + ch.len_utf8()),
            ));
        }
        let mut parser = Parser {
            scanner: Scanner { input, offset: 0 },
            lookahead: None,
            fields: &self.fields,
            nodes: 0,
        };
        let expression = if matches!(parser.peek()?.kind, TokenKind::End) {
            parser.node(ExprKind::MatchAll, span(0, input.len()))?
        } else {
            let expression = parser.or(0, false)?;
            let next = parser.peek()?;
            if !matches!(next.kind, TokenKind::End) {
                return Err(error(ParseErrorKind::UnexpectedToken, next.span));
            }
            expression
        };
        Ok(ParsedQuery {
            source: input.to_owned(),
            expression,
        })
    }
}

fn valid_field(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_QUERY_FIELD_BYTES
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn span(start: usize, end: usize) -> ByteSpan {
    ByteSpan { start, end }
}

fn error(kind: ParseErrorKind, span: ByteSpan) -> ParseError {
    ParseError { kind, span }
}

#[derive(Debug)]
enum TokenKind {
    Term(String),
    Exact(String),
    Words(Vec<(String, ByteSpan)>),
    Prefix(String),
    Field(String),
    And,
    Or,
    Not,
    Minus,
    Open,
    Close,
    End,
}

impl TokenKind {
    fn starts_operand(&self) -> bool {
        matches!(
            self,
            Self::Term(_)
                | Self::Exact(_)
                | Self::Words(_)
                | Self::Prefix(_)
                | Self::Field(_)
                | Self::Not
                | Self::Minus
                | Self::Open
        )
    }
}

#[derive(Debug)]
struct Token {
    kind: TokenKind,
    span: ByteSpan,
}

struct Scanner<'a> {
    input: &'a str,
    offset: usize,
}

impl Scanner<'_> {
    fn current(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn advance(&mut self, ch: char) {
        self.offset += ch.len_utf8();
    }

    // A quote between source alphanumerics is an apostrophe, including within 'words'.
    fn apostrophe(&self) -> bool {
        self.input[..self.offset]
            .chars()
            .next_back()
            .is_some_and(char::is_alphanumeric)
            && self.input[self.offset + 1..]
                .chars()
                .next()
                .is_some_and(char::is_alphanumeric)
    }

    fn escaped(&mut self) -> Result<char, ParseError> {
        let start = self.offset;
        self.offset += 1; // ASCII backslash, confirmed by the caller.
        let ch = self
            .current()
            .ok_or_else(|| error(ParseErrorKind::TrailingEscape, span(start, self.offset)))?;
        self.advance(ch);
        Ok(ch)
    }

    fn next(&mut self) -> Result<Token, ParseError> {
        while let Some(ch) = self.current().filter(|ch| ch.is_whitespace()) {
            self.advance(ch);
        }
        let start = self.offset;
        let kind = match self.current() {
            None => TokenKind::End,
            Some('(') => {
                self.offset += 1;
                TokenKind::Open
            }
            Some(')') => {
                self.offset += 1;
                TokenKind::Close
            }
            Some('-') => {
                self.offset += 1;
                TokenKind::Minus
            }
            Some('"') => self.quoted(false)?,
            Some('\'') => self.quoted(true)?,
            Some(_) if self.input[self.offset..].starts_with("in:") => self.field()?,
            Some(_) => self.word()?,
        };
        Ok(Token {
            kind,
            span: span(start, self.offset),
        })
    }

    fn field(&mut self) -> Result<TokenKind, ParseError> {
        let start = self.offset;
        self.offset += 3;
        let name_start = self.offset;
        while let Some(ch) = self.current() {
            if !(ch.is_ascii_alphanumeric() || ch == '_') {
                break;
            }
            self.advance(ch);
        }
        let name = &self.input[name_start..self.offset];
        if self.current() != Some(':') || !valid_field(name) {
            return Err(error(
                ParseErrorKind::InvalidFieldSyntax,
                span(start, self.offset),
            ));
        }
        self.offset += 1;
        Ok(TokenKind::Field(name.to_owned()))
    }

    fn word(&mut self) -> Result<TokenKind, ParseError> {
        let start = self.offset;
        let mut value = String::new();
        let mut escaped = false;
        let mut stars = Vec::new();
        while let Some(ch) = self.current() {
            if ch.is_whitespace()
                || matches!(ch, '(' | ')' | '"')
                || (ch == '\'' && !self.apostrophe())
            {
                break;
            }
            if ch == '\\' {
                value.push(self.escaped()?);
                escaped = true;
            } else {
                if ch == '*' {
                    stars.push(value.len());
                }
                value.push(ch);
                self.advance(ch);
            }
        }
        if !stars.is_empty() {
            if stars.len() != 1 || stars[0] == 0 || stars[0] + 1 != value.len() {
                return Err(error(
                    ParseErrorKind::InvalidWildcard,
                    span(start, self.offset),
                ));
            }
            value.pop(); // The final decoded character is the unescaped ASCII star.
            return Ok(TokenKind::Prefix(value));
        }
        Ok(match (escaped, value.as_str()) {
            (false, "AND") => TokenKind::And,
            (false, "OR") => TokenKind::Or,
            (false, "NOT") => TokenKind::Not,
            _ => TokenKind::Term(value),
        })
    }

    fn quoted(&mut self, single: bool) -> Result<TokenKind, ParseError> {
        let start = self.offset;
        let delimiter = if single { '\'' } else { '"' };
        self.offset += 1;
        let mut value = String::new();
        let mut words = Vec::new();
        let mut word_start = self.offset;
        loop {
            let ch = self
                .current()
                .ok_or_else(|| error(ParseErrorKind::UnclosedQuote, span(start, self.offset)))?;
            if ch == delimiter && !(single && self.apostrophe()) {
                if single && !value.is_empty() {
                    words.push((value, span(word_start, self.offset)));
                    value = String::new();
                }
                self.advance(ch);
                return if single {
                    if words.is_empty() {
                        Err(error(ParseErrorKind::EmptyGroup, span(start, self.offset)))
                    } else {
                        Ok(TokenKind::Words(words))
                    }
                } else if value.is_empty() {
                    Err(error(ParseErrorKind::EmptyPhrase, span(start, self.offset)))
                } else {
                    Ok(TokenKind::Exact(value))
                };
            }
            if single && ch.is_whitespace() {
                if !value.is_empty() {
                    words.push((std::mem::take(&mut value), span(word_start, self.offset)));
                }
                self.advance(ch);
                word_start = self.offset;
            } else {
                if value.is_empty() {
                    word_start = self.offset;
                }
                if ch == '\\' {
                    value.push(self.escaped()?);
                } else {
                    value.push(ch);
                    self.advance(ch);
                }
            }
        }
    }
}

struct Parser<'a> {
    scanner: Scanner<'a>,
    lookahead: Option<Token>,
    fields: &'a [String],
    nodes: usize,
}

impl Parser<'_> {
    fn peek(&mut self) -> Result<&Token, ParseError> {
        let token = self.take()?;
        Ok(self.lookahead.insert(token))
    }

    fn take(&mut self) -> Result<Token, ParseError> {
        match self.lookahead.take() {
            Some(token) => Ok(token),
            None => self.scanner.next(),
        }
    }

    fn node(&mut self, kind: ExprKind, span: ByteSpan) -> Result<Expr, ParseError> {
        if self.nodes >= MAX_QUERY_NODES {
            return Err(error(ParseErrorKind::TooManyNodes, span));
        }
        self.nodes += 1;
        Ok(Expr { kind, span })
    }

    fn nested(&self, depth: usize, span: ByteSpan) -> Result<usize, ParseError> {
        if depth >= MAX_QUERY_NESTING {
            return Err(error(ParseErrorKind::NestingTooDeep, span));
        }
        Ok(depth + 1)
    }

    fn or(&mut self, depth: usize, scoped: bool) -> Result<Expr, ParseError> {
        let first = self.and(depth, scoped)?;
        let start = first.span.start;
        let mut end = first.span.end;
        let mut expressions = vec![first];
        while matches!(self.peek()?.kind, TokenKind::Or) {
            self.take()?;
            let next = self.and(depth, scoped)?;
            end = next.span.end;
            expressions.push(next);
        }
        self.chain(expressions, false, span(start, end))
    }

    fn and(&mut self, depth: usize, scoped: bool) -> Result<Expr, ParseError> {
        let first = self.operand(depth, scoped)?;
        let start = first.span.start;
        let mut end = first.span.end;
        let mut expressions = vec![first];
        loop {
            let token = self.peek()?;
            if matches!(token.kind, TokenKind::And) {
                self.take()?;
            } else if token.kind.starts_operand() {
                if token.span.start == end {
                    return Err(error(ParseErrorKind::MissingSeparator, token.span));
                }
            } else {
                break;
            }
            let next = self.operand(depth, scoped)?;
            end = next.span.end;
            expressions.push(next);
        }
        self.chain(expressions, true, span(start, end))
    }

    fn chain(
        &mut self,
        mut expressions: Vec<Expr>,
        and: bool,
        span: ByteSpan,
    ) -> Result<Expr, ParseError> {
        if expressions.len() == 1 {
            return expressions
                .pop()
                .ok_or_else(|| error(ParseErrorKind::ExpectedOperand, span));
        }
        self.node(
            if and {
                ExprKind::And(expressions)
            } else {
                ExprKind::Or(expressions)
            },
            span,
        )
    }

    fn operand(&mut self, depth: usize, scoped: bool) -> Result<Expr, ParseError> {
        let token = self.take()?;
        match token.kind {
            TokenKind::Term(value) => self.node(ExprKind::Term(value), token.span),
            TokenKind::Exact(value) => self.node(ExprKind::Exact(value), token.span),
            TokenKind::Prefix(value) => self.node(ExprKind::Prefix(value), token.span),
            TokenKind::Not | TokenKind::Minus => {
                let inner_depth = self.nested(depth, token.span)?;
                let inner = self.operand(inner_depth, scoped)?;
                let range = span(token.span.start, inner.span.end);
                self.node(ExprKind::Not(Box::new(inner)), range)
            }
            TokenKind::Field(name) => {
                if scoped {
                    return Err(error(ParseErrorKind::NestedField, token.span));
                }
                if !self.fields.contains(&name) {
                    return Err(error(ParseErrorKind::UnknownField, token.span));
                }
                let inner_depth = self.nested(depth, token.span)?;
                let next = self.peek()?;
                if next.span.start != token.span.end || !next.kind.starts_operand() {
                    return Err(error(ParseErrorKind::InvalidFieldSyntax, next.span));
                }
                let inner = self.operand(inner_depth, true)?;
                let range = span(token.span.start, inner.span.end);
                self.node(
                    ExprKind::Field {
                        name,
                        expression: Box::new(inner),
                    },
                    range,
                )
            }
            TokenKind::Open => {
                let inner_depth = self.nested(depth, token.span)?;
                if matches!(self.peek()?.kind, TokenKind::Close) {
                    return Err(error(
                        ParseErrorKind::EmptyGroup,
                        span(token.span.start, self.take()?.span.end),
                    ));
                }
                let inner = self.or(inner_depth, scoped)?;
                let close = self.take()?;
                if !matches!(close.kind, TokenKind::Close) {
                    return Err(error(ParseErrorKind::UnclosedParenthesis, token.span));
                }
                self.node(
                    ExprKind::Group {
                        origin: GroupOrigin::Parentheses,
                        expression: Box::new(inner),
                    },
                    span(token.span.start, close.span.end),
                )
            }
            TokenKind::Words(words) => {
                self.nested(depth, token.span)?;
                let mut expressions = Vec::new();
                let mut inner_span = token.span;
                for (index, (value, range)) in words.into_iter().enumerate() {
                    if index == 0 {
                        inner_span.start = range.start;
                    }
                    inner_span.end = range.end;
                    expressions.push(self.node(ExprKind::Term(value), range)?);
                }
                let inner = self.chain(expressions, true, inner_span)?;
                self.node(
                    ExprKind::Group {
                        origin: GroupOrigin::SingleQuotes,
                        expression: Box::new(inner),
                    },
                    token.span,
                )
            }
            _ => Err(error(ParseErrorKind::ExpectedOperand, token.span)),
        }
    }
}
