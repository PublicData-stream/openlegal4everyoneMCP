//! Contract tests for query syntax; matching and provider behavior are out of scope.
use openlegal_domain::search_query::{
    ByteSpan, Expr, ExprKind, GroupOrigin, MAX_QUERY_BYTES, MAX_QUERY_FIELD_BYTES,
    MAX_QUERY_FIELDS, MAX_QUERY_NESTING, MAX_QUERY_NODES, ParseErrorKind, QueryConfigError,
};
use openlegal_normalization::search_query::SearchQueryProcessor;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Shape {
    All,
    Term(String),
    Exact(String),
    Prefix(String),
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Field(String, Box<Self>),
    Group(GroupOrigin, Box<Self>),
}

fn shape(expression: &Expr) -> Shape {
    match &expression.kind {
        ExprKind::MatchAll => Shape::All,
        ExprKind::Term(text) => term(text),
        ExprKind::Exact(text) => Shape::Exact(text.clone()),
        ExprKind::Prefix(text) => Shape::Prefix(text.clone()),
        ExprKind::Not(child) => not(shape(child)),
        ExprKind::And(children) => Shape::And(children.iter().map(shape).collect()),
        ExprKind::Or(children) => Shape::Or(children.iter().map(shape).collect()),
        ExprKind::Field { name, expression } => field(name, shape(expression)),
        ExprKind::Group { origin, expression } => group(*origin, shape(expression)),
    }
}

fn term(value: &str) -> Shape {
    Shape::Term(value.into())
}

fn not(value: Shape) -> Shape {
    Shape::Not(Box::new(value))
}

fn field(name: &str, value: Shape) -> Shape {
    Shape::Field(name.into(), Box::new(value))
}

fn group(origin: GroupOrigin, value: Shape) -> Shape {
    Shape::Group(origin, Box::new(value))
}

fn processor() -> SearchQueryProcessor {
    SearchQueryProcessor::new(&["title", "body"]).unwrap()
}

fn check_span(source: &str, span: ByteSpan) {
    assert!(span.start <= span.end, "reversed span: {span:?}");
    assert!(span.end <= source.len(), "span outside source: {span:?}");
    assert!(source.is_char_boundary(span.start));
    assert!(source.is_char_boundary(span.end));
}

fn check_expression(source: &str, expression: &Expr) -> usize {
    check_span(source, expression.span);
    let children: Vec<&Expr> = match &expression.kind {
        ExprKind::And(children) | ExprKind::Or(children) => {
            assert!(children.len() >= 2);
            children.iter().collect()
        }
        ExprKind::Not(child)
        | ExprKind::Field {
            expression: child, ..
        }
        | ExprKind::Group {
            expression: child, ..
        } => vec![child],
        _ => Vec::new(),
    };
    1 + children
        .into_iter()
        .map(|child| {
            assert!(child.span.start >= expression.span.start);
            assert!(child.span.end <= expression.span.end);
            check_expression(source, child)
        })
        .sum::<usize>()
}

fn syntax_nesting(expression: &Expr) -> usize {
    match &expression.kind {
        ExprKind::Not(child)
        | ExprKind::Field {
            expression: child, ..
        }
        | ExprKind::Group {
            expression: child, ..
        } => 1 + syntax_nesting(child),
        ExprKind::And(children) | ExprKind::Or(children) => {
            children.iter().map(syntax_nesting).max().unwrap_or(0)
        }
        _ => 0,
    }
}

fn expect(source: &str, expected: Shape) {
    let parsed = processor().parse(source).unwrap();
    assert_eq!(parsed.source, source);
    assert_eq!(shape(&parsed.expression), expected, "query: {source:?}");
    assert!(check_expression(source, &parsed.expression) <= MAX_QUERY_NODES);
    assert!(syntax_nesting(&parsed.expression) <= MAX_QUERY_NESTING);
}

fn expect_error(source: &str, kind: ParseErrorKind) {
    let error = processor().parse(source).unwrap_err();
    assert_eq!(error.kind, kind, "query: {source:?}");
    check_span(source, error.span);
}

#[test]
fn boolean_precedence_and_source_order_are_preserved() {
    expect(
        "alpha OR beta gamma AND NOT delta",
        Shape::Or(vec![
            term("alpha"),
            Shape::And(vec![term("beta"), term("gamma"), not(term("delta"))]),
        ]),
    );
    expect("z a z", Shape::And(vec![term("z"), term("a"), term("z")]));
    expect(
        "a OR b OR c",
        Shape::Or(vec![term("a"), term("b"), term("c")]),
    );
    expect(
        "NOT (alpha OR beta) gamma",
        Shape::And(vec![
            not(group(
                GroupOrigin::Parentheses,
                Shape::Or(vec![term("alpha"), term("beta")]),
            )),
            term("gamma"),
        ]),
    );
    expect(
        "NOT(foo)",
        not(group(GroupOrigin::Parentheses, term("foo"))),
    );
    expect("- word", not(term("word")));
    expect("NOT -word", not(not(term("word"))));
}

#[test]
fn single_quote_group_negation_differs_from_exact_phrase_negation() {
    expect(
        "license -'properly installed'",
        Shape::And(vec![
            term("license"),
            not(group(
                GroupOrigin::SingleQuotes,
                Shape::And(vec![term("properly"), term("installed")]),
            )),
        ]),
    );
    expect(
        "license -\"properly installed\"",
        Shape::And(vec![
            term("license"),
            not(Shape::Exact("properly installed".into())),
        ]),
    );
    expect("'word'", group(GroupOrigin::SingleQuotes, term("word")));
    expect("(word)", group(GroupOrigin::Parentheses, term("word")));
    expect(
        "'AND -word in:title:x (a) pre*'",
        group(
            GroupOrigin::SingleQuotes,
            Shape::And(vec![
                term("AND"),
                term("-word"),
                term("in:title:x"),
                term("(a)"),
                term("pre*"),
            ]),
        ),
    );
}

#[test]
fn fields_bind_only_their_operand_and_accept_unary_operands() {
    expect(
        "in:title:alpha beta",
        Shape::And(vec![field("title", term("alpha")), term("beta")]),
    );
    expect("in:title:-word", field("title", not(term("word"))));
    expect("-in:title:word", not(field("title", term("word"))));
    expect(
        "in:title:\"Exact Phrase\"",
        field("title", Shape::Exact("Exact Phrase".into())),
    );
    expect(
        "in:body:'two words'",
        field(
            "body",
            group(
                GroupOrigin::SingleQuotes,
                Shape::And(vec![term("two"), term("words")]),
            ),
        ),
    );
    expect(
        "in:title:(a OR b) in:body:pre*",
        Shape::And(vec![
            field(
                "title",
                group(
                    GroupOrigin::Parentheses,
                    Shape::Or(vec![term("a"), term("b")]),
                ),
            ),
            field("body", Shape::Prefix("pre".into())),
        ]),
    );
}

#[test]
fn literal_case_unicode_punctuation_and_spacing_are_not_normalized() {
    expect(
        "법률 Café e\u{301} A-a",
        Shape::And(vec![
            term("법률"),
            term("Café"),
            term("e\u{301}"),
            term("A-a"),
        ]),
    );
    expect(
        "and or not CANDOR",
        Shape::And(vec![term("and"), term("or"), term("not"), term("CANDOR")]),
    );
    expect(
        "\"  Properly  Installed!  \"",
        Shape::Exact("  Properly  Installed!  ".into()),
    );
    expect("\"   \"", Shape::Exact("   ".into()));
    expect(
        "https://example.test article:5 site:example",
        Shape::And(vec![
            term("https://example.test"),
            term("article:5"),
            term("site:example"),
        ]),
    );
    expect(
        "don't l'année 한'글",
        Shape::And(vec![term("don't"), term("l'année"), term("한'글")]),
    );
    expect(
        "'don't l'année 한'글'",
        group(
            GroupOrigin::SingleQuotes,
            Shape::And(vec![term("don't"), term("l'année"), term("한'글")]),
        ),
    );
}

#[test]
fn escapes_decode_one_scalar_without_creating_operators() {
    expect(
        r"\AND \OR \NOT \-word \in:title:x pre\*",
        Shape::And(vec![
            term("AND"),
            term("OR"),
            term("NOT"),
            term("-word"),
            term("in:title:x"),
            term("pre*"),
        ]),
    );
    expect(
        r"two\ words \(x\) a\\b \n \법",
        Shape::And(vec![
            term("two words"),
            term("(x)"),
            term("a\\b"),
            term("n"),
            term("법"),
        ]),
    );
    expect(r#""a\"b\\c\n""#, Shape::Exact("a\"b\\cn".into()));
    expect(
        r"'two\ words can\'t'",
        group(
            GroupOrigin::SingleQuotes,
            Shape::And(vec![term("two words"), term("can't")]),
        ),
    );
}

#[test]
fn empty_and_unicode_whitespace_queries_have_explicit_meaning() {
    for source in ["", "   ", "\u{a0}\u{2003}\u{3000}"] {
        expect(source, Shape::All);
    }
    expect(
        "a\u{a0}b\u{3000}c",
        Shape::And(vec![term("a"), term("b"), term("c")]),
    );
    expect(
        "'a\u{2003}b'",
        group(
            GroupOrigin::SingleQuotes,
            Shape::And(vec![term("a"), term("b")]),
        ),
    );
}

#[test]
fn prefix_operator_is_distinct_from_literal_stars() {
    expect("법*", Shape::Prefix("법".into()));
    expect("\"pre*\"", Shape::Exact("pre*".into()));
    expect("'pre*'", group(GroupOrigin::SingleQuotes, term("pre*")));
    for source in [
        "*",
        "*word",
        "wo*rd",
        "word**",
        "\"word\"*",
        "'word'*",
        "(word)*",
    ] {
        assert!(
            processor().parse(source).is_err(),
            "accepted wildcard form {source:?}"
        );
    }
    for source in ["*", "*word", "wo*rd", "word**"] {
        expect_error(source, ParseErrorKind::InvalidWildcard);
    }
}

#[test]
fn malformed_operators_groups_and_quotes_return_typed_errors() {
    for source in ["AND word", "OR word", "word AND", "word OR", "NOT", "-"] {
        expect_error(source, ParseErrorKind::ExpectedOperand);
    }
    for source in ["()", "( )", "''", "' \u{2003} '"] {
        expect_error(source, ParseErrorKind::EmptyGroup);
    }
    expect_error("\"\"", ParseErrorKind::EmptyPhrase);
    expect_error("(word", ParseErrorKind::UnclosedParenthesis);
    expect_error("\"word", ParseErrorKind::UnclosedQuote);
    expect_error("'word", ParseErrorKind::UnclosedQuote);
    expect_error("word\\", ParseErrorKind::TrailingEscape);
    for source in ["word\"phrase\"", "word(foo)", "(a)(b)", "\"a\"b"] {
        expect_error(source, ParseErrorKind::MissingSeparator);
    }
    for source in [")", "word )", "word AND OR next"] {
        assert!(
            processor().parse(source).is_err(),
            "accepted malformed query {source:?}"
        );
    }
}

#[test]
fn field_errors_are_distinct_and_nested_scopes_are_rejected() {
    expect_error("in:missing:word", ParseErrorKind::UnknownField);
    for source in ["in:", "in:title", "in::word", "in:title: word"] {
        expect_error(source, ParseErrorKind::InvalidFieldSyntax);
    }
    for source in [
        "in:title:in:body:word",
        "in:title:(word OR in:body:other)",
        "in:title:(NOT in:title:word)",
    ] {
        expect_error(source, ParseErrorKind::NestedField);
    }
    let empty = SearchQueryProcessor::new(&[]).unwrap();
    assert_eq!(
        empty.parse("in:title:word").unwrap_err().kind,
        ParseErrorKind::UnknownField
    );
    assert!(empty.parse("ordinary words").is_ok());
}

#[test]
fn controls_are_rejected_even_when_quoted_or_escaped() {
    for control in ['\0', '\t', '\n', '\r', '\u{7f}', '\u{85}'] {
        for source in [
            format!("a{control}b"),
            format!("\"a{control}b\""),
            format!("a\\{control}b"),
        ] {
            expect_error(&source, ParseErrorKind::ForbiddenControl);
        }
    }
}

#[test]
fn byte_spans_refer_to_original_utf8_not_decoded_text() {
    let parsed = processor().parse("법률 \"é x\" 한\\ 글").unwrap();
    let ExprKind::And(children) = &parsed.expression.kind else {
        panic!("expected conjunction")
    };
    assert_eq!(parsed.expression.span, ByteSpan { start: 0, end: 22 });
    assert_eq!(children[0].span, ByteSpan { start: 0, end: 6 });
    assert_eq!(children[1].span, ByteSpan { start: 7, end: 13 });
    assert_eq!(children[2].span, ByteSpan { start: 14, end: 22 });
    assert_eq!(shape(&children[2]), term("한 글"));
    check_expression(&parsed.source, &parsed.expression);
}

#[test]
fn configuration_limits_are_validated_before_parsing() {
    for fields in [
        &["Title"][..],
        &[""][..],
        &["a-b"][..],
        &["1field"][..],
        &["법"][..],
    ] {
        assert!(matches!(
            SearchQueryProcessor::new(fields),
            Err(QueryConfigError::InvalidFieldName)
        ));
    }
    assert!(matches!(
        SearchQueryProcessor::new(&["title", "title"]),
        Err(QueryConfigError::DuplicateField)
    ));
    let largest_name = "a".repeat(MAX_QUERY_FIELD_BYTES);
    assert!(SearchQueryProcessor::new(&[&largest_name]).is_ok());
    assert!(matches!(
        SearchQueryProcessor::new(&[&format!("{largest_name}a")]),
        Err(QueryConfigError::InvalidFieldName)
    ));
    let names: Vec<_> = (0..MAX_QUERY_FIELDS)
        .map(|i| format!("field_{i}"))
        .collect();
    let mut references: Vec<_> = names.iter().map(String::as_str).collect();
    assert!(SearchQueryProcessor::new(&references).is_ok());
    references.push("another");
    assert!(matches!(
        SearchQueryProcessor::new(&references),
        Err(QueryConfigError::TooManyFields)
    ));
}

#[test]
fn input_limit_counts_utf8_bytes() {
    let accepted = "é".repeat(MAX_QUERY_BYTES / 2);
    expect(&accepted, term(&accepted));
    expect_error(&format!("{accepted}a"), ParseErrorKind::InputTooLong);
    expect_error(
        &"a".repeat(MAX_QUERY_BYTES + 1),
        ParseErrorKind::InputTooLong,
    );
}

#[test]
fn associative_chains_count_one_wrapper_without_consuming_nesting() {
    for separator in [" ", " AND ", " OR "] {
        let accepted = vec!["a"; MAX_QUERY_NODES - 1].join(separator);
        let parsed = processor().parse(&accepted).unwrap();
        assert_eq!(
            check_expression(&accepted, &parsed.expression),
            MAX_QUERY_NODES
        );
        let rejected = vec!["a"; MAX_QUERY_NODES].join(separator);
        expect_error(&rejected, ParseErrorKind::TooManyNodes);
    }
    let accepted = format!("'{}'", vec!["a"; MAX_QUERY_NODES - 2].join(" "));
    let parsed = processor().parse(&accepted).unwrap();
    assert_eq!(
        check_expression(&accepted, &parsed.expression),
        MAX_QUERY_NODES
    );
    expect_error(
        &format!("'{}'", vec!["a"; MAX_QUERY_NODES - 1].join(" ")),
        ParseErrorKind::TooManyNodes,
    );
}

#[test]
fn nesting_limit_covers_groups_unary_chains_and_field_operands() {
    let parenthesized = |depth| format!("{}a{}", "(".repeat(depth), ")".repeat(depth));
    assert!(processor().parse(&parenthesized(MAX_QUERY_NESTING)).is_ok());
    expect_error(
        &parenthesized(MAX_QUERY_NESTING + 1),
        ParseErrorKind::NestingTooDeep,
    );
    for unary in ["NOT ", "-"] {
        assert!(
            processor()
                .parse(&format!("{}a", unary.repeat(MAX_QUERY_NESTING)))
                .is_ok()
        );
        expect_error(
            &format!("{}a", unary.repeat(MAX_QUERY_NESTING + 1)),
            ParseErrorKind::NestingTooDeep,
        );
    }
    assert!(
        processor()
            .parse(&format!(
                "in:title:{}",
                parenthesized(MAX_QUERY_NESTING - 1)
            ))
            .is_ok()
    );
    expect_error(
        &format!("in:title:{}", parenthesized(MAX_QUERY_NESTING)),
        ParseErrorKind::NestingTooDeep,
    );
}

#[test]
fn generated_unicode_queries_preserve_values_and_valid_spans() {
    let parser = processor();
    for scalar in ['법', 'é', '\u{301}', '🦀', 'Ж', '中', 'א'] {
        for count in 1..=40 {
            let word: String = std::iter::repeat_n(scalar, count).collect();
            let source = format!("{word} OR in:title:\"{word} {word}\"");
            let first = parser.parse(&source).unwrap();
            let second = parser.parse(&source).unwrap();
            assert_eq!(first, second);
            assert_eq!(first.source, source);
            assert_eq!(
                shape(&first.expression),
                Shape::Or(vec![
                    term(&word),
                    field("title", Shape::Exact(format!("{word} {word}")))
                ])
            );
            assert!(check_expression(&source, &first.expression) <= MAX_QUERY_NODES);
        }
    }
}

#[test]
fn generated_valid_groups_cover_nested_operators_and_scope_boundaries() {
    for word in ["alpha", "한글", "e\u{301}", "🦀"] {
        let mut source = word.to_owned();
        let mut expected = term(word);
        for step in 0..12 {
            match step % 3 {
                0 => {
                    source = format!("NOT (({source}) OR 'a b')");
                    expected = not(group(
                        GroupOrigin::Parentheses,
                        Shape::Or(vec![
                            group(GroupOrigin::Parentheses, expected),
                            group(
                                GroupOrigin::SingleQuotes,
                                Shape::And(vec![term("a"), term("b")]),
                            ),
                        ]),
                    ));
                }
                1 => {
                    source = format!("({source}) AND in:title:-pre*");
                    expected = Shape::And(vec![
                        group(GroupOrigin::Parentheses, expected),
                        field("title", not(Shape::Prefix("pre".into()))),
                    ]);
                }
                _ => {
                    source = format!("\"{word}  exact\" OR ({source})");
                    expected = Shape::Or(vec![
                        Shape::Exact(format!("{word}  exact")),
                        group(GroupOrigin::Parentheses, expected),
                    ]);
                }
            }
            expect(&source, expected.clone());
        }
    }
}

#[test]
fn escaped_unicode_scalars_have_literal_values_and_original_spans() {
    let parser = processor();
    // Deterministically sample the full scalar range, including supplementary planes.
    for value in (0..=0x10_ffff).step_by(1021) {
        let Some(scalar) = char::from_u32(value) else {
            continue;
        };
        let source = format!("\\{scalar}");
        if scalar.is_control() {
            expect_error(&source, ParseErrorKind::ForbiddenControl);
        } else {
            let parsed = parser.parse(&source).unwrap();
            assert_eq!(shape(&parsed.expression), term(&scalar.to_string()));
            assert_eq!(
                parsed.expression.span,
                ByteSpan {
                    start: 0,
                    end: source.len()
                }
            );
        }
    }
}

#[test]
fn generated_mixed_syntax_always_returns_bounded_valid_spans() {
    let parser = processor();
    let fragments = [
        "법",
        "🦀",
        "é",
        "'",
        "\"",
        "\\",
        "(",
        ")",
        "-",
        "*",
        ":",
        " ",
        "AND",
        "OR",
        "NOT",
        "in:title:",
    ];
    let mut state = 17_u64;
    for length in 1..=160 {
        let mut source = String::new();
        for _ in 0..length {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            source.push_str(fragments[(state >> 32) as usize % fragments.len()]);
        }
        let result = parser.parse(&source);
        assert_eq!(result, parser.parse(&source));
        match result {
            Ok(parsed) => {
                assert_eq!(parsed.source, source);
                assert!(check_expression(&source, &parsed.expression) <= MAX_QUERY_NODES);
                assert!(syntax_nesting(&parsed.expression) <= MAX_QUERY_NESTING);
            }
            Err(error) => check_span(&source, error.span),
        }
    }
}
