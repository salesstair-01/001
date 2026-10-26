//! Lines our output up with the expected .result and scores each statement.
//!
//! The output is a list of segments, each announced by a head (the echoed
//! statement, or an --echo line). Each head is found in the expected text
//! in order; a segment's expected body is the text from its head up to the
//! next head found. A statement passes when its body matches, or when it
//! failed with one of the errors the test expects (`--error`): error
//! messages are not compared, codes are.

use std::collections::HashSet;

use crate::run::Segment;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    /// The statement failed where the test expects it to succeed.
    OurError,
    /// The test expects an error and the statement succeeded.
    MissingError,
    /// Both failed, with different error codes.
    WrongError,
    /// Different rows, warnings or info.
    WrongResult,
    /// The statement's echo was not found in the expected result.
    Unaligned,
}

impl Kind {
    pub fn label(self) -> &'static str {
        match self {
            Kind::OurError => "fails, MySQL succeeds",
            Kind::MissingError => "succeeds, MySQL fails",
            Kind::WrongError => "different error",
            Kind::WrongResult => "different result",
            Kind::Unaligned => "not aligned",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Failure {
    pub key: String,
    pub kind: Kind,
    pub sql: String,
    pub ours: String,
    pub expected: String,
    pub our_error: Option<(u16, String)>,
}

#[derive(Debug, Default)]
pub struct Score {
    pub passed: usize,
    pub failures: Vec<Failure>,
    /// Statements left out because they fail on MySQL too (baseline).
    pub excluded: usize,
}

struct Group<'a> {
    head: &'a str,
    body: String,
    stmts: Vec<&'a Segment>,
}

pub fn score(out: &[Segment], expected: &str, exclude: &HashSet<String>) -> Score {
    let expected = expected.replace("\r\n", "\n");
    // Segments without a head belong to the one before.
    let mut groups: Vec<Group> = vec![];
    for s in out {
        if s.head.is_empty()
            && let Some(g) = groups.last_mut()
        {
            g.body.push_str(&s.body);
            if s.key.is_some() {
                g.stmts.push(s);
            }
            continue;
        }
        groups.push(Group {
            head: &s.head,
            body: s.body.clone(),
            stmts: if s.key.is_some() { vec![s] } else { vec![] },
        });
    }

    // Where each head is in the expected text. Statements first: every
    // place each statement's echo occurs is a candidate, and the longest
    // run of candidates in increasing order wins (a statement like
    // "drop table t1;" occurs many times, and taking the first occurrence
    // after the previous statement can jump far ahead). Then each --echo
    // line is looked for between the statements around it: echo lines are
    // short and repeated ("#").
    let mut pos: Vec<Option<usize>> = vec![None; groups.len()];
    let mut pairs: Vec<(usize, usize)> = vec![]; // (group, offset), offsets descending per group
    for (i, g) in groups.iter().enumerate() {
        if g.head.is_empty() || g.stmts.is_empty() {
            continue;
        }
        let mut c = occurrences(&expected, g.head);
        c.reverse();
        pairs.extend(c.into_iter().map(|p| (i, p)));
    }
    for (i, p) in longest_increasing(&pairs) {
        pos[i] = Some(p);
    }
    let mut lo = 0;
    for i in 0..groups.len() {
        let g = &groups[i];
        if let Some(p) = pos[i] {
            lo = p + g.head.len();
            continue;
        }
        if g.head.is_empty() || !g.stmts.is_empty() {
            continue;
        }
        let hi = pos[i + 1..]
            .iter()
            .flatten()
            .next()
            .copied()
            .unwrap_or(expected.len());
        if lo >= hi {
            continue;
        }
        if let Some(p) = find_line(&expected[..hi], g.head, lo) {
            pos[i] = Some(p);
            lo = p + g.head.len();
        }
    }

    let mut score = Score::default();
    for (i, g) in groups.iter().enumerate() {
        let exp_body = pos[i].map(|p| {
            let start = p + g.head.len();
            let end = pos[i + 1..]
                .iter()
                .flatten()
                .next()
                .copied()
                .unwrap_or(expected.len());
            &expected[start..end.max(start)]
        });
        for s in &g.stmts {
            let key = s.key.clone().unwrap();
            if exclude.contains(&key) {
                score.excluded += 1;
                continue;
            }
            let fail = |kind, exp: &str| Failure {
                key: key.clone(),
                kind,
                sql: s.sql.clone(),
                ours: g.body.clone(),
                expected: exp.to_string(),
                our_error: s.error.as_ref().map(|e| (e.code, e.msg.clone())),
            };
            let Some(exp) = exp_body else {
                score.failures.push(fail(Kind::Unaligned, ""));
                continue;
            };
            let exp_error = exp.starts_with("ERROR ");
            match (&s.error, s.expected.is_empty()) {
                // An expected error with the expected code: the message may differ.
                (Some(e), false) if s.expected.iter().any(|x| x.matches(e.code, &e.state)) => {
                    score.passed += 1
                }
                _ if g.body == exp => score.passed += 1,
                (Some(_), _) if exp_error => score.failures.push(fail(Kind::WrongError, exp)),
                (Some(_), _) => score.failures.push(fail(Kind::OurError, exp)),
                (None, _) if exp_error => score.failures.push(fail(Kind::MissingError, exp)),
                (None, _) => score.failures.push(fail(Kind::WrongResult, exp)),
            }
        }
    }
    score
}

/// Every position where `needle` starts a line.
fn occurrences(hay: &str, needle: &str) -> Vec<usize> {
    let mut out = vec![];
    let mut i = 0;
    while let Some(off) = hay[i..].find(needle) {
        let p = i + off;
        if p == 0 || hay.as_bytes()[p - 1] == b'\n' {
            out.push(p);
        }
        i = p + 1;
    }
    out
}

/// The longest subsequence of `pairs` whose offsets strictly increase.
/// Within one group the offsets are listed in decreasing order, so at most
/// one per group is taken.
fn longest_increasing(pairs: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut tails: Vec<usize> = vec![]; // index into pairs of the best tail per length
    let mut prev: Vec<Option<usize>> = vec![None; pairs.len()];
    for (k, &(_, p)) in pairs.iter().enumerate() {
        let len = tails.partition_point(|&t| pairs[t].1 < p);
        prev[k] = if len > 0 { Some(tails[len - 1]) } else { None };
        if len == tails.len() {
            tails.push(k);
        } else {
            tails[len] = k;
        }
    }
    let mut out = vec![];
    let mut k = tails.last().copied();
    while let Some(i) = k {
        out.push(pairs[i]);
        k = prev[i];
    }
    out.reverse();
    out
}

/// The first position at or after `from` where `needle` starts a line.
fn find_line(hay: &str, needle: &str, from: usize) -> Option<usize> {
    let mut i = from;
    if i > hay.len() {
        return None;
    }
    while let Some(off) = hay[i..].find(needle) {
        let p = i + off;
        if p == 0 || hay.as_bytes()[p - 1] == b'\n' {
            return Some(p);
        }
        i = p + 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stmt(head: &str, body: &str, key: &str) -> Segment {
        Segment {
            head: head.into(),
            body: body.into(),
            key: Some(key.into()),
            sql: head.into(),
            ..Default::default()
        }
    }

    fn echo(text: &str) -> Segment {
        Segment {
            head: format!("{text}\n"),
            ..Default::default()
        }
    }

    #[test]
    fn aligns_and_scores() {
        let expected = "drop table t1;\nselect 1;\n1\n1\n#\ndrop table t1;\nselect 2;\n2\n2\n";
        let out = vec![
            stmt("drop table t1;\n", "", "f:1#1"),
            stmt("select 1;\n", "1\n1\n", "f:2#1"),
            echo("#"),
            stmt("drop table t1;\n", "", "f:1#2"),
            stmt("select 2;\n", "2\n3\n", "f:3#1"),
        ];
        let s = score(&out, expected, &HashSet::new());
        assert_eq!(s.passed, 3);
        assert_eq!(s.failures.len(), 1);
        assert_eq!(s.failures[0].kind, Kind::WrongResult);
        assert_eq!(s.failures[0].expected, "2\n2\n");
    }

    #[test]
    fn a_missing_statement_does_not_shift_the_rest() {
        // Our run lacks the first "drop table t1;" (say an if went the other
        // way): the later statements still find their own output.
        let expected = "drop table t1;\nselect 1;\n1\n1\ndrop table t1;\nselect 2;\n2\n2\n";
        let out = vec![
            stmt("select 1;\n", "1\n1\n", "f:2#1"),
            stmt("drop table t1;\n", "", "f:4#1"),
            stmt("select 2;\n", "2\n2\n", "f:5#1"),
        ];
        let s = score(&out, expected, &HashSet::new());
        assert_eq!((s.passed, s.failures.len()), (3, 0));
    }

    #[test]
    fn expected_error_codes_pass_and_baseline_excludes() {
        use crate::errors::Expect;
        use crate::run::SqlErr;
        let expected = "insert into t values (1);\nERROR 23000: Duplicate entry '1' for key 't.PRIMARY'\nselect 1;\n2\n";
        let mut ins = stmt(
            "insert into t values (1);\n",
            "ERROR 23000: duplicate key\n",
            "f:1#1",
        );
        ins.error = Some(SqlErr {
            code: 1062,
            state: "23000".into(),
            msg: "duplicate key".into(),
        });
        ins.expected = vec![Expect::Code(1062)];
        let out = vec![ins, stmt("select 1;\n", "1\n", "f:2#1")];
        let s = score(&out, expected, &["f:2#1".to_string()].into_iter().collect());
        assert_eq!((s.passed, s.failures.len(), s.excluded), (1, 0, 1));
    }
}
