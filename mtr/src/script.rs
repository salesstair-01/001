//! The mysqltest language: a test file is SQL statements ended by the
//! current delimiter, mysqltest commands (`--echo x`, or `echo x;`), and
//! `if (...) { }` / `while (...) { }` blocks.

/// One step of a test file.
#[derive(Debug, Clone)]
pub enum Stmt {
    /// A statement sent to the server; `delim` is the delimiter it ended with.
    Query {
        sql: String,
        delim: String,
        line: usize,
    },
    /// A mysqltest command, with the rest of its text as the argument.
    Cmd {
        name: String,
        arg: String,
        line: usize,
    },
    If {
        cond: String,
        body: Vec<Stmt>,
        els: Vec<Stmt>,
    },
    While {
        cond: String,
        body: Vec<Stmt>,
    },
}

/// mysqltest's commands; a statement whose first word is one of these is a
/// command, not SQL.
const COMMANDS: &[&str] = &[
    "append_file",
    "assert",
    "cat_file",
    "change_user",
    "character_set",
    "chmod",
    "connect",
    "connection",
    "copy_file",
    "dec",
    "delimiter",
    "die",
    "diff_files",
    "disable_abort_on_error",
    "disable_async_client",
    "disable_connect_log",
    "disable_info",
    "disable_metadata",
    "disable_ps_protocol",
    "disable_query_log",
    "disable_reconnect",
    "disable_result_log",
    "disable_session_track_info",
    "disable_testcase",
    "disable_warnings",
    "disconnect",
    "echo",
    "enable_abort_on_error",
    "enable_async_client",
    "enable_connect_log",
    "enable_info",
    "enable_metadata",
    "enable_ps_protocol",
    "enable_query_log",
    "enable_reconnect",
    "enable_result_log",
    "enable_session_track_info",
    "enable_testcase",
    "enable_warnings",
    "end_timer",
    "error",
    "eval",
    "exec",
    "exec_in_background",
    "execw",
    "exit",
    "expr",
    "file_exists",
    "force-cpdir",
    "force-rmdir",
    "horizontal_results",
    "inc",
    "let",
    "list_files",
    "list_files_append_file",
    "list_files_write_file",
    "lowercase_result",
    "mkdir",
    "move_file",
    "output",
    "partially_sorted_result",
    "perl",
    "ping",
    "query",
    "query_attributes",
    "query_horizontal",
    "query_vertical",
    "real_sleep",
    "reap",
    "remove_file",
    "remove_files_wildcard",
    "replace_column",
    "replace_numeric_round",
    "replace_regex",
    "replace_result",
    "require",
    "reset_connection",
    "result_format",
    "rmdir",
    "save_master_pos",
    "send",
    "send_eval",
    "send_quit",
    "send_shutdown",
    "shutdown_server",
    "skip",
    "skip_if_hypergraph",
    "sleep",
    "sorted_result",
    "source",
    "start_timer",
    "sync_slave_with_master",
    "sync_with_master",
    "system",
    "vertical_results",
    "wait_for_slave_to_stop",
    "write_file",
];

/// Commands whose body follows on the next lines, up to a terminator line
/// (`EOF` unless given).
const HEREDOC: &[&str] = &["write_file", "append_file", "perl"];

pub fn is_command(w: &str) -> bool {
    COMMANDS.contains(&w)
}

pub fn parse(text: &str) -> Vec<Stmt> {
    let mut p = Parser {
        s: text.as_bytes(),
        i: 0,
        line: 1,
        delim: ";".into(),
    };
    let (stmts, _) = p.block(false);
    stmts
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    line: usize,
    delim: String,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.i += 1;
        if c == b'\n' {
            self.line += 1;
        }
        Some(c)
    }

    fn rest_starts(&self, p: &str) -> bool {
        self.s[self.i..].starts_with(p.as_bytes())
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\r' | b'\n')) {
            self.bump();
        }
    }

    fn take_line(&mut self) -> String {
        let start = self.i;
        while let Some(c) = self.peek() {
            if c == b'\n' {
                break;
            }
            self.bump();
        }
        let l = String::from_utf8_lossy(&self.s[start..self.i]).into_owned();
        self.bump();
        l
    }

    /// Statements up to the end of input, or up to a `}` when `inner`.
    /// Returns them and whether the block ended with `}`.
    fn block(&mut self, inner: bool) -> (Vec<Stmt>, bool) {
        let mut out = vec![];
        loop {
            self.skip_ws();
            let Some(c) = self.peek() else {
                return (out, false);
            };
            let line = self.line;
            if c == b'#' {
                self.take_line();
                continue;
            }
            if c == b'}' && inner {
                self.bump();
                return (out, true);
            }
            if self.rest_starts("--") {
                let l = self.take_line();
                let l = l[2..].trim();
                if l.is_empty() {
                    continue;
                }
                let (name, arg) = split_word(l);
                let name = &name.to_ascii_lowercase();
                if name == "delimiter" {
                    self.delim = arg.trim().to_string();
                    continue;
                }
                if name == "disable_parsing" {
                    self.skip_parsing_off();
                    continue;
                }
                let name = name.to_string();
                if HEREDOC.contains(&name.as_str()) {
                    self.heredoc(arg);
                }
                out.push(Stmt::Cmd {
                    name,
                    arg: arg.to_string(),
                    line,
                });
                continue;
            }
            let word = self.word_at();
            let lw = word.to_ascii_lowercase();
            if (lw == "if" || lw == "while") && self.paren_follows(word.len()) {
                for _ in 0..word.len() {
                    self.bump();
                }
                let cond = self.paren();
                let (body, _) = self.braced();
                if lw == "while" {
                    out.push(Stmt::While { cond, body });
                    continue;
                }
                let mut els = vec![];
                self.skip_ws();
                if self.word_at() == "else" {
                    for _ in 0..4 {
                        self.bump();
                    }
                    els = self.braced().0;
                }
                out.push(Stmt::If { cond, body, els });
                continue;
            }
            let text = format_v1(&self.until_delim());
            let text = text.trim_start();
            let (w, arg) = split_word(text);
            // mysqltest's command names are case-insensitive.
            let lw = w.to_ascii_lowercase();
            let w = lw.as_str();
            if is_command(w) {
                if w == "delimiter" {
                    self.delim = arg.trim().to_string();
                    continue;
                }
                if HEREDOC.contains(&w) {
                    self.heredoc(arg);
                }
                out.push(Stmt::Cmd {
                    name: w.to_string(),
                    arg: arg.to_string(),
                    line,
                });
            } else if w == "disable_parsing" {
                self.skip_parsing_off();
            } else if !text.trim().is_empty() {
                out.push(Stmt::Query {
                    sql: text.to_string(),
                    delim: self.delim.clone(),
                    line,
                });
            }
        }
    }

    /// A `{ ... }` block (the brace may be on the next line).
    fn braced(&mut self) -> (Vec<Stmt>, bool) {
        self.skip_ws();
        if self.peek() == Some(b'{') {
            self.bump();
            self.block(true)
        } else {
            (vec![], false)
        }
    }

    fn word_at(&self) -> String {
        let mut j = self.i;
        while j < self.s.len() && (self.s[j].is_ascii_alphanumeric() || self.s[j] == b'_') {
            j += 1;
        }
        String::from_utf8_lossy(&self.s[self.i..j]).into_owned()
    }

    fn paren_follows(&self, n: usize) -> bool {
        let mut j = self.i + n;
        while j < self.s.len() && (self.s[j] == b' ' || self.s[j] == b'\t') {
            j += 1;
        }
        self.s.get(j) == Some(&b'(')
    }

    /// The condition of if/while: from `(` to the `)` that closes it.
    fn paren(&mut self) -> String {
        while self.peek().is_some_and(|c| c != b'(') {
            self.bump();
        }
        self.bump();
        let start = self.i;
        let (mut depth, mut quote) = (1, None::<u8>);
        while let Some(c) = self.peek() {
            match quote {
                Some(q) if c == q => quote = None,
                Some(_) => {}
                None if c == b'`' || c == b'\'' || c == b'"' => quote = Some(c),
                None if c == b'(' => depth += 1,
                None if c == b')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                None => {}
            }
            self.bump();
        }
        let cond = String::from_utf8_lossy(&self.s[start..self.i]).into_owned();
        self.bump();
        cond
    }

    /// Text up to the delimiter (outside quotes).
    fn until_delim(&mut self) -> String {
        let d = self.delim.clone().into_bytes();
        let mut out: Vec<u8> = vec![];
        let mut quote = None::<u8>;
        while self.i < self.s.len() {
            let c = self.s[self.i];
            if let Some(q) = quote {
                if c == b'\\' && q != b'`' && self.i + 1 < self.s.len() {
                    out.push(c);
                    self.bump();
                } else if c == q {
                    quote = None;
                }
                out.push(self.s[self.i]);
                self.bump();
                continue;
            }
            if self.s[self.i..].starts_with(&d) {
                for _ in 0..d.len() {
                    self.bump();
                }
                return String::from_utf8_lossy(&out).into_owned();
            }
            if c == b'\'' || c == b'"' || c == b'`' {
                quote = Some(c);
            }
            out.push(c);
            self.bump();
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    fn heredoc(&mut self, arg: &str) {
        // The terminator is EOF unless the command names another one.
        let words: Vec<&str> = arg.split_whitespace().collect();
        let term = if words.len() >= 2 {
            words[1].trim_end_matches(';')
        } else {
            "EOF"
        };
        let term = if term.is_empty() { "EOF" } else { term };
        while self.peek().is_some() {
            if self.take_line().trim() == term {
                break;
            }
        }
    }

    fn skip_parsing_off(&mut self) {
        while self.peek().is_some() {
            let l = self.take_line();
            let l = l.trim().trim_start_matches('-').trim();
            if l.starts_with("enable_parsing") {
                break;
            }
        }
    }
}

/// The first word of `s` and the rest (leading whitespace removed).
pub fn split_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    let end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(s.len());
    (&s[..end], s[end..].trim_start())
}

/// mysqltest's result format 1 (convert_to_format_v1): after a newline,
/// whitespace is skipped unless the newline followed a quoted string. It
/// applies to the command as read, so to what is echoed and what is sent.
pub fn format_v1(q: &str) -> String {
    let b = q.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let (mut i, mut after_quote) = (0, false);
    while i < b.len() {
        let c = b[i];
        if c == b'\n' && !after_quote {
            out.push(c);
            i += 1;
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            after_quote = false;
        } else if c == b'\'' || c == b'"' || c == b'`' {
            out.push(c);
            i += 1;
            while i < b.len() && b[i] != c {
                out.push(b[i]);
                i += 1;
            }
            if i < b.len() {
                out.push(b[i]);
                i += 1;
            }
            after_quote = true;
        } else {
            out.push(c);
            i += 1;
            after_quote = false;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(stmts: &[Stmt]) -> Vec<String> {
        stmts
            .iter()
            .map(|s| match s {
                Stmt::Query { sql, delim, .. } => format!("Q {sql}{delim}"),
                Stmt::Cmd { name, arg, .. } => format!("C {name} {arg}"),
                Stmt::If { body, els, .. } => format!("IF {} {}", body.len(), els.len()),
                Stmt::While { body, .. } => format!("WHILE {}", body.len()),
            })
            .collect()
    }

    #[test]
    fn statements_commands_and_delimiters() {
        let s = parse(
            "# comment\n--echo hello\nselect 1; select 'a;b';\nDELIMITER |;\ncreate procedure p()\nbegin\n  select 1;\nend|\ndelimiter ;|\nlet $a= 1;\n--error ER_DUP_ENTRY\ninsert into t values (1);\n",
        );
        assert_eq!(
            kinds(&s),
            [
                "C echo hello",
                "Q select 1;",
                "Q select 'a;b';",
                "Q create procedure p()\nbegin\nselect 1;\nend|",
                "C let $a= 1",
                "C error ER_DUP_ENTRY",
                "Q insert into t values (1);",
            ]
        );
    }

    #[test]
    fn blocks() {
        let s = parse(
            "if ($a)\n{\n  select 1;\n}\nelse\n{\n  select 2;\n  select 3;\n}\nwhile ($i) {\n  dec $i;\n}\n",
        );
        assert_eq!(kinds(&s), ["IF 1 2", "WHILE 1"]);
    }

    #[test]
    fn result_format_1() {
        // Indentation after a newline goes, unless the newline follows a quote.
        assert_eq!(
            format_v1("CREATE TABLE t (\n  a int,\n\n  b int)"),
            "CREATE TABLE t (\na int,\nb int)"
        );
        assert_eq!(
            format_v1("CALL p(\"a \"\n   \"b\")"),
            "CALL p(\"a \"\n   \"b\")"
        );
    }

    #[test]
    fn heredoc_and_disabled_parsing() {
        let s = parse(
            "--write_file $f\nselect 1;\nEOF\n--disable_parsing\nselect 2;\n--enable_parsing\nselect 3;\n",
        );
        assert_eq!(kinds(&s), ["C write_file $f", "Q select 3;"]);
    }
}
