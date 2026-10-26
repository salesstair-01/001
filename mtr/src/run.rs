//! Runs a parsed test file against a server and writes what mysqltest would
//! write to its .result file, as segments: each starts with the text that
//! announces it (the echoed statement, an --echo line), so it can be found
//! in the expected result.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Value};
use regex::Regex;

use crate::errors::{Errors, Expect};
use crate::script::{self, Stmt};

/// Where the server is.
#[derive(Clone)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
}

/// One piece of output.
#[derive(Debug, Clone, Default)]
pub struct Segment {
    /// Text that announces the segment in the result (the echoed statement
    /// or an --echo line); empty when nothing is echoed.
    pub head: String,
    /// What follows it: result rows, errors, warnings.
    pub body: String,
    /// For a statement: where it is (`file:line#n`, n counting the times
    /// that line ran), which is what scoring and baselines refer to.
    pub key: Option<String>,
    /// The statement's text, for reports.
    pub sql: String,
    /// The server's error, if the statement failed.
    pub error: Option<SqlErr>,
    /// The errors the test expects (`--error`), if any.
    pub expected: Vec<Expect>,
}

#[derive(Debug, Clone)]
pub struct SqlErr {
    pub code: u16,
    pub state: String,
    pub msg: String,
}

/// A result set (or the OK of a statement without one).
struct Set {
    cols: Option<Vec<String>>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    affected: u64,
    warnings: u16,
    info: String,
}

type Pending = JoinHandle<(Conn, Result<Vec<Set>, SqlErr>)>;

struct Slot {
    conn: Option<Conn>,
    pending: Option<(Pending, String, u16)>, // the sent statement, and its warning count source
    db: String,
}

/// Modifiers that apply to the next statement only.
#[derive(Default)]
struct Next {
    errors: Vec<Expect>,
    sorted: bool,
    replace_column: HashMap<usize, String>,
    replace_result: Vec<(String, String)>,
    replace_regex: Vec<(Regex, String)>,
    lowercase: bool,
}

pub struct Runner<'a> {
    target: Target,
    suite: PathBuf,
    errors: &'a Errors,
    file: String,
    conns: HashMap<String, Slot>,
    cur: String,
    vars: HashMap<String, String>,
    query_log: bool,
    result_log: bool,
    warnings: bool,
    quiet_codes: HashSet<u16>,
    info: bool,
    connect_log: bool,
    vertical: bool,
    next: Next,
    runs: HashMap<String, usize>,
    pub out: Vec<Segment>,
    /// mysqltest features this harness does not implement, met in the file.
    pub unsupported: Vec<String>,
    /// The file asked to be skipped (`skip`), with the reason.
    pub skipped: Option<String>,
    exited: bool,
    testcase_off: bool,
    depth: usize,
}

impl<'a> Runner<'a> {
    pub fn new(target: Target, suite: &Path, errors: &'a Errors, file: &str) -> Runner<'a> {
        let mut vars = HashMap::new();
        let tmp = std::env::temp_dir().join("mtr");
        let _ = std::fs::create_dir_all(&tmp);
        for k in ["MYSQLTEST_VARDIR", "MYSQL_TMP_DIR", "MYSQLD_DATADIR"] {
            vars.insert(k.to_lowercase(), tmp.display().to_string());
        }
        vars.insert("mysql_test_dir".into(), suite.display().to_string());
        vars.insert("master_myport".into(), target.port.to_string());
        Runner {
            target,
            suite: suite.to_path_buf(),
            errors,
            file: file.to_string(),
            conns: HashMap::new(),
            cur: "default".into(),
            vars,
            query_log: true,
            result_log: true,
            warnings: true,
            quiet_codes: HashSet::new(),
            info: false,
            connect_log: false,
            vertical: false,
            next: Next::default(),
            runs: HashMap::new(),
            out: vec![],
            unsupported: vec![],
            skipped: None,
            exited: false,
            testcase_off: false,
            depth: 0,
        }
    }

    /// Runs the test file.
    pub fn run_file(&mut self, path: &Path) -> Result<(), String> {
        let conn = self.connect("test", &self.target.user.clone(), &self.target.pass.clone())?;
        self.conns.insert(
            "default".into(),
            Slot {
                conn: Some(conn),
                pending: None,
                db: "test".into(),
            },
        );
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let stmts = script::parse(&text);
        let name = self.file.clone();
        self.run_block(&stmts, &name);
        // Reap what is still pending, so connections close cleanly.
        let names: Vec<String> = self.conns.keys().cloned().collect();
        for n in names {
            if let Some(slot) = self.conns.get_mut(&n)
                && let Some((h, _, _)) = slot.pending.take()
                && let Ok((c, _)) = h.join()
            {
                slot.conn = Some(c);
            }
        }
        Ok(())
    }

    fn connect(&self, db: &str, user: &str, pass: &str) -> Result<Conn, String> {
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some(self.target.host.clone()))
            .tcp_port(self.target.port)
            .user(Some(user))
            .pass(Some(pass))
            .db_name(if db.is_empty() { None } else { Some(db) })
            .prefer_socket(false);
        Conn::new(opts).map_err(|e| e.to_string())
    }

    fn done(&self) -> bool {
        self.exited || self.skipped.is_some()
    }

    fn run_block(&mut self, stmts: &[Stmt], file: &str) {
        for s in stmts {
            if self.done() {
                return;
            }
            // Between --disable_testcase and --enable_testcase nothing runs.
            if self.testcase_off {
                if let Stmt::Cmd { name, .. } = s
                    && name == "enable_testcase"
                {
                    self.testcase_off = false;
                }
                continue;
            }
            match s {
                Stmt::Query { sql, delim, line } => {
                    let echo = format!("{sql}{delim}");
                    self.statement(sql, &echo, file, *line, false);
                }
                Stmt::Cmd { name, arg, line } => self.command(name, arg, file, *line),
                Stmt::If {
                    cond, body, els, ..
                } => {
                    if self.cond(cond) {
                        self.run_block(body, file);
                    } else {
                        self.run_block(els, file);
                    }
                }
                Stmt::While { cond, body, .. } => {
                    let mut n = 0;
                    while self.cond(cond) && !self.done() {
                        self.run_block(body, file);
                        n += 1;
                        if n > 100_000 {
                            self.unsupported
                                .push("while loop over 100000 iterations".into());
                            break;
                        }
                    }
                }
            }
        }
    }

    fn key(&mut self, file: &str, line: usize) -> String {
        let k = format!("{file}:{line}");
        let n = self.runs.entry(k.clone()).or_default();
        *n += 1;
        format!("{k}#{n}")
    }

    fn emit(&mut self, head: String, body: String) {
        self.out.push(Segment {
            head,
            body,
            ..Default::default()
        });
    }

    fn command(&mut self, name: &str, arg: &str, file: &str, line: usize) {
        // In the `--cmd ...;` form a trailing `;` is part of the argument;
        // it only matters for commands whose argument is text or a query.
        let text = matches!(
            name,
            "echo"
                | "eval"
                | "query"
                | "query_vertical"
                | "query_horizontal"
                | "send"
                | "send_eval"
        );
        let arg = if text {
            arg.trim()
        } else {
            arg.trim().trim_end_matches(';').trim()
        };
        match name {
            "echo" => {
                let t = self.subst(arg);
                self.emit(format!("{t}\n"), String::new());
            }
            "error" => {
                self.next.errors = arg
                    .split(',')
                    .map(|e| self.subst(e.trim()))
                    .filter(|e| !e.is_empty())
                    .filter_map(|e| self.expect(&e))
                    .collect();
            }
            "eval" | "query" | "query_vertical" | "query_horizontal" | "send" | "send_eval" => {
                let sql = if name == "eval" || name == "send_eval" {
                    self.subst(arg)
                } else {
                    arg.to_string()
                };
                // The delimiter mysqltest echoes: the statement ended with it.
                let echo = format!("{sql};");
                if name == "query_vertical" {
                    self.vertical = true;
                }
                if name.starts_with("send") {
                    self.send(&sql, &echo, file, line);
                } else {
                    self.statement(&sql, &echo, file, line, false);
                }
                self.vertical = false;
            }
            "reap" => self.reap(file, line),
            "let" => self.let_(arg),
            "inc" | "dec" => {
                // Variable names are case-insensitive in mysqltest.
                let v = arg.trim_start_matches('$').to_lowercase();
                let n: i64 = self
                    .vars
                    .get(&v)
                    .and_then(|x| x.trim().parse().ok())
                    .unwrap_or(0);
                self.vars
                    .insert(v, (if name == "inc" { n + 1 } else { n - 1 }).to_string());
            }
            "source" => self.source(arg),
            "sorted_result" | "partially_sorted_result" => self.next.sorted = true,
            "lowercase_result" => self.next.lowercase = true,
            "replace_column" => {
                let w: Vec<String> = shell_words(&self.subst(arg));
                for p in w.chunks(2) {
                    if let [c, v] = p
                        && let Ok(c) = c.parse::<usize>()
                    {
                        self.next.replace_column.insert(c, v.clone());
                    }
                }
            }
            "replace_result" => {
                let w: Vec<String> = shell_words(&self.subst(arg));
                for p in w.chunks(2) {
                    if let [a, b] = p {
                        self.next.replace_result.push((a.clone(), b.clone()));
                    }
                }
            }
            "replace_regex" => {
                let a = self.subst(arg);
                for (re, to) in parse_regexes(&a) {
                    self.next.replace_regex.push((re, to));
                }
            }
            "disable_query_log" => self.query_log = false,
            "enable_query_log" => self.query_log = true,
            "disable_result_log" => self.result_log = false,
            "enable_result_log" => self.result_log = true,
            "disable_warnings" => {
                if arg.is_empty() {
                    self.warnings = false;
                } else {
                    for c in self.codes(arg) {
                        self.quiet_codes.insert(c);
                    }
                }
            }
            "enable_warnings" => {
                if arg.is_empty() {
                    self.warnings = true;
                    self.quiet_codes.clear();
                } else {
                    for c in self.codes(arg) {
                        self.quiet_codes.remove(&c);
                    }
                }
            }
            "enable_info" => self.info = true,
            "disable_info" => self.info = false,
            "enable_connect_log" => self.connect_log = true,
            "disable_connect_log" => self.connect_log = false,
            "vertical_results" => self.vertical = true,
            "horizontal_results" => self.vertical = false,
            "connect" => self.connect_cmd(arg),
            "connection" => {
                let n = self.subst(arg);
                if self.connect_log {
                    self.emit(format!("connection {n};\n"), String::new());
                }
                if self.conns.contains_key(&n) {
                    self.cur = n;
                } else {
                    self.unsupported
                        .push(format!("connection {n}: not connected"));
                }
            }
            "disconnect" => {
                let n = self.subst(arg);
                if self.connect_log {
                    self.emit(format!("disconnect {n};\n"), String::new());
                }
                self.conns.remove(&n);
            }
            "character_set" => {
                let cs = self.subst(arg);
                self.raw(&format!("SET NAMES {cs}"));
            }
            "disable_testcase" => self.testcase_off = true,
            "exit" => self.exited = true,
            "skip" => self.skipped = Some(self.subst(arg)),
            "die" => self.skipped = Some(format!("die: {}", self.subst(arg))),
            "sleep" | "real_sleep" => {
                if let Ok(s) = self.subst(arg).parse::<f64>() {
                    std::thread::sleep(std::time::Duration::from_secs_f64(s.min(5.0)));
                }
            }
            "skip_if_hypergraph"
            | "disable_ps_protocol"
            | "enable_ps_protocol"
            | "disable_metadata"
            | "enable_metadata"
            | "disable_abort_on_error"
            | "enable_abort_on_error"
            | "disable_reconnect"
            | "enable_reconnect"
            | "enable_testcase"
            | "disable_async_client"
            | "enable_async_client"
            | "disable_session_track_info"
            | "enable_session_track_info"
            | "start_timer"
            | "end_timer"
            | "ping" => {}
            "remove_file" | "remove_files_wildcard" | "mkdir" | "rmdir" | "force-rmdir" => {}
            other => self.unsupported.push(format!("{other} (line {line})")),
        }
    }

    fn expect(&self, e: &str) -> Option<Expect> {
        if let Some(s) = e.strip_prefix('S') {
            return Some(Expect::State(s.to_string()));
        }
        if let Ok(n) = e.parse::<u16>() {
            return Some(Expect::Code(n));
        }
        self.errors.code(e).map(Expect::Code)
    }

    fn codes(&self, arg: &str) -> Vec<u16> {
        arg.split([',', ' '])
            .map(str::trim)
            .filter(|w| !w.is_empty() && *w != "ONCE")
            .filter_map(|w| match self.expect(w) {
                Some(Expect::Code(c)) => Some(c),
                _ => None,
            })
            .collect()
    }

    fn source(&mut self, arg: &str) {
        let rel = self.subst(arg);
        // The harness can't restart the server: a restart include is a no-op
        // (the statements after it still run, on the same server).
        let base = rel.rsplit('/').next().unwrap_or(&rel);
        if base.contains("restart_mysqld")
            || base.contains("shutdown_mysqld")
            || base.contains("start_mysqld")
        {
            self.unsupported.push(format!("server restart ({rel})"));
            return;
        }
        let path = self.suite.join(&rel);
        let Ok(text) = std::fs::read_to_string(&path) else {
            self.unsupported.push(format!("source {rel}: not found"));
            return;
        };
        if self.depth > 16 {
            return;
        }
        self.depth += 1;
        let stmts = script::parse(&text);
        self.run_block(&stmts, &rel);
        self.depth -= 1;
    }

    fn let_(&mut self, arg: &str) {
        // `let $x= v`, or `let x= v`.
        let rest = arg.trim();
        let rest = rest.strip_prefix('$').unwrap_or(rest);
        let Some(eq) = rest.find('=') else { return };
        let name = rest[..eq].trim().to_string();
        let value = rest[eq + 1..].trim_start();
        let v = self.value(value);
        self.vars.insert(name.to_lowercase(), v);
    }

    /// The value of a `let` right-hand side or an if/while operand.
    fn value(&mut self, v: &str) -> String {
        let v = v.trim();
        if v.len() >= 2 && v.starts_with('`') && v.ends_with('`') {
            let q = self.subst(&v[1..v.len() - 1]);
            return match self.query_rows(&q) {
                Some((_, rows)) => rows
                    .first()
                    .and_then(|r| r.first().cloned())
                    .map(|c| {
                        c.map(|b| String::from_utf8_lossy(&b).into_owned())
                            .unwrap_or("NULL".into())
                    })
                    .unwrap_or_default(),
                None => String::new(),
            };
        }
        if let Some(inner) = v
            .strip_prefix("query_get_value(")
            .and_then(|x| x.strip_suffix(')'))
        {
            // query_get_value(query, column, row): the query may contain commas.
            let parts: Vec<&str> = inner.rsplitn(3, ',').collect();
            if parts.len() == 3 {
                let (row, col, q) = (
                    parts[0].trim(),
                    parts[1].trim(),
                    self.subst(parts[2].trim()),
                );
                let row: usize = row.parse().unwrap_or(1);
                if let Some((cols, rows)) = self.query_rows(&q) {
                    let ci = cols.iter().position(|c| c == col);
                    return match (ci, rows.get(row.wrapping_sub(1))) {
                        (Some(ci), Some(r)) => r[ci]
                            .clone()
                            .map(|b| String::from_utf8_lossy(&b).into_owned())
                            .unwrap_or("NULL".into()),
                        _ => "No such row".into(),
                    };
                }
            }
            return String::new();
        }
        self.subst(v)
    }

    fn cond(&mut self, c: &str) -> bool {
        let c = c.trim();
        let (neg, c) = match c.strip_prefix('!') {
            Some(r) => (true, r.trim()),
            None => (false, c),
        };
        let r = if c.starts_with('`') {
            truthy(&self.value(c))
        } else {
            let ops = ["==", "!=", "<=", ">=", "<", ">"];
            match ops.iter().find_map(|op| c.find(op).map(|i| (i, *op))) {
                Some((i, op)) => {
                    let (a, b) = (self.value(&c[..i]), self.value(&c[i + op.len()..]));
                    compare(&a, &b, op)
                }
                None => truthy(&self.value(c)),
            }
        };
        r != neg
    }

    /// $name -> its value (`\$` is a literal dollar).
    fn subst(&self, s: &str) -> String {
        let b = s.as_bytes();
        let mut out = String::with_capacity(s.len());
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'\\' && b.get(i + 1) == Some(&b'$') {
                out.push('$');
                i += 2;
                continue;
            }
            if b[i] == b'$' {
                let mut j = i + 1;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
                if j > i + 1 {
                    let name = &s[i + 1..j];
                    match self.vars.get(&name.to_lowercase()) {
                        Some(v) => out.push_str(v),
                        None => out.push_str(&std::env::var(name).unwrap_or_default()),
                    }
                    i = j;
                    continue;
                }
            }
            let ch = s[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
        out
    }

    fn connect_cmd(&mut self, arg: &str) {
        let a = self.subst(arg);
        let inner = a.trim().trim_start_matches('(').trim_end_matches(')');
        let p: Vec<String> = inner.split(',').map(|x| x.trim().to_string()).collect();
        let name = p.first().cloned().unwrap_or_default();
        let user = p
            .get(2)
            .filter(|u| !u.is_empty())
            .cloned()
            .unwrap_or(self.target.user.clone());
        let pass = p.get(3).cloned().unwrap_or_default();
        let pass = if pass.is_empty() && user == self.target.user {
            self.target.pass.clone()
        } else {
            pass
        };
        let db = match p.get(4).map(String::as_str) {
            None | Some("") => "test".to_string(),
            Some("*NO-ONE*") => String::new(),
            Some(d) => d.to_string(),
        };
        if self.connect_log {
            self.emit(format!("connect  {inner};\n"), String::new());
        }
        match self.connect(&db, &user, &pass) {
            Ok(c) => {
                self.conns.insert(
                    name.clone(),
                    Slot {
                        conn: Some(c),
                        pending: None,
                        db,
                    },
                );
                self.cur = name;
            }
            Err(e) => {
                // A connect expected to fail (--error) is fine; otherwise note it.
                if self.next.errors.is_empty() {
                    self.unsupported.push(format!("connect {name}: {e}"));
                }
                self.next = Next::default();
            }
        }
    }

    /// Runs a statement for the harness itself (no output).
    fn raw(&mut self, sql: &str) {
        if let Some(c) = self.conn() {
            let _ = c.query_drop(sql);
        }
    }

    fn query_rows(&mut self, q: &str) -> Option<(Vec<String>, Vec<Vec<Option<Vec<u8>>>>)> {
        let c = self.conn()?;
        match exec(c, q) {
            Ok(sets) => sets.into_iter().find_map(|s| s.cols.map(|c| (c, s.rows))),
            Err(_) => None,
        }
    }

    fn conn(&mut self) -> Option<&mut Conn> {
        let slot = self.conns.get_mut(&self.cur)?;
        if let Some((h, _, _)) = slot.pending.take()
            && let Ok((c, _)) = h.join()
        {
            slot.conn = Some(c);
        }
        slot.conn.as_mut()
    }

    fn statement(&mut self, sql: &str, echo: &str, file: &str, line: usize, _sent: bool) {
        let key = self.key(file, line);
        if std::env::var_os("MTR_TRACE").is_some() {
            eprintln!(
                "mtr: {key}: {}",
                sql.chars().take(160).collect::<String>().replace('\n', " ")
            );
        }
        let head = if self.query_log {
            format!("{echo}\n")
        } else {
            String::new()
        };
        // MTR's helper procedures (mtr.add_suppression and the like) do
        // nothing visible: accept them.
        if sql
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("call mtr.")
        {
            self.finish(head, String::new(), key, sql, None);
            return;
        }
        let res = match self.conn() {
            Some(c) => exec(c, sql),
            None => Err(SqlErr {
                code: 2006,
                state: "HY000".into(),
                msg: "no connection".into(),
            }),
        };
        let lost = matches!(&res, Err(e) if e.code == 2013 || e.code == 2006);
        let body = self.format(&res);
        self.finish(head, body, key, sql, res.err());
        if lost {
            self.reconnect();
        }
    }

    fn send(&mut self, sql: &str, echo: &str, file: &str, line: usize) {
        let key = self.key(file, line);
        let head = if self.query_log {
            format!("{echo}\n")
        } else {
            String::new()
        };
        let Some(slot) = self.conns.get_mut(&self.cur) else {
            return;
        };
        let Some(mut c) = slot.conn.take() else {
            return;
        };
        let q = sql.to_string();
        let h = std::thread::spawn(move || {
            let r = exec(&mut c, &q);
            (c, r)
        });
        slot.pending = Some((h, key.clone(), 0));
        // The statement's output comes with the reap.
        self.out.push(Segment {
            head,
            key: None,
            sql: sql.to_string(),
            ..Default::default()
        });
    }

    fn reap(&mut self, file: &str, line: usize) {
        let Some(slot) = self.conns.get_mut(&self.cur) else {
            return;
        };
        let Some((h, key, _)) = slot.pending.take() else {
            self.unsupported
                .push(format!("reap without send (line {line})"));
            return;
        };
        let _ = file;
        let res = match h.join() {
            Ok((c, r)) => {
                slot.conn = Some(c);
                r
            }
            Err(_) => Err(SqlErr {
                code: 2013,
                state: "HY000".into(),
                msg: "lost".into(),
            }),
        };
        let body = self.format(&res);
        self.finish(String::new(), body, key, "", res.err());
    }

    fn finish(
        &mut self,
        head: String,
        body: String,
        key: String,
        sql: &str,
        error: Option<SqlErr>,
    ) {
        // As mysqltest: $mysql_errno is the last statement's error (0 for none).
        let code = error.as_ref().map_or(0, |e| e.code);
        self.vars.insert("mysql_errno".into(), code.to_string());
        let name = if code == 0 {
            "0".into()
        } else {
            self.errors.name(code).unwrap_or_default()
        };
        self.vars.insert("mysql_errname".into(), name);
        let expected = std::mem::take(&mut self.next.errors);
        self.next = Next::default();
        self.out.push(Segment {
            head,
            body,
            key: Some(key),
            sql: sql.to_string(),
            error,
            expected,
        });
    }

    fn reconnect(&mut self) {
        let user = self.target.user.clone();
        let pass = self.target.pass.clone();
        let db = self
            .conns
            .get(&self.cur)
            .map(|s| s.db.clone())
            .unwrap_or("test".into());
        if let Ok(c) = self.connect(&db, &user, &pass)
            && let Some(slot) = self.conns.get_mut(&self.cur)
        {
            slot.conn = Some(c);
        }
    }

    /// The statement's output as mysqltest writes it.
    fn format(&mut self, res: &Result<Vec<Set>, SqlErr>) -> String {
        let mut out = String::new();
        match res {
            Err(e) => {
                out.push_str(&self.fix(&format!("ERROR {}: {}", e.state, e.msg)));
                out.push('\n');
            }
            Ok(sets) => {
                let mut warn = false;
                for s in sets {
                    if self.result_log
                        && let Some(cols) = &s.cols
                    {
                        out.push_str(&self.table(cols, &s.rows));
                    }
                    warn |= s.warnings > 0;
                    if self.info {
                        out.push_str(&format!("affected rows: {}\n", s.affected));
                        if !s.info.is_empty() {
                            out.push_str(&format!("info: {}\n", s.info));
                        }
                    }
                }
                if warn && self.warnings && self.result_log {
                    out.push_str(&self.show_warnings());
                }
            }
        }
        out
    }

    fn show_warnings(&mut self) -> String {
        let Some(c) = self.conn() else {
            return String::new();
        };
        let Ok(sets) = exec(c, "SHOW WARNINGS") else {
            return String::new();
        };
        let mut lines = vec![];
        for s in sets {
            for r in s.rows {
                let f: Vec<String> = r.iter().map(cell).collect();
                let code: u16 = f.get(1).and_then(|c| c.parse().ok()).unwrap_or(0);
                if !self.quiet_codes.contains(&code) {
                    lines.push(self.fix(&f.join("\t")));
                }
            }
        }
        if lines.is_empty() {
            return String::new();
        }
        format!("Warnings:\n{}\n", lines.join("\n"))
    }

    fn table(&self, cols: &[String], rows: &[Vec<Option<Vec<u8>>>]) -> String {
        let mut lines: Vec<String> = rows
            .iter()
            .map(|r| {
                let f: Vec<String> = r
                    .iter()
                    .enumerate()
                    .map(|(i, v)| match self.next.replace_column.get(&(i + 1)) {
                        Some(x) => x.clone(),
                        None => cell(v),
                    })
                    .collect();
                if self.vertical {
                    cols.iter()
                        .zip(f)
                        .map(|(c, v)| format!("{c}\t{v}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    f.join("\t")
                }
            })
            .map(|l| self.fix(&l))
            .collect();
        if self.next.sorted {
            lines.sort();
        }
        let mut out = String::new();
        if !self.vertical {
            out.push_str(&self.fix(&cols.join("\t")));
            out.push('\n');
        }
        for l in lines {
            out.push_str(&l);
            out.push('\n');
        }
        out
    }

    /// The --replace_result / --replace_regex / --lowercase_result modifiers.
    fn fix(&self, s: &str) -> String {
        let mut s = s.to_string();
        for (a, b) in &self.next.replace_result {
            s = s.replace(a.as_str(), b);
        }
        for (re, to) in &self.next.replace_regex {
            s = re.replace_all(&s, to.as_str()).into_owned();
        }
        if self.next.lowercase {
            s = s.to_lowercase();
        }
        s
    }
}

fn cell(v: &Option<Vec<u8>>) -> String {
    match v {
        None => "NULL".into(),
        Some(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

/// The most rows the harness reads from one result set.
const MAX_ROWS: usize = 200_000;

fn exec(c: &mut Conn, sql: &str) -> Result<Vec<Set>, SqlErr> {
    let conv = |e: mysql::Error| match e {
        mysql::Error::MySqlError(e) => SqlErr {
            code: e.code,
            state: e.state,
            msg: e.message,
        },
        other => SqlErr {
            code: 2013,
            state: "HY000".into(),
            msg: other.to_string(),
        },
    };
    let mut sets = vec![];
    {
        let mut qr = c.query_iter(sql).map_err(conv)?;
        let mut overflow = false;
        while let Some(mut rs) = qr.iter() {
            let cols: Vec<String> = rs
                .columns()
                .as_ref()
                .iter()
                .map(|c| c.name_str().into_owned())
                .collect();
            let mut rows = vec![];
            let mut over = false;
            for row in rs.by_ref() {
                let row = row.map_err(conv)?;
                rows.push(row.unwrap().into_iter().map(value).collect());
                if rows.len() > MAX_ROWS {
                    over = true;
                    break;
                }
            }
            if over {
                overflow = true;
                break;
            }
            sets.push(Set {
                cols: if cols.is_empty() { None } else { Some(cols) },
                rows,
                affected: 0,
                warnings: 0,
                info: String::new(),
            });
        }
        if overflow {
            // No test expects this many rows (a runaway plan): stop reading.
            // Dropping the result would read the rest, so it is forgotten,
            // and the connection is reported lost, which replaces it.
            std::mem::forget(qr);
            return Err(SqlErr {
                code: 2013,
                state: "HY000".into(),
                msg: format!("result over {MAX_ROWS} rows, abandoned by the harness"),
            });
        }
    }
    // The statement's final OK packet, as mysqltest reads it
    // (mysql_affected_rows, mysql_warning_count, mysql_info).
    if sets.is_empty() {
        sets.push(Set {
            cols: None,
            rows: vec![],
            affected: 0,
            warnings: 0,
            info: String::new(),
        });
    }
    let last = sets.last_mut().unwrap();
    last.affected = c.affected_rows();
    last.warnings = c.warnings();
    last.info = c.info_str().into_owned();
    Ok(sets)
}

fn value(v: Value) -> Option<Vec<u8>> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(b),
        other => Some(other.as_sql(true).trim_matches('\'').as_bytes().to_vec()),
    }
}

fn truthy(v: &str) -> bool {
    let v = v.trim();
    match v.parse::<f64>() {
        Ok(n) => n != 0.0,
        Err(_) => !v.is_empty() && v != "NULL",
    }
}

fn compare(a: &str, b: &str, op: &str) -> bool {
    let (a, b) = (a.trim(), b.trim());
    if let (Ok(x), Ok(y)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return match op {
            "==" => x == y,
            "!=" => x != y,
            "<" => x < y,
            "<=" => x <= y,
            ">" => x > y,
            _ => x >= y,
        };
    }
    match op {
        "==" => a == b,
        "!=" => a != b,
        "<" => a < b,
        "<=" => a <= b,
        ">" => a > b,
        _ => a >= b,
    }
}

/// Words of a replace_result / replace_column argument (quotes group).
fn shell_words(s: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut quote = None::<char>;
    let mut any = false;
    for c in s.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => cur.push(c),
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                any = true;
            }
            None if c.is_whitespace() => {
                if !cur.is_empty() || any {
                    out.push(std::mem::take(&mut cur));
                    any = false;
                }
            }
            None => cur.push(c),
        }
    }
    if !cur.is_empty() || any {
        out.push(cur);
    }
    out
}

/// `/from/to/ /from2/to2/i`: the delimiter is the first character of each.
fn parse_regexes(s: &str) -> Vec<(Regex, String)> {
    let mut out = vec![];
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let d = chars[i];
        i += 1;
        let mut parts = vec![String::new(), String::new()];
        for p in parts.iter_mut() {
            while i < chars.len() && chars[i] != d {
                if chars[i] == '\\' && i + 1 < chars.len() && chars[i + 1] == d {
                    p.push(d);
                    i += 2;
                    continue;
                }
                p.push(chars[i]);
                i += 1;
            }
            i += 1;
        }
        let mut flags = String::new();
        while i < chars.len() && chars[i].is_ascii_alphabetic() {
            flags.push(chars[i]);
            i += 1;
        }
        let pat = if flags.contains('i') {
            format!("(?i){}", parts[0])
        } else {
            parts[0].clone()
        };
        // \1 in the replacement is ${1} in Rust's regex.
        let to = parts[1].replace('$', "$$");
        let to = Regex::new(r"\\(\d)")
            .unwrap()
            .replace_all(&to, "$${$1}")
            .into_owned();
        if let Ok(re) = Regex::new(&pat) {
            out.push((re, to));
        }
    }
    out
}
