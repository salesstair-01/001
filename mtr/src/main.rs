//! mtr: runs MySQL's own test files (mysql-test/t/*.test, the mysqltest
//! language) against sqld and compares each statement's output with
//! MySQL's recorded result (mysql-test/r/*.result).
//!
//!     scripts/mtr/fetch.sh                      # MySQL 8.0.46's tests (GPL: not committed)
//!     cargo run --release --manifest-path mtr/Cargo.toml -- \
//!         -bin bin -list scripts/mtr/files.txt -summary scripts/mtr/results.md
//!
//! With `-dsn host:port` it runs against that server instead (a real MySQL,
//! to check the harness: `-baseline-out` records the statements that fail
//! there, and `-baseline` leaves them out of sqld's score). Each file runs
//! in a fresh `test` database; every other database is dropped first, so
//! point it only at a scratch server.

mod compare;
mod errors;
mod run;
mod script;

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;

use compare::{Failure, Kind, Score};
use errors::Errors;
use run::{Runner, Target};

struct Args {
    suite: PathBuf,
    errors: Option<PathBuf>,
    dsn: Option<String>,
    bin: PathBuf,
    list: Option<PathBuf>,
    files: Vec<String>,
    summary: Option<PathBuf>,
    failures: Option<PathBuf>,
    baseline: Option<PathBuf>,
    baseline_out: Option<PathBuf>,
    min_pass_rate: Option<f64>,
    timeout: Duration,
    label: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        suite: PathBuf::from("scripts/mtr/mysql-server/mysql-test"),
        errors: None,
        dsn: None,
        bin: PathBuf::from("bin"),
        list: None,
        files: vec![],
        summary: None,
        failures: None,
        baseline: None,
        baseline_out: None,
        min_pass_rate: None,
        timeout: Duration::from_secs(300),
        label: "sqld".into(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let flag = arg.trim_start_matches('-');
        let mut val = || {
            it.next()
                .unwrap_or_else(|| die(&format!("{arg} needs a value")))
        };
        match flag {
            "suite" => a.suite = val().into(),
            "errors" => a.errors = Some(val().into()),
            "dsn" => a.dsn = Some(val()),
            "bin" => a.bin = val().into(),
            "list" => a.list = Some(val().into()),
            "summary" => a.summary = Some(val().into()),
            "failures" => a.failures = Some(val().into()),
            "baseline" => a.baseline = Some(val().into()),
            "baseline-out" => a.baseline_out = Some(val().into()),
            "min-pass-rate" => a.min_pass_rate = val().parse().ok(),
            "timeout" => a.timeout = Duration::from_secs(val().parse().unwrap_or(300)),
            "label" => a.label = val(),
            "h" | "help" => {
                eprintln!(
                    "{}",
                    include_str!("main.rs")
                        .lines()
                        .take(14)
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                std::process::exit(0);
            }
            _ if arg.starts_with('-') => die(&format!("unknown flag {arg}")),
            _ => a.files.push(arg),
        }
    }
    a
}

fn die(msg: &str) -> ! {
    eprintln!("mtr: {msg}");
    std::process::exit(2)
}

/// kvd and sqld from -bin, on free ports, with a store in a temp directory.
struct Cluster {
    procs: Vec<Child>,
    port: u16,
}

impl Cluster {
    fn start(bin: &Path) -> Cluster {
        let dir = std::env::temp_dir().join(format!("mtr-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (kv, sq) = (free_port(), free_port());
        let log = |n: &str| std::fs::File::create(dir.join(n)).unwrap();
        let kvd = Command::new(bin.join("kvd"))
            .args(["-listen", &format!("127.0.0.1:{kv}"), "-store-dir"])
            .arg(dir.join("store"))
            .stdout(log("kvd.log"))
            .stderr(log("kvd.err"))
            .spawn()
            .unwrap_or_else(|e| die(&format!("start {}: {e}", bin.join("kvd").display())));
        wait_port(kv);
        let sqld = Command::new(bin.join("sqld"))
            .args([
                "-listen",
                &format!("127.0.0.1:{sq}"),
                "-kv",
                &format!("127.0.0.1:{kv}"),
                "-partition",
                "1",
            ])
            .stdout(log("sqld.log"))
            .stderr(log("sqld.err"))
            .spawn()
            .unwrap_or_else(|e| die(&format!("start {}: {e}", bin.join("sqld").display())));
        wait_port(sq);
        eprintln!("mtr: kvd on {kv}, sqld on {sq} (logs in {})", dir.display());
        Cluster {
            procs: vec![kvd, sqld],
            port: sq,
        }
    }
}

impl Cluster {
    /// Which process has exited, if one has (a crash, or the OOM killer).
    fn exited(&mut self) -> Option<String> {
        for (p, name) in self.procs.iter_mut().zip(["kvd", "sqld"]) {
            if let Ok(Some(st)) = p.try_wait() {
                return Some(format!("{name} exited ({st})"));
            }
        }
        None
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for p in self.procs.iter_mut().rev() {
            let _ = p.kill();
            let _ = p.wait();
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_port(p: u16) {
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", p)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    die(&format!("nothing listens on port {p}"));
}

/// Drops every non-system database and creates an empty `test`.
fn reset(t: &Target) -> Result<(), String> {
    let opts = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(t.host.clone()))
        .tcp_port(t.port)
        .user(Some(t.user.clone()))
        .pass(Some(t.pass.clone()))
        .prefer_socket(false)
        // A server still busy with a runaway statement must not hang the run.
        .tcp_connect_timeout(Some(Duration::from_secs(10)))
        .read_timeout(Some(Duration::from_secs(60)))
        .write_timeout(Some(Duration::from_secs(60)));
    let mut c = mysql::Conn::new(opts).map_err(|e| e.to_string())?;
    // Global settings a test may have left behind (MTR's check-testcase
    // would report them). Best effort: sqld has fewer of them.
    for q in [
        "SET SESSION TRANSACTION READ WRITE",
        "SET GLOBAL TRANSACTION READ WRITE",
        "SET GLOBAL read_only = OFF",
        "SET GLOBAL sql_mode = DEFAULT",
        "SET GLOBAL time_zone = DEFAULT",
        "SET GLOBAL autocommit = 1",
        "SET GLOBAL transaction_isolation = DEFAULT",
        "SET GLOBAL max_allowed_packet = 67108864",
        "SET GLOBAL innodb_lock_wait_timeout = DEFAULT",
    ] {
        let _ = c.query_drop(q);
    }
    let dbs: Vec<String> = c.query("SHOW DATABASES").map_err(|e| e.to_string())?;
    for db in dbs {
        if !["information_schema", "mysql", "performance_schema", "sys"]
            .contains(&db.to_lowercase().as_str())
        {
            c.query_drop(format!("DROP DATABASE `{}`", db.replace('`', "``")))
                .map_err(|e| e.to_string())?;
        }
    }
    c.query_drop("CREATE DATABASE test")
        .map_err(|e| e.to_string())
}

/// Kills every other session on the server (after a file timed out).
fn kill_sessions(t: &Target) {
    let opts = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(t.host.clone()))
        .tcp_port(t.port)
        .user(Some(t.user.clone()))
        .pass(Some(t.pass.clone()))
        .prefer_socket(false);
    let Ok(mut c) = mysql::Conn::new(opts) else {
        return;
    };
    let ids: Vec<u64> = c
        .query("SELECT id FROM information_schema.processlist WHERE id <> CONNECTION_ID() AND user <> 'event_scheduler' AND command <> 'Daemon'")
        .unwrap_or_default();
    for id in ids {
        let _ = c.query_drop(format!("KILL {id}"));
    }
}

struct FileReport {
    name: String,
    score: Score,
    skipped: Option<String>,
    unsupported: Vec<String>,
    secs: f64,
}

fn main() {
    let a = parse_args();
    let errors_path = a
        .errors
        .clone()
        .unwrap_or_else(|| a.suite.join("../share/messages_to_clients.txt"));
    let errors: &'static Errors =
        Box::leak(Box::new(Errors::load(&errors_path).unwrap_or_else(|e| {
            die(&format!("{}: {e}", errors_path.display()))
        })));
    let mut files = a.files.clone();
    if let Some(l) = &a.list {
        let text =
            std::fs::read_to_string(l).unwrap_or_else(|e| die(&format!("{}: {e}", l.display())));
        files.extend(
            text.lines()
                .map(|l| l.split('#').next().unwrap().trim().to_string())
                .filter(|l| !l.is_empty()),
        );
    }
    if files.is_empty() {
        die("no test files (name them, or -list a file of names)");
    }
    let baseline: HashSet<String> = match &a.baseline {
        Some(p) => std::fs::read_to_string(p)
            .unwrap_or_else(|e| die(&format!("{}: {e}", p.display())))
            .lines()
            .map(str::to_string)
            .collect(),
        None => HashSet::new(),
    };

    let mut cluster = None;
    let mut target = match &a.dsn {
        Some(d) => {
            let (host, port) = d.rsplit_once(':').unwrap_or((d.as_str(), "3306"));
            Target {
                host: host.into(),
                port: port.parse().unwrap_or(3306),
                user: "root".into(),
                pass: String::new(),
            }
        }
        None => {
            let c = Cluster::start(&a.bin);
            let port = c.port;
            cluster = Some(c);
            Target {
                host: "127.0.0.1".into(),
                port,
                user: "root".into(),
                pass: String::new(),
            }
        }
    };

    let mut reports = vec![];
    for name in &files {
        let test = a.suite.join("t").join(format!("{name}.test"));
        let result = a.suite.join("r").join(format!("{name}.result"));
        let (Ok(expected), true) = (std::fs::read_to_string(&result), test.exists()) else {
            eprintln!("mtr: {name}: no such test");
            continue;
        };
        eprintln!("mtr: {name}: running");
        if let Err(e) = reset(&target) {
            // With kvd and sqld of our own, start fresh ones and try again.
            let Some(old) = cluster.take() else {
                die(&format!("reset before {name}: {e}"));
            };
            eprintln!("mtr: reset before {name}: {e}; restarting kvd and sqld");
            drop(old);
            let c = Cluster::start(&a.bin);
            target.port = c.port;
            cluster = Some(c);
            if let Err(e) = reset(&target) {
                die(&format!("reset before {name}: {e}"));
            }
        }
        let t0 = Instant::now();
        let (tx, rx) = mpsc::channel();
        let (t, suite, n) = (target.clone(), a.suite.clone(), name.clone());
        std::thread::spawn(move || {
            let mut r = Runner::new(t, &suite, errors, &n);
            let res = r.run_file(&test);
            let _ = tx.send((res, r.out, r.unsupported, r.skipped));
        });
        let report = match rx.recv_timeout(a.timeout) {
            Ok((Err(e), ..)) => {
                eprintln!("mtr: {name}: {e}");
                FileReport {
                    name: name.clone(),
                    score: Score::default(),
                    skipped: Some(e),
                    unsupported: vec![],
                    secs: 0.0,
                }
            }
            Ok((Ok(()), out, unsupported, skipped)) => {
                let base: HashSet<String> = baseline
                    .iter()
                    .filter_map(|k| k.strip_prefix(&format!("{name}\t")))
                    .map(str::to_string)
                    .collect();
                let score = if skipped.is_some() {
                    Score::default()
                } else {
                    compare::score(&out, &expected, &base)
                };
                FileReport {
                    name: name.clone(),
                    score,
                    skipped,
                    unsupported,
                    secs: t0.elapsed().as_secs_f64(),
                }
            }
            Err(_) => {
                eprintln!("mtr: {name}: timed out after {:?}", a.timeout);
                // The abandoned run still holds connections and locks: end
                // them before the next file.
                if cluster.is_some() {
                    drop(cluster.take());
                    let c = Cluster::start(&a.bin);
                    target.port = c.port;
                    cluster = Some(c);
                } else {
                    kill_sessions(&target);
                }
                FileReport {
                    name: name.clone(),
                    score: Score::default(),
                    skipped: Some(format!("timed out after {}s", a.timeout.as_secs())),
                    unsupported: vec![],
                    secs: a.timeout.as_secs_f64(),
                }
            }
        };
        let mut report = report;
        // A query engine that died during the file (a crash, or out of
        // memory) is a finding: note it and start a new one.
        if let Some(why) = cluster.as_mut().and_then(Cluster::exited) {
            eprintln!("mtr: {name}: {why}; restarting kvd and sqld");
            report.skipped = Some(format!("{why} during this file"));
            drop(cluster.take());
            let c = Cluster::start(&a.bin);
            target.port = c.port;
            cluster = Some(c);
        }
        let s = &report.score;
        eprintln!(
            "mtr: {name}: {}/{} passed{}{} ({:.1}s)",
            s.passed,
            s.passed + s.failures.len(),
            if s.excluded > 0 {
                format!(", {} excluded", s.excluded)
            } else {
                String::new()
            },
            report
                .skipped
                .as_ref()
                .map(|r| format!(", skipped: {r}"))
                .unwrap_or_default(),
            report.secs
        );
        reports.push(report);
    }

    if let Some(p) = &a.baseline_out {
        let mut out = String::new();
        for r in &reports {
            for f in &r.score.failures {
                let _ = writeln!(out, "{}\t{}", r.name, f.key);
            }
        }
        std::fs::write(p, out).unwrap_or_else(|e| die(&format!("{}: {e}", p.display())));
    }
    if let Some(p) = &a.failures {
        std::fs::write(p, failures_tsv(&reports))
            .unwrap_or_else(|e| die(&format!("{}: {e}", p.display())));
    }
    let (passed, total) = reports.iter().fold((0, 0), |(p, t), r| {
        (
            p + r.score.passed,
            t + r.score.passed + r.score.failures.len(),
        )
    });
    let rate = if total > 0 {
        100.0 * passed as f64 / total as f64
    } else {
        0.0
    };
    let summary = summary(&a, &reports, passed, total, rate);
    match &a.summary {
        Some(p) => {
            std::fs::write(p, &summary).unwrap_or_else(|e| die(&format!("{}: {e}", p.display())))
        }
        None => print!("{summary}"),
    }
    eprintln!("mtr: {passed}/{total} statements passed ({rate:.1}%)");
    if let Some(min) = a.min_pass_rate
        && rate < min
    {
        eprintln!("mtr: pass rate {rate:.1}% is below {min}%");
        std::process::exit(1);
    }
}

fn one_line(s: &str, n: usize) -> String {
    let s = s
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n");
    if s.chars().count() > n {
        format!("{}…", s.chars().take(n).collect::<String>())
    } else {
        s
    }
}

fn failures_tsv(reports: &[FileReport]) -> String {
    let mut out = String::from("file\tkey\tkind\tour_error\tstatement\tours\texpected\n");
    for r in reports {
        for f in &r.score.failures {
            let _ = writeln!(
                out,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                r.name,
                f.key,
                f.kind.label(),
                f.our_error
                    .as_ref()
                    .map(|(c, _)| c.to_string())
                    .unwrap_or_default(),
                one_line(&f.sql, 300),
                one_line(&f.ours, 300),
                one_line(&f.expected, 300)
            );
        }
    }
    out
}

/// An error message with names, numbers and quoted parts generalised, so
/// failures for the same reason count together.
fn error_pattern(msg: &str) -> String {
    let re_q = regex::Regex::new(r#"'[^']*'|"[^"]*"|`[^`]*`"#).unwrap();
    let re_n = regex::Regex::new(r"\b\d+\b").unwrap();
    let m = re_q.replace_all(msg, "'…'");
    let m = re_n.replace_all(&m, "N");
    one_line(&m, 140)
}

fn summary(a: &Args, reports: &[FileReport], passed: usize, total: usize, rate: f64) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# MySQL test suite results ({})\n", a.label);
    let _ = writeln!(
        s,
        "Generated by `mtr` (`mtr/`): MySQL 8.0.46's `mysql-test` files in \
         `scripts/mtr/files.txt`, each statement's output compared with MySQL's \
         recorded `.result`. A statement passes when its output matches, or when it \
         fails with the error code the test expects (messages are not compared). \
         Statements that fail on MySQL 8.0.46 itself under this harness are left out \
         (\"excluded\").\n"
    );
    let _ = writeln!(s, "**{passed} of {total} statements pass ({rate:.1}%).**\n");
    let _ = writeln!(
        s,
        "| file | passed | failed | excluded | pass rate | note |\n|---|---:|---:|---:|---:|---|"
    );
    for r in reports {
        let n = r.score.passed + r.score.failures.len();
        let rate = if n > 0 {
            format!("{:.1}%", 100.0 * r.score.passed as f64 / n as f64)
        } else {
            "–".into()
        };
        let note = r.skipped.clone().unwrap_or_default();
        let _ = writeln!(
            s,
            "| {} | {} | {} | {} | {} | {} |",
            r.name,
            r.score.passed,
            r.score.failures.len(),
            r.score.excluded,
            rate,
            one_line(&note, 80)
        );
    }
    let mut kinds: BTreeMap<Kind, usize> = BTreeMap::new();
    let mut errs: BTreeMap<String, (usize, String)> = BTreeMap::new();
    for r in reports {
        for f in &r.score.failures {
            *kinds.entry(f.kind).or_default() += 1;
            if let Some((code, msg)) = &f.our_error
                && matches!(f.kind, Kind::OurError | Kind::WrongError)
            {
                let e = errs
                    .entry(format!("{code} {}", error_pattern(msg)))
                    .or_insert((0, String::new()));
                e.0 += 1;
                if e.1.is_empty() {
                    e.1 = example(r, f);
                }
            }
        }
    }
    let _ = writeln!(
        s,
        "\n## Failures by kind\n\n| kind | statements |\n|---|---:|"
    );
    for (k, n) in &kinds {
        let _ = writeln!(s, "| {} | {n} |", k.label());
    }
    let mut errs: Vec<_> = errs.into_iter().collect();
    errs.sort_by(|a, b| b.1.0.cmp(&a.1.0));
    let _ = writeln!(
        s,
        "\n## Our errors where MySQL succeeds (top 60)\n\n| statements | error | example |\n|---:|---|---|"
    );
    for (e, (n, ex)) in errs.iter().take(60) {
        let _ = writeln!(
            s,
            "| {n} | {} | {} |",
            e.replace('|', "\\|"),
            ex.replace('|', "\\|")
        );
    }
    let mut un: BTreeMap<String, usize> = BTreeMap::new();
    for r in reports {
        for u in &r.unsupported {
            let w = u.split([' ', '(']).next().unwrap_or(u).to_string();
            *un.entry(w).or_default() += 1;
        }
    }
    if !un.is_empty() {
        let _ = writeln!(
            s,
            "\n## mysqltest commands the harness skips\n\n| command | times |\n|---|---:|"
        );
        for (u, n) in un {
            let _ = writeln!(s, "| `{u}` | {n} |");
        }
    }
    s
}

fn example(r: &FileReport, f: &Failure) -> String {
    format!(
        "{} `{}`",
        f.key
            .replace(&format!("{}:", r.name), &format!("{}.test:", r.name)),
        one_line(&f.sql, 80).replace('`', "'")
    )
}
