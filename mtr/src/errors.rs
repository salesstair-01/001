//! MySQL error names (`ER_DUP_ENTRY`) to codes, read from the server's
//! `share/messages_to_clients.txt`, plus the client errors tests name.

use std::collections::HashMap;

pub struct Errors {
    by_name: HashMap<String, u16>,
}

impl Errors {
    /// Reads messages_to_clients.txt: `start-error-number N` sets the next
    /// code, and each line starting with a name takes the next code.
    pub fn load(path: &std::path::Path) -> std::io::Result<Errors> {
        let text = std::fs::read_to_string(path)?;
        let mut by_name = HashMap::new();
        let mut next: Option<u16> = None;
        for line in text.lines() {
            if let Some(n) = line.strip_prefix("start-error-number") {
                next = n.trim().parse().ok();
                continue;
            }
            let Some(code) = next else { continue };
            let name: String = line
                .chars()
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            if name.len() > 2 && line.starts_with(|c: char| c.is_ascii_uppercase()) {
                by_name.insert(name, code);
                next = Some(code + 1);
            }
        }
        for (n, c) in [
            ("CR_UNKNOWN_ERROR", 2000),
            ("CR_CONNECTION_ERROR", 2002),
            ("CR_CONN_HOST_ERROR", 2003),
            ("CR_SERVER_GONE_ERROR", 2006),
            ("CR_SERVER_LOST", 2013),
            ("CR_COMMANDS_OUT_OF_SYNC", 2014),
            ("CR_PARAMS_NOT_BOUND", 2031),
            ("CR_NO_PREPARE_STMT", 2030),
        ] {
            by_name.insert(n.to_string(), c);
        }
        Ok(Errors { by_name })
    }

    pub fn code(&self, name: &str) -> Option<u16> {
        self.by_name.get(name).copied()
    }

    pub fn name(&self, code: u16) -> Option<String> {
        self.by_name
            .iter()
            .find(|(_, c)| **c == code)
            .map(|(n, _)| n.clone())
    }
}

/// An error a statement is expected to return (`--error`).
#[derive(Debug, Clone, PartialEq)]
pub enum Expect {
    Code(u16),
    State(String),
}

impl Expect {
    pub fn matches(&self, code: u16, state: &str) -> bool {
        match self {
            Expect::Code(c) => *c == code,
            Expect::State(s) => s == state,
        }
    }
}
