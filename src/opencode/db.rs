//! Read an opencode session tree from its SQLite DB (`opencode.db`).
//!
//! Native-only. The DB is opened **read-only** in WAL mode and never written -
//! the same "your data never leaves your machine" property as the Claude JSONL
//! path, just a different local source. opencode writes the DB concurrently, so
//! WAL lets us read a consistent snapshot without blocking it.
//!
//! This module does the IO (open, query) and hands rows to
//! [`super::translate`], which renders them into the synthetic Claude JSONL the
//! rest of zoetrope folds. Live-follow is a poll-by-watermark
//! (`max(time_created)`), not a byte-tail - opencode has no append-only log.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

use super::schema::{MessageData, PartData};
use super::translate::{MessageRow, SessionRows};

/// Default opencode data directory: `~/.local/share/opencode`.
pub fn default_data_dir() -> Option<PathBuf> {
    #[allow(deprecated)]
    let home = std::env::home_dir()
        .filter(|h| !h.as_os_str().is_empty())
        .or_else(|| std::env::var_os("HOME").map(PathBuf::from))?;
    Some(home.join(".local").join("share").join("opencode"))
}

/// The `opencode.db` path inside a data dir.
pub fn db_path(data_dir: &Path) -> PathBuf {
    data_dir.join("opencode.db")
}

/// Whether a path looks like an opencode DB (a file named `opencode.db`) or an
/// opencode data dir containing one.
pub fn is_opencode_target(path: &Path) -> bool {
    if path.file_name().and_then(|n| n.to_str()) == Some("opencode.db") {
        return true;
    }
    path.is_dir() && db_path(path).is_file()
}

/// Resolve a user-supplied target to an actual `opencode.db` file: the file
/// itself, or `<dir>/opencode.db`.
pub fn resolve_db(path: &Path) -> Option<PathBuf> {
    if path.file_name().and_then(|n| n.to_str()) == Some("opencode.db") && path.is_file() {
        return Some(path.to_path_buf());
    }
    let candidate = db_path(path);
    candidate.is_file().then_some(candidate)
}

/// A one-line summary of a root session, for the picker list.
#[derive(Debug, Clone)]
pub struct SessionSummary {
    pub id: String,
    pub title: String,
    /// The working directory the session ran in (the "repo" the picker shows).
    pub directory: String,
    /// Number of direct child sessions (subagents) - a hint at graph richness.
    pub subagents: i64,
    /// Newest message time (epoch millis), for ordering and "last active".
    pub last_active: i64,
}

/// A read-only handle to an opencode DB.
pub struct OpencodeDb {
    conn: Connection,
}

impl OpencodeDb {
    /// Open the DB read-only (WAL, immutable-safe). Fails if the file is
    /// missing or unreadable.
    pub fn open(db: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .with_context(|| format!("opening opencode db {}", db.display()))?;
        // Read-uncommitted is unnecessary in WAL; a plain read sees the last
        // committed snapshot, which is exactly what we want.
        Ok(Self { conn })
    }

    /// The id of the most-recently-ACTIVE root session whose `directory` matches
    /// `dir` (the live-follow default for a project). Ranked by newest message
    /// time (not `session.time_created`), so `zoe --opencode` follows the session
    /// you are actually working in right now, not merely the newest-created one.
    /// Falls back to the most-recently-active root session overall if none match.
    pub fn latest_session_for_dir(&self, dir: &Path) -> Result<Option<String>> {
        let dir = dir.to_string_lossy().to_string();
        // Rank by the newest message in the session, falling back to the
        // session's own created time when it has no messages yet.
        let mut stmt = self.conn.prepare(
            "select s.id from session s where s.parent_id is null and s.directory = ?1 \
             order by coalesce( \
                 (select max(time_created) from message m where m.session_id = s.id), \
                 s.time_created \
             ) desc limit 1",
        )?;
        let mut rows = stmt.query([&dir])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(row.get::<_, String>(0)?));
        }
        // Fallback: most-recently-active root session anywhere.
        self.latest_session()
    }

    /// The most-recently-active root session id overall (ranked by newest
    /// message time, falling back to the session's created time).
    pub fn latest_session(&self) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "select s.id from session s where s.parent_id is null \
             order by coalesce( \
                 (select max(time_created) from message m where m.session_id = s.id), \
                 s.time_created \
             ) desc limit 1",
        )?;
        let mut rows = stmt.query([])?;
        Ok(rows.next()?.map(|r| r.get::<_, String>(0)).transpose()?)
    }

    /// List root sessions for the picker, most-recently-active first. When `dir`
    /// is given, sessions for that directory sort first (so the project you are
    /// in is at the top), then everything else - the picker still shows the whole
    /// history so you can jump to another repo's session.
    pub fn list_sessions(&self, dir: Option<&Path>, limit: usize) -> Result<Vec<SessionSummary>> {
        let dir_str = dir.map(|d| d.to_string_lossy().to_string());
        // last_active = newest message time, falling back to created time.
        // Order: matching-directory first (when a dir is given), then by recency.
        let sql = "select s.id, s.title, s.directory, \
                (select count(*) from session c where c.parent_id = s.id) as subs, \
                coalesce( \
                    (select max(time_created) from message m where m.session_id = s.id), \
                    s.time_created \
                ) as last_active \
             from session s where s.parent_id is null \
             order by (case when ?1 is not null and s.directory = ?1 then 0 else 1 end) asc, \
                      last_active desc \
             limit ?2";
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::params![dir_str, limit as i64], |r| {
            Ok(SessionSummary {
                id: r.get(0)?,
                title: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                directory: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                subagents: r.get(3)?,
                last_active: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Load a single session's rows (metadata + messages + parts), ordered by
    /// `time_created` then id for determinism.
    pub fn load_session_rows(&self, session_id: &str) -> Result<Option<SessionRows>> {
        // Session metadata.
        let mut stmt = self
            .conn
            .prepare("select id, parent_id, agent, title from session where id = ?1")?;
        let mut rows = stmt.query([session_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let id: String = row.get(0)?;
        let parent_id: Option<String> = row.get(1)?;
        let agent: Option<String> = row.get(2)?;
        let title: Option<String> = row.get(3)?;

        // Messages.
        let mut msg_stmt = self.conn.prepare(
            "select id, time_created, data from message where session_id = ?1 \
             order by time_created asc, id asc",
        )?;
        let msg_iter = msg_stmt.query_map([session_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;

        let mut messages: Vec<MessageRow> = Vec::new();
        for m in msg_iter {
            let (mid, time_created, data_raw) = m?;
            let data = MessageData::parse(&data_raw);
            let parts = self.load_parts(&mid)?;
            messages.push(MessageRow {
                id: mid,
                time_created,
                data,
                parts,
            });
        }

        Ok(Some(SessionRows {
            id,
            parent_id,
            agent,
            title,
            messages,
        }))
    }

    /// Load one message's parts, ordered for determinism.
    fn load_parts(&self, message_id: &str) -> Result<Vec<PartData>> {
        let mut stmt = self.conn.prepare(
            "select data from part where message_id = ?1 order by time_created asc, id asc",
        )?;
        let iter = stmt.query_map([message_id], |r| r.get::<_, String>(0))?;
        let mut parts = Vec::new();
        for p in iter {
            parts.push(PartData::parse(&p?));
        }
        Ok(parts)
    }

    /// The direct child sessions of a root (`parent_id = root`).
    pub fn child_ids(&self, root_id: &str) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("select id from session where parent_id = ?1 order by time_created asc")?;
        let iter = stmt.query_map([root_id], |r| r.get::<_, String>(0))?;
        Ok(iter.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Load a root session and all of its direct children - everything the
    /// translator needs to render the tree.
    pub fn load_tree(&self, root_id: &str) -> Result<Option<(SessionRows, Vec<SessionRows>)>> {
        let Some(root) = self.load_session_rows(root_id)? else {
            return Ok(None);
        };
        let mut children = Vec::new();
        for cid in self.child_ids(root_id)? {
            if let Some(rows) = self.load_session_rows(&cid)? {
                children.push(rows);
            }
        }
        Ok(Some((root, children)))
    }

    /// The high-water mark of activity across a session tree. The live poll
    /// compares this against the last-seen value to decide whether to reload.
    ///
    /// Read from the `message` and `part` tables, NOT `session.time_updated` -
    /// the session row's `time_updated` does not reliably advance when messages
    /// or parts are written (observed stale by days on live sessions), so a
    /// watermark based on it would freeze the live view. `part.time_updated`
    /// tracks a tool flipping `running` → `completed`, which is exactly the
    /// activity we want to catch.
    pub fn tree_watermark(&self, root_id: &str) -> Result<i64> {
        // The set of session ids in the tree: the root plus its direct children.
        // A single query over message+part scoped to those ids, taking the max
        // of created/updated across both.
        let mut stmt = self.conn.prepare(
            "select max(w) from (\
                 select coalesce(max(max(time_created), max(time_updated)), 0) as w \
                 from message where session_id = ?1 or session_id in \
                     (select id from session where parent_id = ?1) \
                 union all \
                 select coalesce(max(max(time_created), max(time_updated)), 0) as w \
                 from part where session_id = ?1 or session_id in \
                     (select id from session where parent_id = ?1)\
             )",
        )?;
        let mut rows = stmt.query([root_id])?;
        Ok(rows
            .next()?
            .map(|r| r.get::<_, i64>(0))
            .transpose()?
            .unwrap_or(0))
    }
}
