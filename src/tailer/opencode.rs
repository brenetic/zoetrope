//! opencode feeder (native).
//!
//! opencode has no append-only log to byte-tail - its sessions live in a SQLite
//! DB. So this feeder is a **poll-by-watermark** loop: bulk-load the session
//! tree and hand it to the App as one [`UiEvent::ReplayLoaded`], then poll the
//! tree's activity watermark (max message/part time). When it advances, reload
//! the tree and emit the items newer than what was already sent as a
//! [`UiEvent::Batch`] - the App's real live path, which stamps freshness
//! (transport reads `Live`), folds incrementally, and animates new tool-call
//! chips instead of re-pinning to the end.
//!
//! The model is a pure function of the fact set (see `ARCHITECTURE.md §1.1`) and
//! every update is idempotent, so re-emitting an item whose DB row was updated
//! in place (a tool `running` → `completed`) re-folds cleanly.
//!
//! When no explicit session id is given (`zoe --opencode`), the feeder follows
//! the LATEST session for the directory and auto-switches to a newer one that
//! appears while watching - after a short idle, so an active session is never
//! abandoned mid-work. An explicit id is pinned and never switched.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::opencode::{self, db::OpencodeDb};

use super::{Flow, TailRequest, UiEvent, Update};

/// Poll interval for the opencode watermark. Matches the JSONL tailer's cadence.
const POLL_INTERVAL: Duration = Duration::from_millis(300);

/// Re-scan for a newer session every N poll ticks (~2s at 300ms). Only when
/// following the "latest" session (no explicit id) - an explicit id is pinned.
const SWITCH_SCAN_EVERY: u32 = 7;

/// Only auto-switch to a newer session after the current one has been quiet this
/// many poll ticks (~6s at 300ms). Long enough that a mid-session lull never
/// trips it, so two concurrent sessions can't leapfrog.
const SWITCH_IDLE_TICKS: u32 = 20;

/// A resolved opencode watch target: the DB file plus which session to follow.
pub struct OpencodeTarget {
    pub db_path: PathBuf,
    /// The session to follow. `None` → discover the latest for `dir` (or overall).
    pub session_id: Option<String>,
    /// The project directory used to pick the latest session when `session_id`
    /// is `None`.
    pub dir: Option<PathBuf>,
    /// Live-follow (poll for changes) vs a one-shot replay of a fixed session.
    pub follow: bool,
    pub speed: f64,
}

/// Run the opencode feeder for one target. Returns [`Flow::Switch`] on a
/// `Watch` request (unused for opencode today, but kept uniform) or
/// [`Flow::Exit`] when the channel closes.
pub(crate) async fn run_opencode(
    target: OpencodeTarget,
    ui_tx: &mpsc::Sender<UiEvent>,
    req_rx: &mut mpsc::Receiver<TailRequest>,
) -> Flow {
    let db = match OpencodeDb::open(&target.db_path) {
        Ok(db) => db,
        Err(e) => {
            let _ = ui_tx
                .send(UiEvent::Error(format!("opencode db: {e}")))
                .await;
            return wait_for_switch(req_rx).await;
        }
    };

    // "Auto" mode: no explicit id was given, so we follow the LATEST session and
    // may switch to a newer one that appears while watching. An explicit id is
    // pinned (never auto-switched).
    let auto = target.session_id.is_none();

    // Resolve the session to follow (once; re-resolved on auto-switch below).
    let mut session_id = match &target.session_id {
        Some(id) => id.clone(),
        None => match resolve_latest(&db, target.dir.as_deref()) {
            Some(id) => id,
            None => {
                let _ = ui_tx
                    .send(UiEvent::Error("no opencode sessions found".into()))
                    .await;
                return wait_for_switch(req_rx).await;
            }
        },
    };

    // Outer loop: each iteration follows ONE session. An auto-switch sets
    // `session_id` to a newer one and continues. All DB access is synchronous
    // and its results owned, so the (non-`Sync`) `db` connection is never held
    // across an await - that keeps the spawned task `Send`.
    'session: loop {
        // Announce the session id FIRST (a `SessionReset`), exactly like the
        // Claude live tailer's announce. This makes the App adopt the id so the
        // `ReplayLoaded` and subsequent `Batch`es (all stamped with it) are not
        // dropped by `is_current` - the no-id live path starts the App with an
        // empty id, so without this every event is silently discarded and the
        // graph never appears. On an empty graph the App treats a same-id reset
        // as a harmless announce (it does not clobber a camera choice).
        if ui_tx
            .send(UiEvent::SessionReset {
                session_id: session_id.clone(),
            })
            .await
            .is_err()
        {
            return Flow::Exit;
        }

        // Bulk load + hand off. Track the timestamp of the newest item already
        // handed to the App; on live polls we reload the tree and send ONLY the
        // items past this mark as a `Batch` (not a full `ReplayLoaded` reload) -
        // the App's real live path: it stamps freshness (transport reads
        // `Live`), folds incrementally, and animates new tool-call chips instead
        // of re-pinning to the end.
        let loaded = load_tree_sync(&db, &session_id);
        let mut sent_through = match &loaded {
            Loaded::Replay(items, _) => max_ts(items),
            _ => None,
        };
        if send_loaded(loaded, &session_id, target.speed, ui_tx)
            .await
            .is_err()
        {
            return Flow::Exit;
        }

        // A fixed replay: no polling. Wait for a switch/exit.
        if !target.follow {
            return wait_for_switch(req_rx).await;
        }

        // Live-follow: poll the DB watermark; on advance, reload and emit the new
        // items as a batch. In `auto` mode, also periodically re-resolve the
        // latest session and switch when a newer one appears (after a short idle).
        let mut watermark = db.tree_watermark(&session_id).unwrap_or(0);
        let mut idle_ticks: u32 = 0;
        let mut ticks: u32 = 0;
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        loop {
            // Await ONLY on the timer/request; never with the DB borrowed.
            tokio::select! {
                _ = tick.tick() => {}
                req = req_rx.recv() => {
                    return match req {
                        Some(TailRequest::Watch(p)) => Flow::Switch(p),
                        None => Flow::Exit,
                    };
                }
            }
            ticks = ticks.wrapping_add(1);

            let current = db.tree_watermark(&session_id).unwrap_or(watermark);
            if current > watermark {
                watermark = current;
                idle_ticks = 0;

                // Reload the tree and slice off the items newer than what we've
                // sent. The model is idempotent, so re-sending an item whose row
                // was updated in place (a tool `running` → `completed`) re-folds
                // cleanly; we bound the batch by timestamp so we don't resend the
                // whole history each tick.
                let updates: Vec<Update> = match load_tree_sync(&db, &session_id) {
                    Loaded::Replay(items, _) => items
                        .into_iter()
                        .filter(|it| match (it.ts(), sent_through) {
                            (Some(ts), Some(c)) => ts >= c,
                            (Some(_), None) => true,
                            _ => false,
                        })
                        .map(|it| it.update)
                        .collect(),
                    _ => Vec::new(),
                };

                if !updates.is_empty() {
                    sent_through = updates.iter().filter_map(update_ts).max().or(sent_through);
                    if ui_tx
                        .send(UiEvent::Batch {
                            session_id: session_id.clone(),
                            updates,
                        })
                        .await
                        .is_err()
                    {
                        return Flow::Exit;
                    }
                }
            } else {
                idle_ticks = idle_ticks.saturating_add(1);
            }

            // Auto-switch: after a brief idle, re-resolve the latest session for
            // the dir; if it is a DIFFERENT (newer) session, follow it. The
            // `'session` loop head emits the `SessionReset` that adopts the new
            // id. Throttled so the history scan doesn't run every tick, and
            // idle-gated so an active session is never abandoned mid-work.
            if auto && idle_ticks >= SWITCH_IDLE_TICKS && ticks.is_multiple_of(SWITCH_SCAN_EVERY) {
                let latest = resolve_latest(&db, target.dir.as_deref());
                if let Some(next) = latest
                    && next != session_id
                {
                    session_id = next;
                    continue 'session;
                }
            }
        }
    }
}

/// The newest resolved timestamp across a set of replay items.
fn max_ts(items: &[super::ReplayItem]) -> Option<chrono::DateTime<chrono::Utc>> {
    items.iter().filter_map(|i| i.ts()).max()
}

/// The envelope timestamp of an update, for advancing the live sent-mark.
fn update_ts(update: &Update) -> Option<chrono::DateTime<chrono::Utc>> {
    match update {
        Update::Entry { entry, .. } => super::entry_timestamp(entry),
        Update::SubagentMeta { .. } => None,
    }
}

/// The outcome of a synchronous DB load, ready to send without holding the
/// (non-`Sync`) DB handle across an await.
enum Loaded {
    Replay(Vec<super::ReplayItem>, crate::state::SessionInfo),
    Missing,
    Failed(String),
}

/// Load a session tree from the DB synchronously (no await - the `rusqlite`
/// `Connection` is not `Sync`, so it must never be borrowed across an await).
fn load_tree_sync(db: &OpencodeDb, session_id: &str) -> Loaded {
    match opencode::load_replay(db, session_id) {
        Ok(Some((items, info))) => Loaded::Replay(items, info),
        Ok(None) => Loaded::Missing,
        Err(e) => Loaded::Failed(e.to_string()),
    }
}

/// Send an already-loaded tree over the UI channel. Owns its inputs, so no DB
/// handle is captured. Returns `Err(())` if the channel is closed.
async fn send_loaded(
    loaded: Loaded,
    session_id: &str,
    speed: f64,
    ui_tx: &mpsc::Sender<UiEvent>,
) -> Result<(), ()> {
    match loaded {
        Loaded::Replay(items, info) => {
            let speed = if speed > 0.0 { speed } else { 1.0 };
            ui_tx
                .send(UiEvent::ReplayLoaded {
                    session_id: session_id.to_string(),
                    items,
                    speed,
                    info,
                })
                .await
                .map_err(|_| ())
        }
        Loaded::Missing => {
            let _ = ui_tx
                .send(UiEvent::Error(format!(
                    "opencode session not found: {session_id}"
                )))
                .await;
            Ok(())
        }
        Loaded::Failed(e) => {
            let _ = ui_tx
                .send(UiEvent::Error(format!("opencode load: {e}")))
                .await;
            Ok(())
        }
    }
}

/// Pick the latest session for a directory (or overall), best-effort.
fn resolve_latest(db: &OpencodeDb, dir: Option<&Path>) -> Option<String> {
    match dir {
        Some(d) => db.latest_session_for_dir(d).ok().flatten(),
        None => db.latest_session().ok().flatten(),
    }
}

/// Block until a `Watch` request (switch) or channel close (exit).
async fn wait_for_switch(req_rx: &mut mpsc::Receiver<TailRequest>) -> Flow {
    match req_rx.recv().await {
        Some(TailRequest::Watch(p)) => Flow::Switch(p),
        None => Flow::Exit,
    }
}
