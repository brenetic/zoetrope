//! opencode feeder (native).
//!
//! opencode has no append-only log to byte-tail - its sessions live in a SQLite
//! DB. So this feeder is a **poll-by-watermark** loop: bulk-load the session
//! tree, hand it to the App as one [`UiEvent::ReplayLoaded`], then poll the
//! tree's `max(time_updated)` and, when it advances, re-render and re-hand the
//! whole tree (a [`UiEvent::SessionReset`] first so the App rebuilds cleanly).
//!
//! Reloading the whole tree on each change is deliberately simple and correct:
//! the model is a pure function of the fact set (see `ARCHITECTURE.md §1.1`), so
//! a full rebuild lands on exactly the right state. Sessions are small enough
//! (tens of messages, hundreds of parts) that this is cheap, and it sidesteps
//! having to compute a per-row delta against opencode's mutable rows.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::opencode::{self, db::OpencodeDb};

use super::{Flow, TailRequest, UiEvent};

/// Poll interval for the opencode watermark. Matches the JSONL tailer's cadence.
const POLL_INTERVAL: Duration = Duration::from_millis(300);

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

    // Resolve which session to follow.
    let session_id = match &target.session_id {
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

    // Bulk load + hand off. The DB read is synchronous and its result owned, so
    // the (non-`Sync`) connection is never held across an await.
    let loaded = load_tree_sync(&db, &session_id);
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

    // Live-follow: poll the watermark and reload on change.
    let mut watermark = db.tree_watermark(&session_id).unwrap_or(0);
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

        let current = db.tree_watermark(&session_id).unwrap_or(watermark);
        if current <= watermark {
            continue;
        }
        watermark = current;
        // Read the fresh tree and re-hand it as a plain `ReplayLoaded` - NO
        // `SessionReset`. The model is order-independent and idempotent (re-
        // folding known facts is a no-op), so the App re-folds into the existing
        // graph, mutating nodes in place and adding new ones. A reset would wipe
        // the graph and flash it back on every poll; this keeps it stable and
        // lets new tool calls / spawns animate in. In Live mode the App pins to
        // the head, so the view always reflects the latest state.
        let loaded = load_tree_sync(&db, &session_id);
        if send_loaded(loaded, &session_id, target.speed, ui_tx)
            .await
            .is_err()
        {
            return Flow::Exit;
        }
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
