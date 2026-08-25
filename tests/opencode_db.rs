//! Integration test: read a real opencode session tree from a checked-in
//! SQLite fixture and assert it folds into the expected flow graph.
//!
//! The fixture (`tests/fixtures/opencode/sample.db`) is a trimmed export of a
//! real opencode session: one root (`plan` agent) that spawned two `explore`
//! subagents via `task` parts - one that completed, one that errored. Tool
//! outputs were stripped to keep it small; the graph-bearing structure
//! (statuses, callIDs, `metadata.sessionId` links) is intact.

#![cfg(feature = "native")]

use std::path::PathBuf;

use zoetrope::opencode::{self, db::OpencodeDb};
use zoetrope::state::session::{AgentKind, AgentStatus, MAIN_ID};

fn fixture_db() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/opencode/sample.db")
}

const ROOT: &str = "ses_fea6d69cbffe3x3zsCADBhwVM8";
const CHILD_COMPLETED: &str = "ses_fea693e01ffeYIfI2tIHwmuk4B";
const CHILD_ERRORED: &str = "ses_fea62ae5effescmTv47GbaWd3b";

#[test]
fn folds_real_session_tree_into_main_plus_two_subagents() {
    let db = OpencodeDb::open(&fixture_db()).expect("open fixture db");
    let model = opencode::build_model(&db, ROOT)
        .expect("build model")
        .expect("root session exists in fixture");

    // The root is the main agent.
    let main = model.agent(MAIN_ID).expect("main agent");
    assert_eq!(main.kind, AgentKind::Main);

    // Both children became subagent nodes parented under main.
    for child in [CHILD_COMPLETED, CHILD_ERRORED] {
        let node = model
            .agent(child)
            .unwrap_or_else(|| panic!("child {child} became a node"));
        assert_eq!(node.kind, AgentKind::Subagent, "{child} is a subagent");
        assert_eq!(
            node.parent.as_deref(),
            Some(MAIN_ID),
            "{child} is parented under main"
        );
        assert_eq!(
            node.agent_type.as_deref(),
            Some("explore"),
            "{child} carries its agent type"
        );
    }

    // The errored task must surface as a failed subagent - the whole point of
    // preserving the spawn `is_error` join.
    let errored = model.agent(CHILD_ERRORED).unwrap();
    assert_eq!(
        errored.status,
        AgentStatus::Failed,
        "the errored task's child shows as failed"
    );

    // Three agents total: main + two subagents.
    assert_eq!(model.agent_count(), 3, "main + 2 subagents");
}

#[test]
fn tool_calls_spread_across_real_time() {
    // The whole point of the live view: tool calls must land at their OWN
    // `state.time`, not clumped at their message timestamp. Assert the main
    // agent's tool calls carry distinct timestamps spanning more than a moment,
    // so the scrubber sparkline and chips have something to animate over.
    let db = OpencodeDb::open(&fixture_db()).expect("open fixture db");
    let model = opencode::build_model(&db, ROOT)
        .expect("build model")
        .expect("root exists");

    let main = model.agent(MAIN_ID).expect("main agent");
    let mut stamps: Vec<i64> = main
        .tool_calls
        .iter()
        .filter_map(|t| t.ts)
        .map(|t| t.timestamp_millis())
        .collect();
    stamps.sort_unstable();
    stamps.dedup();

    assert!(
        stamps.len() >= 3,
        "expected several distinctly-timed tool calls, got {}",
        stamps.len()
    );
    let span_ms = stamps.last().unwrap() - stamps.first().unwrap();
    assert!(
        span_ms > 1_000,
        "tool calls should span real time (got {span_ms}ms) - otherwise the \
         timeline has nothing to animate"
    );
}

#[test]
fn watermark_reflects_message_and_part_activity() {
    // The live poll's change-detection depends on this. `session.time_updated`
    // is unreliable (observed stale by days on live sessions), so the watermark
    // must track the newest message/part time. Assert it equals the max part
    // time in the fixture, not the (older) session row time.
    let db = OpencodeDb::open(&fixture_db()).expect("open fixture db");
    let watermark = db.tree_watermark(ROOT).expect("watermark");

    // The newest part in the whole fixture tree (root + children) - the value a
    // correct watermark must reach.
    let conn = rusqlite::Connection::open_with_flags(
        fixture_db(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open raw");
    let max_part: i64 = conn
        .query_row(
            "select coalesce(max(max(time_created), max(time_updated)), 0) from part \
             where session_id = ?1 or session_id in \
                 (select id from session where parent_id = ?1)",
            [ROOT],
            |r| r.get(0),
        )
        .expect("query max part");

    assert!(max_part > 0, "fixture has dated parts");
    assert!(
        watermark >= max_part,
        "watermark ({watermark}) must reach the newest part time ({max_part})"
    );
}

#[test]
fn latest_session_lookup_finds_the_root() {
    let db = OpencodeDb::open(&fixture_db()).expect("open fixture db");
    let latest = db.latest_session().expect("query latest");
    assert_eq!(
        latest.as_deref(),
        Some(ROOT),
        "the only root session in the fixture is the latest"
    );
}

/// End-to-end: drive the real feeder task the way the no-id live path does
/// (an App that starts with an EMPTY id), and prove the emitted events populate
/// the graph. This is the regression guard for the bug where the feeder went
/// straight to `ReplayLoaded` without first announcing the session id, so
/// `is_current` dropped every event and the live view stayed empty.
#[tokio::test]
async fn feeder_no_id_live_sequence_populates_the_app() {
    use tokio::sync::mpsc;
    use zoetrope::state::{App, Mode};
    use zoetrope::tailer::{self, OpencodeTarget, UiEvent};

    let (_req_tx, req_rx) = mpsc::channel(8);
    let (ui_tx, mut ui_rx) = mpsc::channel(64);

    // `follow: false` so the feeder announces + hands off once, then parks
    // waiting for a switch (no infinite poll loop to abort).
    let target = OpencodeTarget {
        db_path: fixture_db(),
        session_id: None, // the no-id path - resolves to the latest itself
        dir: None,
        follow: false,
        speed: 8.0,
    };
    tokio::spawn(async move {
        let _ = tailer::run_opencode_task(req_rx, ui_tx, target).await;
    });

    // The App starts with an EMPTY id, exactly like `run_tui_opencode` for the
    // "Latest (live)" choice.
    let mut app = App::new(String::new(), Mode::Live);

    // Drain the events the feeder emits (announce + hand-off), with a timeout so
    // a regression can't hang the suite.
    let deadline = std::time::Duration::from_secs(5);
    let mut saw_reset = false;
    let mut saw_replay = false;
    while !(saw_reset && saw_replay) {
        match tokio::time::timeout(deadline, ui_rx.recv()).await {
            Ok(Some(ev)) => {
                if matches!(ev, UiEvent::SessionReset { .. }) {
                    saw_reset = true;
                }
                if matches!(ev, UiEvent::ReplayLoaded { .. }) {
                    saw_replay = true;
                }
                app.handle_ui_event(ev);
            }
            _ => break,
        }
    }

    assert!(saw_reset, "feeder must ANNOUNCE the session id first");
    assert!(saw_replay, "feeder must then hand off the loaded tree");

    // The App adopted the id and folded the tree: the graph is populated.
    assert_eq!(
        app.current_session_id, ROOT,
        "the App adopted the announced session id"
    );
    assert_eq!(
        app.session.agent_count(),
        3,
        "main + 2 subagents rendered into the model (was 0 before the fix)"
    );
    assert!(
        app.session.agent(MAIN_ID).is_some(),
        "the main agent node exists"
    );
}

/// End-to-end: follow a growing DB and prove new activity arrives as a `Batch`
/// that folds into the model. Copies the fixture to a temp DB, follows it live,
/// then inserts a new tool part and asserts the feeder emits a batch that grows
/// the main agent's tool count. This guards the watermark-poll → Batch → fold
/// path that makes the live view actually update.
#[tokio::test]
async fn feeder_live_follow_emits_batch_on_new_activity() {
    use tokio::sync::mpsc;
    use zoetrope::state::{App, Mode};
    use zoetrope::tailer::{self, OpencodeTarget, UiEvent};

    // Work on a writable copy so we can simulate growth.
    let tmp = std::env::temp_dir().join(format!("zoetrope-oc-live-{}.db", std::process::id()));
    std::fs::copy(fixture_db(), &tmp).expect("copy fixture");

    let (_req_tx, req_rx) = mpsc::channel(8);
    let (ui_tx, mut ui_rx) = mpsc::channel(256);

    let target = OpencodeTarget {
        db_path: tmp.clone(),
        session_id: Some(ROOT.to_string()),
        dir: None,
        follow: true, // live-follow: poll for growth
        speed: 8.0,
    };
    tokio::spawn(async move {
        let _ = tailer::run_opencode_task(req_rx, ui_tx, target).await;
    });

    let mut app = App::new(ROOT.to_string(), Mode::Live);

    // Drain the initial announce + hand-off.
    let deadline = std::time::Duration::from_secs(5);
    let mut got_replay = false;
    while !got_replay {
        match tokio::time::timeout(deadline, ui_rx.recv()).await {
            Ok(Some(ev)) => {
                if matches!(ev, UiEvent::ReplayLoaded { .. }) {
                    got_replay = true;
                }
                app.handle_ui_event(ev);
            }
            _ => panic!("feeder did not hand off the initial tree"),
        }
    }
    let tools_before = app.session.agent(MAIN_ID).unwrap().tool_calls.len();

    // Insert a NEW tool part on the root's newest message, dated in the future so
    // it is unambiguously past the sent-mark and advances the watermark.
    {
        let conn = rusqlite::Connection::open(&tmp).expect("open rw");
        let (msg_id, base_ts): (String, i64) = conn
            .query_row(
                "select id, time_created from message where session_id = ?1 \
                 order by time_created desc limit 1",
                [ROOT],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("newest message");
        let ts = base_ts + 60_000;
        let data = format!(
            r#"{{"type":"tool","tool":"bash","callID":"live_new_call","state":{{"status":"completed","time":{{"start":{ts},"end":{ts}}}}}}}"#
        );
        conn.execute(
            "insert into part (id, message_id, session_id, time_created, time_updated, data) \
             values (?1, ?2, ?3, ?4, ?4, ?5)",
            rusqlite::params!["prt_live_new", msg_id, ROOT, ts, data],
        )
        .expect("insert new part");
    }

    // The feeder polls at ~300ms; wait for a Batch that grows the tool count.
    let mut grew = false;
    let overall = std::time::Duration::from_secs(5);
    let start = std::time::Instant::now();
    while start.elapsed() < overall {
        match tokio::time::timeout(std::time::Duration::from_secs(2), ui_rx.recv()).await {
            Ok(Some(ev)) => {
                app.handle_ui_event(ev);
                if app.session.agent(MAIN_ID).map(|a| a.tool_calls.len()) > Some(tools_before) {
                    grew = true;
                    break;
                }
            }
            _ => break,
        }
    }

    let _ = std::fs::remove_file(&tmp);
    assert!(
        grew,
        "a new tool part must arrive as a Batch and grow the model (was {tools_before})"
    );
}
