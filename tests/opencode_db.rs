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
fn latest_session_lookup_finds_the_root() {
    let db = OpencodeDb::open(&fixture_db()).expect("open fixture db");
    let latest = db.latest_session().expect("query latest");
    assert_eq!(
        latest.as_deref(),
        Some(ROOT),
        "the only root session in the fixture is the latest"
    );
}
