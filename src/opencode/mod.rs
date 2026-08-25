//! opencode session source.
//!
//! opencode stores its sessions in a local SQLite DB (`opencode.db`), not the
//! append-only JSONL Claude Code writes. This module reads that DB and renders
//! each session tree into the synthetic Claude-format JSONL the rest of zoetrope
//! already folds (see [`translate`]), so the model, timeline, graph, and UI are
//! reused unchanged. It is **native-only** and **read-only**.
//!
//! - [`schema`] - serde models for the message/part `data` JSON blobs.
//! - [`translate`] - opencode rows → synthetic Claude JSONL.
//! - [`db`] - the read-only SQLite reader + live-follow watermark (native).

pub mod schema;
pub mod translate;

#[cfg(feature = "native")]
pub mod db;
#[cfg(feature = "native")]
pub mod picker;

use crate::state::SessionInfo;
use crate::tailer::{DemoSubagent, ReplayItem, replay_from_session};

use translate::RenderedSession;

/// Turn a rendered opencode session into the replay stream + info the App
/// expects from `UiEvent::ReplayLoaded`.
///
/// Each child session becomes a `DemoSubagent` (its own transcript + meta),
/// joined to the parent's spawning `Agent` call by `callID`. This is the exact
/// shape [`replay_from_session`] consumes, so an opencode session and a Claude
/// session produce the same graph through the same code.
pub fn replay_from_rendered(rendered: &RenderedSession) -> (Vec<ReplayItem>, SessionInfo) {
    let subs: Vec<DemoSubagent> = rendered
        .children
        .iter()
        .map(|c| DemoSubagent {
            agent_id: &c.session_id,
            meta: &c.meta_json,
            transcript: &c.transcript,
            workflow: None,
            journal: false,
        })
        .collect();
    replay_from_session(&rendered.main, &subs)
}

/// Load an opencode session tree from a DB and render it into the replay stream
/// and info the App consumes. The single native entry point shared by the TUI
/// (bulk load) and `inspect`.
#[cfg(feature = "native")]
pub fn load_replay(
    db: &db::OpencodeDb,
    session_id: &str,
) -> anyhow::Result<Option<(Vec<ReplayItem>, SessionInfo)>> {
    let Some((root, children)) = db.load_tree(session_id)? else {
        return Ok(None);
    };
    let rendered = translate::render_session(&root, &children);
    Ok(Some(replay_from_rendered(&rendered)))
}

/// Fully fold an opencode session into a [`SessionModel`](crate::state::session::SessionModel),
/// the point-in-time view `inspect` prints. Liveness is derived against the
/// wall clock, matching the Claude `parse_session_fully` path.
#[cfg(feature = "native")]
pub fn build_model(
    db: &db::OpencodeDb,
    session_id: &str,
) -> anyhow::Result<Option<crate::state::session::SessionModel>> {
    let Some((items, _info)) = load_replay(db, session_id)? else {
        return Ok(None);
    };
    let mut model = crate::state::session::SessionModel::new(session_id.to_string());
    for item in &items {
        model.apply_update(&item.update);
    }
    model.recompute_workflow_status();
    model.recompute_liveness(Some(chrono::Utc::now()));
    Ok(Some(model))
}

#[cfg(test)]
mod tests {
    use super::schema::{MessageData, PartData};
    use super::translate::{MessageRow, SessionRows, render_session};
    use super::*;

    /// Build a tiny two-session tree by hand (no DB) and assert the rendered
    /// JSONL folds into the expected graph: a main agent that spawned one
    /// subagent, joined by the task `callID`.
    #[test]
    fn renders_a_spawn_edge_that_folds_into_a_subagent() {
        let root = SessionRows {
            id: "ses_root".into(),
            parent_id: None,
            agent: Some("plan".into()),
            title: Some("root".into()),
            messages: vec![
                MessageRow {
                    id: "msg_u1".into(),
                    time_created: 1_000,
                    data: MessageData {
                        role: Some("user".into()),
                        ..Default::default()
                    },
                    parts: vec![PartData {
                        kind: Some("text".into()),
                        text: Some("do the thing".into()),
                        ..Default::default()
                    }],
                },
                MessageRow {
                    id: "msg_a1".into(),
                    time_created: 2_000,
                    data: MessageData {
                        role: Some("assistant".into()),
                        model_id: Some("claude-opus-4-8".into()),
                        ..Default::default()
                    },
                    parts: vec![PartData {
                        kind: Some("tool".into()),
                        tool: Some("task".into()),
                        call_id: Some("call_1".into()),
                        state: Some(super::schema::ToolState {
                            status: Some("completed".into()),
                            input: serde_json::json!({
                                "description": "explore it",
                                "subagent_type": "explore"
                            }),
                            metadata: Some(super::schema::ToolMetadata {
                                session_id: Some("ses_child".into()),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                },
            ],
        };
        let child = SessionRows {
            id: "ses_child".into(),
            parent_id: Some("ses_root".into()),
            agent: Some("explore".into()),
            title: Some("explore it (@explore subagent)".into()),
            messages: vec![MessageRow {
                id: "msg_c1".into(),
                time_created: 2_500,
                data: MessageData {
                    role: Some("assistant".into()),
                    ..Default::default()
                },
                parts: vec![PartData {
                    kind: Some("tool".into()),
                    tool: Some("read".into()),
                    call_id: Some("call_read".into()),
                    state: Some(super::schema::ToolState {
                        status: Some("completed".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            }],
        };

        let rendered = render_session(&root, &[child]);
        let (items, _info) = replay_from_rendered(&rendered);

        // Fold the whole stream into a model.
        let mut model = crate::state::session::SessionModel::new("ses_root".into());
        for item in &items {
            model.apply_update(&item.update);
        }
        model.recompute_liveness(Some(chrono::Utc::now()));

        // Main + one subagent.
        let main = model
            .agent(crate::state::session::MAIN_ID)
            .expect("main agent exists");
        assert_eq!(main.kind, crate::state::session::AgentKind::Main);

        let child = model
            .agent("ses_child")
            .expect("child session became a subagent node");
        assert_eq!(child.kind, crate::state::session::AgentKind::Subagent);
        assert_eq!(
            child.parent.as_deref(),
            Some(crate::state::session::MAIN_ID)
        );
        assert_eq!(child.agent_type.as_deref(), Some("explore"));
    }
}
