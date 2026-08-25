//! Translate an opencode session tree into the synthetic Claude-format JSONL the
//! rest of zoetrope already understands.
//!
//! Rather than teach the [`SessionModel`](crate::state::session::SessionModel)
//! an opencode dialect, we render opencode rows into the exact JSON shapes the
//! Claude transcript parser ([`crate::transcript::parse_line`]) consumes, then
//! hand them to [`replay_from_session`](crate::tailer::replay_from_session). The
//! whole proven pipeline (parse → fold → derive) is reused unchanged, and we
//! depend only on the *public* JSONL contract, not on any internal struct.
//!
//! ## The mapping
//!
//! | opencode | synthetic Claude |
//! |---|---|
//! | root session (no `parent_id`) | the `Source::Main` transcript |
//! | child session (`parent_id` set) | a `Source::Sub(child_id)` transcript + a subagent `meta` |
//! | a `task` part (`state.metadata.sessionId` = child) | a main `assistant` line with an `Agent` `tool_use` whose `id` = the part's `callID`, plus a `tool_result` carrying its status |
//! | the child session's `agent` | the meta's `agentType` (and the `Agent` input's `subagent_type`) |
//! | a non-`task` tool part | a `tool_use` block + a `tool_result` line (status → `is_error`) |
//! | a user message | a `user` entry |
//! | an assistant text/reasoning part | `text`/`thinking` content on the assistant line |
//!
//! The spawn join is Claude's own: the parent's `Agent` `tool_use.id` equals the
//! child meta's `toolUseId` (both the opencode `callID`), so an errored/completed
//! `task` resolves the child exactly like a Claude `Agent` tool_result does.

use chrono::{DateTime, TimeZone, Utc};

use super::schema::{MessageData, PartData};

/// One session's rows, as read from the DB, ready to render to JSONL.
pub struct SessionRows {
    pub id: String,
    pub parent_id: Option<String>,
    pub agent: Option<String>,
    pub title: Option<String>,
    pub messages: Vec<MessageRow>,
}

/// A message row plus its ordered parts.
pub struct MessageRow {
    pub id: String,
    pub time_created: i64,
    pub data: MessageData,
    pub parts: Vec<PartData>,
}

/// The rendered transcript for one child session: its JSONL plus the meta and
/// the `callID` that ties it to the parent's spawning `Agent` call.
pub struct RenderedChild {
    pub session_id: String,
    pub meta_json: String,
    pub transcript: String,
}

/// The full rendered session tree: the root's main transcript and one entry per
/// child, in the shape [`replay_from_session`](crate::tailer::replay_from_session)
/// wants (feed each child as a `DemoSubagent`).
pub struct RenderedSession {
    pub session_id: String,
    pub main: String,
    pub children: Vec<RenderedChild>,
}

/// Epoch-millis → an ISO8601 timestamp string (Claude's envelope format).
fn iso(ms: i64) -> String {
    let dt: DateTime<Utc> = Utc
        .timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now);
    // Millisecond precision + trailing Z, matching the Claude transcript format.
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// JSON-escape a string for embedding in a hand-built JSONL line.
fn esc(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// Render one session's messages into transcript JSONL lines under a given
/// `agent_id` (empty for the main transcript → no `agentId`/`isSidechain`).
///
/// `spawns` collects `(call_id, child_session_id, subagent_type, description)`
/// for every `task` part seen, so the caller can emit child metas joined by
/// `call_id`.
fn render_transcript(
    rows: &SessionRows,
    agent_id: Option<&str>,
    spawns: &mut Vec<Spawn>,
) -> String {
    let mut lines: Vec<Line> = Vec::new();

    // Envelope builder shared by every emitted line. `ts_ms` is the line's own
    // timestamp (a tool uses its own start/end, not the message time) so events
    // spread across the real timeline instead of clumping at message boundaries
    // - that spread is what drives the scrubber sparkline, the chips, and the
    // edge animation.
    let envelope = |uuid: &str, ts_ms: i64, extra: &str| -> String {
        let mut e = format!(
            "\"uuid\":{},\"parentUuid\":null,\"timestamp\":{},\"sessionId\":{}",
            esc(uuid),
            esc(&iso(ts_ms)),
            esc(&rows.id),
        );
        if let Some(id) = agent_id {
            e.push_str(&format!(",\"agentId\":{},\"isSidechain\":true", esc(id)));
        } else {
            e.push_str(",\"isSidechain\":false");
        }
        if !extra.is_empty() {
            e.push(',');
            e.push_str(extra);
        }
        e
    };

    for msg in &rows.messages {
        let msg_ms = msg.time_created;
        let role = msg.data.role.as_deref().unwrap_or("user");
        let model = msg.data.model_id.as_deref().unwrap_or("");
        let model_field = if model.is_empty() {
            String::new()
        } else {
            format!("\"model\":{},", esc(model))
        };

        if role == "user" {
            let text = msg
                .parts
                .iter()
                .filter(|p| p.kind.as_deref() == Some("text"))
                .filter_map(|p| p.text.as_deref())
                .collect::<Vec<_>>()
                .join("\n");
            let content = if text.is_empty() {
                esc("(prompt)")
            } else {
                esc(&text)
            };
            lines.push(Line {
                ts: msg_ms,
                seq: 0,
                json: format!(
                    "{{\"type\":\"user\",{},\"origin\":{{\"kind\":\"human\"}},\"message\":{{\"role\":\"user\",\"content\":{}}}}}",
                    envelope(&msg.id, msg_ms, ""),
                    content,
                ),
            });
            continue;
        }

        // Assistant turn. Text/reasoning ride the message timestamp; each tool
        // call gets its OWN pair of lines at its own start/end time.
        let mut text_blocks: Vec<String> = Vec::new();
        for part in &msg.parts {
            match part.kind.as_deref() {
                Some("text") => {
                    if let Some(t) = &part.text
                        && !t.is_empty()
                    {
                        text_blocks.push(format!("{{\"type\":\"text\",\"text\":{}}}", esc(t)));
                    }
                }
                Some("reasoning") => {
                    if let Some(t) = &part.text
                        && !t.is_empty()
                    {
                        text_blocks
                            .push(format!("{{\"type\":\"thinking\",\"thinking\":{}}}", esc(t)));
                    }
                }
                _ => {}
            }
        }

        // The text/reasoning assistant line (also carries token usage so it is
        // attributed once per message, not per tool call).
        if !text_blocks.is_empty() {
            let usage = msg
                .data
                .tokens
                .as_ref()
                .map(|t| format!(",\"usage\":{{\"output_tokens\":{}}}", t.output))
                .unwrap_or_default();
            lines.push(Line {
                ts: msg_ms,
                seq: 1,
                json: format!(
                    "{{\"type\":\"assistant\",{},\"message\":{{\"role\":\"assistant\",{}\"content\":[{}]{}}}}}",
                    envelope(&msg.id, msg_ms, ""),
                    model_field,
                    text_blocks.join(","),
                    usage,
                ),
            });
        }

        // One tool call → one timestamped assistant `tool_use` line at its
        // start, plus a `tool_result`/notification at its end.
        for (i, part) in msg.parts.iter().enumerate() {
            match part.kind.as_deref() {
                Some("tool") => render_tool_part(
                    part,
                    &msg.id,
                    i,
                    msg_ms,
                    &model_field,
                    &envelope,
                    &mut lines,
                    spawns,
                ),
                Some("subtask") => render_subtask_part(
                    part,
                    &msg.id,
                    i,
                    msg_ms,
                    &model_field,
                    &envelope,
                    &mut lines,
                    spawns,
                ),
                _ => {}
            }
        }
    }

    // Stable sort by (timestamp, seq): tool calls now interleave with text in
    // real time. seq keeps a tool_use before its result at the same instant.
    lines.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.seq.cmp(&b.seq)));
    let mut out = String::new();
    for line in lines {
        out.push_str(&line.json);
        out.push('\n');
    }
    out
}

/// One emitted JSONL line with the timestamp + intra-instant ordering used to
/// place it on the timeline. `seq` breaks ties at an identical `ts` (a
/// `tool_use` at `seq` before its `tool_result` at `seq+…`).
struct Line {
    ts: i64,
    seq: u8,
    json: String,
}

/// A discovered spawn: the parent's `Agent` call and the child it launched.
pub struct Spawn {
    pub call_id: String,
    pub child_session_id: Option<String>,
    pub subagent_type: Option<String>,
    pub description: Option<String>,
}

/// Render a `tool` part into a timestamped `tool_use` assistant line at its
/// start and a `tool_result` (or spawn `<task-notification>`) at its end. A
/// `task` tool becomes an `Agent` spawn (recorded in `spawns`).
///
/// Using the tool's OWN `state.time` (not the message time) is what makes the
/// timeline live: each call lands at the instant it ran, so the scrubber shows
/// where the work happened and pending calls (no `end`) drive the in-flight chip.
#[allow(clippy::too_many_arguments)]
fn render_tool_part(
    part: &PartData,
    msg_id: &str,
    part_idx: usize,
    msg_ms: i64,
    model_field: &str,
    envelope: &dyn Fn(&str, i64, &str) -> String,
    lines: &mut Vec<Line>,
    spawns: &mut Vec<Spawn>,
) {
    let tool = part.tool.as_deref().unwrap_or("tool");
    let call_id = part.call_id.clone().unwrap_or_default();
    let state = part.state.as_ref();
    let status = state.and_then(|s| s.status.as_deref()).unwrap_or("pending");
    let time = state.and_then(|s| s.time.as_ref());
    // Start defaults to the message time; end to the recorded end (None → the
    // call is still pending, which is exactly the in-flight signal we want).
    let start_ms = time.and_then(|t| t.start).unwrap_or(msg_ms);
    let end_ms = time.and_then(|t| t.end);
    let uuid = format!("{msg_id}_p{part_idx}");

    if tool == "task" {
        let ti = part.task_input().unwrap_or_default();
        let child = state
            .and_then(|s| s.metadata.as_ref())
            .and_then(|m| m.session_id.clone());
        let input = format!(
            "{{\"description\":{},\"subagent_type\":{}}}",
            esc(ti.description.as_deref().unwrap_or("")),
            esc(ti.subagent_type.as_deref().unwrap_or("general")),
        );
        // The spawning `Agent` tool_use, at the task's start.
        lines.push(Line {
            ts: start_ms,
            seq: 2,
            json: format!(
                "{{\"type\":\"assistant\",{},\"message\":{{\"role\":\"assistant\",{}\"content\":[{{\"type\":\"tool_use\",\"id\":{},\"name\":\"Agent\",\"input\":{}}}]}}}}",
                envelope(&uuid, start_ms, ""),
                model_field,
                esc(&call_id),
                input,
            ),
        });
        // opencode's `task.status` is the real outcome → route through the
        // authoritative `<task-notification>` channel (keyed on the child
        // session id) so it outranks async supersession. Dated at the task end.
        if let Some(child_id) = &child {
            let status_word = match status {
                "completed" => Some("completed"),
                "error" => Some("failed"),
                _ => None,
            };
            if let Some(word) = status_word {
                let at = end_ms.unwrap_or(start_ms);
                let payload = format!(
                    "<task-notification>\n<task-id>{child_id}</task-id>\n<status>{word}</status>\n</task-notification>"
                );
                lines.push(Line {
                    ts: at,
                    seq: 9,
                    json: format!(
                        "{{\"type\":\"user\",{},\"message\":{{\"role\":\"user\",\"content\":{}}}}}",
                        envelope(&format!("{uuid}_n"), at, ""),
                        esc(&payload),
                    ),
                });
            }
        }
        spawns.push(Spawn {
            call_id,
            child_session_id: child,
            subagent_type: ti.subagent_type,
            description: ti.description,
        });
        return;
    }

    // Ordinary tool call: tool_use at start, tool_result at end.
    lines.push(Line {
        ts: start_ms,
        seq: 2,
        json: format!(
            "{{\"type\":\"assistant\",{},\"message\":{{\"role\":\"assistant\",{}\"content\":[{{\"type\":\"tool_use\",\"id\":{},\"name\":{},\"input\":{{}}}}]}}}}",
            envelope(&uuid, start_ms, ""),
            model_field,
            esc(&call_id),
            esc(tool),
        ),
    });
    if status == "completed" || status == "error" {
        let at = end_ms.unwrap_or(start_ms);
        let is_err = if status == "error" {
            ",\"is_error\":true"
        } else {
            ""
        };
        lines.push(Line {
            ts: at,
            seq: 3,
            json: format!(
                "{{\"type\":\"user\",{},\"message\":{{\"role\":\"user\",\"content\":[{{\"type\":\"tool_result\",\"tool_use_id\":{},\"content\":\"\"{}}}]}}}}",
                envelope(&format!("{uuid}_r"), at, ""),
                esc(&call_id),
                is_err,
            ),
        });
    }
}

/// Render a `subtask` part (a slash-command sub-agent captured inline) as an
/// `Agent` spawn. It has no child session row, so it becomes a leaf spawn whose
/// child transcript is empty - the node still appears with its description.
#[allow(clippy::too_many_arguments)]
fn render_subtask_part(
    part: &PartData,
    msg_id: &str,
    part_idx: usize,
    msg_ms: i64,
    model_field: &str,
    envelope: &dyn Fn(&str, i64, &str) -> String,
    lines: &mut Vec<Line>,
    spawns: &mut Vec<Spawn>,
) {
    let call_id = format!("subtask_{msg_id}_{part_idx}");
    let uuid = format!("{msg_id}_p{part_idx}");
    let input = format!(
        "{{\"description\":{},\"subagent_type\":{}}}",
        esc(part.description.as_deref().unwrap_or("subtask")),
        esc(part.agent.as_deref().unwrap_or("general")),
    );
    lines.push(Line {
        ts: msg_ms,
        seq: 2,
        json: format!(
            "{{\"type\":\"assistant\",{},\"message\":{{\"role\":\"assistant\",{}\"content\":[{{\"type\":\"tool_use\",\"id\":{},\"name\":\"Agent\",\"input\":{}}}]}}}}",
            envelope(&uuid, msg_ms, ""),
            model_field,
            esc(&call_id),
            input,
        ),
    });
    spawns.push(Spawn {
        call_id,
        child_session_id: None,
        subagent_type: part.agent.clone(),
        description: part.description.clone(),
    });
}

/// Render a subagent `meta.json` joining a child to the parent's spawning call.
fn render_meta(agent_type: Option<&str>, description: Option<&str>, tool_use_id: &str) -> String {
    format!(
        "{{\"agentType\":{},\"description\":{},\"toolUseId\":{}}}",
        esc(agent_type.unwrap_or("subagent")),
        esc(description.unwrap_or("")),
        esc(tool_use_id),
    )
}

/// Translate a root session and its already-loaded children into a
/// [`RenderedSession`]. `children` are keyed by session id.
pub fn render_session(root: &SessionRows, children: &[SessionRows]) -> RenderedSession {
    let mut spawns: Vec<Spawn> = Vec::new();
    let main = render_transcript(root, None, &mut spawns);

    // Index child sessions by id so a spawn can look up its child's rows.
    let mut rendered_children: Vec<RenderedChild> = Vec::new();
    for spawn in &spawns {
        let Some(child_id) = &spawn.child_session_id else {
            continue;
        };
        let Some(child) = children.iter().find(|c| &c.id == child_id) else {
            continue;
        };
        let mut child_spawns: Vec<Spawn> = Vec::new();
        let transcript = render_transcript(child, Some(child_id), &mut child_spawns);
        let agent_type = child.agent.as_deref().or(spawn.subagent_type.as_deref());
        let description = spawn.description.as_deref().or(child.title.as_deref());
        let meta_json = render_meta(agent_type, description, &spawn.call_id);
        rendered_children.push(RenderedChild {
            session_id: child_id.clone(),
            meta_json,
            transcript,
        });
    }

    // A child session present in `children` but never referenced by a task part
    // (e.g. parent_id set but the spawn row is missing) still deserves a node -
    // attach it with a synthetic meta keyed on its own id.
    for child in children {
        if rendered_children.iter().any(|r| r.session_id == child.id) {
            continue;
        }
        let mut child_spawns: Vec<Spawn> = Vec::new();
        let transcript = render_transcript(child, Some(&child.id), &mut child_spawns);
        let meta_json = render_meta(child.agent.as_deref(), child.title.as_deref(), &child.id);
        rendered_children.push(RenderedChild {
            session_id: child.id.clone(),
            meta_json,
            transcript,
        });
    }

    RenderedSession {
        session_id: root.id.clone(),
        main,
        children: rendered_children,
    }
}
