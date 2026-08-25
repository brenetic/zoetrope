//! Serde models for the JSON payloads opencode stores in its SQLite DB.
//!
//! opencode keeps the row *envelope* (ids, timestamps, session link) in real
//! SQL columns and the payload in a JSON `data` blob typed (upstream)
//! `V1MessageData` / `V1PartData`. These structs decode just the fields the
//! graph needs; everything else is ignored. Every field is optional and unknown
//! variants fall through, mirroring the defensive posture the Claude transcript
//! parser already takes - opencode's payload shape is internal and mid-refactor
//! (a v1→v2 model is landing), so a missing or new field must never be fatal.

use serde::Deserialize;

/// A `message` row's `data` blob. `role` selects user vs assistant; the rest is
/// per-role metadata the graph surfaces (model, tokens, completion).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MessageData {
    #[serde(default)]
    pub role: Option<String>,
    /// Sub-session agent name (`explore`, `general`, `plan`, …). On the child
    /// session's own messages; also on the root.
    #[serde(default)]
    pub agent: Option<String>,
    /// Assistant-only: the model id (e.g. `claude-opus-4-8`).
    #[serde(rename = "modelID", default)]
    pub model_id: Option<String>,
    /// Assistant-only: `null`/absent while the turn is still in flight - the
    /// in-flight ground-truth signal, analogous to a pending tool call.
    #[serde(default)]
    pub finish: Option<String>,
    /// Assistant-only token usage.
    #[serde(default)]
    pub tokens: Option<Tokens>,
    #[serde(default)]
    pub time: Option<TimeCreatedCompleted>,
}

/// Assistant token usage. Only `output` reaches a card, but the shape is kept
/// whole for future use; all fields default to 0.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Tokens {
    #[serde(default)]
    pub input: u64,
    #[serde(default)]
    pub output: u64,
    #[serde(default)]
    pub reasoning: u64,
}

/// A message's `time` object (epoch millis).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TimeCreatedCompleted {
    #[serde(default)]
    pub created: Option<i64>,
    #[serde(default)]
    pub completed: Option<i64>,
}

/// A `part` row's `data` blob. `type` selects the variant; the graph cares about
/// `tool` (tool calls, incl. `task` spawns), `text`/`reasoning` (assistant
/// text), and `subtask` (slash-command sub-agents).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PartData {
    #[serde(rename = "type", default)]
    pub kind: Option<String>,

    // --- tool parts ---
    /// The tool name (`bash`, `read`, `task`, …). `task` is the spawn edge.
    #[serde(default)]
    pub tool: Option<String>,
    /// The provider tool-call id - the spawn/completion join key for `task`.
    #[serde(rename = "callID", default)]
    pub call_id: Option<String>,
    #[serde(default)]
    pub state: Option<ToolState>,

    // --- text / reasoning parts ---
    #[serde(default)]
    pub text: Option<String>,

    // --- subtask parts (slash-command sub-agents) ---
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
}

/// The `state` object on a tool part: status + optional timing + `task` linkage.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ToolState {
    /// `pending` (in flight), `completed`, or `error`. Absent → treated pending.
    #[serde(default)]
    pub status: Option<String>,
    /// Tool input; for `task` this is `{description, prompt, subagent_type}`.
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub time: Option<ToolTime>,
    #[serde(default)]
    pub metadata: Option<ToolMetadata>,
}

/// A tool call's `state.time` span (epoch millis).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ToolTime {
    #[serde(default)]
    pub start: Option<i64>,
    #[serde(default)]
    pub end: Option<i64>,
}

/// A tool call's `state.metadata`. For a `task` part, `sessionId` names the
/// spawned child session - the second, independent spawn join (the first being
/// `session.parent_id`).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ToolMetadata {
    #[serde(rename = "sessionId", default)]
    pub session_id: Option<String>,
}

/// The typed `task`-input view (mirrors Claude's `AgentToolInput`).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TaskInput {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub subagent_type: Option<String>,
}

impl MessageData {
    /// Parse a message `data` blob defensively - a decode failure yields an
    /// empty record rather than dropping the row, so an unknown payload shape
    /// still contributes its envelope (id/time) to the timeline.
    pub fn parse(raw: &str) -> Self {
        serde_json::from_str(raw).unwrap_or_default()
    }
}

impl PartData {
    /// Parse a part `data` blob defensively (see [`MessageData::parse`]).
    pub fn parse(raw: &str) -> Self {
        serde_json::from_str(raw).unwrap_or_default()
    }

    /// The typed `task` input, if this is a `task` tool part.
    pub fn task_input(&self) -> Option<TaskInput> {
        let state = self.state.as_ref()?;
        serde_json::from_value(state.input.clone()).ok()
    }
}
