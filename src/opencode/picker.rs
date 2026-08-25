//! A small interactive session picker for opencode (native).
//!
//! Shown for a bare `zoe --opencode` when several sessions exist: a list of
//! recent sessions with their repo/directory, title, subagent count, and last
//! active time, so you can choose a meaningful one instead of guessing an id.
//! It runs its own short terminal loop BEFORE the main graph TUI starts, then
//! hands the chosen id back.

use std::path::Path;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use super::db::{OpencodeDb, SessionSummary};

/// Outcome of running the picker.
pub enum Picked {
    /// Follow the latest session for the cwd (no explicit id), enabling
    /// auto-switch. Chosen with the "Latest (live)" row or when only one exists.
    Latest,
    /// Replay a specific session id.
    Session(String),
    /// The user cancelled (q / esc) - the caller should exit without launching.
    Cancelled,
}

/// Show the picker for the given directory. Returns the choice.
///
/// If there are no sessions, returns `Cancelled` (the caller reports the empty
/// state). If there is exactly one, returns it directly without a prompt.
pub fn run(db: &OpencodeDb, dir: Option<&Path>) -> Result<Picked> {
    let sessions = db.list_sessions(dir, 200).unwrap_or_default();
    if sessions.is_empty() {
        return Ok(Picked::Cancelled);
    }

    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &sessions, dir);
    ratatui::restore();
    result
}

/// The blocking select loop. Kept separate so terminal restore always runs.
fn run_loop(
    terminal: &mut ratatui::DefaultTerminal,
    sessions: &[SessionSummary],
    dir: Option<&Path>,
) -> Result<Picked> {
    let mut selected: usize = 0;
    // Row 0 is the "Latest (live)" option; sessions follow at offset +1.
    let row_count = sessions.len() + 1;

    loop {
        terminal.draw(|f| draw(f, sessions, dir, selected))?;

        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(Picked::Cancelled),
            KeyCode::Char('c')
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                return Ok(Picked::Cancelled);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                selected = (selected + 1).min(row_count - 1);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                selected = selected.saturating_sub(1);
            }
            KeyCode::Home | KeyCode::Char('g') => selected = 0,
            KeyCode::End | KeyCode::Char('G') => selected = row_count - 1,
            KeyCode::Enter => {
                if selected == 0 {
                    return Ok(Picked::Latest);
                }
                return Ok(Picked::Session(sessions[selected - 1].id.clone()));
            }
            _ => {}
        }
    }
}

/// Human-friendly "time ago" for a millis timestamp.
fn ago(ms: i64) -> String {
    let now = chrono::Utc::now().timestamp_millis();
    let secs = (now - ms).max(0) / 1000;
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86_400)
    }
}

/// The last path component of a directory (the "repo" name), falling back to the
/// full path when there is no separator.
fn repo_name(dir: &str) -> &str {
    dir.rsplit('/').find(|s| !s.is_empty()).unwrap_or(dir)
}

fn draw(f: &mut Frame, sessions: &[SessionSummary], dir: Option<&Path>, selected: usize) {
    let area = f.area();
    f.render_widget(Clear, area);

    let [header, list_area, footer] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .areas(area);

    // Header.
    let cwd = dir
        .map(|d| d.to_string_lossy().to_string())
        .unwrap_or_default();
    f.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                " zoetrope · pick an opencode session",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(Span::styled(
                format!(" cwd: {cwd}    ↑/↓ move · enter open · q cancel"),
                Style::default().add_modifier(Modifier::DIM),
            )),
        ]),
        header,
    );

    // Rows: the "Latest (live)" option, then one per session.
    let block = Block::default().borders(Borders::ALL);
    let inner = block.inner(list_area);
    f.render_widget(block, list_area);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(row_line(
        selected == 0,
        "● Latest (live)",
        "follow the newest session for this directory, auto-switching",
        "",
        inner.width,
    ));
    for (i, s) in sessions.iter().enumerate() {
        let repo = repo_name(&s.directory);
        let subs = if s.subagents > 0 {
            format!("{} sub", s.subagents)
        } else {
            "single".to_string()
        };
        let meta = format!("{repo} · {subs} · {}", ago(s.last_active));
        let title = if s.title.is_empty() {
            s.id.clone()
        } else {
            s.title.clone()
        };
        lines.push(row_line(
            selected == i + 1,
            &title,
            &meta,
            &s.id,
            inner.width,
        ));
    }

    // Simple viewport: scroll so the selected row stays visible.
    let height = inner.height as usize;
    let offset = selected.saturating_sub(height.saturating_sub(1));
    let visible: Vec<Line> = lines.into_iter().skip(offset).take(height).collect();
    f.render_widget(Paragraph::new(visible), inner);

    // Footer.
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!(" {} session(s)", sessions.len()),
            Style::default().add_modifier(Modifier::DIM),
        ))),
        footer,
    );
}

/// Render one list row: a title with a dim metadata suffix, highlighted when
/// selected. `id` is shown dim at the end when there is room (disambiguates
/// same-titled sessions).
fn row_line<'a>(sel: bool, title: &str, meta: &str, id: &str, width: u16) -> Line<'a> {
    let marker = if sel { "▶ " } else { "  " };
    let base = if sel {
        Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED)
    } else {
        Style::default()
    };
    let dim = if sel {
        base
    } else {
        Style::default().add_modifier(Modifier::DIM)
    };

    // Budget the title so the metadata fits.
    let w = width as usize;
    let meta_full = if id.is_empty() {
        meta.to_string()
    } else if meta.is_empty() {
        String::new()
    } else {
        format!("{meta}  {id}")
    };
    let reserve = meta_full.len() + 4;
    let title_budget = w.saturating_sub(reserve).max(8);
    let title = crate::ui::truncate(title, title_budget);

    Line::from(vec![
        Span::styled(marker.to_string(), base),
        Span::styled(format!("{title}  "), base),
        Span::styled(meta_full, dim),
    ])
}
