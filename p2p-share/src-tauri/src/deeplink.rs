//! `fly://` deeplinks: parse receive links, stash their tickets, and
//! deliver them to the UI.
//!
//! Canonical form: `fly://receive?ticket=<ticket>` (what the sender's QR
//! encodes). Links arriving while the app runs go straight to the UI as
//! a `fly-open-url` event; links the app was launched with land in the
//! pending slot, which the UI drains on mount (events emitted before the
//! webview loads would be lost).

use std::sync::Mutex;
use tauri::{AppHandle, Emitter, Manager, State};

/// Custom scheme, without `://`.
pub const SCHEME: &str = "fly";

/// Event carrying one deeplink ticket to the UI.
pub const OPEN_URL_EVENT: &str = "fly-open-url";

/// Tickets from launch-time links, waiting for the UI to drain them.
#[derive(Default)]
pub struct PendingTickets {
    inner: Mutex<Vec<String>>,
}

impl PendingTickets {
    pub fn push(&self, ticket: String) {
        if let Ok(mut slot) = self.inner.lock() {
            slot.push(ticket);
        }
    }

    pub fn take(&self) -> Vec<String> {
        self.inner.lock().map(|mut slot| std::mem::take(&mut *slot)).unwrap_or_default()
    }
}

/// Parse a `fly://receive?ticket=...` link. Returns the ticket, or
/// `None` for anything else (wrong scheme/host, missing ticket).
/// Surrounding/inner whitespace is stripped: tickets stay one base32
/// word even when a chat app wrapped them across lines.
pub fn parse_receive_link(url: &url::Url) -> Option<String> {
    if url.scheme() != SCHEME {
        return None;
    }
    if url.host_str() != Some("receive") {
        return None;
    }
    let ticket: String = url
        .query_pairs()
        .find(|(key, _)| key == "ticket")
        .map(|(_, value)| value.chars().filter(|c| !c.is_whitespace()).collect())?;
    if ticket.is_empty() {
        return None;
    }
    Some(ticket)
}

/// Deliver opened URLs: stash every ticket for the UI and emit one
/// event per link. Malformed links are ignored, never errors.
pub fn handle_urls(app: &AppHandle, urls: Vec<url::Url>) {
    for url in urls {
        let Some(ticket) = parse_receive_link(&url) else {
            continue;
        };
        app.state::<PendingTickets>().push(ticket.clone());
        let _ = app.emit(OPEN_URL_EVENT, ticket);
    }
}

/// Drain launch-time tickets (cold-start links the UI never saw as
/// events). Called once when the UI mounts.
#[tauri::command]
pub fn take_pending_tickets(state: State<'_, PendingTickets>) -> Vec<String> {
    state.take()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Option<String> {
        parse_receive_link(&url::Url::parse(text).unwrap())
    }

    #[test]
    fn valid_link_yields_ticket() {
        assert_eq!(
            parse("fly://receive?ticket=blobabc123"),
            Some("blobabc123".to_string())
        );
    }

    #[test]
    fn whitespace_wrapped_ticket_is_cleaned() {
        assert_eq!(
            parse("fly://receive?ticket=blobabc%20123"),
            Some("blobabc123".to_string())
        );
    }

    #[test]
    fn malformed_links_are_rejected() {
        assert_eq!(parse("https://receive?ticket=blobabc"), None);
        assert_eq!(parse("fly://send?ticket=blobabc"), None);
        assert_eq!(parse("fly://receive"), None);
        assert_eq!(parse("fly://receive?ticket="), None);
        assert_eq!(parse("fly://receive?other=x"), None);
    }

    #[test]
    fn pending_slot_stacks_and_drains() {
        let slot = PendingTickets::default();
        assert!(slot.take().is_empty());
        slot.push("a".to_string());
        slot.push("b".to_string());
        assert_eq!(slot.take(), vec!["a".to_string(), "b".to_string()]);
        assert!(slot.take().is_empty());
    }
}
