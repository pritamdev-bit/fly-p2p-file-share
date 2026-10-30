//! `fly://` deeplinks: parse receive links, stash their tickets, and
//! deliver them to the UI.
//!
//! Canonical form: `fly://receive?ticket=<ticket>` (what the sender's QR
//! encodes). Links arriving while the app runs go straight to the UI as
//! a `fly-open-url` event; links the app was launched with land in the
//! pending slot, which the UI drains on mount (events emitted before the
//! webview loads would be lost).

use crate::inspect;
use anyhow::{Result, anyhow, bail};
use std::{path::Path, sync::Mutex};
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

/// Decode the first QR code in an image file (PNG/JPEG/GIF), returning
/// its text. Powers "upload a QR screenshot" receives on desktops, where
/// holding a camera up to the screen is impractical.
pub fn decode_qr_file(path: &Path) -> Result<String> {
    let image = image::ImageReader::open(path)
        .and_then(|reader| reader.with_guessed_format())
        .map_err(|_| anyhow!("Couldn't read that image (PNG, JPEG, or GIF)."))?
        .decode()
        .map_err(|_| anyhow!("Couldn't read that image (PNG, JPEG, or GIF)."))?;
    let mut prepared = rqrr::PreparedImage::prepare(image.to_luma8());
    let grids = prepared.detect_grids();
    let Some(grid) = grids.first() else {
        bail!("No QR code found in that image.");
    };
    let (_, content) = grid
        .decode()
        .map_err(|_| anyhow!("Couldn't read that QR code."))?;
    Ok(content)
}

/// Turn decoded QR text into a ticket: a `fly://` link, an `https://`
/// bridge share link (ticket in the fragment, `?ticket=` fallback), or
/// the raw ticket itself. Anything else is rejected with a plain
/// message. Any bridge host is accepted — the ticket check is the real
/// gate, and the domain is expected to move over time.
pub fn ticket_from_qr_text(text: &str) -> Result<String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    if compact.is_empty() {
        bail!("That QR code is empty.");
    }
    if let Ok(url) = url::Url::parse(&compact) {
        if let Some(ticket) = parse_receive_link(&url) {
            return Ok(ticket);
        }
        if matches!(url.scheme(), "http" | "https") {
            let candidate = url
                .fragment()
                .filter(|fragment| !fragment.is_empty())
                .map(str::to_owned)
                .or_else(|| {
                    url.query_pairs()
                        .find(|(key, _)| key == "ticket")
                        .map(|(_, value)| value.into_owned())
                });
            if let Some(ticket) = candidate {
                let ticket: String =
                    ticket.chars().filter(|c| !c.is_whitespace()).collect();
                inspect::inspect(&ticket)?;
                return Ok(ticket);
            }
        }
        bail!("That QR code isn't a fly ticket or link.");
    }
    inspect::inspect(&compact)?;
    Ok(compact)
}

/// Decode a user-picked QR image into a receive ticket. The path always
/// comes from the file dialog, never from webview input.
#[tauri::command]
pub fn decode_qr_ticket(path: String) -> Result<String, String> {
    let text = decode_qr_file(Path::new(&path)).map_err(|error| format!("{error:#}"))?;
    ticket_from_qr_text(&text).map_err(|error| format!("{error:#}"))
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

    fn testdata(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join(name)
    }

    #[test]
    fn qr_fixture_decodes_to_link() {
        let text = decode_qr_file(&testdata("qr-fly-link.gif")).expect("fixture must decode");
        assert_eq!(text, "fly://receive?ticket=blobtestqr123");
    }

    #[test]
    fn image_without_qr_is_an_error() {
        let icon =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("icons").join("32x32.png");
        let error = format!("{:#}", decode_qr_file(&icon).unwrap_err());
        assert!(error.contains("No QR code"), "got: {error}");
        let missing = testdata("does-not-exist.png");
        let error = format!("{:#}", decode_qr_file(&missing).unwrap_err());
        assert!(error.contains("Couldn't read"), "got: {error}");
    }

    #[test]
    fn qr_text_accepts_link_or_raw_ticket() {
        use iroh::{EndpointAddr, SecretKey};
        use iroh_blobs::{BlobFormat, Hash, ticket::BlobTicket};
        use std::collections::BTreeSet;

        assert_eq!(
            ticket_from_qr_text("fly://receive?ticket=blobabc").unwrap(),
            "blobabc"
        );
        let addr = EndpointAddr {
            id: SecretKey::generate().public(),
            addrs: BTreeSet::new(),
        };
        let raw = BlobTicket::new(addr, Hash::from_bytes([5u8; 32]), BlobFormat::HashSeq).to_string();
        assert_eq!(ticket_from_qr_text(&raw).unwrap(), raw);
        // Bridge share links: fragment first, ?ticket= fallback, any host.
        assert_eq!(
            ticket_from_qr_text(&format!("https://fly.pritam.codes/r#{raw}")).unwrap(),
            raw
        );
        assert_eq!(
            ticket_from_qr_text(&format!("https://example.com/r?ticket={raw}")).unwrap(),
            raw
        );
        assert!(ticket_from_qr_text("https://example.com").is_err());
        assert!(ticket_from_qr_text("https://example.com/r#not-a-ticket").is_err());
        assert!(ticket_from_qr_text("   ").is_err());
    }
}
