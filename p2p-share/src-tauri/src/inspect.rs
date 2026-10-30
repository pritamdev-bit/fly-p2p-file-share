//! Offline ticket inspection: parse a ticket and describe it without
//! connecting to anyone. Lets users check what a ticket points at (and
//! whose it is) in-app, instead of pasting live tickets into decoder
//! websites.

use anyhow::{Result, anyhow};
use iroh::TransportAddr;
use iroh_blobs::ticket::BlobTicket;
use serde::Serialize;
use std::str::FromStr;

/// What a ticket carries, for the "Ticket details" disclosure.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TicketInfo {
    /// Full sender node ID; the UI truncates it for display.
    pub fingerprint: String,
    /// Collection hash the ticket points at.
    pub hash: String,
    /// "id" (endpoint only, needs discovery) or "full" (carries addresses).
    pub kind: String,
    /// Relay URLs embedded in the ticket (full tickets only).
    pub relays: Vec<String>,
    /// Direct addresses embedded in the ticket (full tickets only).
    pub addrs: Vec<String>,
}

/// Parse `ticket_text` without any network use.
pub fn inspect(ticket_text: &str) -> Result<TicketInfo> {
    let ticket = BlobTicket::from_str(ticket_text.trim())
        .map_err(|_| anyhow!("That doesn't look like a fly ticket. Check for missing characters."))?;
    let addr = ticket.addr();
    let mut relays = Vec::new();
    let mut addrs = Vec::new();
    for transport in &addr.addrs {
        match transport {
            TransportAddr::Relay(url) => relays.push(url.to_string()),
            TransportAddr::Ip(sock) => addrs.push(sock.to_string()),
            // Foreign tickets may carry transports we never mint;
            // say so instead of silently dropping them.
            _ => addrs.push("(custom transport)".to_string()),
        }
    }
    Ok(TicketInfo {
        fingerprint: addr.id.to_string(),
        hash: ticket.hash().to_hex().to_string(),
        kind: if addr.addrs.is_empty() { "id" } else { "full" }.to_string(),
        relays,
        addrs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{EndpointAddr, SecretKey};
    use iroh_blobs::{BlobFormat, Hash};
    use std::collections::BTreeSet;

    fn ticket_with(addrs: BTreeSet<TransportAddr>) -> (String, String) {
        let id = SecretKey::generate().public();
        let addr = EndpointAddr { id, addrs };
        let hash = Hash::from_bytes([3u8; 32]);
        let ticket = BlobTicket::new(addr, hash, BlobFormat::HashSeq).to_string();
        (ticket, id.to_string())
    }

    #[test]
    fn id_ticket_carries_no_addresses() {
        let (ticket, id) = ticket_with(BTreeSet::new());
        let info = inspect(&ticket).expect("valid ticket must parse");
        assert_eq!(info.fingerprint, id);
        assert_eq!(info.kind, "id");
        assert!(info.relays.is_empty());
        assert!(info.addrs.is_empty());
        assert_eq!(info.hash.len(), 64, "hash must be full hex");
    }

    #[test]
    fn full_ticket_lists_relay_and_direct() {
        let relay: iroh::RelayUrl = "https://relay.example.com".parse().unwrap();
        let (ticket, id) = ticket_with(BTreeSet::from([
            TransportAddr::Relay(relay),
            TransportAddr::Ip("10.0.0.5:2222".parse().unwrap()),
        ]));
        let info = inspect(&ticket).expect("valid ticket must parse");
        assert_eq!(info.fingerprint, id);
        assert_eq!(info.kind, "full");
        assert_eq!(info.relays, vec!["https://relay.example.com/".to_string()]);
        assert_eq!(info.addrs, vec!["10.0.0.5:2222".to_string()]);
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(inspect("not a ticket").is_err());
        assert!(inspect("").is_err());
    }
}
