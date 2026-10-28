use serde::Serialize;

/// Progress and status events streamed from Rust to the UI over a
/// [`tauri::ipc::Channel`]. Channels stay ordered under rapid async emission;
/// never use plain events for progress.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "event", content = "data")]
pub enum TransferEvent {
    Progress { done: u64, total: u64 },
    /// Emitted whenever the sender-side receiver count changes (including
    /// back to zero). Added in Phase 3; the plan expected it in Phase 4,
    /// but real serving needs it the moment transfers are real.
    Sending { receivers: u32 },
    Finished,
    /// Sender-side only: the receiver finished the download and told us.
    /// Arrives over a tiny second protocol after export, best-effort —
    /// absence of this event does not mean the transfer failed.
    Delivered,
    /// Sender-side only: the endpoint's addresses changed (network
    /// switch, relay move), so a fresh full ticket was minted. The UI
    /// replaces the displayed ticket and tells the user to re-send it.
    /// Only full tickets go stale; id tickets carry no addresses.
    TicketUpdated { ticket: String },
    // Failures travel as command `Err` strings, not events. Phase 4 may add:
    // WaitingForReceiver, fingerprint + diagnostics payloads.
}
