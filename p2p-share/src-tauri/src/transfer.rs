//! Thin Tauri adapters over [`crate::share`] and [`crate::download`].
//!
//! These translate between IPC types (`Channel`, command args/results) and
//! the core logic, which speaks only mpsc + anyhow and stays testable
//! without a Tauri context.

use crate::{
    download,
    events::TransferEvent,
    inspect,
    launch,
    prune,
    share::{self, ShareSession, TicketType},
};
use iroh::Endpoint;
use serde::Serialize;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tauri::{AppHandle, Manager, State, ipc::Channel};
use tokio::sync::mpsc;

/// What the UI needs once a share starts.
#[derive(Serialize)]
pub struct ShareInfo {
    pub ticket: String,
    pub fingerprint: String,
    pub port: Option<u16>,
}

enum ShareSlot {
    Empty,
    Starting { cancelled: bool },
    Active(ShareSession),
}

impl Default for ShareSlot {
    fn default() -> Self {
        Self::Empty
    }
}

/// How a running download was stopped: pause keeps the ticket's cache
/// for Resume, cancel deletes it so a re-receive starts from zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelMode {
    Pause,
    Cancel,
}

/// Control block for the running download. Lets the UI stop it
/// gracefully: closing the endpoint sends QUIC close frames, so the
/// sender releases the receiver slot immediately instead of holding it
/// until a connection timeout (which used to force 3-4 Resume clicks).
#[derive(Default)]
pub struct ReceiveControl {
    endpoint: Mutex<Option<Endpoint>>,
    cancelled: Mutex<Option<CancelMode>>,
}

impl ReceiveControl {
    /// Called by the download task once its endpoint exists.
    pub fn register(&self, endpoint: &Endpoint) {
        if let Ok(mut slot) = self.endpoint.lock() {
            *slot = Some(endpoint.clone());
        }
    }

    /// Graceful stop: record the mode, then close the endpoint. The
    /// running task's network calls fail promptly and it winds down on
    /// its own — no task abort, so no torn-down state.
    pub async fn cancel(&self, mode: CancelMode) {
        if let Ok(mut slot) = self.cancelled.lock() {
            *slot = Some(mode);
        }
        let endpoint = self
            .endpoint
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(endpoint) = endpoint {
            let _ = tokio::time::timeout(Duration::from_secs(5), endpoint.close()).await;
        }
    }

    pub fn cancel_mode(&self) -> Option<CancelMode> {
        self.cancelled.lock().ok().and_then(|slot| *slot)
    }

    pub fn was_cancelled(&self) -> bool {
        self.cancel_mode().is_some()
    }
}

/// What the UI needs once a download finishes.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiveInfo {
    pub name: String,
    /// True when every byte was already cached, so nothing transferred.
    pub from_cache: bool,
}

/// What the UI needs to show download-cache usage.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheInfo {
    pub bytes: u64,
    pub caches: u32,
}

/// What the UI needs after a manual cache clear.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheCleared {
    pub freed_bytes: u64,
    pub removed: u32,
}

#[derive(Default)]
pub struct AppState {
    /// Sync mutex, deliberately never held across `.await`
    /// (short critical sections only).
    share: Mutex<ShareSlot>,
    /// Completed downloads for the active share. Reset on every new
    /// share; the forwarder stops the session at the download limit.
    deliveries: Mutex<u32>,
    /// Async mutex: held across the whole download on purpose, so a
    /// second Start click fails fast instead of corrupting the cache.
    receive_lock: tokio::sync::Mutex<()>,
    /// Control block for the running download task, if any. Sync mutex,
    /// short critical sections only.
    receive_control: Mutex<Option<Arc<ReceiveControl>>>,
    /// Full path of the last successfully received file. The open/reveal
    /// commands act on exactly this — the UI passes no path, so it can
    /// never aim them at anything else.
    last_received: Mutex<Option<std::path::PathBuf>>,
}

fn spawn_forwarder(
    mut events: mpsc::UnboundedReceiver<TransferEvent>,
    channel: Channel<TransferEvent>,
) {
    tauri::async_runtime::spawn(async move {
        while let Some(event) = events.recv().await {
            if channel.send(event).is_err() {
                break;
            }
        }
    });
}

/// Take an active session out of the slot for shutdown, leaving
/// Starting/Empty untouched. The caller shuts it down after the lock
/// drops (shutdown awaits; the sync guard is not `Send`).
fn take_active_session(slot: &mut ShareSlot) -> Option<ShareSession> {
    match std::mem::replace(slot, ShareSlot::Empty) {
        ShareSlot::Active(session) => Some(session),
        other => {
            *slot = other;
            None
        }
    }
}

/// Auto-stop: the receiver confirmed delivery, so this share is done.
/// Stops serving (invalidating the ticket) and frees the slot.
async fn autostop_on_delivered(state: &AppState) {
    let session = state
        .share
        .lock()
        .ok()
        .and_then(|mut slot| take_active_session(&mut slot));
    if let Some(session) = session {
        session.shutdown().await;
    }
}

/// Sender-side forwarder: on Delivered it stops the session BEFORE
/// telling the UI, so the moment the UI hears "delivered" the slot is
/// free and re-sharing works.
/// Record one completed download; true when the share has served its
/// download limit and must stop. A poisoned lock stops (safe direction).
fn note_delivery(state: &AppState, max_downloads: u32) -> bool {
    match state.deliveries.lock() {
        Ok(mut count) => {
            *count = count.saturating_add(1);
            *count >= max_downloads
        }
        Err(_) => true,
    }
}

/// Sleep `after`, then stop whatever share is active. One task per share,
/// run inside the forwarder; a manual/auto stop drops the event senders
/// and the forwarder — timer included — exits first, so a stale timer can
/// never kill a later share.
async fn expire_share_after(state: &AppState, after: Duration) {
    tokio::time::sleep(after).await;
    autostop_on_delivered(state).await;
}

fn spawn_share_forwarder(
    app: AppHandle,
    mut events: mpsc::UnboundedReceiver<TransferEvent>,
    channel: Channel<TransferEvent>,
    limits: share::ShareLimits,
) {
    tauri::async_runtime::spawn(async move {
        let expiry = async {
            match limits.expires_after {
                Some(after) => expire_share_after(app.state::<AppState>().inner(), after).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(expiry);
        loop {
            tokio::select! {
                event = events.recv() => {
                    let Some(event) = event else { break };
                    if matches!(event, TransferEvent::Delivered)
                        && note_delivery(app.state::<AppState>().inner(), limits.max_downloads)
                    {
                        autostop_on_delivered(app.state::<AppState>().inner()).await;
                    }
                    if channel.send(event).is_err() {
                        break;
                    }
                }
                _ = &mut expiry, if limits.expires_after.is_some() => {
                    let _ = channel.send(TransferEvent::Expired);
                    break;
                }
            }
        }
    });
}

fn lock_share<'a>(state: &'a State<'a, AppState>) -> Result<std::sync::MutexGuard<'a, ShareSlot>, String> {
    state.share.lock().map_err(|_| "internal state error".to_string())
}

#[tauri::command]
pub async fn send_file(
    app: AppHandle,
    path: String,
    ticket_type: String,
    max_downloads: u32,
    expires_secs: u64,
    on_event: Channel<TransferEvent>,
) -> Result<ShareInfo, String> {
    let ticket_type = TicketType::parse(&ticket_type).map_err(|error| error.to_string())?;
    let limits = share::parse_limits(max_downloads, expires_secs)
        .map_err(|error| error.to_string())?;
    let state = app.state::<AppState>();
    {
        let mut slot = lock_share(&state)?;
        match *slot {
            ShareSlot::Empty => *slot = ShareSlot::Starting { cancelled: false },
            ShareSlot::Starting { .. } => {
                return Err("A share is already starting. Stop it first.".to_string());
            }
            ShareSlot::Active(_) => {
                return Err("Already sharing a file. Stop sharing first.".to_string());
            }
        }
    }
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    spawn_share_forwarder(app.clone(), events_rx, on_event, limits);
    let started = share::start_share(Path::new(&path), ticket_type, events_tx).await;
    // Decide under the lock; act after it. The sync guard must never be
    // held across an `.await` (it is not `Send`).
    enum PostStart {
        Keep(ShareInfo),
        Cancelled(ShareSession),
        Failed(String),
    }
    let action = {
        let mut slot = lock_share(&state)?;
        match started {
            Ok(started) => {
                if matches!(*slot, ShareSlot::Starting { cancelled: true }) {
                    *slot = ShareSlot::Empty;
                    PostStart::Cancelled(started.session)
                } else {
                    *state
                        .deliveries
                        .lock()
                        .map_err(|_| "internal state error".to_string())? = 0;
                    let info = ShareInfo {
                        ticket: started.ticket,
                        fingerprint: started.fingerprint,
                        port: started.port,
                    };
                    *slot = ShareSlot::Active(started.session);
                    PostStart::Keep(info)
                }
            }
            Err(error) => {
                *slot = ShareSlot::Empty;
                PostStart::Failed(format!("{error:#}"))
            }
        }
    };
    match action {
        PostStart::Keep(info) => Ok(info),
        PostStart::Cancelled(session) => {
            session.shutdown().await;
            Err("Sharing stopped.".to_string())
        }
        PostStart::Failed(message) => Err(message),
    }
}

#[tauri::command]
pub async fn receive_file(
    app: AppHandle,
    ticket: String,
    save_dir: String,
    on_event: Channel<TransferEvent>,
) -> Result<ReceiveInfo, String> {
    let state = app.state::<AppState>();
    let _guard = state
        .receive_lock
        .try_lock()
        .map_err(|_| "A download is already running.".to_string())?;
    let store_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("cannot locate app data: {error}"))?
        .join("blobs");
    // Best-effort: drop orphaned ticket caches older than the TTL
    // (crashes/kills leave them behind; cancel deletes its own),
    // then enforce the size cap oldest-first.
    prune::prune_old_dirs(&store_dir, "ticket-", prune::CACHE_TTL);
    prune::enforce_cache_cap(
        &store_dir,
        "ticket-",
        prune::CACHE_SIZE_CAP,
        prune::CACHE_EVICT_GRACE,
    );
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    spawn_forwarder(events_rx, on_event);
    // The download runs as a task so `cancel_receive` can stop it. The
    // lock stays in the command (not the task). Partial cache survives a
    // cancel and resumes next run.
    let control = Arc::new(ReceiveControl::default());
    *state
        .receive_control
        .lock()
        .map_err(|_| "internal state error".to_string())? = Some(control.clone());
    // The task moves save_dir in; keep a copy for the last-received record.
    let received_dir = save_dir.clone();
    let task = tokio::spawn({
        let control = control.clone();
        async move {
            download::receive(&ticket, Path::new(&save_dir), &store_dir, events_tx, control).await
        }
    });
    let result = task.await;
    *state
        .receive_control
        .lock()
        .map_err(|_| "internal state error".to_string())? = None;
    match result {
        // Success wins even if a stop landed during export: the file
        // is there, so claiming otherwise would be false.
        Ok(Ok((name, from_cache))) => {
            // The name is what export actually created (`file (1).zip`
            // when the folder already had one), so joining is exact.
            *state
                .last_received
                .lock()
                .map_err(|_| "internal state error".to_string())? =
                Some(Path::new(&received_dir).join(&name));
            Ok(ReceiveInfo { name, from_cache })
        }
        Ok(Err(_)) if control.cancel_mode() == Some(CancelMode::Pause) => {
            Err("Download paused.".to_string())
        }
        Ok(Err(_)) if control.cancel_mode() == Some(CancelMode::Cancel) => {
            Err("Download cancelled.".to_string())
        }
        Ok(Err(error)) => Err(format!("{error:#}")),
        Err(join) => Err(format!("download task failed: {join}")),
    }
}

async fn stop_receive(app: &AppHandle, mode: CancelMode) -> Result<String, String> {
    let state = app.state::<AppState>();
    let control = state
        .receive_control
        .lock()
        .map_err(|_| "internal state error".to_string())?
        .take();
    match control {
        Some(control) => {
            control.cancel(mode).await;
            Ok(match mode {
                CancelMode::Pause => "Download paused.".to_string(),
                CancelMode::Cancel => "Download cancelled.".to_string(),
            })
        }
        None => Ok("No download running.".to_string()),
    }
}

#[tauri::command]
pub async fn pause_receive(app: AppHandle) -> Result<String, String> {
    stop_receive(&app, CancelMode::Pause).await
}

#[tauri::command]
pub async fn cancel_receive(app: AppHandle) -> Result<String, String> {
    stop_receive(&app, CancelMode::Cancel).await
}

#[tauri::command]
pub async fn preview_ticket(app: AppHandle, ticket: String) -> Result<download::FilePreview, String> {
    let store_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("cannot locate app data: {error}"))?
        .join("blobs");
    prune::prune_old_dirs(&store_dir, "ticket-", prune::CACHE_TTL);
    prune::enforce_cache_cap(
        &store_dir,
        "ticket-",
        prune::CACHE_SIZE_CAP,
        prune::CACHE_EVICT_GRACE,
    );
    download::preview(&ticket, &store_dir)
        .await
        .map_err(|error| format!("{error:#}"))
}

/// Download-cache usage for the Diagnostics view. Lock-free on
/// purpose: sizes may be mid-write fuzzy, but the display must never
/// block on a transfer.
#[tauri::command]
pub fn get_cache_info(app: AppHandle) -> Result<CacheInfo, String> {
    let store_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("cannot locate app data: {error}"))?
        .join("blobs");
    let (bytes, caches) = prune::cache_usage(&store_dir, "ticket-");
    Ok(CacheInfo { bytes, caches })
}

/// Delete every ticket cache and report what went away. Refuses while
/// a download runs (its cache is live); waits for a running lookup
/// instead of deleting under it. Same lock order as receive_file
/// (receive lock, then store guard), so no deadlock either way.
#[tauri::command]
pub async fn clear_cache(app: AppHandle, state: State<'_, AppState>) -> Result<CacheCleared, String> {
    let busy = "A download is running; pause or cancel it before clearing the cache.".to_string();
    let _guard = state.receive_lock.try_lock().map_err(|_| busy.clone())?;
    if state
        .receive_control
        .lock()
        .map_err(|_| "internal state error".to_string())?
        .is_some()
    {
        return Err(busy);
    }
    let _store_guard = download::STORE_ACCESS.write().await;
    let store_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("cannot locate app data: {error}"))?
        .join("blobs");
    let (freed_bytes, removed) = prune::clear_caches(&store_dir, "ticket-");
    Ok(CacheCleared {
        freed_bytes,
        removed,
    })
}

/// Offline ticket inspection: parse only, never connects. Lets the UI
/// show whose ticket this is before the user downloads anything.
#[tauri::command]
pub fn inspect_ticket(ticket: String) -> Result<inspect::TicketInfo, String> {
    inspect::inspect(&ticket).map_err(|error| format!("{error:#}"))
}

/// Open the last received file with its default application. Takes no
/// path: only the backend-recorded receive path can be opened.
#[tauri::command]
pub fn open_received(app: AppHandle) -> Result<String, String> {
    let state = app.state::<AppState>();
    let slot = state
        .last_received
        .lock()
        .map_err(|_| "internal state error".to_string())?;
    let path = launch::resolve_recorded(&slot).map_err(|error| format!("{error:#}"))?;
    launch::open_path(&path).map_err(|error| format!("{error:#}"))?;
    Ok("Opened.".to_string())
}

/// Show the last received file in its folder. Takes no path (see above).
#[tauri::command]
pub fn reveal_received(app: AppHandle) -> Result<String, String> {
    let state = app.state::<AppState>();
    let slot = state
        .last_received
        .lock()
        .map_err(|_| "internal state error".to_string())?;
    let path = launch::resolve_recorded(&slot).map_err(|error| format!("{error:#}"))?;
    launch::reveal_path(&path).map_err(|error| format!("{error:#}"))?;
    Ok("Opened.".to_string())
}

#[tauri::command]
pub async fn stop_sharing(app: AppHandle) -> Result<String, String> {
    // Take the session out under the lock; shut it down after. The sync
    // guard must never be held across an `.await` (it is not `Send`).
    let state = app.state::<AppState>();
    let action = {
        let mut slot = lock_share(&state)?;
        match std::mem::replace(&mut *slot, ShareSlot::Empty) {
            ShareSlot::Active(session) => Some(session),
            ShareSlot::Starting { .. } => {
                *slot = ShareSlot::Starting { cancelled: true };
                return Ok("Stopping…".to_string());
            }
            ShareSlot::Empty => return Ok("Not sharing.".to_string()),
        }
    };
    if let Some(session) = action {
        session.shutdown().await;
        Ok("Sharing stopped.".to_string())
    } else {
        Ok("Not sharing.".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_test_dir(case: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("fly-test-{}-{}-{case}", std::process::id(), nanos))
    }

    #[test]
    fn take_active_session_leaves_other_slots_alone() {
        let mut slot = ShareSlot::Empty;
        assert!(take_active_session(&mut slot).is_none());
        assert!(matches!(slot, ShareSlot::Empty));

        let mut slot = ShareSlot::Starting { cancelled: false };
        assert!(take_active_session(&mut slot).is_none());
        assert!(matches!(slot, ShareSlot::Starting { cancelled: false }));
    }

    /// Delivered auto-stops the share: the slot frees up for re-share
    /// and the ticket stops working.
    #[tokio::test]
    async fn delivered_autostops_share_and_invalidates_ticket() {
        let _serial = crate::TEST_SERIAL.lock().await;
        let base = unique_test_dir("autostop");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let out2_dir = base.join("out2");
        let cache_dir = base.join("cache");
        let cache2_dir = base.join("cache2");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![9u8; 65536]).unwrap();

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            events_tx,
        )
        .await
        .expect("share");
        let state = AppState::default();
        *state.share.lock().unwrap() = ShareSlot::Active(started.session);

        let (recv_tx, _recv_rx) = mpsc::unbounded_channel();
        let (name, _) = download::receive(
            &started.ticket,
            &out_dir,
            &cache_dir,
            recv_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("receive");
        assert_eq!(name, "input.bin");
        // Drain sender events until Delivered, as the forwarder would.
        let mut delivered = false;
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_secs(15), events_rx.recv()).await
        {
            if matches!(event, TransferEvent::Delivered) {
                delivered = true;
                break;
            }
        }
        assert!(delivered, "sender never heard the completion ping");

        // The forwarder's Delivered branch: stop before telling the UI.
        autostop_on_delivered(&state).await;
        assert!(
            matches!(*state.share.lock().unwrap(), ShareSlot::Empty),
            "slot must be free for re-share after Delivered"
        );
        // The ticket is dead: a second receive fails instead of serving.
        let (recv2_tx, _recv2_rx) = mpsc::unbounded_channel();
        let retry = download::receive(
            &started.ticket,
            &out2_dir,
            &cache2_dir,
            recv2_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await;
        assert!(retry.is_err(), "ticket must be invalid after auto-stop");
        std::fs::remove_dir_all(&base).ok();
    }

    /// Delivery counting: only the max-th download stops the share.
    #[test]
    fn deliveries_stop_at_max() {
        let state = AppState::default();
        assert!(!note_delivery(&state, 2), "first of two must keep sharing");
        assert!(note_delivery(&state, 2), "second of two must stop");
    }

    /// Counting with a limit of two serves twice, then dies: two
    /// sequential receives succeed, the third fails. Mirrors the
    /// forwarder's Delivered branch exactly.
    #[tokio::test]
    async fn second_download_stops_share_at_two() {
        let _serial = crate::TEST_SERIAL.lock().await;
        let base = unique_test_dir("maxtwo");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let out2_dir = base.join("out2");
        let out3_dir = base.join("out3");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![9u8; 65536]).unwrap();

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            events_tx,
        )
        .await
        .expect("share");
        let state = AppState::default();
        *state.share.lock().unwrap() = ShareSlot::Active(started.session);

        for (n, out) in [&out_dir, &out2_dir].iter().enumerate() {
            let cache = base.join(format!("cache{n}"));
            let (recv_tx, _recv_rx) = mpsc::unbounded_channel();
            let (name, _) = download::receive(
                &started.ticket,
                out,
                &cache,
                recv_tx,
                Arc::new(ReceiveControl::default()),
            )
            .await
            .unwrap_or_else(|_| panic!("receive {} must succeed", n + 1));
            assert_eq!(name, "input.bin");
            // Drain sender events until Delivered, as the forwarder would.
            let mut delivered = false;
            while let Ok(Some(event)) =
                tokio::time::timeout(Duration::from_secs(15), events_rx.recv()).await
            {
                if matches!(event, TransferEvent::Delivered) {
                    delivered = true;
                    break;
                }
            }
            assert!(delivered, "sender never heard completion ping {}", n + 1);
            // The forwarder's Delivered branch: count, stop only at max.
            if note_delivery(&state, 2) {
                autostop_on_delivered(&state).await;
            }
        }
        assert!(
            matches!(*state.share.lock().unwrap(), ShareSlot::Empty),
            "slot must be free after the second download"
        );
        // The ticket is dead: a third receive fails instead of serving.
        let (recv3_tx, _recv3_rx) = mpsc::unbounded_channel();
        let retry = download::receive(
            &started.ticket,
            &out3_dir,
            &base.join("cache3"),
            recv3_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await;
        assert!(retry.is_err(), "ticket must be invalid after two downloads");
        std::fs::remove_dir_all(&base).ok();
    }

    /// Expiry stops an idle share: the slot frees up without any download.
    #[tokio::test]
    async fn expiry_stops_an_idle_share() {
        let _serial = crate::TEST_SERIAL.lock().await;
        let base = unique_test_dir("expiry");
        let src_dir = base.join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![9u8; 1024]).unwrap();

        let (events_tx, _events_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            events_tx,
        )
        .await
        .expect("share");
        let state = AppState::default();
        *state.share.lock().unwrap() = ShareSlot::Active(started.session);

        expire_share_after(&state, Duration::from_millis(50)).await;
        assert!(
            matches!(*state.share.lock().unwrap(), ShareSlot::Empty),
            "slot must be free after expiry"
        );
        std::fs::remove_dir_all(&base).ok();
    }
}
