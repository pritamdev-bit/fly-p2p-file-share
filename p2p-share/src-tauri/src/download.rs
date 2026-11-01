//! Incoming transfers: parse a ticket, download, verify, export.
//!
//! The filename always comes from the hashed collection metadata, never
//! from side-channel text. Progress streams to the UI over an mpsc channel
//! (the Tauri adapter forwards it into the IPC `Channel`).

use crate::{
    disk,
    events::TransferEvent,
    export::{export_file, sanitize_filename, validate_single_segment},
    share::{DONE_ALPN, METADATA_NAME, ShareMetadata, build_endpoint},
    transfer::{CancelMode, ReceiveControl},
};
use anyhow::{Context, Result, anyhow, bail};
use iroh::{Endpoint, EndpointAddr, endpoint::Connection};
use iroh_blobs::{
    BlobFormat, BlobsProtocol, Hash,
    api::{Store, remote::GetProgressItem},
    format::collection::Collection,
    hashseq::HashSeq,
    store::fs::FsStore,
    ticket::BlobTicket,
};
use n0_future::StreamExt;
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{RwLock, mpsc},
    time::timeout,
};

/// Timeouts: explicit everywhere so a stalled transfer always ends.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const METADATA_TIMEOUT: Duration = Duration::from_secs(60);
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// Reachability probe after a stall: long enough for a relayed dial,
/// short enough that a dead sender fails fast instead of burning retries.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// The completion ping is a courtesy: quick timeout, never fails the run.
const DONE_TIMEOUT: Duration = Duration::from_secs(5);

/// Same-process retry reuses already-downloaded ranges automatically.
/// Attempts after the first reconnect, so a network switch can recover.
const RETRY_LIMIT: u32 = 3;

/// Serializes ticket-store access across lookups and downloads. A
/// download holds the write guard for its whole run; a lookup holds a
/// read guard for its few seconds. This exists because opening the
/// same ticket store twice concurrently hangs forever (observed on
/// Windows: the second `FsStore::load` never returns, even after the
/// first store is dropped) — and a task stuck there keeps the receive
/// lock held, wedging the app until restart. Lookups never wait for a
/// download (`try_read` + busy error); downloads wait for lookups.
pub(crate) static STORE_ACCESS: RwLock<()> = RwLock::const_new(());

/// What a ticket points at, with the connection ready to pull from.
struct TicketTarget {
    endpoint: Endpoint,
    sender: EndpointAddr,
    store: Store,
    _protocol: BlobsProtocol,
    connection: Connection,
    cache_dir: PathBuf,
    collection: Hash,
    file_name: String,
    file_hash: Hash,
    total: u64,
}

/// What the receiver UI shows before downloading anything.
#[derive(Debug, Serialize)]
pub struct FilePreview {
    pub name: String,
    pub size: u64,
}

/// Parse the ticket, connect, and read the hashed file details.
/// Shared by preview (name + size only) and receive (full download).
async fn connect_and_describe(
    ticket_text: &str,
    base_dir: &Path,
    control: &ReceiveControl,
) -> Result<TicketTarget> {
    let ticket = BlobTicket::from_str(ticket_text.trim())
        .map_err(|_| anyhow!("That doesn't look like a fly ticket. Check for missing characters."))?;
    // Short tickets carry no addresses and resolve the sender over
    // discovery (DNS/Pkarr). When one fails to connect, say so: "full
    // works, short fails" almost always means filtered DNS.
    let id_kind = ticket.addr().addrs.is_empty();
    let dns_hint = if id_kind {
        " This is a short ticket, which finds the sender over discovery (DNS/Pkarr). If a full ticket from the same sender works but this fails, your network may be filtering DNS."
    } else {
        ""
    };
    // One cache dir per ticket (keyed by collection hash, so id and full
    // tickets for the same file share it). Pause keeps the dir for
    // resume; cancel deletes it. Orphaned dirs (crash, pause-forever)
    // are pruned by the Phase 4 TTL pass.
    let cache_dir = base_dir.join(format!("ticket-{}", ticket.hash().to_hex()));
    std::fs::create_dir_all(&cache_dir).context("cannot open transfer cache")?;
    let endpoint = build_endpoint().await?;
    control.register(&endpoint);
    // A stop that landed before the endpoint was registered could not
    // close anything; fail here instead, before contacting the sender.
    if control.was_cancelled() {
        anyhow::bail!("Download stopped.");
    }
    let store = Store::from(
        FsStore::load(&cache_dir)
            .await
            .context("cannot open transfer cache")?,
    );
    // Held for the whole transfer: backs the `Store` handle below.
    let _protocol = BlobsProtocol::new(&store, None);
    // Full-ticket relay/direct addresses are used as-is; id-only tickets
    // resolve through discovery (DNS/Pkarr).
    let sender = ticket.addr().clone();
    let connection = timeout(
        CONNECT_TIMEOUT,
        endpoint.connect(sender.clone(), iroh_blobs::protocol::ALPN),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "Couldn't reach the sender (connection timed out). Are they still sharing?{dns_hint}"
        )
    })?
    .context(format!("Couldn't reach the sender.{dns_hint}"))?;
    let remote = store.remote();
    let collection = timeout(
        METADATA_TIMEOUT,
        fetch_collection(&remote, &connection, &store, ticket.hash()),
    )
    .await
    .map_err(|_| anyhow!("Timed out waiting for transfer details."))??;
    let (file_name, file_hash, total) =
        describe_collection(&collection, &store, &remote, &connection).await?;
    Ok(TicketTarget {
        endpoint,
        sender,
        store,
        _protocol,
        connection,
        cache_dir,
        collection: ticket.hash(),
        file_name,
        file_hash,
        total,
    })
}

/// Look up a ticket's file name + size without downloading the file.
/// Refuses while a download runs (busy) instead of blocking on it.
pub async fn preview(ticket_text: &str, base_dir: &Path) -> Result<FilePreview> {
    let _guard = STORE_ACCESS
        .try_read()
        .map_err(|_| anyhow!("Lookup is busy: a download is using the transfer cache."))?;
    // Previews are short and not user-cancellable; the control is a dummy.
    let control = ReceiveControl::default();
    let target = connect_and_describe(ticket_text, base_dir, &control).await?;
    let preview = FilePreview {
        name: target.file_name.clone(),
        size: target.total,
    };
    close_endpoint(&target.endpoint).await;
    Ok(preview)
}

/// Download `ticket_text` into `save_dir`, caching blobs under `base_dir`
/// (one subdir per ticket). Returns the received file name plus whether
/// every byte was already cached, so the UI can say so honestly.
pub async fn receive(
    ticket_text: &str,
    save_dir: &Path,
    base_dir: &Path,
    events: mpsc::UnboundedSender<TransferEvent>,
    control: Arc<ReceiveControl>,
) -> Result<(String, bool)> {
    // Wait for any running lookup (seconds) before touching the store:
    // opening it twice concurrently hangs (see `STORE_ACCESS`).
    let _guard = STORE_ACCESS.write().await;
    let target = connect_and_describe(ticket_text, base_dir, &control).await?;
    // Disk preflight: refuse before downloading anything when the save
    // folder or the ticket cache cannot hold the transfer.
    let cached = target
        .store
        .remote()
        .local((target.file_hash, BlobFormat::Raw))
        .await
        .map(|info| info.local_bytes())
        .unwrap_or(0);
    let (need_save, need_cache) = disk::space_needs(target.total, cached);
    disk::ensure_space(
        disk::available_space(save_dir)?,
        need_save,
        save_dir,
        "in the save folder",
    )?;
    disk::ensure_space(
        disk::available_space(&target.cache_dir)?,
        need_cache,
        &target.cache_dir,
        "in the download cache",
    )?;
    let from_cache = match download_with_retry(&target, &events, &control).await {
        Ok(from_cache) => from_cache,
        Err(error) => {
            if control.cancel_mode() == Some(CancelMode::Cancel) {
                // True cancel (not pause): drop the store first to
                // release file locks, then delete this ticket's whole
                // cache dir so a re-receive starts from zero. Best
                // effort — leftovers just resume as usual.
                let cache_dir = target.cache_dir.clone();
                drop(target);
                if let Err(error) = std::fs::remove_dir_all(&cache_dir) {
                    eprintln!("fly: cancel cleanup of {} failed: {error}", cache_dir.display());
                }
            } else if !control.was_cancelled() {
                // No stop closed this endpoint (pause/cancel do); say
                // goodbye explicitly so the sender frees its receiver
                // slot at once instead of on connection timeout.
                close_endpoint(&target.endpoint).await;
            }
            return Err(error);
        }
    };
    // The export may suffix the name (`file (1).zip`) when the save
    // folder already has one — the UI and the open/reveal record must
    // use what was actually created, not the ticket name.
    let final_path =
        export_file(&target.store, target.file_hash, &target.file_name, save_dir).await?;
    let received_name = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| target.file_name.clone());
    // Make the store durable before returning: a rapid re-receive opens
    // this same dir again, and must see the complete bitfield. Best
    // effort — if it fails, the next fetch just re-downloads some ranges.
    let _ = target.store.sync_db().await;
    // Tell the sender "I got it". Best-effort: a failure here (sender
    // already stopped sharing, network gone) must not fail the receive.
    notify_sender(&target.endpoint, &target.sender, target.collection).await;
    let _ = events.send(TransferEvent::Finished);
    close_endpoint(&target.endpoint).await;
    Ok((received_name, from_cache))
}

/// Dial the sender's completion-ping protocol and send the collection
/// hash (which identifies the share). Best-effort by design: the result
/// is deliberately ignored.
async fn notify_sender(endpoint: &Endpoint, sender: &EndpointAddr, collection: Hash) {
    let ping = async {
        let connection = endpoint
            .connect(sender.clone(), DONE_ALPN)
            .await
            .context("done-ping connect failed")?;
        let (mut send, mut recv) = connection.open_bi().await.context("no ping stream")?;
        send.write_all(collection.as_bytes())
            .await
            .context("ping write failed")?;
        send.finish().context("ping finish failed")?;
        // Wait for the ack: it proves the sender processed the ping
        // before we tear the endpoint down.
        let mut ack = [0u8; 1];
        recv.read_exact(&mut ack).await.context("no ping ack")?;
        Ok::<(), anyhow::Error>(())
    };
    if timeout(DONE_TIMEOUT, ping).await.is_err() {
        eprintln!("fly: completion ping timed out");
    }
}

/// Fetch one blob fully, ignoring progress (for small metadata).
async fn fetch_to_end(
    remote: &iroh_blobs::api::remote::Remote,
    connection: &Connection,
    hash: Hash,
    format: BlobFormat,
) -> Result<()> {
    let progress = remote.fetch(connection.clone(), (hash, format));
    let mut stream = progress.stream();
    while let Some(item) = stream.next().await {
        match item {
            GetProgressItem::Done(_) => return Ok(()),
            GetProgressItem::Error(cause) => bail!("download failed: {cause}"),
            GetProgressItem::Progress(_) => {}
        }
    }
    Err(anyhow!("download ended unexpectedly"))
}

/// Fetch ONLY a collection's framing blobs (root sequence + name table),
/// then parse it. Everything must be requested as `Raw`: `HashSeq` would
/// recursively pull every child — including the whole file.
async fn fetch_collection(
    remote: &iroh_blobs::api::remote::Remote,
    connection: &Connection,
    db: &Store,
    collection_hash: Hash,
) -> Result<Collection> {
    fetch_to_end(remote, connection, collection_hash, BlobFormat::Raw).await?;
    let root_bytes = db
        .get_bytes(collection_hash)
        .await
        .context("downloaded data is incomplete")?;
    let sequence = HashSeq::try_from(root_bytes).context("transfer details are corrupt")?;
    let meta_hash = sequence
        .iter()
        .next()
        .context("transfer details are empty")?;
    fetch_to_end(remote, connection, meta_hash, BlobFormat::Raw).await?;
    Collection::load(collection_hash, db)
        .await
        .context("downloaded data is incomplete")
}

/// Split the collection into our metadata entry plus the single file entry.
/// Fetches only the tiny metadata blob; the file blob stays remote.
async fn describe_collection(
    collection: &Collection,
    db: &Store,
    remote: &iroh_blobs::api::remote::Remote,
    connection: &Connection,
) -> Result<(String, Hash, u64)> {
    let mut metadata_hash = None;
    let mut files = Vec::new();
    for (name, hash) in collection.iter() {
        if name == METADATA_NAME {
            metadata_hash = Some(*hash);
        } else {
            files.push((name.clone(), *hash));
        }
    }
    let metadata_hash = metadata_hash.context("ticket is missing file details")?;
    if files.len() != 1 {
        bail!("unsupported share: expected exactly one file");
    }
    // Bounded like every other network wait: an untimed stall here used
    // to hang receives forever on a tarpitted connection.
    timeout(
        METADATA_TIMEOUT,
        fetch_to_end(remote, connection, metadata_hash, BlobFormat::Raw),
    )
    .await
    .map_err(|_| anyhow!("Timed out waiting for transfer details."))??;
    let bytes = db
        .get_bytes(metadata_hash)
        .await
        .context("file details are incomplete")?;
    let metadata: ShareMetadata =
        serde_json::from_slice(&bytes).context("file details are corrupt")?;
    validate_single_segment(&metadata.name)?;
    let (entry_name, entry_hash) = files.into_iter().next().expect("checked above");
    if entry_name != metadata.name {
        bail!("share contents are inconsistent");
    }
    // Sanitize once here so preview, export, and the returned name all
    // agree on what will actually be created (a Linux sender can share
    // `CON` or `a:b`, which Windows cannot create as-is).
    Ok((sanitize_filename(&metadata.name), entry_hash, metadata.size))
}

/// The stall message: a dead sender must read as one, not as a
/// cryptic timeout. Progress is always intact (cache survives), so the
/// message points at Resume.
fn sender_offline() -> anyhow::Error {
    anyhow!("The sender went offline. Your progress is saved; Resume when they're back to continue.")
}

/// Short reachability check: can we still dial the sender at all?
/// Used to tell "sender is gone" (fail fast) from "transient stall"
/// (retry as before). The probe connection is dropped on success; the
/// endpoint close at the end of the transfer reaps it.
async fn probe_sender(endpoint: &Endpoint, sender: &EndpointAddr) -> Result<()> {
    timeout(
        PROBE_TIMEOUT,
        endpoint.connect(sender.clone(), iroh_blobs::protocol::ALPN),
    )
    .await
    .map_err(|_| anyhow!("sender unreachable"))?
    .map(|_| ())
    .map_err(|_| anyhow!("sender unreachable"))
}

/// Shut an endpoint down without hanging the task: the graceful drain
/// waits for peers, so it is bounded — a wedged connection must never
/// wedge the transfer (or the receive lock it holds).
async fn close_endpoint(endpoint: &Endpoint) {
    if timeout(Duration::from_secs(10), endpoint.close())
        .await
        .is_err()
    {
        eprintln!("fly: endpoint close timed out; continuing");
    }
}

/// Fetch the file blob with progress, retrying transient failures.
/// Already-downloaded ranges are reused on retry automatically.
/// Returns whether every byte was already cached (nothing transferred).
async fn download_with_retry(
    target: &TicketTarget,
    events: &mpsc::UnboundedSender<TransferEvent>,
    control: &ReceiveControl,
) -> Result<bool> {
    let remote = target.store.remote();
    let (hash, total) = (target.file_hash, target.total);
    let mut attempt = 0u32;
    let mut done = 0u64;
    let mut from_cache = false;
    let mut active = target.connection.clone();
    loop {
        attempt += 1;
        // Resume accounting: fetch progress counts only bytes downloaded
        // in this request, so re-seed the bar from what's cached at the
        // start of every attempt. Fresh downloads seed 0.
        let base = remote
            .local((hash, BlobFormat::Raw))
            .await
            .map(|info| info.local_bytes())
            .unwrap_or(0)
            .min(total);
        if attempt == 1 {
            from_cache = base == total;
        }
        done = done.max(base);
        let _ = events.send(TransferEvent::Progress { done, total });
        // Attempt 1 reuses the metadata connection. Later attempts dial
        // fresh: after a network change the old connection is dead, and
        // retrying on it would fail every time. The previous attempt's
        // connection is closed first: it is still open and holds the
        // sender's single receiver slot, so a redial without closing
        // would be rejected by our own stale connection. The backoff
        // sleep after a failed attempt gives the close time to land.
        if attempt > 1 {
            active.close(0u32.into(), b"retry");
            active = timeout(
                CONNECT_TIMEOUT,
                target
                    .endpoint
                    .connect(target.sender.clone(), iroh_blobs::protocol::ALPN),
            )
            .await
            .map_err(|_| {
                anyhow!(
                    "Couldn't reach the sender (reconnect timed out). \
                     They may have gone offline; your progress is saved, \
                     so Resume to continue."
                )
            })?
            .context("Couldn't reach the sender")?;
        }
        let connection = &active;
        // Report which pipe carries this attempt (and any mid-transfer
        // path upgrade) so the UI can show direct vs relayed live.
        crate::share::watch_connection_path(connection, events.clone());
        let progress = remote.fetch(connection.clone(), (hash, BlobFormat::Raw));
        let mut stream = progress.stream();
        let mut failed: Option<anyhow::Error> = None;
        let mut finished = false;
        loop {
            match timeout(IDLE_TIMEOUT, stream.next()).await {
                Err(_) => {
                    // Idle timeout: nothing arrived for 120s. A closed
                    // connection means the sender is gone — say so at
                    // once instead of burning retries against the dead.
                    // Otherwise probe: an unreachable sender fails fast
                    // with the same message, a reachable one retries.
                    // No diagnosis while WE are stopping: a probe dial
                    // during endpoint teardown only delays the wind-down
                    // (and on a loaded machine can stall it past the
                    // close cap, leaving store files locked).
                    if !control.was_cancelled()
                        && (connection.close_reason().is_some()
                            || probe_sender(&target.endpoint, &target.sender).await.is_err())
                    {
                        return Err(sender_offline());
                    }
                    // Only surfaces after RETRY_LIMIT stalls against a
                    // sender that is still dialable — a sick network,
                    // not a dead sender.
                    failed = Some(anyhow!(
                        "The transfer stalled: no data arrived after several retries. \
                         Your progress is saved; Resume to try again."
                    ));
                    break;
                }
                Ok(None) => {
                    failed = Some(anyhow!("download ended unexpectedly"));
                    break;
                }
                Ok(Some(GetProgressItem::Progress(offset))) => {
                    // Never let the bar move backwards on retooled offsets.
                    done = done.max(base.saturating_add(offset)).min(total);
                    let _ = events.send(TransferEvent::Progress { done, total });
                }
                Ok(Some(GetProgressItem::Done(_))) => {
                    finished = true;
                    break;
                }
                Ok(Some(GetProgressItem::Error(cause))) => {
                    // A dead connection (sender stopped sharing, QUIC gave
                    // up) plus a failed redial is the sender going away
                    // mid-transfer — not a transient blip worth retrying.
                    // Skipped while WE are stopping (see the stall
                    // branch): the stop already decided the outcome.
                    if !control.was_cancelled()
                        && connection.close_reason().is_some()
                        && probe_sender(&target.endpoint, &target.sender).await.is_err()
                    {
                        return Err(sender_offline());
                    }
                    failed = Some(anyhow!("download failed: {cause}"));
                    break;
                }
            }
        }
        if finished {
            break;
        }
        if attempt >= RETRY_LIMIT {
            return Err(failed.unwrap_or_else(|| anyhow!("download failed")));
        }
        // Back off between attempts: a network switch needs a moment.
        tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
    }
    let _ = events.send(TransferEvent::Progress { done: total, total });
    Ok(from_cache)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share;
    use std::path::PathBuf;

    /// Within one test, share + receive still run concurrently — that
    /// is the product scenario; cross-test parallelism is not (see
    /// `crate::TEST_SERIAL`).
    use crate::TEST_SERIAL as SERIAL;

    /// Wait for a genuinely partial progress event (some but not all
    /// bytes), so a test can stop a transfer mid-flight.
    async fn wait_for_partial(rx: &mut mpsc::UnboundedReceiver<TransferEvent>) -> bool {
        let begin = std::time::Instant::now();
        while begin.elapsed() < Duration::from_secs(60) {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(TransferEvent::Progress { done, total })) => {
                    if done > 0 && done < total {
                        return true;
                    }
                }
                // Path reports interleave with progress; ignore them.
                Ok(Some(TransferEvent::Path { .. })) => {}
                _ => return false,
            }
        }
        false
    }
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_dir(case: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("fly-test-{}-{}-{case}", std::process::id(), nanos))
    }

    /// Full pipeline in one process: share a file, receive it elsewhere,
    /// compare bytes. Exercises import, ticket, connect, download, export.
    #[tokio::test]
    async fn share_receive_roundtrip_same_process() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("roundtrip");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let content: Vec<u8> = (0..65536u32).map(|i| (i.wrapping_mul(7)) as u8).collect();
        std::fs::write(src_dir.join("input.bin"), &content).unwrap();

        let (send_tx, mut send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let (recv_tx, _recv_rx) = mpsc::unbounded_channel();
        let (name, from_cache) = receive(
            &started.ticket,
            &out_dir,
            &cache_dir,
            recv_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("receive");
        assert_eq!(name, "input.bin");
        assert!(!from_cache, "fresh receive must transfer bytes");
        assert_eq!(
            std::fs::read(out_dir.join("input.bin")).unwrap(),
            content
        );
        // The receiver must tell the sender it finished.
        let mut delivered = false;
        let deadline = tokio::time::sleep(Duration::from_secs(10));
        tokio::pin!(deadline);
        while !delivered {
            tokio::select! {
                event = send_rx.recv() => {
                    match event {
                        Some(TransferEvent::Delivered) => delivered = true,
                        None => break,
                        _ => {}
                    }
                }
                _ = &mut deadline => break,
            }
        }
        assert!(delivered, "sender never received the completion ping");
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Sender death mid-transfer must read as "went offline", not a
    /// cryptic timeout — and it must fail fast, not burn retries.
    #[tokio::test]
    async fn sender_shutdown_mid_transfer_reports_offline() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("offline");
        let src_dir = base.join("src");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![7u8; 65536]).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let control = ReceiveControl::default();
        let target = connect_and_describe(&started.ticket, &cache_dir, &control)
            .await
            .expect("describe");
        // Live sender: the probe passes.
        probe_sender(&target.endpoint, &target.sender)
            .await
            .expect("live sender must be reachable");
        // Sender stops sharing before the file fetch starts.
        started.session.shutdown().await;

        let (events_tx, _events_rx) = mpsc::unbounded_channel();
        let begin = std::time::Instant::now();
        let error = download_with_retry(&target, &events_tx, &control)
            .await
            .expect_err("fetch against a dead sender must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("offline"),
            "must read as sender-offline, got: {message}"
        );
        assert!(
            begin.elapsed() < Duration::from_secs(60),
            "must fail fast instead of hanging on retries"
        );
        target.endpoint.close().await;
        std::fs::remove_dir_all(&base).ok();
    }

    fn dir_bytes(dir: &Path) -> u64 {
        let mut total = 0u64;
        let mut entries = vec![dir.to_path_buf()];
        while let Some(dir) = entries.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let entry = entry.unwrap();
                let meta = entry.metadata().unwrap();
                if meta.is_dir() {
                    entries.push(entry.path());
                } else {
                    total += meta.len();
                }
            }
        }
        total
    }

    /// Previewing a 128 MiB file must not pull its bytes: only the
    /// collection root + metadata blob (a few KB) may land in the cache.
    /// (Fetching the ticket hash as HashSeq would download everything.)
    #[tokio::test]
    async fn preview_does_not_download_file() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("preview-big");
        let src_dir = base.join("src");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let big: Vec<u8> = (0..(128u32 * 1024 * 1024)).map(|i| (i.wrapping_mul(7)) as u8).collect();
        std::fs::write(src_dir.join("big.bin"), &big).unwrap();
        drop(big);

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(&src_dir.join("big.bin"), share::TicketType::Full, send_tx)
            .await
            .expect("share");
        let found = preview(&started.ticket, &cache_dir).await.expect("preview");
        assert_eq!(found.name, "big.bin");
        assert_eq!(found.size, 128 * 1024 * 1024);
        let cached = dir_bytes(&cache_dir);
        // FsStore preallocates a ~1 MiB data file, so "a few KB" reads as
        // ~1 MiB on disk. Anything near 128 MiB means file bytes leaked in.
        assert!(
            cached < 2 * 1024 * 1024,
            "preview pulled file bytes into the cache ({cached} bytes)"
        );
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// A re-receive against a warm cache must report full progress from
    /// the first event (the bytes are reused, not re-downloaded) and
    /// export again successfully.
    #[tokio::test]
    async fn cached_rereceive_reports_full_progress() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("recache");
        let src_dir = base.join("src");
        let out_first = base.join("out1");
        let out_second = base.join("out2");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let content: Vec<u8> = (0..32768u32).map(|i| (i.wrapping_mul(3)) as u8).collect();
        std::fs::write(src_dir.join("again.bin"), &content).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("again.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let (first_tx, _first_rx) = mpsc::unbounded_channel();
        receive(
            &started.ticket,
            &out_first,
            &cache_dir,
            first_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("first receive");

        let (second_tx, mut second_rx) = mpsc::unbounded_channel();
        let (name, from_cache) = receive(
            &started.ticket,
            &out_second,
            &cache_dir,
            second_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("second receive");
        assert_eq!(name, "again.bin");
        assert!(from_cache, "warm re-receive must report from-cache");
        assert_eq!(
            std::fs::read(out_second.join("again.bin")).unwrap(),
            content
        );
        let mut progress_events = 0u32;
        let mut finished = false;
        while let Ok(event) = second_rx.try_recv() {
            match event {
                TransferEvent::Progress { done, total } => {
                    progress_events += 1;
                    assert_eq!(done, total, "re-receive must start at full progress");
                    assert_eq!(total, content.len() as u64);
                }
                TransferEvent::Finished => finished = true,
                _ => {}
            }
        }
        assert!(progress_events >= 1, "expected progress events");
        assert!(finished, "expected Finished");
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Cancel a receive mid-transfer the way the UI does (graceful
    /// close), then re-receive immediately: the sender must have released
    /// the receiver slot, so resume works on the first try.
    #[tokio::test]
    async fn cancel_then_immediate_rereceive() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("pauseresume");
        let src_dir = base.join("src");
        let out_first = base.join("out1");
        let out_second = base.join("out2");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        // 128 MiB: big enough that aborting on the first partial
        // progress event always lands mid-transfer (~7s window).
        let big: Vec<u8> = (0..(128u32 * 1024 * 1024)).map(|i| (i.wrapping_mul(7)) as u8).collect();
        std::fs::write(src_dir.join("big.bin"), &big).unwrap();
        drop(big);

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(&src_dir.join("big.bin"), share::TicketType::Full, send_tx)
            .await
            .expect("share");
        let ticket = started.ticket.clone();

        // Start the receive as a task, like transfer.rs does.
        let cache_clone = cache_dir.clone();
        let out_clone = out_first.clone();
        let ticket_clone = ticket.clone();
        let (first_tx, mut first_rx) = mpsc::unbounded_channel();
        let control = Arc::new(ReceiveControl::default());
        let task_control = control.clone();
        let task = tokio::spawn(async move {
            receive(&ticket_clone, &out_clone, &cache_clone, first_tx, task_control).await
        });
        assert!(
            wait_for_partial(&mut first_rx).await,
            "never saw a partial progress event"
        );
        // Graceful pause, exactly like pause_receive does.
        control.cancel(CancelMode::Pause).await;
        let stopped = task.await.expect("task must wind down, not abort");
        assert!(stopped.is_err(), "paused receive must fail");
        assert!(control.was_cancelled());

        // Immediate re-receive must work first try: the close frames the
        // pause sent already freed the sender's receiver slot. The cache
        // survives a pause, so this resumes instead of restarting.
        let (second_tx, _second_rx) = mpsc::unbounded_channel();
        let (name, _) = receive(
            &ticket,
            &out_second,
            &cache_dir,
            second_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("immediate re-receive must succeed");
        assert_eq!(name, "big.bin");
        assert_eq!(std::fs::read(out_second.join("big.bin")).unwrap().len(), 128 * 1024 * 1024);
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Cancel (not pause) deletes the ticket's cache dir, so a re-receive
    /// starts from zero instead of resuming.
    #[tokio::test]
    async fn cancel_drops_partial_data() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("canceldrop");
        let src_dir = base.join("src");
        let out_first = base.join("out1");
        let out_second = base.join("out2");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let big: Vec<u8> = (0..(32u32 * 1024 * 1024)).map(|i| (i.wrapping_mul(11)) as u8).collect();
        std::fs::write(src_dir.join("drop.bin"), &big).unwrap();
        drop(big);

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(&src_dir.join("drop.bin"), share::TicketType::Full, send_tx)
            .await
            .expect("share");
        let ticket = started.ticket.clone();

        let cache_clone = cache_dir.clone();
        let out_clone = out_first.clone();
        let ticket_clone = ticket.clone();
        let (first_tx, mut first_rx) = mpsc::unbounded_channel();
        let control = Arc::new(ReceiveControl::default());
        let task_control = control.clone();
        let task = tokio::spawn(async move {
            receive(&ticket_clone, &out_clone, &cache_clone, first_tx, task_control).await
        });
        assert!(
            wait_for_partial(&mut first_rx).await,
            "never saw a partial progress event"
        );
        control.cancel(CancelMode::Cancel).await;
        let stopped = task.await.expect("task must wind down, not abort");
        assert!(stopped.is_err(), "cancelled receive must fail");
        // This ticket was the only cache user: its dir must be gone.
        let remaining: Vec<_> = std::fs::read_dir(&cache_dir)
            .map(|entries| entries.flatten().collect())
            .unwrap_or_default();
        assert!(
            remaining.is_empty(),
            "cancel must delete the ticket cache dir"
        );

        // Re-receive restarts from zero: some progress event is partial.
        let (second_tx, mut second_rx) = mpsc::unbounded_channel();
        let (name, from_cache) = receive(
            &ticket,
            &out_second,
            &cache_dir,
            second_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("re-receive must succeed");
        assert_eq!(name, "drop.bin");
        assert!(!from_cache, "fresh re-receive must transfer bytes");
        assert_eq!(std::fs::read(out_second.join("drop.bin")).unwrap().len(), 32 * 1024 * 1024);
        let mut saw_partial = false;
        while let Ok(event) = second_rx.try_recv() {
            if let TransferEvent::Progress { done, total } = event {
                if done < total {
                    saw_partial = true;
                }
            }
        }
        assert!(saw_partial, "re-receive must start from zero");
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Preview reads the hashed name + size without downloading the file.
    #[tokio::test]
    async fn preview_reports_name_and_size() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("preview");
        let src_dir = base.join("src");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("hello.txt"), b"hello preview").unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(&src_dir.join("hello.txt"), share::TicketType::Id, send_tx)
            .await
            .expect("share");
        let found = preview(&started.ticket, &cache_dir).await.expect("preview");
        assert_eq!(found.name, "hello.txt");
        assert_eq!(found.size, 13);
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Impatient start: the receive begins while the ticket lookup still
    /// runs. Both must succeed — the receive waits for the lookup's
    /// short store access instead of failing on the locked cache dir.
    #[tokio::test]
    async fn preview_then_immediate_receive_both_succeed() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("overlap");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let content: Vec<u8> = (0..65536u32).map(|i| (i.wrapping_mul(7)) as u8).collect();
        std::fs::write(src_dir.join("input.bin"), &content).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let ticket = started.ticket.clone();

        // Lookup first, like a pasted ticket; wait until it has the
        // ticket's store open, then start the receive mid-lookup.
        let cache_clone = cache_dir.clone();
        let ticket_clone = ticket.clone();
        let lookup = tokio::spawn(async move { preview(&ticket_clone, &cache_clone).await });
        let hash_hex = BlobTicket::from_str(&ticket).unwrap().hash().to_hex();
        let store_file = cache_dir
            .join(format!("ticket-{hash_hex}"))
            .join("blobs.db");
        let begin = std::time::Instant::now();
        while !store_file.exists() && begin.elapsed() < Duration::from_secs(60) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(store_file.exists(), "lookup never opened the store");

        let (recv_tx, _recv_rx) = mpsc::unbounded_channel();
        let received = timeout(
            Duration::from_secs(150),
            receive(
                &ticket,
                &out_dir,
                &cache_dir,
                recv_tx,
                Arc::new(ReceiveControl::default()),
            ),
        )
        .await
        .expect("receive hung while a lookup ran")
        .expect("receive must wait for the lookup, not fail on it");
        assert_eq!(received.0, "input.bin");
        let found = timeout(Duration::from_secs(150), lookup)
            .await
            .expect("lookup hung")
            .expect("lookup task failed")
            .expect("lookup must succeed");
        assert_eq!(found.name, "input.bin");
        assert_eq!(std::fs::read(out_dir.join("input.bin")).unwrap(), content);
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// A lookup attempted while a download runs must fail fast as busy,
    /// never block on the minutes-long transfer (nor report a cryptic
    /// store-lock error). The UI never triggers this — Start is disabled
    /// during lookup — but a stray call must stay harmless.
    #[tokio::test]
    async fn preview_during_running_receive_is_busy_not_stuck() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("busyprobe");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        // 32 MiB: big enough to still be mid-flight when the lookup lands.
        let big: Vec<u8> = (0..(32u32 * 1024 * 1024)).map(|i| (i.wrapping_mul(11)) as u8).collect();
        std::fs::write(src_dir.join("drop.bin"), &big).unwrap();
        drop(big);

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(&src_dir.join("drop.bin"), share::TicketType::Full, send_tx)
            .await
            .expect("share");
        let ticket = started.ticket.clone();

        let cache_clone = cache_dir.clone();
        let out_clone = out_dir.clone();
        let ticket_clone = ticket.clone();
        let (first_tx, mut first_rx) = mpsc::unbounded_channel();
        let control = Arc::new(ReceiveControl::default());
        let task_control = control.clone();
        let task = tokio::spawn(async move {
            receive(&ticket_clone, &out_clone, &cache_clone, first_tx, task_control).await
        });
        assert!(
            wait_for_partial(&mut first_rx).await,
            "never saw a partial progress event"
        );
        let begin = std::time::Instant::now();
        let error = timeout(Duration::from_secs(60), preview(&ticket, &cache_dir))
            .await
            .expect("lookup hung during a download")
            .expect_err("lookup during a download must not run");
        let message = format!("{error:#}");
        assert!(
            message.contains("busy"),
            "lookup during a download must read as busy, got: {message}"
        );
        assert!(
            begin.elapsed() < Duration::from_secs(30),
            "busy lookup must fail fast, not block on the transfer"
        );
        control.cancel(CancelMode::Cancel).await;
        let stopped = task.await.expect("task must wind down, not abort");
        assert!(stopped.is_err(), "cancelled receive must fail");
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// The stall probe dials the sender's blob protocol, which takes the
    /// single receiver slot. It must release that slot (close the probe
    /// connection) instead of pinning it: otherwise the next dial — a
    /// retry or a re-receive — is rejected and the sender looks busy
    /// forever, with its connection count stuck at 1.
    #[tokio::test]
    async fn probe_releases_sender_slot() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("probeslot");
        let src_dir = base.join("src");
        let probe_dir = base.join("probecache");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![7u8; 65536]).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let ticket = BlobTicket::from_str(&started.ticket).unwrap();
        let sender = ticket.addr().clone();
        let collection = ticket.hash();

        let endpoint = build_endpoint().await.expect("endpoint");
        probe_sender(&endpoint, &sender)
            .await
            .expect("live sender must be reachable");
        // Let the probe's close land before dialing again.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let connection = timeout(
            CONNECT_TIMEOUT,
            endpoint.connect(sender.clone(), iroh_blobs::protocol::ALPN),
        )
        .await
        .expect("redial hung")
        .expect("redial rejected: the probe pinned the sender slot");
        // A rejected connection serves nothing; fetch on it fails.
        let store = Store::from(FsStore::load(&probe_dir).await.expect("probe store"));
        let _protocol = BlobsProtocol::new(&store, None);
        timeout(
            METADATA_TIMEOUT,
            fetch_to_end(&store.remote(), &connection, collection, BlobFormat::Raw),
        )
        .await
        .expect("fetch hung")
        .expect("fetch on the redial must succeed: the slot must be free");
        endpoint.close().await;
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// A receiver that vanishes without goodbye (app kill, power loss)
    /// sends no close frames; the sender still must not show it
    /// forever. QUIC's idle timeout reaps the dead connection, so the
    /// receiver count drops back — slowly, but bounded.
    #[tokio::test]
    async fn receiver_vanish_frees_sender_count() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("vanish");
        let src_dir = base.join("src");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![7u8; 65536]).unwrap();

        let (send_tx, mut send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let control = ReceiveControl::default();
        let target = connect_and_describe(&started.ticket, &cache_dir, &control)
            .await
            .expect("describe");
        // The sender saw the receiver arrive.
        assert!(
            wait_for_sending(&mut send_rx, 1, Duration::from_secs(30)).await,
            "sender never counted the receiver"
        );
        // Vanish: drop everything WITHOUT close, like a process kill.
        drop(target);
        // The count must drop back via idle timeout (~30s), not never.
        assert!(
            wait_for_sending(&mut send_rx, 0, Duration::from_secs(90)).await,
            "sender still shows the vanished receiver after 90s"
        );
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Receiving twice into the same folder must not fail: the second
    /// export takes a ` (1)` suffix, and the returned name is what was
    /// actually created (the open/reveal record joins it).
    #[tokio::test]
    async fn second_receive_into_same_folder_suffixes() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("dupsuffix");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let content: Vec<u8> = (0..65536u32).map(|i| (i.wrapping_mul(7)) as u8).collect();
        std::fs::write(src_dir.join("input.bin"), &content).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let (first_tx, _first_rx) = mpsc::unbounded_channel();
        let (first, _) = receive(
            &started.ticket,
            &out_dir,
            &cache_dir,
            first_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("first receive");
        assert_eq!(first, "input.bin");
        let (second_tx, _second_rx) = mpsc::unbounded_channel();
        let (second, from_cache) = receive(
            &started.ticket,
            &out_dir,
            &cache_dir,
            second_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("second receive must suffix, not fail");
        assert_eq!(second, "input (1).bin");
        assert!(from_cache, "second receive must come from cache");
        assert_eq!(std::fs::read(out_dir.join("input.bin")).unwrap(), content);
        assert_eq!(
            std::fs::read(out_dir.join("input (1).bin")).unwrap(),
            content
        );
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// A short ticket that cannot connect must point at DNS filtering,
    /// not just report the unreachable sender.
    #[tokio::test]
    async fn short_ticket_connect_failure_names_dns() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("dnshint");
        let src_dir = base.join("src");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![7u8; 1024]).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(&src_dir.join("input.bin"), share::TicketType::Id, send_tx)
            .await
            .expect("share");
        let ticket = started.ticket.clone();
        // Sender gone before the receiver dials.
        started.session.shutdown().await;

        let control = ReceiveControl::default();
        let error = match connect_and_describe(&ticket, &cache_dir, &control).await {
            Ok(_) => panic!("dialing a dead sender must fail"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(
            message.contains("DNS"),
            "short-ticket failure must name DNS, got: {message}"
        );
        std::fs::remove_dir_all(&base).ok();
    }

    /// RAII env override for tests: restores the previous value (or
    /// absence) on drop, so a panicking test cannot leak config into
    /// later serial tests.
    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            // SAFETY: SERIAL-serialized tests are the only readers of
            // this var in the test binary, so no concurrent access.
            unsafe { std::env::set_var(key, value) };
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: same as above.
            unsafe {
                match self.prev.take() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    /// Both sides report the pipe they agree on: every report is a
    /// valid value, and the two views of the same connection share at
    /// least one. Which pipe wins the race (direct vs relay) depends on
    /// the network, so the test asserts agreement, not a side. A grace
    /// drain follows the transfer because a hole-punch upgrade can land
    /// just after the last byte.
    #[tokio::test]
    async fn path_events_agree_both_sides() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("pathdirect");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let content: Vec<u8> = (0..65536u32).map(|i| (i.wrapping_mul(7)) as u8).collect();
        std::fs::write(src_dir.join("input.bin"), &content).unwrap();

        let (send_tx, mut send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let (recv_tx, mut recv_rx) = mpsc::unbounded_channel();
        let (name, _) = receive(
            &started.ticket,
            &out_dir,
            &cache_dir,
            recv_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("receive");
        assert_eq!(name, "input.bin");
        assert_eq!(
            std::fs::read(out_dir.join("input.bin")).unwrap(),
            content
        );
        let mut views = Vec::new();
        for (side, rx) in [("receiver", &mut recv_rx), ("sender", &mut send_rx)] {
            let paths = drain_paths(rx).await;
            assert!(!paths.is_empty(), "{side} never reported its path");
            for path in &paths {
                assert!(
                    path == "direct" || path == "relayed",
                    "{side} reported a bogus path: {path:?}"
                );
            }
            views.push(paths);
        }
        let agree = views[0].iter().any(|path| views[1].contains(path));
        assert!(
            agree,
            "sides disagree on the pipe: {:?} vs {:?}",
            views[0],
            views[1]
        );
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Drain path reports for a few seconds past the transfer.
    async fn drain_paths(rx: &mut mpsc::UnboundedReceiver<TransferEvent>) -> Vec<String> {
        let mut paths = Vec::new();
        let begin = std::time::Instant::now();
        while begin.elapsed() < Duration::from_secs(3) {
            match timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(TransferEvent::Path { path })) => paths.push(path),
                Ok(_) => {}
                Err(_) => {}
            }
        }
        // Plus anything already queued without waiting.
        while let Ok(event) = rx.try_recv() {
            if let TransferEvent::Path { path } = event {
                paths.push(path);
            }
        }
        paths
    }

    /// A connection that can only use the relay (no direct addresses
    /// to dial) classifies as relayed. Exercises the relayed branch of
    /// the path indicator against a real relay; both ends are pinned to
    /// it so the dial cannot wander to another home relay.
    #[tokio::test]
    async fn relay_only_dial_reports_relayed_path() {
        let _serial = SERIAL.lock().await;
        let _env = EnvGuard::set("FLY_RELAY_URL", "https://euc1-1.relay.n0.iroh.link./");
        let base = unique_test_dir("pathrelayed");
        let src_dir = base.join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("input.bin"), vec![7u8; 1024]).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let ticket = BlobTicket::from_str(&started.ticket).unwrap();
        let relay_url: iroh::RelayUrl = "https://euc1-1.relay.n0.iroh.link./".parse().unwrap();
        let addr = EndpointAddr::from_parts(
            ticket.addr().id,
            [iroh::TransportAddr::Relay(relay_url)],
        );
        let endpoint = build_endpoint().await.expect("endpoint");
        let connection = timeout(
            CONNECT_TIMEOUT,
            endpoint.connect(addr, iroh_blobs::protocol::ALPN),
        )
        .await
        .expect("relay dial hung")
        .expect("relay dial failed");
        let begin = std::time::Instant::now();
        let mut classified = None;
        while begin.elapsed() < Duration::from_secs(15) {
            if let Some(path) = share::connection_path(&connection) {
                classified = Some(path);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(
            classified,
            Some("relayed"),
            "relay-only dial must classify as relayed"
        );
        endpoint.close().await;
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Size enforcement must never eat a live transfer: with a zero
    /// cap (everything over budget), the grace period still protects
    /// the running download and it completes normally.
    #[tokio::test]
    async fn eviction_spares_running_transfer() {
        let _serial = SERIAL.lock().await;
        let base = unique_test_dir("evictlive");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let big: Vec<u8> = (0..(32u32 * 1024 * 1024)).map(|i| (i.wrapping_mul(11)) as u8).collect();
        std::fs::write(src_dir.join("drop.bin"), &big).unwrap();
        drop(big);

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(&src_dir.join("drop.bin"), share::TicketType::Full, send_tx)
            .await
            .expect("share");
        let ticket = started.ticket.clone();
        let hash_hex = BlobTicket::from_str(&ticket).unwrap().hash().to_hex();

        let cache_clone = cache_dir.clone();
        let out_clone = out_dir.clone();
        let ticket_clone = ticket.clone();
        let (first_tx, mut first_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            receive(
                &ticket_clone,
                &out_clone,
                &cache_clone,
                first_tx,
                Arc::new(ReceiveControl::default()),
            )
            .await
        });
        assert!(
            wait_for_partial(&mut first_rx).await,
            "never saw a partial progress event"
        );
        // Zero cap with the real grace: a live transfer must survive.
        crate::prune::enforce_cache_cap(
            &cache_dir,
            "ticket-",
            0,
            crate::prune::CACHE_EVICT_GRACE,
        );
        assert!(
            cache_dir.join(format!("ticket-{hash_hex}")).exists(),
            "eviction must not delete a running transfer's cache"
        );
        let (name, _) = task
            .await
            .expect("task must complete")
            .expect("receive must complete after eviction attempt");
        assert_eq!(name, "drop.bin");
        assert_eq!(
            std::fs::read(out_dir.join("drop.bin")).unwrap().len(),
            32 * 1024 * 1024
        );
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// End-to-end proof that `FLY_RELAY_URL` is honored: with the env
    /// pointing at explicit relays, a share + receive still completes.
    /// It names public n0 relays in FQDN form (bare hostnames stall on
    /// networks with slow DNS search domains) — no private infra needed
    /// to exercise the Custom path.
    #[tokio::test]
    async fn custom_relay_env_roundtrip() {
        let _serial = SERIAL.lock().await;
        let _env = EnvGuard::set(
            "FLY_RELAY_URL",
            "https://euc1-1.relay.n0.iroh.link./, https://aps1-1.relay.n0.iroh.link./",
        );
        let base = unique_test_dir("customrelay");
        let src_dir = base.join("src");
        let out_dir = base.join("out");
        let cache_dir = base.join("cache");
        std::fs::create_dir_all(&src_dir).unwrap();
        let content: Vec<u8> = (0..65536u32).map(|i| (i.wrapping_mul(7)) as u8).collect();
        std::fs::write(src_dir.join("input.bin"), &content).unwrap();

        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let started = share::start_share(
            &src_dir.join("input.bin"),
            share::TicketType::Full,
            send_tx,
        )
        .await
        .expect("share");
        let (recv_tx, _recv_rx) = mpsc::unbounded_channel();
        let (name, _) = receive(
            &started.ticket,
            &out_dir,
            &cache_dir,
            recv_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("receive over the custom relay");
        assert_eq!(name, "input.bin");
        assert_eq!(
            std::fs::read(out_dir.join("input.bin")).unwrap(),
            content
        );
        started.session.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// Drain sender events until the receiver count reads `want`, or
    /// the deadline passes. Other events (delivery pings on unrelated
    /// shares, ticket updates) are ignored.
    async fn wait_for_sending(
        rx: &mut mpsc::UnboundedReceiver<TransferEvent>,
        want: u32,
        limit: Duration,
    ) -> bool {
        let begin = std::time::Instant::now();
        while begin.elapsed() < limit {
            match timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(TransferEvent::Sending { receivers })) if receivers == want => {
                    return true;
                }
                Ok(None) => return false,
                _ => {}
            }
        }
        false
    }
}
