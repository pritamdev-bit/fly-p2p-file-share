//! Incoming transfers: parse a ticket, download, verify, export.
//!
//! The filename always comes from the hashed collection metadata, never
//! from side-channel text. Progress streams to the UI over an mpsc channel
//! (the Tauri adapter forwards it into the IPC `Channel`).

use crate::{
    events::TransferEvent,
    export::{export_file, validate_single_segment},
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
use tokio::{sync::mpsc, time::timeout};

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
#[derive(Serialize)]
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
        anyhow!("Couldn't reach the sender (connection timed out). Are they still sharing?")
    })?
    .context("Couldn't reach the sender")?;
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
pub async fn preview(ticket_text: &str, base_dir: &Path) -> Result<FilePreview> {
    // Previews are short and not user-cancellable; the control is a dummy.
    let control = ReceiveControl::default();
    let target = connect_and_describe(ticket_text, base_dir, &control).await?;
    let preview = FilePreview {
        name: target.file_name.clone(),
        size: target.total,
    };
    target.endpoint.close().await;
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
    let target = connect_and_describe(ticket_text, base_dir, &control).await?;
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
            }
            return Err(error);
        }
    };
    export_file(&target.store, target.file_hash, &target.file_name, save_dir).await?;
    // Make the store durable before returning: a rapid re-receive opens
    // this same dir again, and must see the complete bitfield. Best
    // effort — if it fails, the next fetch just re-downloads some ranges.
    let _ = target.store.sync_db().await;
    // Tell the sender "I got it". Best-effort: a failure here (sender
    // already stopped sharing, network gone) must not fail the receive.
    notify_sender(&target.endpoint, &target.sender, target.collection).await;
    let _ = events.send(TransferEvent::Finished);
    target.endpoint.close().await;
    Ok((target.file_name, from_cache))
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
    Ok((metadata.name, entry_hash, metadata.size))
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
        // retrying on it would fail every time.
        let fresh: Option<Connection> = if attempt == 1 {
            None
        } else {
            Some(
                timeout(
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
                .context("Couldn't reach the sender")?,
            )
        };
        let connection = fresh.as_ref().unwrap_or(&target.connection);
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
}
