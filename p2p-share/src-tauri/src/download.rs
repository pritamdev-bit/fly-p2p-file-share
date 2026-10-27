//! Incoming transfers: parse a ticket, download, verify, export.
//!
//! The filename always comes from the hashed collection metadata, never
//! from side-channel text. Progress streams to the UI over an mpsc channel
//! (the Tauri adapter forwards it into the IPC `Channel`).

use crate::{
    events::TransferEvent,
    export::{export_file, validate_single_segment},
    share::{DONE_ALPN, METADATA_NAME, ShareMetadata, build_endpoint},
    transfer::ReceiveControl,
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
use std::{path::Path, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

/// Timeouts: explicit everywhere so a stalled transfer always ends.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const METADATA_TIMEOUT: Duration = Duration::from_secs(60);
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
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
    store_dir: &Path,
    control: &ReceiveControl,
) -> Result<TicketTarget> {
    let ticket = BlobTicket::from_str(ticket_text.trim())
        .map_err(|_| anyhow!("That doesn't look like a fly ticket. Check for missing characters."))?;
    std::fs::create_dir_all(store_dir).context("cannot open transfer cache")?;
    let endpoint = build_endpoint().await?;
    control.register(&endpoint);
    // A cancel that landed before the endpoint was registered could not
    // close anything; fail here instead, before contacting the sender.
    if control.was_cancelled() {
        anyhow::bail!("Download cancelled.");
    }
    let store = Store::from(
        FsStore::load(store_dir)
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
        collection: ticket.hash(),
        file_name,
        file_hash,
        total,
    })
}

/// Look up a ticket's file name + size without downloading the file.
pub async fn preview(ticket_text: &str, store_dir: &Path) -> Result<FilePreview> {
    // Previews are short and not user-cancellable; the control is a dummy.
    let control = ReceiveControl::default();
    let target = connect_and_describe(ticket_text, store_dir, &control).await?;
    let preview = FilePreview {
        name: target.file_name.clone(),
        size: target.total,
    };
    target.endpoint.close().await;
    Ok(preview)
}

/// Download `ticket_text` into `save_dir`, using `store_dir` as the
/// persistent blob cache. Returns the received file name.
pub async fn receive(
    ticket_text: &str,
    save_dir: &Path,
    store_dir: &Path,
    events: mpsc::UnboundedSender<TransferEvent>,
    control: Arc<ReceiveControl>,
) -> Result<String> {
    let target = connect_and_describe(ticket_text, store_dir, &control).await?;
    download_with_retry(&target, &events).await?;
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
    Ok(target.file_name)
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

/// Fetch the file blob with progress, retrying transient failures.
/// Already-downloaded ranges are reused on retry automatically.
async fn download_with_retry(
    target: &TicketTarget,
    events: &mpsc::UnboundedSender<TransferEvent>,
) -> Result<()> {
    let remote = target.store.remote();
    let (hash, total) = (target.file_hash, target.total);
    let mut attempt = 0u32;
    let mut done = 0u64;
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
                .map_err(|_| anyhow!("Couldn't reach the sender (reconnect timed out)."))?
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
                    failed = Some(anyhow!("stalled: no data for 120s"));
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share;
    use std::path::PathBuf;

    /// The transfer tests serialize on this lock. Each spins up live
    /// endpoints against shared public relays + loopback, and running
    /// them in parallel flakes (stalled fetches, wedged shutdowns).
    /// Within one test, share + receive still run concurrently — that is
    /// the product scenario; cross-test parallelism is not.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
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
        let name = receive(
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
        let name = receive(
            &started.ticket,
            &out_second,
            &cache_dir,
            second_tx,
            Arc::new(ReceiveControl::default()),
        )
        .await
        .expect("second receive");
        assert_eq!(name, "again.bin");
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
        let base = unique_test_dir("cancelresume");
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
        // Wait for a genuinely partial progress event, then abort.
        let mut saw_partial = false;
        let begin = std::time::Instant::now();
        while begin.elapsed() < Duration::from_secs(60) {
            match tokio::time::timeout(Duration::from_secs(5), first_rx.recv()).await {
                Ok(Some(TransferEvent::Progress { done, total })) => {
                    if done > 0 && done < total {
                        saw_partial = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(saw_partial, "never saw a partial progress event");
        // Graceful cancel, exactly like cancel_receive does.
        control.cancel().await;
        let cancelled = task.await.expect("task must wind down, not abort");
        assert!(cancelled.is_err(), "cancelled receive must fail");
        assert!(control.was_cancelled());

        // Immediate re-receive must work first try: the close frames the
        // cancel sent already freed the sender's receiver slot.
        let (second_tx, _second_rx) = mpsc::unbounded_channel();
        let name = receive(
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
