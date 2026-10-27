//! Outgoing shares: import a file, serve it over iroh, mint a ticket.
//!
//! Clean-room implementation written against the `iroh` 1.3.0 and
//! `iroh-blobs` 0.103.1 APIs. `sendmer` (AGPL-3.0-only) served only as a
//! behavioral reference; no code was copied from it.

use crate::events::TransferEvent;
use anyhow::{Context, Result, anyhow, bail};
use iroh::{
    Endpoint, RelayMode, SecretKey,
    endpoint::{Connection, PortmapperConfig, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{
    BlobFormat, BlobsProtocol, Hash,
    api::{
        Store, TempTag,
        blobs::{AddPathOptions, AddProgressItem, ImportMode},
    },
    format::collection::Collection,
    store::fs::FsStore,
    ticket::BlobTicket,
};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Semaphore, mpsc};

/// Collection key for the filename/size metadata blob (always first).
pub const METADATA_NAME: &str = "fly-metadata.json";

/// ALPN for the completion ping: after a successful export, the receiver
/// dials this, sends the 32 collection-hash bytes on one uni stream, and
/// closes. Anything else on this ALPN is ignored.
pub(crate) const DONE_ALPN: &[u8] = b"fly/done/0";

/// Filename + size carried inside the hashed collection, so the receiver
/// learns the real name from the ticket instead of trusting chat text.
#[derive(Serialize, Deserialize)]
pub struct ShareMetadata {
    pub name: String,
    pub size: u64,
}

/// Which addresses a ticket carries.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TicketType {
    /// Endpoint ID only; the receiver resolves addresses via discovery
    /// (DNS/Pkarr). Short and QR-friendly, but needs working discovery.
    Id,
    /// Endpoint ID + relay URL + direct addresses. Works even when
    /// discovery is filtered; longer string.
    Full,
}

impl TicketType {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "id" => Ok(Self::Id),
            "full" => Ok(Self::Full),
            other => Err(anyhow!("unknown ticket type '{other}' (use 'id' or 'full')")),
        }
    }
}

/// A live outgoing share. Shutting the router down stops serving (which
/// invalidates the ticket); dropping the tag releases the blobs for GC.
pub struct ShareSession {
    router: Router,
    _store: Store,
    _tag: TempTag,
    temp_dir: PathBuf,
}

impl ShareSession {
    /// Stop serving and remove the temporary store directory.
    pub async fn shutdown(self) {
        // Bounded: a stuck connection must never hang Stop sharing.
        match tokio::time::timeout(Duration::from_secs(10), self.router.shutdown()).await {
            Err(_) => eprintln!("fly: router shutdown timed out; continuing"),
            Ok(Err(error)) => eprintln!("fly: router shutdown error: {error:?}"),
            Ok(Ok(())) => {}
        }
        match tokio::fs::remove_dir_all(&self.temp_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => eprintln!("fly: sender temp cleanup failed: {error}"),
        }
    }
}

/// Counts concurrent receivers and reports them to the UI.
#[derive(Debug)]
struct ConnectionGate {
    /// One permit = one concurrent receiver (plan: max 1 in v1).
    semaphore: Semaphore,
    active: AtomicU32,
    events: mpsc::UnboundedSender<TransferEvent>,
}

/// [`BlobsProtocol`] wrapper enforcing the v1 single-receiver cap.
struct CappedProtocol {
    inner: BlobsProtocol,
    gate: Arc<ConnectionGate>,
}

// Manual Debug: none of the inner state is ever logged.
impl std::fmt::Debug for CappedProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CappedProtocol").finish_non_exhaustive()
    }
}

impl ProtocolHandler for CappedProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let Ok(_permit) = self.gate.semaphore.try_acquire() else {
            // Second concurrent receiver: close the connection without
            // serving anything. The receiver reports a download failure.
            return Ok(());
        };
        let receivers = self.gate.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.gate.events.send(TransferEvent::Sending { receivers });
        let result = self.inner.accept(connection).await;
        let receivers = self.gate.active.fetch_sub(1, Ordering::SeqCst) - 1;
        let _ = self.gate.events.send(TransferEvent::Sending { receivers });
        result
    }
}

/// Handler for the completion ping ([`DONE_ALPN`]). Expects exactly the
/// 32 collection-hash bytes, then acknowledges with one byte so the
/// receiver knows the ping landed before it tears down. Anything else is
/// ignored. Never counted as a receiver and never fails the share.
struct DoneHandler {
    expected: Hash,
    events: mpsc::UnboundedSender<TransferEvent>,
}

impl std::fmt::Debug for DoneHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DoneHandler").finish_non_exhaustive()
    }
}

impl ProtocolHandler for DoneHandler {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let outcome: Result<()> = async {
            let (mut send, mut recv) = connection.accept_bi().await.context("no ping stream")?;
            let bytes = recv.read_to_end(64).await.context("unreadable ping")?;
            if bytes.len() == 32 {
                let mut raw = [0u8; 32];
                raw.copy_from_slice(&bytes);
                if Hash::from_bytes(raw) == self.expected {
                    let _ = self.events.send(TransferEvent::Delivered);
                }
            }
            // Always ack (even on mismatch) so the receiver never hangs.
            send.write_all(b"1").await.context("ack write failed")?;
            send.finish().context("ack finish failed")?;
            Ok(())
        }
        .await;
        if let Err(error) = outcome {
            eprintln!("fly: bad completion ping: {error:#}");
        }
        Ok(())
    }
}

/// Build a fresh ephemeral endpoint: N0 preset (DNS/Pkarr discovery only,
/// no DHT/mDNS), default relay map, proxy from environment, no UPnP
/// gateway probing (pre-launch proposal: disabled).
///
/// A new identity is generated per call, so every share (and every receive)
/// gets a fresh endpoint ID. That is stricter than the plan's per-launch
/// default, and each ticket is independent anyway.
pub(crate) async fn build_endpoint() -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .secret_key(SecretKey::generate())
        .alpns(vec![
            iroh_blobs::protocol::ALPN.to_vec(),
            DONE_ALPN.to_vec(),
        ])
        .relay_mode(RelayMode::Default)
        .proxy_from_env()
        .portmapper_config(PortmapperConfig::Disabled)
        .bind()
        .await
        .context("failed to bind P2P endpoint")
}

pub struct StartedShare {
    pub session: ShareSession,
    pub ticket: String,
    pub fingerprint: String,
}

pub async fn start_share(
    path: &Path,
    ticket_type: TicketType,
    events: mpsc::UnboundedSender<TransferEvent>,
) -> Result<StartedShare> {
    validate_share_path(path)?;
    let endpoint = build_endpoint().await?;
    let temp_dir = unique_temp_dir("fly-send-")?;
    std::fs::create_dir_all(&temp_dir).context("cannot create sender workspace")?;
    // Disk-backed store (never in-memory): flat memory for any file size.
    // A temp dir is enough on the sender side — the source files already
    // live on disk, and the dir is removed again on stop.
    let store = Store::from(
        FsStore::load(&temp_dir)
            .await
            .context("cannot open sender store")?,
    );
    let tag = import_file(&store, path).await?;
    let gate = Arc::new(ConnectionGate {
        semaphore: Semaphore::new(1),
        active: AtomicU32::new(0),
        events: events.clone(),
    });
    let protocol = BlobsProtocol::new(&store, None);
    let router = Router::builder(endpoint.clone())
        .accept(
            iroh_blobs::protocol::ALPN,
            CappedProtocol {
                inner: protocol,
                gate,
            },
        )
        .accept(
            DONE_ALPN,
            DoneHandler {
                expected: tag.hash(),
                events,
            },
        )
        .spawn();
    // Give relay registration + address publication a head start (id
    // tickets especially need the publisher running before anyone
    // resolves them). Best effort: a slow network must not fail the share.
    if tokio::time::timeout(Duration::from_secs(10), endpoint.online())
        .await
        .is_err()
    {
        eprintln!("fly: endpoint online probe timed out after 10s; continuing");
    }
    let mut addr = router.endpoint().addr();
    if ticket_type == TicketType::Id {
        addr.addrs = Default::default();
    }
    let ticket = BlobTicket::new(addr, tag.hash(), BlobFormat::HashSeq).to_string();
    let fingerprint = router.endpoint().id().to_string();
    Ok(StartedShare {
        session: ShareSession {
            router,
            _store: store,
            _tag: tag,
            temp_dir,
        },
        ticket,
        fingerprint,
    })
}

fn validate_share_path(path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|_| anyhow!("cannot access '{}': no such file", path.display()))?;
    if meta.file_type().is_symlink() {
        bail!("sharing symlinks is not supported; pick the real file");
    }
    if meta.is_dir() {
        bail!("sharing folders arrives in a later version; pick a single file");
    }
    if !meta.is_file() {
        bail!("can only share regular files");
    }
    Ok(())
}

/// Import one file plus a metadata blob into `db`, wrapped in a collection.
/// Returns the collection tag (which pins everything it references).
async fn import_file(db: &Store, path: &Path) -> Result<TempTag> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("filename must be valid UTF-8")?
        .to_owned();
    let size = std::fs::metadata(path).context("cannot stat file")?.len();
    let metadata = ShareMetadata {
        name: name.clone(),
        size,
    };
    let metadata_bytes = serde_json::to_vec(&metadata).context("cannot encode metadata")?;
    let metadata_tag = db
        .add_bytes(metadata_bytes)
        .temp_tag()
        .await
        .context("cannot store metadata")?;
    // Copy (not reference): the share must survive the user moving or
    // deleting the source file while serving.
    let import = db.add_path_with_opts(AddPathOptions {
        path: path.to_path_buf(),
        mode: ImportMode::Copy,
        format: BlobFormat::Raw,
    });
    let mut stream = import.stream().await;
    let mut file_tag = None;
    while let Some(item) = stream.next().await {
        match item {
            AddProgressItem::Done(tag) => {
                file_tag = Some(tag);
                break;
            }
            AddProgressItem::Error(cause) => bail!("failed to import file: {cause}"),
            _ => {}
        }
    }
    let file_tag = file_tag.context("import ended without producing a tag")?;
    let mut collection = Collection::default();
    collection.push(METADATA_NAME.to_owned(), metadata_tag.hash());
    collection.push(name, file_tag.hash());
    let tag = collection.store(db).await.context("cannot store collection")?;
    drop(metadata_tag);
    drop(file_tag);
    Ok(tag)
}

fn unique_temp_dir(prefix: &str) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Ok(std::env::temp_dir().join(format!("{prefix}{}-{}", std::process::id(), nanos)))
}
