//! Outgoing shares: import a file, serve it over iroh, mint a ticket.
//!
//! Clean-room implementation written against the `iroh` 1.3.0 and
//! `iroh-blobs` 0.103.1 APIs. `sendmer` (AGPL-3.0-only) served only as a
//! behavioral reference; no code was copied from it.

use crate::{events::TransferEvent, prune};
use anyhow::{Context, Result, anyhow, bail};
use iroh::{
    Endpoint, EndpointAddr, RelayMode, SecretKey, Watcher,
    endpoint::{BindOpts, Builder, Connection, PathEvent, PortmapperConfig, presets},
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
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
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
        watch_connection_path(&connection, self.gate.events.clone());
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

/// Env override for self-hosters: `FLY_RELAY_URL` holds one relay URL
/// (or several, comma-separated) that endpoints home to instead of
/// n0's public relays. Empty or unset means the default map;
/// unparseable entries warn and are skipped. This covers relayed
/// traffic only — short-ticket discovery still uses the N0 preset's
/// DNS/Pkarr. Sender and receiver should use the same relay set.
/// Prefer FQDN form (`https://relay.example.com./`): bare hostnames
/// stall on networks with slow DNS search domains.
fn relay_mode_for(value: Option<&str>) -> RelayMode {
    let mut urls = Vec::new();
    if let Some(value) = value {
        for part in value.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            match part.parse::<iroh::RelayUrl>() {
                Ok(url) => urls.push(url),
                Err(_) => eprintln!("fly: ignoring unparseable relay URL {part:?}"),
            }
        }
    }
    if urls.is_empty() {
        return RelayMode::Default;
    }
    eprintln!("fly: using custom relays {urls:?}");
    RelayMode::Custom(iroh::RelayMap::from_iter(urls))
}

/// The UDP port fly listens on out of the box: nobody configures
/// anything, and anyone who wants direct connections forwards this one
/// port on their router. `FLY_PORT` overrides it (`0` means a random
/// ephemeral port). Forwarding only matters on the dialed side (the
/// sender); the relay stays as the automatic fallback either way.
pub(crate) const DEFAULT_PORT: u16 = 7823;

/// Env override for strict NATs: `FLY_PORT` pins the endpoint's UDP port,
/// so one router port-forward lets the other side dial straight in
/// (a symmetric NAT otherwise forces relayed traffic). Unset or blank
/// means the default port; garbage warns and falls back to ephemeral
/// instead of breaking the share.
fn fixed_port_for(value: Option<&str>) -> Option<u16> {
    let raw = match value {
        None => return Some(DEFAULT_PORT),
        Some(raw) => raw.trim(),
    };
    if raw.is_empty() {
        return Some(DEFAULT_PORT);
    }
    match raw.parse::<u16>() {
        Ok(0) | Err(_) => {
            eprintln!("fly: ignoring unparseable FLY_PORT {raw:?}");
            None
        }
        Ok(port) => Some(port),
    }
}

fn endpoint_builder(port: Option<u16>) -> Result<Builder> {
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(SecretKey::generate())
        .alpns(vec![
            iroh_blobs::protocol::ALPN.to_vec(),
            DONE_ALPN.to_vec(),
        ])
        .relay_mode(relay_mode_for(
            std::env::var("FLY_RELAY_URL").ok().as_deref(),
        ))
        .proxy_from_env()
        // Automatic UPnP/PCP/NAT-PMP mapping: the app asks the router
        // itself to open the fixed port, so home routers yield direct
        // connections with zero user setup. Where the router refuses
        // (mobile data, CGNAT), the relay stays as the fallback.
        .portmapper_config(PortmapperConfig::default());
    if let Some(port) = port {
        // Pin both families to the same port. IPv6 is best-effort (a
        // v4-only host must still share); a failed IPv4 bind is handled
        // by the caller, which falls back to an ephemeral port.
        builder = builder
            .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
            .context("bad fixed IPv4 bind address")?;
        builder = builder
            .bind_addr_with_opts(
                SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port),
                BindOpts::default().set_is_required(false),
            )
            .context("bad fixed IPv6 bind address")?;
    }
    Ok(builder)
}

/// Build a fresh endpoint: N0 preset (DNS/Pkarr discovery only,
/// no DHT/mDNS), default relay map (or `FLY_RELAY_URL` when set), proxy
/// from environment, automatic UPnP/PCP/NAT-PMP port mapping, fixed UDP
/// port (or `FLY_PORT` when set).
///
/// A new identity is generated per call, so every share (and every receive)
/// gets a fresh endpoint ID. That is stricter than the plan's per-launch
/// default, and each ticket is independent anyway.
pub(crate) async fn build_endpoint() -> Result<Endpoint> {
    build_endpoint_with(fixed_port_for(std::env::var("FLY_PORT").ok().as_deref()))
        .await
        .map(|(endpoint, _)| endpoint)
}

/// The endpoint plus the fixed port it holds, if any: `None` means an
/// ephemeral port (explicitly requested, or a taken fixed port that fell
/// back). Callers that report the port (the share UI) use this, so a
/// fallback never advertises a port the app does not hold.
async fn build_endpoint_with(port: Option<u16>) -> Result<(Endpoint, Option<u16>)> {
    match endpoint_builder(port)?.bind().await {
        Ok(endpoint) => {
            if let Some(port) = port {
                eprintln!(
                    "fly: listening on fixed UDP port {port} (forward it for direct connections)"
                );
            }
            Ok((endpoint, port))
        }
        // A taken fixed port must never break a share: warn loudly and
        // retry with an ephemeral port, exactly the old behavior.
        Err(err) if port.is_some() => {
            eprintln!("fly: fixed UDP port unavailable ({err:#}); using a random port");
            let endpoint = endpoint_builder(None)?
                .bind()
                .await
                .context("failed to bind P2P endpoint")?;
            Ok((endpoint, None))
        }
        Err(err) => Err(err).context("failed to bind P2P endpoint"),
    }
}

/// Which pipe carries `connection`, from its selected path: "direct"
/// for an IP path, "relayed" otherwise. None while no path is
/// selected yet.
pub(crate) fn connection_path(connection: &Connection) -> Option<&'static str> {
    connection
        .paths()
        .iter()
        .find(|path| path.is_selected())
        .map(|path| if path.is_ip() { "direct" } else { "relayed" })
}

/// Report the current path now, then watch for path changes until the
/// connection closes (hole-punching can upgrade mid-transfer).
/// Best-effort: a dead event channel just ends the watch.
pub(crate) fn watch_connection_path(
    connection: &Connection,
    events: mpsc::UnboundedSender<TransferEvent>,
) {
    if let Some(path) = connection_path(connection) {
        let _ = events.send(TransferEvent::Path {
            path: path.to_string(),
        });
    }
    let mut path_events = connection.path_events();
    tokio::spawn(async move {
        while let Some(event) = path_events.next().await {
            if let PathEvent::Selected { remote_addr, .. } = event {
                let path = if remote_addr.is_ip() {
                    "direct"
                } else {
                    "relayed"
                };
                if events
                    .send(TransferEvent::Path {
                        path: path.to_string(),
                    })
                    .is_err()
                {
                    break;
                }
            }
        }
    });
}

pub struct StartedShare {
    pub session: ShareSession,
    pub ticket: String,
    pub fingerprint: String,
    pub port: Option<u16>,
}

pub async fn start_share(
    path: &Path,
    ticket_type: TicketType,
    events: mpsc::UnboundedSender<TransferEvent>,
) -> Result<StartedShare> {
    validate_share_path(path)?;
    // Best-effort: drop orphaned sender workspaces older than the TTL
    // (crashes/kills leave them behind; Stop sharing deletes its own).
    prune::prune_old_dirs(&std::env::temp_dir(), "fly-send-", prune::CACHE_TTL);
    let (endpoint, port) =
        build_endpoint_with(fixed_port_for(std::env::var("FLY_PORT").ok().as_deref())).await?;
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
    // The ticket watcher (spawned below for full tickets) needs its own
    // sender: `events` itself moves into the done-ping handler.
    let ticket_events = events.clone();
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
    let ticket = mint_ticket(&router.endpoint().addr(), ticket_type, tag.hash());
    let fingerprint = router.endpoint().id().to_string();
    if ticket_type == TicketType::Full {
        spawn_ticket_watcher(router.endpoint(), &ticket, tag.hash(), &ticket_events);
    }
    Ok(StartedShare {
        session: ShareSession {
            router,
            _store: store,
            _tag: tag,
            temp_dir,
        },
        ticket,
        fingerprint,
        port,
    })
}

/// Build the ticket string for one address snapshot. Id tickets keep
/// the endpoint ID only (discovery resolves the rest, so they never go
/// stale); full tickets embed relay + direct addresses.
fn mint_ticket(addr: &EndpointAddr, ticket_type: TicketType, hash: Hash) -> String {
    let mut addr = addr.clone();
    if ticket_type == TicketType::Id {
        addr.addrs = Default::default();
    }
    BlobTicket::new(addr, hash, BlobFormat::HashSeq).to_string()
}

/// Watch the endpoint's addresses for the life of the share; whenever
/// they change (network switch, relay move), mint a fresh full ticket
/// and push it to the UI so the displayed ticket never goes stale.
/// The task ends with the share: `closed().run_until` stops it when the
/// endpoint closes, and the weak events sender stops it if the session
/// is dropped any other way. It holds no endpoint clone and no strong
/// event sender, so it can never keep the share alive by itself.
fn spawn_ticket_watcher(
    endpoint: &Endpoint,
    minted_ticket: &str,
    hash: Hash,
    events: &mpsc::UnboundedSender<TransferEvent>,
) {
    let closed = endpoint.closed();
    let addr_stream = endpoint.watch_addr().stream();
    let weak_events = events.downgrade();
    let mut current = minted_ticket.to_owned();
    tokio::spawn(closed.run_until(async move {
        tokio::pin!(addr_stream);
        while let Some(addr) = addr_stream.next().await {
            let ticket = mint_ticket(&addr, TicketType::Full, hash);
            // The stream yields the current address first, and flapping
            // networks re-yield known states: only genuine changes
            // reach the UI (this also wins the race where the address
            // changed between minting and watching).
            if ticket == current {
                continue;
            }
            current = ticket.clone();
            let Some(events) = weak_events.upgrade() else {
                break;
            };
            if events
                .send(TransferEvent::TicketUpdated { ticket })
                .is_err()
            {
                break;
            }
        }
    }));
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

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::TransportAddr;
    use std::collections::BTreeSet;

    fn addr_with(id: &iroh::EndpointId, ip: &str) -> EndpointAddr {
        EndpointAddr {
            id: id.clone(),
            addrs: BTreeSet::from([TransportAddr::Ip(ip.parse().unwrap())]),
        }
    }

    /// Id tickets must be immune to address changes (nothing carried,
    /// nothing to go stale); full tickets must reflect them (that is
    /// what the watcher re-mints on).
    #[test]
    fn mint_ticket_strips_id_keeps_full() {
        let id = SecretKey::generate().public();
        let before = addr_with(&id, "127.0.0.1:1111");
        let after = addr_with(&id, "10.0.0.5:2222");
        let hash = Hash::from_bytes([7u8; 32]);
        assert_eq!(
            mint_ticket(&before, TicketType::Id, hash),
            mint_ticket(&after, TicketType::Id, hash),
            "id tickets must not depend on addresses"
        );
        assert_ne!(
            mint_ticket(&before, TicketType::Full, hash),
            mint_ticket(&after, TicketType::Full, hash),
            "full tickets must change with addresses"
        );
    }

    /// Unset/blank means n0's default relays; a URL means a custom
    /// single-relay map; garbage warns and falls back instead of
    /// breaking endpoint creation.
    #[test]
    fn relay_env_selects_map() {
        assert!(matches!(relay_mode_for(None), RelayMode::Default));
        assert!(matches!(relay_mode_for(Some("")), RelayMode::Default));
        assert!(matches!(
            relay_mode_for(Some("   ")),
            RelayMode::Default
        ));
        assert!(matches!(
            relay_mode_for(Some("not a url at all !!!")),
            RelayMode::Default
        ));
        match relay_mode_for(Some("https://relay.example.com")) {
            RelayMode::Custom(map) => assert_eq!(map.len(), 1),
            _ => panic!("URL must select a custom map"),
        }
        // Comma-separated lists build a failover map; blank and
        // garbage entries are skipped without breaking the rest.
        match relay_mode_for(Some(
            "https://one.example.com./, , not a url, https://two.example.com./",
        )) {
            RelayMode::Custom(map) => assert_eq!(map.len(), 2),
            _ => panic!("URL list must select a custom map"),
        }
    }

    /// Unset/blank means the default port; a bare number pins it; zero
    /// and garbage warn and fall back to ephemeral instead of breaking
    /// shares.
    #[test]
    fn fixed_port_parses() {
        assert_eq!(fixed_port_for(None), Some(DEFAULT_PORT));
        assert_eq!(fixed_port_for(Some("")), Some(DEFAULT_PORT));
        assert_eq!(fixed_port_for(Some("   ")), Some(DEFAULT_PORT));
        assert_eq!(fixed_port_for(Some("0")), None);
        assert_eq!(fixed_port_for(Some("abc")), None);
        assert_eq!(fixed_port_for(Some("99999")), None);
        assert_eq!(fixed_port_for(Some("-1")), None);
        assert_eq!(fixed_port_for(Some("7823")), Some(7823));
        assert_eq!(fixed_port_for(Some("  9000  ")), Some(9000));
    }

    /// A fixed port that is already taken must never break endpoint
    /// creation: the builder warns and falls back to an ephemeral port.
    #[tokio::test]
    async fn fixed_port_taken_falls_back_to_ephemeral() {
        let taken = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = taken.local_addr().unwrap().port();
        let (endpoint, _) = build_endpoint_with(Some(port))
            .await
            .expect("taken fixed port must fall back to ephemeral");
        endpoint.close().await;
        drop(taken);
    }

    /// A free fixed port is actually held: a second bind of it fails
    /// while the endpoint lives, so a router forward reaches this app.
    #[tokio::test]
    async fn fixed_port_free_binds_and_holds() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let (endpoint, pinned) = build_endpoint_with(Some(port))
            .await
            .expect("free fixed port must bind");
        assert_eq!(pinned, Some(port), "fixed bind must report its port");
        // Probe the wildcard, not loopback: Windows lets a loopback bind
        // overlap a wildcard one, but two wildcards always conflict.
        let clash = std::net::UdpSocket::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port,
        ));
        assert!(
            clash.is_err(),
            "endpoint must hold the fixed port, not share it"
        );
        endpoint.close().await;
    }
}
