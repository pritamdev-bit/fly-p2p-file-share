# P2P File Transfer Desktop App: Tauri + Rust Plan

**Goal:** A desktop app for Windows, Linux and macOS that sends a file of any size directly from one PC to another over the internet. Both PCs run the same app. Small installer, networking done in Rust.

**In scope for v1:** one file at a time, send and receive in the same app, progress bar, integrity check, resume after a dropped connection, max one *concurrent* receiver per ticket. (Serving does **not** auto-stop after a download in v1 — the sender stops it with **Stop sharing**. Auto-stop is deferred to Phase 4: the sender cannot reliably observe receiver completion, and Phase 1 proved `bytes_served` is not a done-signal.)

**Out of scope for v1:** accounts, transfer history, mobile apps, multiple simultaneous transfers. (Folders are a possible stretch goal, see Phase 5. Same-network mDNS discovery is a later enhancement, deliberately off by default.)

> **Revision note (2026-10-16):** this plan was updated from `p2p-file-transfer-research.md`, which checked every claim against primary sources (Tauri docs, docs.rs for `iroh` 1.3.0 / `iroh-blobs` 0.103.1, docs.iroh.computer, `sendmer` source, BLAKE3/wry sources). Inline pointers like `(research I4)` / `(research issue 6)` / `(research V3)` refer to that report's Improvements / Issues / Vulnerabilities items, where the citations live. Biggest changes: `sendmer` is **AGPL-3.0-only** (read-only reference — do not copy or depend); the ticket carries **no filename** (names travel in a hashed collection); resume needs a **persistent store**; public relays are **dev/test-only** (relay strategy is a pre-launch decision); tickets are long, reusable bearer [REDACTED] that go stale.

> **API note:** `iroh` 1.x is now semver-stable (1.3.0 observed 2026-10-16), but `iroh-blobs` is still 0.x (0.103.1) and its README carries a production-quality caveat — pin exact versions in `Cargo.toml`, commit `Cargo.lock`, build with `cargo --locked`, and review the `iroh-blobs` changelog on every bump; never float `^0.103`. Copy Rust code only from the docs for your pinned version. Where I show code, it is the Tauri side, checked against Tauri's v2 docs. Still confirm every name in your IDE's autocomplete.

---

## 0. What changed from the Electron plan

| Electron plan | This plan | Why |
|---|---|---|
| WebRTC in the UI (browser engine) | Networking in the Rust backend | Linux's webview (WebKitGTK) has WebRTC disabled by default, so it can't live in the UI |
| Copy-paste **two** codes (offer and answer) | **One** ticket (short id-only form by default, full form when discovery is unavailable) | The library below produces a single ticket |
| Chunks pass through IPC (slow) | Files never leave Rust | UI only receives progress updates |
| ~200-300 MB installed | Expect a small installer, measure it | Uses the OS webview |
| Resume and integrity built by you | Provided by the transfer library, given a persistent store | Less code to write and test |

---

## 1. Concepts (read before building)

1. **Streaming makes "any file size" work.** Read a piece, send it, repeat. The receiver writes pieces to disk as they arrive. Memory use stays flat — but only with a disk-backed `FsStore` plus streaming export, not an in-memory store. (research I2)
2. **NAT blocks direct connections.** Home PCs sit behind routers, so one PC can't simply connect to the other.
3. **Hole punching** gets around NAT: both sides send packets toward each other at the same time, so each router opens a path.
4. **Relay fallback:** when hole punching fails, a relay server forwards the traffic. It is the one case where a server carries the data. Two corrections to the first draft: a relay is **always** in the loop at minimum for the introduction (long-lived encrypted connections on TCP port 443), even for "direct" transfers — so relay reachability on 443 is a hard requirement; and the public n0 relays are **dev/test-only** (rate-limited, no uptime, performance, or privacy guarantees). Production means paid Shared/Dedicated relays or self-hosting — decide before launch, not in Phase 5. (research I9, issue 8)
5. **Dial by public key (iroh's idea):** each app has a key pair. Instead of "connect to this IP", you say "connect to this key", and the library figures out how to reach it, tries a direct path, and falls back to a relay. Default to an **ephemeral key per launch**: it bounds ticket lifetime and avoids a long-term trackable identity. A persistent key enables re-dial by ID but becomes a tracking identifier — don't persist one without a privacy decision. (research I6)
6. **Verified streaming:** the file is hashed in a tree (BLAKE3), so the receiver can check each piece as it arrives. This gives integrity checking and resume almost for free. Integrity covers only content **under the ticket's hash** — filenames must travel inside the hashed collection (see 8), never as side-channel chat text. (research issue 7)
7. **Ticket:** one string that says "download this content from this peer." Full tickets are **long** (roughly 200 characters: endpoint ID + relay URL + direct addresses) — plan the UI for copy/QR, not transcription. Short **id-only** tickets plus live discovery are the QR-friendly default. Tickets are **reusable bearer [REDACTED]**: anyone holding one can download, multiple times; sharing a ticket shares your IP address; and the dialing info **goes stale** when networks change. (research issue 1, issue 2, V2, I7)
8. **Filename transport (missing from the first draft):** `BlobTicket` has no filename field. Names travel in a **collection** (first element by convention an app-defined metadata blob), covered by the ticket's hash — including for v1 single-file shares. (research I1)
9. **Stop sharing:** a ticket stays valid while the sender's router is alive. v1 stops serving only via the explicit **Stop sharing** button (closes the router) — there is no auto-stop after a download, so a ticket can be reused until stopped (that is also why §6 treats tickets as reusable bearer [REDACTED] regardless). Auto-stop arrives in Phase 4 combined with idle detection. (research V2)

---

## 2. Architecture

```
   PC A (Tauri app)                                   PC B (Tauri app)
┌───────────────────────┐                         ┌───────────────────────┐
│ React UI (webview)    │                         │ React UI (webview)    │
│  buttons, progress    │                         │  buttons, progress    │
│ ─ ─ invoke/Channel ─ ─│                         │ ─ ─ invoke/Channel ─ ─│
│ Rust backend          │◄══ encrypted QUIC ═════►│ Rust backend          │
│  iroh + iroh-blobs    │   direct, or via relay  │  iroh + iroh-blobs    │
│  reads/writes files   │                         │  reads/writes files   │
└───────────────────────┘                         └───────────────────────┘
```

**Flow (pull model):**
1. Sender picks a file. Rust imports it into a disk-backed store (report "Hashing..." for big files), keeps the blob `Tag` alive, and starts serving. The UI shows a **ticket** (id-only short form by default, full form when discovery is unavailable) plus a **sender fingerprint** for out-of-band verification.
2. Sender gives the ticket to the receiver (any chat app, or QR). UI wording: forwarding the ticket = granting the download.
3. Receiver pastes the ticket, sees the **filename from the hashed collection metadata**, and picks a save folder. Rust connects (relay on 443 must be reachable), downloads with connect/metadata/idle timeouts, exports through a validated staging pipeline, verifies, and saves.
4. **The sender's app must stay open and the device awake** until the transfer finishes, because the receiver pulls the file from it. Only one receiver at a time is served; serving continues until **Stop sharing** (no auto-stop in v1).

| Part | Job |
|---|---|
| React UI | File picker, ticket + fingerprint display, ticket input, filename preview, progress bar, errors |
| Tauri commands | `send_file`, `receive_file`, `stop_sharing` (+ copy-diagnostics), called from the UI |
| Tauri channel | Streams progress from Rust to the UI |
| Rust transfer code | Hashing, serving, connecting, downloading, staged + validated export, timeouts, receiver caps |
| Managed state | Shared endpoint/router **and the blob `Tag`** — the tag, not just the node, keeps the served blob from being garbage-collected |

---

## 3. Tech stack

| Need | Choice | Notes |
|---|---|---|
| Desktop shell | Tauri v2 (2.12.4 stable observed; 3.0 alpha exists — stay on v2) | Uses each OS's webview |
| UI | React + TypeScript | Same as you use now |
| P2P networking | `iroh` 1.x (1.3.0 observed; semver-stable) | Hole punching, relay fallback, encrypted QUIC connections; pin exact |
| File transfer | `iroh-blobs` 0.x (0.103.1 observed) | BLAKE3 verified streaming, resume via persistent store; pin exact, changelog on every bump; production caveat means soak-test large transfers |
| Reference code | `sendmer` 0.11.0 — **read-only** | **AGPL-3.0-only, single maintainer: do not copy its code or depend on the crate** in a closed app; reimplement its export/timeout patterns clean-room. Evaluate n0's `sendme` as the primary reference instead (verify license/state first — UNVERIFIED) |
| File picker | Tauri dialog plugin | `@tauri-apps/plugin-dialog` + `tauri-plugin-dialog` (v2.8.3 observed); needs `dialog:allow-open` + `dialog:allow-save`; requires Rust 1.90 or newer |
| Copy ticket | Tauri clipboard-manager plugin | `@tauri-apps/plugin-clipboard-manager` + `tauri-plugin-clipboard-manager` (v2.5.0 observed; note the `-manager` suffix — there is no `plugin-clipboard` package); first-party; works outside the webview sandbox |
| Updates | Tauri updater plugin — **decision required before v1** | Signatures are mandatory; lose the signing key and you cannot publish updates |
| Release trust | Sigstore/cosign + SBOM on GitHub releases | Independent of OS code signing |
| Toolchain | `rust-toolchain.toml` + committed `Cargo.lock` + `cargo --locked` + `cargo audit` in CI | Reproducible, audited builds |
| Packaging | Tauri's built-in bundler | Windows installer, macOS app/dmg, Linux deb/AppImage |

---

## 4. Two approaches (start with A)

**A. iroh + iroh-blobs (recommended).** The libraries already do connection, hole punching, relay fallback, verified streaming and resume. Your Rust code is mostly glue: start a node, add a file (as a collection + metadata blob so the filename travels under the ticket hash), produce a ticket, download from a ticket, export through a validated pipeline, report progress.

**B. Your own protocol.** Use iroh (or `webrtc-rs`) only for the connection, and write the message flow from the Electron plan yourself (offer, accept, chunks, done, hash). More control (for example an accept/reject prompt on the *sender* side), but much more code and many more bugs. Only choose this if A can't do something you need.

**API stability (updated):** `iroh` 1.x is a semver-stable major line (`Endpoint::bind()` / `Endpoint::builder(...)`), so the old "API still moving" warning no longer applies to it. `iroh-blobs` is still 0.x — no stability guarantee — so keep the pin-exact-versions discipline for it. Note the `iroh-blobs` README production-quality caveat and resolve it with maintainers plus large-transfer soak tests rather than skipping it. (research issue 3, issue 4)

**Third parties and network realities (read carefully):**
- iroh's **public relay servers**: dev/test-only, rate-limited, no SLA or privacy guarantees — and always used at minimum for the introduction. Content is end-to-end encrypted (a relay sees who talks to whom, when, and how much — not content), so self-hosting buys control of the path, not content visibility. Decide pre-launch: paid Shared/Dedicated or self-hosted (`RelayMode::Custom`, self-host guide exists). (research I9, issue 8, V6)
- iroh's **discovery** (N0 preset: DNS/Pkarr via `iroh.link`): endpoints auto-publish addressing info (home relay, possibly direct addresses) to lookup services. Some enterprise DNS products strip TXT records — then id-only dialing fails while full tickets still work, so add that branch to error messages. Keep DHT (public swarm) and mDNS **off** unless deliberately chosen, and document the choice. (research issue 9, V14, V6)
- **UPnP/NAT-PMP port mapping is on by default** (asks the gateway for an external port). For a consumer app, consider disabling it and relying on hole-punching + relay; at minimum disclose it. (research I12, V7)
- **Proxy-mandating networks** need an app change (`.proxy_from_env()` / `.proxy_url()`), or the app cannot reach a relay at all; proxied deployments usually end up relaying everything. (research I11)
- Whatever chat app carries the ticket — it is part of the trust base (bearer [REDACTED] plus sender IPs inside). (research V2, V10)

For a "my servers only" setup, relay and discovery can be self-hosted — but treat that as a **pre-launch decision**, not Phase 5 polish.

---

## 5. Phases

### Phase 0: Setup and the Linux gate (2-3 days)

- [ ] Install Rust, Node, and Tauri's OS prerequisites (on Linux this includes WebKitGTK and its dev packages; follow Tauri's prerequisites page for your distro).
- [ ] Add `rust-toolchain.toml` (Rust 1.90 or newer — the dialog plugin requires it) and build with `cargo --locked` from day one.
- [ ] Create a Tauri v2 + React + TypeScript project and confirm `npm run tauri dev` opens a window.
- [ ] Start the **pre-launch decisions list** (relay tier or self-host, updater channel, ephemeral identity default, UPnP on/off, DHT/mDNS off, 1-receiver/session caps) — each is mostly one-line config, all are harder to change after ship.
- [ ] **Gate:** build the hello-world app on **Linux** (a VM is fine) and confirm it opens. If Linux is a problem, you want to know now.

Folder layout to aim for:

```
p2p-share/
  src/                    # React UI
    App.tsx
    components/           # SendPanel, ReceivePanel, Progress, Fingerprint, Diagnostics
    api.ts                # wrappers around invoke()
  src-tauri/
    Cargo.toml            # pinned iroh, iroh-blobs versions
    rust-toolchain.toml   # pinned toolchain, MSRV >= 1.90
    src/
      main.rs / lib.rs    # Tauri setup, registers commands
      transfer.rs         # send/receive logic (iroh)
      share.rs            # serving session: router, blob Tag, fingerprint, caps
      events.rs           # TransferEvent type
      diagnostics.rs      # copy-diagnostics bundle (never tickets/paths)
    capabilities/         # plugin permissions only (dialog, clipboard) — app commands need none in a single-window app
    # no fs/opener/shell plugins — all file IO stays in Rust behind your commands
```

### Phase 1: Prove P2P works on your real networks (2-3 days, no UI)

**Goal:** confirm hole punching works between two real home networks *before* building anything.

- [ ] Install and run `sendmer` (per its README) on two PCs on different networks (one on a phone hotspot). **Read-only reference (AGPL-3.0-only): read it, learn from it — do not copy its code or add it as a dependency.**
- [ ] Note its ephemeral identity: tickets die with the process (unless `IROH_SECRET` is set). Our default matches.
- [ ] Compare ticket types (`id` vs full/relay/addresses): note the lengths, and which work where.
- [ ] Send a 1-2 GB file with `send`, copy the ticket, use `receive` on the other PC.
- [ ] Interrupt the download halfway and observe the two resume modes: **in-process retry** (same process reuses data already obtained) vs **opt-in cross-process resume** (needs the same `--cache-dir`). Kill the receiver, restart the *app*, resume with the same cache dir — that is our Phase 4 bar.
- [ ] From the strict network, run `iroh-doctor report` — the documented first diagnostic step. Note relay reachability on 443 and UDP behavior.
- [ ] Read the export pipeline (staging dir, `Component::Normal` validation, `symlink_metadata` exists-check, per-OS atomic no-replace rename, success only after the `Done` item) and the timeout/caps model (retry limit 3, 250 ms backoff, connect/metadata/idle timeouts, max receivers/rate/session-lifetime options) as the **spec to reimplement clean-room**.

**Done when:** a multi-GB file transfers across two different networks; you can explain what a ticket contains (endpoint ID + relay URL + direct addresses; reusable; goes stale; shares your IP); and the resume semantics are written down for Phase 3/4.

### Phase 2: Tauri shell with fake transfers (3-4 days)

**Goal:** the UI and the Rust-to-UI plumbing work, with no networking yet.

Tauri v2 gives you three tools for UI-to-Rust communication: **commands** (request and response), **events** (notifications), and **channels** (high-frequency streaming). For progress, use a **channel** — events can arrive unordered under rapid async emission, so never use events for progress.

Rust side (sketch, with the real work left as a `todo!`):

```rust
use serde::Serialize;
use tauri::ipc::Channel;

#[derive(Clone, Serialize)]
#[serde(tag = "event", content = "data")]
enum TransferEvent {
    Progress { done: u64, total: u64 },
    Finished,
    Failed { message: String },
    // Phase 3 added Sending { receivers: u32 } early (real serving needs it immediately).
    // Phase 4 may add: WaitingForReceiver, fingerprint + diagnostics payloads.
}

#[tauri::command]
async fn send_file(path: String, on_event: Channel<TransferEvent>) -> Result<String, String> {
    // Phase 3: hash the file, start serving it, return the ticket string.
    // For now, fake it: send a few Progress events, return "FAKE-TICKET".
    todo!()
}

// In your builder: .invoke_handler(tauri::generate_handler![send_file, receive_file])
```

Frontend side:

```ts
import { invoke, Channel } from "@tauri-apps/api/core";

const onEvent = new Channel<TransferEvent>();
onEvent.onmessage = (msg) => { /* update progress bar */ };

const ticket = await invoke<string>("send_file", { path, onEvent });
```

Notes from Tauri's docs (corrected):
- A command not listed in `generate_handler!` silently fails when called from the UI (one `generate_handler!`, unique names).
- Tauri v2 denies **plugin permissions** by default — but your own registered commands are allowed for all windows and webviews unless you scope them via `AppManifest::commands` in `build.rs` (only needed if you ever add a second window). So: no capability entries for your own commands; the dialog plugin needs `dialog:allow-open` + `dialog:allow-save`.
- Keep shared state in `app.manage` with `Mutex` interior mutability (no `Arc` needed); `AppHandle` clones cheaply. The endpoint/router **and the blob `Tag`** live here.
- Rust argument `on_event` becomes `onEvent` in the JS call.

Tasks:
- [ ] Send panel: pick a file (dialog plugin), show ticket + sender fingerprint, copy-ticket button (clipboard plugin), Stop sharing button, progress bar. Drag-and-drop a file onto the window works too (`tauri://drag-drop`, first file wins in v1).
- [ ] Receive panel: paste a ticket, filename preview (from metadata in Phase 3; fake it now), pick save folder, progress bar.
- [ ] Copy-diagnostics button (node ID, relay reachability, NAT type — never tickets or paths).
- [ ] Fake the progress in Rust with a timer so you can finish the UI.

**Done when:** the whole UI works end-to-end with fake data.

### Phase 3: Real transfer with iroh-blobs (1-2 weeks)

**Goal:** replace the fake with real sending and receiving.

Steps (follow the iroh quickstart and `sendmer` — read-only — for exact behavior at your pinned versions):
- [ ] `send_file`: ephemeral key per launch (default); `FsStore` in the app data dir (never `MemStore` — flat memory depends on it); add the file via a **collection + metadata blob carrying the filename** (covered by the ticket hash — report "Hashing..." for big files); build the ticket (**id-only by default**, full form when discovery is unavailable); return ticket + fingerprint. Keep node/router **and the blob `Tag`** alive in managed state after the command returns. N0 preset (DNS/Pkarr only; DHT/mDNS off); UPnP per your pre-launch decision (proposal: disabled); `.proxy_from_env()` so proxy networks can reach a relay.
- [ ] `receive_file`: parse the ticket, connect with explicit connect/metadata/idle timeouts, download, then export through the safety pipeline — staging dir, every path component must be `Component::Normal`, `symlink_metadata` exists-check, atomic no-replace commit per OS (`renameat2(RENAME_NOREPLACE)` / `renamex_np(RENAME_EXCL)` / `MoveFileExW` without replace), success only after the `Done` progress item. The filename comes from the hashed metadata, never from chat text. Enforce **max 1 concurrent receiver** sender-side. (No stop-on-completion in v1 — see §1.9; deferred to Phase 4.)
- [ ] Map the library's progress events (hashing, per-chunk, done/error) into your `TransferEvent` and send them through the channel.
- [ ] Treat relay reachability on TCP 443 as a hard requirement; surface relay-vs-direct in diagnostics (UI badge later).
- [ ] Shut the node down cleanly when the app closes, on cancel, and on Stop sharing (closes the router — invalidates the ticket).

Rust tips:
- Expect to fight the borrow checker and async code here. Keep functions small and return `Result<_, String>` to the UI at first, then improve the errors later.
- Rust runs your async commands on Tauri's async runtime. Check Tauri's docs for how async commands and spawned tasks work before you start background serving.

**Done when:** two PCs on different networks transfer a 1 GB file through your UI with progress; the hash matches; the filename arrives via the ticket; Stop sharing invalidates the ticket; a second concurrent receiver is rejected.

> **Phase 3 implementation notes (2026-10-19):** built as `src-tauri/src/{share,download,export,transfer}.rs` + `preview_ticket` command. Two gotchas worth keeping: (1) fetching the ticket hash as `HashSeq` recursively pulls **the entire file** — the filename preview and metadata path must request the collection root, its name-table blob, and the metadata blob each as `Raw` (three tiny fetches; covered by the `preview_does_not_download_file` regression test). (2) Each share mints a fresh ephemeral endpoint (stricter than the per-launch default in §1.5), so every ticket is independent. `Sending { receivers }` shipped early (see §5 sketch). Auto-stop deferred — see §1.9.

### Phase 4: Usability and reliability (about 1 week)

- [ ] **Resume:** persistent `FsStore` in the app data dir keyed by content hash, with TTL/pruning (e.g. 7 days). Interrupt a download, **restart the app**, paste the same ticket again — it continues instead of starting over. Test in-process retry separately from cross-process resume. (In-process recovery already shipped: attempts after the first dial a fresh connection, so a receiver-side network switch can recover without restarting. Each ticket gets its own cache subdir: pause-then-Resume continues from it, cancel deletes it so a re-receive starts from zero.)
- [ ] ~~**Cancel** on both sides~~ (shipped early: receiver Cancel aborts the task and keeps ticket + folder so Start resumes from the surviving cache; sender side is Stop sharing) — still to do: an idle-timeout-driven "sender went offline" message (a stalled transfer must never hang forever).
- [ ] **Sender status:** ~~show "waiting for receiver", "sending to a peer"~~ (done in Phase 3 via `Sending { receivers }`), ~~"done"~~ (done for real: after export the receiver dials back over a tiny `fly/done/0` protocol and the sender shows a Delivered celebration — no idle heuristics needed) — still to do: regenerate the displayed ticket when the endpoint's addresses change (tickets go stale), plus the deferred auto-stop. (The old idle-detection "done" plan is obsolete: receiver completion now arrives as an explicit signal, which is what Phase 1's stuck-sender evidence always pointed at.)
- [ ] **Copy ticket** button (clipboard plugin), QR code option, auto-clear the ticket display after use, and an in-app ticket inspector so users never paste live tickets into websites. (Decided 2026-10-22: `fly://` deeplinks that open straight into Receive ship together with QR here — the QR encodes the deeplink.)
- [ ] **Sender fingerprint confirm:** truncated node ID on both sides; document that the chat channel is part of the trust base.
- [ ] **Disk space check** before downloading: budget roughly 2x the file size with copy-based export (blob copy + final file, plus ~6% BLAKE3 outboard at 1 KiB chunks) — or use `ExportMode::TryReference` and state the choice in the UI.
- [ ] **Duplicate filenames:** `file (1).zip` UX on top of an atomic no-replace commit (check-then-rename in two steps is racy).
- [ ] ~~**Speed** and time remaining in the progress UI.~~ (speed shipped early: overall average on Receive) — time remaining still to do.
- [ ] **OS filename rules pass:** reserved names, ADS colons, trailing dots, non-UTF8 — verify against platform docs (UNVERIFIED in research).
- [ ] **Sleep:** "keep this device awake" wording (QUIC connections do not survive sleep); resume-after-sleep goes in the test matrix.
- [ ] **Errors:** friendly messages for a bad ticket; "works with full ticket, fails with short ticket" pointing at DNS filtering; proxy/relay unreachable; UDP-blocked relay-only note; guidance for users who clicked Cancel on the firewall prompt.

### Phase 5: Optional polish (1-2 weeks, pick what you need)

- [ ] **Relay/discovery hosting — decided before launch:** paid Shared/Dedicated or self-hosted (`RelayMode::Custom`, self-host guide exists). Bandwidth costs for relayed transfers, so consider limits. Either way, document the metadata-visibility tradeoff.
- [ ] **Folders:** collections support them with the same libraries; reimplement the pattern from the read-only reference.
- [ ] **Accept/reject on the sender side:** not possible with the simple pull model. This is the main reason to consider approach B.
- [ ] **Same-network mDNS fast path:** skip public discovery on the LAN. Deliberately off by default (privacy); a later enhancement.
- [ ] **Nicer tickets:** QR code, session-lifetime option, receiver/rate/size caps as options. (Short numeric codes rejected for v1: they need a rendezvous server mapping codes to tickets, with abuse-control and metadata-privacy costs out of proportion to the convenience.)
- [ ] **Sigstore + SBOM** on GitHub releases, if not done in Phase 6.

### Phase 6: Package and ship (about 1 week)

- [ ] Build with Tauri's bundler on each OS (use GitHub Actions with Windows, macOS and Ubuntu runners). **MSI installers can only be created on Windows**; NSIS cross-compilation is a last resort — the per-OS runners are the correct approach, not a convenience.
- [ ] Windows: WebView2 is required. Default `downloadBootstrapper` costs 0 MB but needs internet; `embedBootstrapper` is about 1.8 MB; `offlineInstaller` about 127 MB; `fixedVersion` about 180 MB; `skip` is not recommended. Windows 10 (April 2018 update and newer) and Windows 11 ship the runtime with the OS. Keep the default and document the offline-installer tradeoff.
- [ ] Linux: test both the `.deb` and the AppImage on clean machines **newer and older** than the build host (webkit2gtk 4.0 vs 4.1 soname split; the `.deb` needs system WebKitGTK).
- [ ] macOS: signing and notarization need an Apple Developer account ($99/year). Test on both Apple Silicon and Intel if you can.
- [ ] Windows: unsigned apps trigger SmartScreen warnings. Plan signing before public release.
- [ ] **Updater: ship a channel or explicitly decide against one before v1.** Updater signatures are mandatory (`tauri signer generate`, pubkey in config, private key via env) — lose the key and you cannot publish updates. Until an updater exists, version checks should at minimum point users at a download page; never silently run outdated crypto.
- [ ] **Sigstore + SBOM** on GitHub releases (independent of OS signing).
- [ ] The OS firewall prompt on first run is **expected** (the app listens on UDP) — verify it reads sensibly and add in-app guidance for users who clicked Cancel.
- [ ] **Measure the final installer sizes** and record them. The Rust networking stack adds some size, but expect it to stay well below an Electron app. Confirm with real numbers.

---

## 6. Security checklist

- [ ] **AGPL: `sendmer` is read-only.** No copied code, no dependency in a closed app. (If n0's `sendme` is adopted as the reference instead, verify its license/state first — UNVERIFIED.)
- [ ] **Minimal Tauri surface:** `dialog:allow-open` + `dialog:allow-save` only (message dialogs need `dialog:allow-message`; `ask`/`confirm` are deprecated aliases), plus `clipboard-manager:allow-write-text` for copy-ticket. **Never** add fs/opener/shell plugins — all file IO stays in Rust behind your commands. Scope app commands via `AppManifest::commands` only if a second window is ever added.
- [ ] The UI never passes arbitrary paths for the backend to read without the user having picked them in the dialog.
- [ ] **Treat tickets as reusable bearer [REDACTED].** Anyone holding one can download, multiple times. Send only to the intended person; UI wording: forwarding = granting. Provide **Stop sharing** (closes the router); auto-stop after a completed download is deferred to Phase 4; consider a session-lifetime option. Tickets may reveal network addresses — don't post publicly, and warn users never to paste live tickets into ticket-inspector websites (ship an in-app inspector instead).
- [ ] **Filenames and paths are hostile input:** validate every remote path component is `Component::Normal` (no `..`, absolute paths, or prefixes) — never `join()` a remote name onto the save dir unvalidated. Reject symlinks in remote metadata; use `symlink_metadata` (never follow destination links) before commit; commit with an atomic no-replace primitive. Filenames arrive inside the hashed collection, never as trusted chat text.
- [ ] Never auto-open a received file — and never preview one in the webview either.
- [ ] Don't log tickets, file paths, or lower-level connection text — in app logs, crash reports, or the diagnostics bundle.
- [ ] **Sender authentication:** fingerprint-confirm UX (truncated node ID on both sides); document that the chat channel is part of the trust base.
- [ ] **Resource caps:** max 1 concurrent receiver in v1; stop-serving-on-completion deferred to Phase 4 (manual Stop sharing until then); consider max-upload-rate / session-lifetime / max-total-size options.
- [ ] **Disclose relay/discovery metadata** in-app ("how private is this?"): relays see who exchanges traffic, when, and how much; endpoints auto-publish addressing info to lookup services. Mitigations: ephemeral identity default, paid/self-hosted relay, DHT/mDNS kept off and documented.
- [ ] **UPnP/NAT-PMP:** disabled or disclosed — never silently reconfiguring the user's gateway.
- [ ] **Clipboard hygiene:** prefer QR + in-app paste; auto-clear ticket displays; never log clipboard contents.
- [ ] **Supply chain:** exact pins + committed `Cargo.lock` + `cargo --locked` + `cargo audit` in CI; pinned toolchain; review the `iroh-blobs` changelog on every bump, never float `^0.103`.
- [ ] **Updates:** updater signing keys managed safely; Sigstore + SBOM on releases; outdated versions point at the download page.

---

## 7. Testing matrix

| Test | Why |
|---|---|
| Two networks (one on phone hotspot), 1 GB file | Real hole punching (Phase 1 and 3) |
| Same test with a 10-20 GB file + throttled-network soak | "Any size", flat memory, firewall UDP idle timeouts (30 s or more recommended), `iroh-blobs` caveat |
| Windows to Mac, Mac to Linux, Linux to Windows | Cross-OS differences |
| Windows 10 and Windows 11 | WebView2 differences |
| Linux `.deb` and AppImage on clean installs newer *and* older than the build host | Missing system libraries, webkit2gtk 4.0/4.1 soname split |
| Kill the receiver mid-download, **restart the app**, paste the ticket again | Cross-process resume (needs the persistent store) |
| Kill the sender mid-download; sleep the sender mid-download | Error handling; resume-after-sleep |
| Strict office or campus Wi-Fi + proxy-mandating network | Relay fallback, proxy support, and the UI says what is happening |
| UDP-blocked network (relay-only) | Relay-only path works and the UI says so |
| Short (id-only) vs full ticket on filtered DNS | "Full works, short fails" points at DNS filtering |
| Change the sender network while "waiting for receiver" | Ticket staleness — the regenerated ticket still works |
| Click Cancel on the firewall prompt, then transfer | Cancel-path guidance; relay-only or a clear failure |
| Bad or truncated ticket | Error message |
| Second receiver with the same live ticket | Rejected (max 1 receiver); serving stops after first completion |
| Receiver disk nearly full | 2x space check (or TryReference behavior) |
| Very long filename, non-English characters, spaces | Filename handling |
| `..`, absolute paths, symlinks in remote names | Traversal/symlink defenses |
| Paste a ticket into the in-app inspector (and confirm the warning against external sites) | No secret leakage via decoders |
| Outdated client version | Points at the download page, never silently runs old crypto |

---

## 8. Risks

| Risk | Mitigation |
|---|---|
| AGPL contamination via `sendmer` | Read-only reference + clean-room reimplementation; verify n0 `sendme` license if adopted |
| `iroh-blobs` 0.x churn + production-quality caveat | Pin exact, changelog on every bump, large-file soak tests |
| Public relay limits (rate caps, no SLA/privacy) | Pre-launch relay decision: paid tier or self-host |
| Relay/discovery metadata exposure | In-app disclosure; ephemeral identity default; self-host option; DHT/mDNS off |
| Ticket staleness breaks "waiting" sessions | Id-only tickets + live discovery; regenerate display on address change |
| Enterprise DNS strips TXT records | Full-ticket fallback + dedicated error branch |
| Proxy-mandating networks unreachable | `.proxy_from_env()` before the strict-network test |
| No update channel | Decide before v1; guard signing keys; Sigstore + SBOM |
| Sleep stalls/splits transfers | "Keep device awake" wording; resume-after-sleep test |
| UPnP silently reconfigures gateways | Disable (proposal) or disclose |
| Clipboard snooping of tickets | QR + in-app paste; auto-clear; never log |
| Rust learning curve slows Phase 3 | Phase 1 and 2 let you learn in small steps; minimal glue; reimplement proven patterns |
| Hole punching fails for some users | Relay fallback (+443 reachability requirement); test on strict networks |
| Sender must stay online (pull model) | Clear "keep this window open / device awake" messaging |
| Webview rendering differs per OS | Keep the UI simple, test all three OSes early |
| Linux packaging/soname issues | Phase 0 gate, newer+older clean-machine tests |
| Unsigned apps scare users | Plan signing before public release |

---

## 9. Rough timeline (side-project pace)

| Phase | Time |
|---|---|
| 0. Setup and Linux gate | 2-3 days |
| 1. Prove P2P with `sendmer` | 2-3 days |
| 2. Tauri shell with fake transfers | 3-4 days |
| 3. Real transfer with iroh-blobs | 1-2 weeks |
| 4. Usability and reliability | about 1 week |
| 5. Optional polish | 1-2 weeks (optional) |
| 6. Package and ship | about 1 week |
| **Total** | **about 5-7 weeks, plus optional Phase 5** |

Rust is new to you, so Phase 3 is the most likely to run long. You have a usable app after Phase 4.

**Pre-launch decisions (before v1 — not Phase 5):** relay tier or self-host · updater channel (or an explicit no) · ephemeral identity default · UPnP off (proposal) · DHT/mDNS off and documented · 1 receiver + session-lifetime caps.

---

## 10. Check before relying on these

Pinned versions observed 2026-10-16 (re-check before building): `tauri` 2.12.4 · `iroh` 1.3.0 · `iroh-blobs` 0.103.1 · `sendmer` 0.11.0 (reference only) · `wry` 0.57.0. Full citations live in `p2p-file-transfer-research.md`.

- **iroh:** endpoint setup (`Endpoint::bind()` / builder) and `presets::N0` for your pinned version; `RelayMode` (including `Custom` and `Disabled` for no-relay tests); tickets doc (contents, reuse, staleness); relays + security Q&A (E2EE vs metadata); address-lookup doc (auto-publish, Pkarr/DNS, mDNS/DHT options); configuring-networks (firewall, proxy `_from_env`, UPnP/`PortmapperConfig`, DNS-TXT failures, UDP idle timeouts, `iroh-doctor`); dedicated-infrastructure (self-host guide, managed tiers); the `llms.txt` page index.
- **iroh-blobs:** adding a file, collections + metadata blobs, `BlobTicket` shape, tags/GC, `MemStore` vs `FsStore`, `export_with_opts` + `ExportMode`, progress items, resume semantics — and the scope of the README production-quality note (resolve with maintainers/testing).
- **`sendmer` (READ-ONLY, AGPL-3.0-only):** README (cache-dir resume, ticket types, retry/timeouts, sender caps, `SendHandle`, Sigstore releases) and `receiver`/`endpoint`/`args` source — behavior spec only, never copied code.
- **n0 `sendme`** current state and license as the alternative primary reference — **UNVERIFIED**, check before using.
- **Tauri v2:** `#[tauri::command]`, single `generate_handler!`, `tauri::ipc::Channel` vs events for progress, `Channel` from `@tauri-apps/api/core`, state management (`app.manage`, `Mutex`), capabilities (app-commands default-allow, `AppManifest::commands`).
- **Tauri plugins:** dialog (`@tauri-apps/plugin-dialog` / `tauri-plugin-dialog`, `allow-open`/`allow-save`, Rust 1.90+), clipboard-manager (`@tauri-apps/plugin-clipboard-manager` / `tauri-plugin-clipboard-manager`, `allow-write-text`), updater (mandatory signatures, key handling).
- **Windows WebView2 install options** (bootstrapper table) and **Linux package dependencies** in the bundler docs; MSI-Windows-only constraint.
- **OS filename rules** during Phase 4 (reserved names, ADS, trailing dots, non-UTF8) — **UNVERIFIED** in research, verify against platform docs.
- The WebKitGTK-WebRTC background claim is likewise **UNVERIFIED** — but it doesn't affect the build (Rust networking stands on IPC cost, installer size, and the resume library).
