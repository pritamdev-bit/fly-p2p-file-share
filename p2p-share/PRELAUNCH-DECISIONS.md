# Pre-launch decisions

Started in Phase 0 per `../p2p-file-transfer-plan-tauri.md`. Each item is mostly
one-line config, but all are harder to change after ship. Status: PROPOSED unless
marked DECIDED.

| # | Decision | Plan default / proposal | Status |
|---|---|---|---|
| 1 | Relay tier | Paid Shared/Dedicated or self-hosted (`RelayMode::Custom`, wired to `FLY_RELAY_URL`). Public n0 relays are dev/test-only (rate-limited, no SLA/privacy). | DECIDED — self-hosted on our VPS (`https://relay.pritam.codes`, iroh-relay v1.3.0 on Debian); app default map is ours plus n0 public as fallback; `FLY_RELAY_URL` still fully overrides |
| 2 | Updater channel | Ship Tauri updater (signatures mandatory; guard `TAURI_SIGNING_PRIVATE_KEY`) or record an explicit no. Until one exists, version checks point at a download page. | PROPOSED |
| 3 | Node identity lifetime | Ephemeral key per launch (bounds ticket lifetime, no long-term trackable identity). | PROPOSED |
| 4 | UPnP/NAT-PMP port mapping | Disabled (rely on hole-punching + relay) — or disclose that the app asks the gateway for a port. | DECIDED — enabled (`PortmapperConfig::default`); the app maps its fixed port itself, relay stays as fallback |
| 5 | DHT / mDNS lookup | Off. Use the N0 preset (DNS/Pkarr + ticket-embedded addresses) only; document the choice. | PROPOSED |
| 6 | Receiver / session caps | Max 1 concurrent receiver in v1; auto-stop serving after first completed download; consider session-lifetime option. | DECIDED — downloads allowed 1-3 with auto-stop at the limit; share expiry Never/15 min/1 h; rate/size caps skipped by design (see plan Phase 5) |

References: plan sections 4, 6, 9 and `../p2p-file-transfer-research.md` (I6, I9, I12, V2, V6, V7, V11, V12, V14).

## 1. Relay hosting (detail)

**Status:** code support shipped; tier choice still the owner's call.
Recommendation: stay on n0's public relays through the tester phase,
move to paid Shared/Dedicated or self-hosted before public launch.

**What the relay sees (either tier).** File bytes are end-to-end
encrypted (QUIC TLS between sender and receiver): a relay operator can
never see file names or contents. It does see metadata: both endpoint
IDs, connection timing and duration, bytes transferred, and IP
addresses. Direct (hole-punched) transfers cost the relay nothing
beyond introduction; relayed transfers cost it bandwidth — hence rate
limits on the public tier and metering (or your own server bill) on
paid/self-hosted.

**Scope of `FLY_RELAY_URL`.** One URL, or several comma-separated for
failover; sender and receiver should use the same set. It switches
relayed traffic only — short-ticket discovery still uses n0's
DNS/Pkarr, so discovery metadata stays with n0 either way. Prefer FQDN
form with a trailing dot (`https://relay.example.com./`): bare
hostnames stall on networks with slow DNS search domains (observed in
tests). Serve real TLS from the relay; there is intentionally no
insecure-test knob.

**Self-host pointers.** The `iroh-relay` crate we already depend on is
the server; n0's relay docs cover deployment. Budget for bandwidth on
top of the box: every relayed byte transits it.
