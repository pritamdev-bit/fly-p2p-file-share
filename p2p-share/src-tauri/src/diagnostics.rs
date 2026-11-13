use crate::share::build_endpoint;
use iroh::Watcher;
use serde::Serialize;
use std::time::Duration;

/// Real diagnostics bundle. The no-secrets rule: never tickets or paths.
/// The node ID is a fresh ephemeral identity per call — fly keeps no
/// long-term identity, so it differs every time by design.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsInfo {
    pub node_id: String,
    pub relay_reachable: bool,
    pub nat_type: String,
    /// Which relay this device homed to: the same latency-driven pick
    /// the sender and receiver endpoints make. None when no relay
    /// connected in time.
    pub home_relay: Option<String>,
    /// One entry per configured relay: the fastest measured probe, or
    /// missing when the probe never completed. A relay the app cannot
    /// probe shows up as missing instead of vanishing.
    pub relays: Vec<RelayProbe>,
}

/// One relay's probe result for the speed list.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayProbe {
    pub url: String,
    pub latency_ms: Option<u64>,
    pub preferred: bool,
}

const ONLINE_TIMEOUT: Duration = Duration::from_secs(15);
const REPORT_TIMEOUT: Duration = Duration::from_secs(10);

#[tauri::command]
pub async fn get_diagnostics() -> Result<DiagnosticsInfo, String> {
    let endpoint = build_endpoint()
        .await
        .map_err(|error| format!("{error:#}"))?;
    let node_id = endpoint.id().to_string();
    // online() resolves once any home relay connects.
    let relay_reachable = tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online())
        .await
        .is_ok();
    let report = tokio::time::timeout(REPORT_TIMEOUT, endpoint.net_report().initialized())
        .await
        .ok();
    let nat_type = match &report {
        Some(report) => classify_nat(report),
        None => "Unknown".to_string(),
    };
    let relays = relay_speeds(report.as_ref());
    let home_relay = current_home_relay(&endpoint).await;
    endpoint.close().await;
    Ok(DiagnosticsInfo {
        node_id,
        relay_reachable,
        nat_type,
        home_relay,
        relays,
    })
}

/// Fastest measured probe per configured relay, preferred first. A
/// relay with no completed probe keeps its slot with no time, so a
/// relay the app cannot reach is visible as missing instead of
/// vanishing.
fn relay_speeds(report: Option<&iroh::unstable_net_report::NetReport>) -> Vec<RelayProbe> {
    use std::collections::BTreeMap;
    let mut best: BTreeMap<String, u64> = BTreeMap::new();
    if let Some(report) = report {
        for (_, url, latency) in report.relay_latency.iter() {
            let ms = latency.as_millis() as u64;
            best.entry(url.to_string())
                .and_modify(|prev| *prev = (*prev).min(ms))
                .or_insert(ms);
        }
    }
    let preferred = report
        .and_then(|r| r.preferred_relay.clone())
        .map(|url| url.to_string());
    let mut out: Vec<RelayProbe> = crate::share::configured_relay_map()
        .urls::<Vec<iroh::RelayUrl>>()
        .into_iter()
        .map(|url| {
            let key = url.to_string();
            RelayProbe {
                preferred: Some(&key) == preferred.as_ref(),
                latency_ms: best.get(&key).copied(),
                url: key,
            }
        })
        .collect();
    out.sort_by_key(|p| {
        (
            !p.preferred,
            p.latency_ms.is_none(),
            p.latency_ms.unwrap_or(u64::MAX),
        )
    });
    out
}

/// Which relay this device homed to (the same latency-driven pick the
/// sender and receiver endpoints make). online() already guarantees a
/// connected entry exists; the short poll covers the tick it takes the
/// watcher to replay it.
async fn current_home_relay(endpoint: &iroh::Endpoint) -> Option<String> {
    let mut watcher = endpoint.home_relay_status();
    for _ in 0..50 {
        if let Some(status) = watcher.get().into_iter().find(|s| s.is_connected()) {
            return Some(status.url().to_string());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The home relay read reports a real relay URL once the endpoint
    /// is online: this is the row the UI shows, so it must resolve. The
    /// speed list covers every configured relay, ours included, even
    /// when a probe never completes.
    #[tokio::test]
    async fn home_relay_resolves_when_online() {
        let endpoint = build_endpoint().await.expect("bind diagnostics endpoint");
        let online = tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await;
        assert!(online.is_ok(), "a relay must be reachable");
        let report = tokio::time::timeout(REPORT_TIMEOUT, endpoint.net_report().initialized())
            .await
            .expect("net report must finish");
        let home = current_home_relay(&endpoint).await;
        let relays = relay_speeds(Some(&report));
        endpoint.close().await;
        let url = home.expect("a connected home relay must be reported");
        assert!(
            url.starts_with("https://"),
            "relay URL must be absolute, got {url}"
        );
        assert_eq!(relays.len(), 5, "ours plus the four n0 relays");
        assert!(
            relays.iter().any(|r| r.url.contains("relay.pritam.codes")),
            "our relay must be listed even when unprobed"
        );
    }
}

/// Human NAT summary from a net report. There is no direct "NAT type"
/// field; mapping stability plus UDP reachability is what can be observed.
fn classify_nat(report: &iroh::unstable_net_report::NetReport) -> String {
    if !report.has_udp() {
        return "UDP blocked".to_string();
    }
    match report.mapping_varies_by_dest() {
        Some(true) => "Symmetric NAT".to_string(),
        _ => "Cone NAT".to_string(),
    }
}
