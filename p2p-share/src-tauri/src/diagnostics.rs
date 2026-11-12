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
    let nat_type = match tokio::time::timeout(
        REPORT_TIMEOUT,
        endpoint.net_report().initialized(),
    )
    .await
    {
        Ok(report) => classify_nat(&report),
        Err(_) => "Unknown".to_string(),
    };
    let home_relay = current_home_relay(&endpoint).await;
    endpoint.close().await;
    Ok(DiagnosticsInfo {
        node_id,
        relay_reachable,
        nat_type,
        home_relay,
    })
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
    /// is online: this is the row the UI shows, so it must resolve.
    #[tokio::test]
    async fn home_relay_resolves_when_online() {
        let endpoint = build_endpoint().await.expect("bind diagnostics endpoint");
        let online = tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await;
        assert!(online.is_ok(), "a relay must be reachable");
        let home = current_home_relay(&endpoint).await;
        endpoint.close().await;
        let url = home.expect("a connected home relay must be reported");
        assert!(
            url.starts_with("https://"),
            "relay URL must be absolute, got {url}"
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
