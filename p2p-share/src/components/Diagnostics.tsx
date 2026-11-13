import { useEffect, useState } from "react";
import { copyText, fetchDiagnostics } from "../api";
import type { DiagnosticsInfo } from "../api";
import { AlertIcon, CheckIcon, CopyIcon } from "./icons";

interface Props {
  open: boolean;
  onClose: () => void;
}

export default function Diagnostics({ open, onClose }: Props) {
  const [info, setInfo] = useState<DiagnosticsInfo | null>(null);
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!open) return;
    setInfo(null);
    setError("");
    fetchDiagnostics()
      .then(setInfo)
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err));
      });
  }, [open ]);

  if (!open) return null;

  async function handleCopy() {
    if (!info) return;
    const shortHost = (url: string) => {
      try {
        return new URL(url).hostname.replace(/\.$/, "");
      } catch {
        return url;
      }
    };
    const text = [
      `node: ${info.nodeId}`,
      `relay reachable: ${info.relayReachable ? "yes" : "no"}`,
      `nat: ${info.natType}`,
      `home relay: ${info.homeRelay ? shortHost(info.homeRelay) : "none"}`,
      ...info.relays.map(
        (r) =>
          `relay ${shortHost(r.url)}: ${r.latencyMs == null ? "no answer" : `${r.latencyMs} ms`}${
            r.preferred ? " (preferred)" : ""
          }`,
      ),
    ].join("\n");
    try {
      await copyText(text);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="card modal-card"
        role="dialog"
        aria-modal="true"
        aria-label="Diagnostics"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="card-title">Diagnostics</h2>
        <p className="card-sub">
          Safe to share with support. It never shows codes or file paths.
        </p>
        {info && (
          <div className="diag-list">
            <div className="diag-row">
              <span>Node ID</span>
              <code>{info.nodeId}</code>
            </div>
            <div className="diag-row">
              <span>Relay reachable</span>
              <code>{info.relayReachable ? "yes" : "no"}</code>
            </div>
            <div className="diag-row">
              <span>NAT type</span>
              <code>{info.natType}</code>
            </div>
            <div className="diag-row">
              <span>Relay in use</span>
              <code>
                {info.homeRelay
                  ? new URL(info.homeRelay).hostname.replace(/\.$/, "")
                  : "Not connected"}
              </code>
            </div>
            {info.relays.length > 0 && (
              <div className="diag-speed">
                {info.relays.map((relay) => {
                  let host = relay.url;
                  try {
                    host = new URL(relay.url).hostname.replace(/\.$/, "");
                  } catch {
                    // Keep the raw URL when it does not parse.
                  }
                  return (
                    <div className="diag-row" key={relay.url}>
                      <span>
                        {host}
                        {relay.preferred ? " · preferred" : ""}
                      </span>
                      <code>
                        {relay.latencyMs == null
                          ? "no answer"
                          : `${relay.latencyMs} ms`}
                      </code>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
        {info && info.natType === "UDP blocked" && (
          <div className="banner banner-warn" role="note">
            UDP is blocked on this network. Transfers still work through the
            relay, but they may be slower than direct connections.
          </div>
        )}
        {info && !info.relayReachable && (
          <div className="banner banner-warn" role="note">
            No relay is reachable. Check VPN, proxy, or firewall settings. If
            your system showed a network access prompt for fly, allow it.
            Without relay access, transfers outside your local network fail.
          </div>
        )}
        {error && (
          <div className="banner banner-error" role="alert">
            <AlertIcon size={16} />
            {error}
          </div>
        )}
        <div className="modal-actions">
          <button className="btn-ghost" onClick={onClose}>
            Close
          </button>
          <button className="btn-primary" onClick={handleCopy} disabled={!info}>
            {copied ? <CheckIcon size={16} /> : <CopyIcon size={16} />}
            {copied ? "Copied" : "Copy diagnostics"}
          </button>
        </div>
      </div>
    </div>
  );
}
