import { useEffect, useState } from "react";
import { clearCache, copyText, fetchCacheInfo, fetchDiagnostics, formatBytes } from "../api";
import type { CacheInfo, DiagnosticsInfo } from "../api";
import { AlertIcon, CheckIcon, CopyIcon } from "./icons";

interface Props {
  open: boolean;
  onClose: () => void;
}

export default function Diagnostics({ open, onClose }: Props) {
  const [info, setInfo] = useState<DiagnosticsInfo | null>(null);
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);
  const [cache, setCache] = useState<CacheInfo | null>(null);
  const [clearing, setClearing] = useState(false);
  const [cleared, setCleared] = useState("");

  useEffect(() => {
    if (!open) return;
    setInfo(null);
    setError("");
    setCache(null);
    setCleared("");
    fetchDiagnostics()
      .then(setInfo)
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err));
      });
    fetchCacheInfo()
      .then(setCache)
      .catch(() => setCache({ bytes: 0, caches: 0 }));
  }, [open]);

  if (!open) return null;

  async function handleClear() {
    setCleared("");
    setClearing(true);
    try {
      const result = await clearCache();
      const folders = result.removed === 1 ? "folder" : "folders";
      setCleared(`Cleared ${formatBytes(result.freedBytes)} (${result.removed} ${folders}).`);
      setCache(await fetchCacheInfo());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setClearing(false);
    }
  }

  async function handleCopy() {
    if (!info) return;
    const text = [
      `node: ${info.nodeId}`,
      `relay reachable: ${info.relayReachable ? "yes" : "no"}`,
      `nat: ${info.natType}`,
      ...(cache ? [`cache: ${formatBytes(cache.bytes)} in ${cache.caches} folders`] : []),
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
          Safe to share with support. Never contains tickets or file paths.
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
            {cache && (
              <div className="diag-row">
                <span>Download cache</span>
                <code>
                  {formatBytes(cache.bytes)} · {cache.caches}{" "}
                  {cache.caches === 1 ? "folder" : "folders"}
                </code>
              </div>
            )}
          </div>
        )}
        {cache && cache.bytes > 0 && (
          <div className="modal-actions">
            <button
              className="btn-ghost btn-small"
              onClick={handleClear}
              disabled={clearing}
            >
              {clearing ? "Clearing…" : "Clear cache"}
            </button>
          </div>
        )}
        {cleared && (
          <div className="banner banner-info" role="status">
            {cleared}
          </div>
        )}
        {info && info.natType === "UDP blocked" && (
          <div className="banner banner-warn" role="note">
            UDP is blocked on this network: transfers still work through the
            relay, but may be slower than direct connections.
          </div>
        )}
        {info && !info.relayReachable && (
          <div className="banner banner-warn" role="note">
            No relay is reachable: check VPN, proxy, or firewall settings. If
            your system showed a network-access prompt for fly, allow it —
            without relay access, transfers outside your local network fail.
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
