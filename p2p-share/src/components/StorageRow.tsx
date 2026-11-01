import { useEffect, useState } from "react";
import { clearCache, fetchCacheInfo, formatBytes } from "../api";
import type { CacheInfo } from "../api";

/** Download cache, always visible in the sidebar: most people never
    open Diagnostics, so cache size and the Clear button live here. */
export default function StorageRow() {
  const [cache, setCache] = useState<CacheInfo | null>(null);
  const [clearing, setClearing] = useState(false);
  const [note, setNote] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    const load = () => {
      fetchCacheInfo()
        .then((info) => {
          if (!cancelled) setCache(info);
        })
        .catch(() => {
          if (!cancelled) setCache({ bytes: 0, caches: 0 });
        });
    };
    load();
    const timer = window.setInterval(load, 10000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  async function handleClear() {
    setNote("");
    setError("");
    setClearing(true);
    try {
      const result = await clearCache();
      const folders = result.removed === 1 ? "folder" : "folders";
      setNote(
        `Cleared ${formatBytes(result.freedBytes)} (${result.removed} ${folders}).`,
      );
      setCache(await fetchCacheInfo());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setClearing(false);
    }
  }

  const bytes = cache?.bytes ?? 0;
  const folders = cache?.caches ?? 0;
  return (
    <div className="storage-block">
      <div className="storage-top">
        <div>
          <div className="storage-label">Download cache</div>
          <div className="storage-size">
            {bytes === 0
              ? "Empty"
              : `${formatBytes(bytes)} · ${folders} ${folders === 1 ? "folder" : "folders"}`}
          </div>
        </div>
        <button
          className="btn-ghost btn-small"
          onClick={handleClear}
          disabled={clearing || bytes === 0}
        >
          {clearing ? "Clearing…" : "Clear"}
        </button>
      </div>
      {note && (
        <div className="storage-note" role="status">
          {note}
        </div>
      )}
      {error && (
        <div className="storage-error" role="alert">
          {error}
        </div>
      )}
    </div>
  );
}
