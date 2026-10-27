import { useEffect, useRef, useState } from "react";
import {
  cancelReceive,
  fileNameOf,
  formatBytes,
  formatSpeed,
  normalizeTicket,
  pickSaveDir,
  previewTicket,
  receiveFile,
} from "../api";
import type { FilePreview, TransferEvent } from "../api";
import Progress from "./Progress";
import {
  AlertIcon,
  FolderIcon,
  LinkIcon,
  LockIcon,
  ShieldIcon,
  ZapIcon,
} from "./icons";

type Status = "idle" | "receiving" | "finished" | "error";

export default function ReceivePanel({ active }: { active: boolean }) {
  const [ticket, setTicket] = useState("");
  const [saveDir, setSaveDir] = useState<string | null>(null);
  const [preview, setPreview] = useState<FilePreview | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [lookingUp, setLookingUp] = useState(false);
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(
    null,
  );
  const [speed, setSpeed] = useState<number | null>(null);
  const [status, setStatus] = useState<Status>("idle");
  const [message, setMessage] = useState("");
  const [savedName, setSavedName] = useState<string | null>(null);
  const [resumable, setResumable] = useState(false);
  // Speed is the overall average since the first progress event — stable,
  // and honest across stalls and retries.
  const speedStart = useRef<{ at: number; done: number } | null>(null);
  // Which stop button was pressed: pause keeps everything for Resume,
  // cancel clears the form. Read when the stopped download settles.
  const pendingAction = useRef<"pause" | "cancel" | null>(null);

  // Look up the real file name + size shortly after the ticket stops
  // changing. An empty ticket clears the preview without a backend call.
  // Skipped while the tab is hidden; re-runs when it becomes visible.
  useEffect(() => {
    if (!active) return;
    const value = normalizeTicket(ticket);
    setPreview(null);
    setPreviewError("");
    if (value === "") {
      setLookingUp(false);
      return;
    }
    setLookingUp(true);
    let cancelled = false;
    const timer = window.setTimeout(() => {
      previewTicket(value)
        .then((found) => {
          if (cancelled) return;
          setPreview(found);
          setPreviewError("");
        })
        .catch((err: unknown) => {
          if (cancelled) return;
          setPreview(null);
          setPreviewError(err instanceof Error ? err.message : String(err));
        })
        .finally(() => {
          if (!cancelled) setLookingUp(false);
        });
    }, 600);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [ticket, active]);

  function handleEvent(event: TransferEvent) {
    if (event.event === "Progress") {
      const { done, total } = event.data;
      setProgress({ done, total });
      const now = performance.now();
      if (!speedStart.current) {
        speedStart.current = { at: now, done };
        setSpeed(null);
      } else {
        const elapsed = (now - speedStart.current.at) / 1000;
        if (elapsed > 0.5) {
          setSpeed((done - speedStart.current.done) / elapsed);
        }
      }
    } else if (event.event === "Finished") {
      setStatus("finished");
      setResumable(false);
      pendingAction.current = null;
    }
  }

  async function handleChooseDir() {
    try {
      const picked = await pickSaveDir();
      if (picked) setSaveDir(picked);
    } catch (err) {
      setStatus("error");
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleStart() {
    setProgress(null);
    setSpeed(null);
    speedStart.current = null;
    setSavedName(null);
    setMessage("");
    setResumable(false);
    pendingAction.current = null;
    setStatus("receiving");
    try {
      const name = await receiveFile(
        normalizeTicket(ticket),
        saveDir ?? "",
        handleEvent,
      );
      setSavedName(name);
    } catch (err) {
      const text = err instanceof Error ? err.message : String(err);
      if (text === "Download cancelled." && pendingAction.current === "cancel") {
        // True cancel: clear the whole form.
        pendingAction.current = null;
        setTicket("");
        setSaveDir(null);
        setPreview(null);
        setPreviewError("");
        setProgress(null);
        setSpeed(null);
        speedStart.current = null;
        setSavedName(null);
        setResumable(false);
        setStatus("idle");
        setMessage("Download cancelled.");
      } else if (text === "Download cancelled.") {
        // Pause: back to idle with a neutral note, keeping the ticket +
        // folder so Resume continues from the surviving cache.
        pendingAction.current = null;
        setStatus("idle");
        setResumable(true);
        setMessage("Download paused. Resume to continue.");
      } else {
        pendingAction.current = null;
        setStatus("error");
        setMessage(text);
      }
    }
  }

  async function stopWith(action: "pause" | "cancel") {
    // One backend command stops the in-flight attempt either way; pause
    // vs cancel differ only in what the UI keeps afterwards.
    pendingAction.current = action;
    try {
      await cancelReceive();
    } catch (err) {
      pendingAction.current = null;
      setStatus("error");
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  const canStart =
    status !== "receiving" && normalizeTicket(ticket) !== "" && saveDir !== null;

  return (
    <div className="view">
      <h1 className="view-title">Receive a file</h1>
      <p className="view-subtitle">
        Enter a transfer ticket from the sender to start downloading.
      </p>

      <div className="card">
        <div className="ticket-head">
          <span className="ticket-icon">
            <LinkIcon size={20} />
          </span>
          <div>
            <h2 className="card-title">Transfer ticket</h2>
            <p className="card-sub">Paste the ticket code from the sender.</p>
          </div>
        </div>
        <input
          className="input ticket-input"
          value={ticket}
          onChange={(e) => {
            setTicket(e.currentTarget.value);
            setResumable(false);
          }}
          placeholder="Paste ticket here…"
          spellCheck={false}
          autoComplete="off"
        />
        {normalizeTicket(ticket) !== "" && (
          <div className="preview-card">
            {preview ? (
              <>
                <div className="preview-name">{fileNameOf(preview.name)}</div>
                <div className="preview-sub">{formatBytes(preview.size)} — verified from the ticket.</div>
              </>
            ) : lookingUp ? (
              <div className="preview-sub">Looking up the file…</div>
            ) : previewError ? (
              <div className="preview-error">{previewError}</div>
            ) : null}
          </div>
        )}
        <div className="dir-row">
          <button className="btn-ghost" onClick={handleChooseDir}>
            <FolderIcon size={16} />
            {saveDir ? "Change save folder" : "Choose save folder"}
          </button>
          <span className="dir-path" title={saveDir ?? ""}>
            {saveDir ?? "No folder chosen"}
          </span>
        </div>
        {status === "receiving" ? (
          <div className="receive-actions">
            <button
              className="btn-ghost btn-large"
              onClick={() => void stopWith("pause")}
            >
              Pause
            </button>
            <button
              className="btn-danger-outline btn-large"
              onClick={() => void stopWith("cancel")}
            >
              Cancel
            </button>
          </div>
        ) : (
          <button
            className="btn-primary btn-block btn-large"
            onClick={handleStart}
            disabled={!canStart}
          >
            {resumable ? "Resume download" : "Start receiving"}
          </button>
        )}
        {status === "receiving" && !progress && (
          <div className="connecting-hint">Connecting to the sender…</div>
        )}
        {progress && (status === "receiving" || status === "finished") && (
          <div className="receive-progress">
            <Progress done={progress.done} total={progress.total} />
            {speed !== null && (
              <div className="speed-line">{formatSpeed(speed)}</div>
            )}
          </div>
        )}
        {status === "finished" && (
          <div className="banner banner-ok" role="status">
            Saved{savedName ? ` as ${savedName}` : ""}.
          </div>
        )}
        {status === "idle" && message && (
          <div className="banner banner-info" role="status">
            {message}
          </div>
        )}
        {status === "error" && message && (
          <div className="banner banner-error" role="alert">
            <AlertIcon size={16} />
            {message}
          </div>
        )}
      </div>

      <div className="card trust-row">
        <div className="trust-item">
          <LockIcon size={18} />
          <div>
            <div className="trust-title">End-to-end encrypted</div>
            <div className="trust-sub">Transferred directly between devices.</div>
          </div>
        </div>
        <div className="trust-item">
          <ZapIcon size={18} />
          <div>
            <div className="trust-title">No account needed</div>
            <div className="trust-sub">Just a ticket code.</div>
          </div>
        </div>
        <div className="trust-item">
          <ShieldIcon size={18} />
          <div>
            <div className="trust-title">Verified integrity</div>
            <div className="trust-sub">Checked as it downloads.</div>
          </div>
        </div>
      </div>
    </div>
  );
}
