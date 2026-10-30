import { useEffect, useRef, useState } from "react";
import {
  cancelReceive,
  decodeQrTicket,
  fileNameOf,
  formatBytes,
  formatEta,
  formatSpeed,
  inspectTicket,
  isExecutableName,
  normalizeTicket,
  openReceived,
  pauseReceive,
  pickQrImage,
  pickSaveDir,
  previewTicket,
  receiveFile,
  revealReceived,
} from "../api";
import type { FilePreview, TicketInfo, TransferEvent } from "../api";
import type { IncomingTicket } from "../App";
import Fingerprint from "./Fingerprint";
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

export default function ReceivePanel({
  active,
  incomingTicket,
  onBusyChange,
}: {
  active: boolean;
  incomingTicket: IncomingTicket | null;
  onBusyChange: (busy: boolean) => void;
}) {
  const [ticket, setTicket] = useState("");
  const [saveDir, setSaveDir] = useState<string | null>(null);
  const [preview, setPreview] = useState<FilePreview | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [lookingUp, setLookingUp] = useState(false);
  // Offline ticket parse (never connects): whose ticket this is, shown
  // in the Ticket details disclosure. Failures stay silent — the
  // debounced preview reports bad tickets soon enough.
  const [ticketInfo, setTicketInfo] = useState<TicketInfo | null>(null);
  // Executable guard: opening a runnable file needs one explicit extra
  // click. Launch failures show inline without leaving finished state.
  const [confirmingOpen, setConfirmingOpen] = useState(false);
  const [launchError, setLaunchError] = useState("");
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(
    null,
  );
  const [speed, setSpeed] = useState<number | null>(null);
  const [status, setStatus] = useState<Status>("idle");
  const [message, setMessage] = useState("");
  const [savedName, setSavedName] = useState<string | null>(null);
  const [fromCache, setFromCache] = useState(false);
  const [resumable, setResumable] = useState(false);
  // Speed is the overall average since the first progress event — stable,
  // and honest across stalls and retries. Display updates are throttled
  // so the number doesn't flicker with every progress event.
  const speedStart = useRef<{ at: number; done: number } | null>(null);
  const lastSpeedUpdate = useRef(0);
  // Set while a pause/cancel click is winding the backend down, so the
  // buttons acknowledge immediately instead of flipping abruptly later.
  const [stopping, setStopping] = useState<"pause" | "cancel" | null>(null);
  // Read by the preview effect so it stays quiet while receiving (the
  // receive owns the ticket's cache dir during a download).
  const statusRef = useRef(status);
  statusRef.current = status;

  // A new ticket means a new transfer: clear any leftover
  // finished/cancelled/error state (never touch a live download).
  function resetForNewTicket() {
    setStatus("idle");
    setMessage("");
    setSavedName(null);
    setFromCache(false);
    setProgress(null);
    setSpeed(null);
    setTicketInfo(null);
    setConfirmingOpen(false);
    setLaunchError("");
    setResumable(false);
    speedStart.current = null;
  }

  // Deeplinks land here: prefill the ticket (never into a live download).
  useEffect(() => {
    if (!incomingTicket) return;
    if (statusRef.current === "receiving") return;
    resetForNewTicket();
    setTicket(incomingTicket.ticket);
    // resetForNewTicket only calls setState + a ref write.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [incomingTicket]);

  // Report live downloads so the app can confirm before quitting.
  useEffect(() => {
    onBusyChange(status === "receiving");
  }, [status, onBusyChange]);

  // Look up the real file name + size shortly after the ticket stops
  // changing. An empty ticket clears the preview without a backend call.
  // Skipped while the tab is hidden or a download runs (the receive owns
  // the ticket's cache dir); re-runs when it becomes visible.
  useEffect(() => {
    if (!active || statusRef.current === "receiving") return;
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

  // Inspect on every change: the parse is offline (microseconds), so no
  // debounce needed, and it works even when the sender is unreachable.
  useEffect(() => {
    if (!active) return;
    const value = normalizeTicket(ticket);
    if (value === "") {
      setTicketInfo(null);
      return;
    }
    let cancelled = false;
    inspectTicket(value)
      .then((info) => {
        if (!cancelled) setTicketInfo(info);
      })
      .catch(() => {
        if (!cancelled) setTicketInfo(null);
      });
    return () => {
      cancelled = true;
    };
  }, [ticket, active]);

  function handleEvent(event: TransferEvent) {
    if (event.event === "Progress") {
      const { done, total } = event.data;
      setProgress({ done, total });
      const now = performance.now();
      if (!speedStart.current) {
        speedStart.current = { at: now, done };
        lastSpeedUpdate.current = now;
        setSpeed(null);
      } else {
        const elapsed = (now - speedStart.current.at) / 1000;
        if (elapsed > 0.5 && now - lastSpeedUpdate.current > 500) {
          lastSpeedUpdate.current = now;
          setSpeed((done - speedStart.current.done) / elapsed);
        }
      }
    } else if (event.event === "Finished") {
      setStatus("finished");
      setResumable(false);
      setStopping(null);
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

  // Desktop alternative to pointing a camera at the screen: pick a QR
  // screenshot, decode it, and use the ticket it carries.
  async function handleUploadQr() {
    try {
      const picked = await pickQrImage();
      if (!picked) return;
      const ticket = await decodeQrTicket(picked);
      resetForNewTicket();
      setTicket(ticket);
    } catch (err) {
      setStatus("error");
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  async function runLaunch(action: () => Promise<string>) {
    setLaunchError("");
    try {
      await action();
    } catch (err) {
      setLaunchError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleOpen() {
    // Runnable files need an explicit second click; everything else (and
    // the confirmation itself) opens straight away.
    if (savedName && isExecutableName(savedName) && !confirmingOpen) {
      setConfirmingOpen(true);
      return;
    }
    setConfirmingOpen(false);
    await runLaunch(openReceived);
  }

  async function handleReveal() {
    await runLaunch(revealReceived);
  }

  async function handleStart() {
    setProgress(null);
    setSpeed(null);
    speedStart.current = null;
    lastSpeedUpdate.current = 0;
    setStopping(null);
    setSavedName(null);
    setFromCache(false);
    setMessage("");
    setResumable(false);
    setConfirmingOpen(false);
    setLaunchError("");
    setStatus("receiving");
    try {
      const info = await receiveFile(
        normalizeTicket(ticket),
        saveDir ?? "",
        handleEvent,
      );
      setSavedName(info.name);
      setFromCache(info.fromCache);
    } catch (err) {
      const text = err instanceof Error ? err.message : String(err);
      if (text === "Download cancelled.") {
        // True cancel: clear the whole form. The backend also deleted
        // this ticket's cache, so a re-receive starts from zero.
        setStopping(null);
        setTicket("");
        setSaveDir(null);
        setPreview(null);
        setPreviewError("");
        setTicketInfo(null);
        setProgress(null);
        setSpeed(null);
        speedStart.current = null;
        setSavedName(null);
        setFromCache(false);
        setResumable(false);
        setConfirmingOpen(false);
        setLaunchError("");
        setStatus("idle");
        setMessage("Download cancelled.");
      } else if (text === "Download paused.") {
        // Pause: back to idle with a neutral note, keeping the ticket +
        // folder so Resume continues from the surviving cache.
        setStopping(null);
        setStatus("idle");
        setResumable(true);
        setMessage("Download paused. Resume to continue.");
      } else {
        setStopping(null);
        setStatus("error");
        setMessage(text);
      }
    }
  }

  async function stopWith(action: "pause" | "cancel") {
    // Pause keeps the ticket's cache for Resume; cancel deletes it.
    setStopping(action);
    try {
      if (action === "pause") {
        await pauseReceive();
      } else {
        await cancelReceive();
      }
    } catch (err) {
      setStopping(null);
      setStatus("error");
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  const canStart =
    status !== "receiving" && normalizeTicket(ticket) !== "" && saveDir !== null;
  // Time remaining from the same overall-average speed, so it updates at
  // the same throttled 2 Hz and never flickers. Hidden until a speed
  // exists, and once the bar is full (finished shows the Saved banner).
  const eta =
    speed !== null && speed > 0 && progress && progress.done < progress.total
      ? formatEta((progress.total - progress.done) / speed)
      : null;

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
            if (status !== "receiving") resetForNewTicket();
          }}
          placeholder="Paste ticket here…"
          spellCheck={false}
          autoComplete="off"
        />
        <button
          className="btn-ghost btn-small qr-upload"
          onClick={handleUploadQr}
          disabled={status === "receiving"}
        >
          Upload a QR code instead
        </button>
        {ticketInfo && status !== "receiving" && (
          <details className="inspector">
            <summary className="inspector-summary">Ticket details</summary>
            <Fingerprint value={ticketInfo.fingerprint} />
            <div className="inspector-row">
              <span className="inspector-label">Type</span>
              <span>
                {ticketInfo.kind === "id"
                  ? "Short — addresses resolve over discovery."
                  : "Full — carries its own addresses."}
              </span>
            </div>
            {ticketInfo.relays.map((relay) => (
              <div className="inspector-row" key={relay}>
                <span className="inspector-label">Relay</span>
                <code className="inspector-value">{relay}</code>
              </div>
            ))}
            {ticketInfo.addrs.map((addr) => (
              <div className="inspector-row" key={addr}>
                <span className="inspector-label">Address</span>
                <code className="inspector-value">{addr}</code>
              </div>
            ))}
            <div className="inspector-note">
              Compare the fingerprint with the sender's screen before downloading —
              only trust it if the ticket came through a channel you trust.
              Never paste tickets into decoder websites; inspect them here.
            </div>
          </details>
        )}
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
              disabled={stopping !== null}
            >
              {stopping === "pause" ? "Pausing…" : "Pause"}
            </button>
            <button
              className="btn-danger-outline btn-large"
              onClick={() => void stopWith("cancel")}
              disabled={stopping !== null}
            >
              {stopping === "cancel" ? "Cancelling…" : "Cancel"}
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
              <div className="speed-line">
                {formatSpeed(speed)}
                {eta ? ` · ${eta}` : ""}
              </div>
            )}
          </div>
        )}
        {status === "finished" && (
          <>
            <div className="banner banner-ok" role="status">
              Saved{savedName ? ` as ${savedName}` : ""}
              {fromCache ? " — already downloaded, restored from cache." : "."}
            </div>
            <div className="launch-row">
              <button className="btn-ghost" onClick={handleOpen}>
                Open file
              </button>
              <button className="btn-ghost" onClick={handleReveal}>
                Show in folder
              </button>
            </div>
            {confirmingOpen && savedName && (
              <div className="banner banner-warn" role="alertdialog" aria-label="Confirm opening executable file">
                <AlertIcon size={16} />
                <div>
                  <div>
                    {fileNameOf(savedName)} can run programs on this computer.
                    Open it only if you trust the sender.
                  </div>
                  <div className="launch-confirm-actions">
                    <button className="btn-primary btn-small" onClick={handleOpen}>
                      Open anyway
                    </button>
                    <button
                      className="btn-ghost btn-small"
                      onClick={() => setConfirmingOpen(false)}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              </div>
            )}
            {launchError && (
              <div className="banner banner-error" role="alert">
                <AlertIcon size={16} />
                {launchError}
              </div>
            )}
          </>
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
