import { useEffect, useMemo, useRef, useState } from "react";
import qrcode from "qrcode-generator";
import {
  buildShareLink,
  copyText,
  fileNameOf,
  onFileDrop,
  pickFile,
  sendFile,
  stopSharing,
} from "../api";
import type { ShareInfo, TicketType, TransferEvent } from "../api";
import Fingerprint from "./Fingerprint";
import { AlertIcon, CheckIcon, CopyIcon, FileIcon, LockIcon } from "./icons";

type Status = "idle" | "preparing" | "sharing" | "delivered" | "stopped" | "error";

function truncateMiddle(value: string, head = 14, tail = 10): string {
  if (value.length <= head + tail + 1) return value;
  return `${value.slice(0, head)}…${value.slice(-tail)}`;
}

export default function SendPanel({
  active,
  onBusyChange,
}: {
  active: boolean;
  onBusyChange: (busy: boolean) => void;
}) {
  const [filePath, setFilePath] = useState<string | null>(null);
  const [ticketType, setTicketType] = useState<TicketType>("id");
  const [share, setShare] = useState<ShareInfo | null>(null);
  const [receivers, setReceivers] = useState(0);
  const [status, setStatus] = useState<Status>("idle");
  const [message, setMessage] = useState("");
  const [copied, setCopied] = useState(false);
  const [linkCopied, setLinkCopied] = useState(false);
  // Set when the backend re-mints the ticket after a network change;
  // anything the user copied or sent before that is stale.
  const [ticketNote, setTicketNote] = useState("");
  const [dragging, setDragging] = useState(false);
  // QR encodes the share link (regenerates with the ticket).
  const [showQr, setShowQr] = useState(false);
  const qrSvg = useMemo(() => {
    if (!share) return "";
    const qr = qrcode(0, "M");
    qr.addData(buildShareLink(share.ticket));
    qr.make();
    return qr.createSvgTag(6, 0);
  }, [share]);
  // The drop listener registers once; refs keep its checks current.
  const statusRef = useRef(status);
  statusRef.current = status;
  const ticketTypeRef = useRef(ticketType);
  ticketTypeRef.current = ticketType;
  const activeRef = useRef(active);
  activeRef.current = active;

  // Report live shares so the app can confirm before quitting.
  useEffect(() => {
    onBusyChange(status === "sharing" || status === "preparing");
  }, [status, onBusyChange]);

  function handleEvent(event: TransferEvent) {
    // The sender only ever hears Sending + Delivered + TicketUpdated:
    // pulls are receiver-driven, so no sender-side progress exists.
    if (event.event === "Sending") {
      setReceivers(event.data.receivers);
    } else if (event.event === "Delivered") {
      // Auto-clear: the backend already stopped the share, so the
      // ticket is dead — drop it from the display (and the QR).
      setShare(null);
      setShowQr(false);
      setCopied(false);
      setLinkCopied(false);
      setTicketNote("");
      setStatus("delivered");
    } else if (event.event === "TicketUpdated") {
      // The network changed and the backend re-minted the ticket.
      // Anything copied or sent before is stale — say so plainly.
      setShare((prev) => (prev ? { ...prev, ticket: event.data.ticket } : prev));
      setCopied(false);
      setTicketNote("Network changed — ticket refreshed. Copy and send the new ticket.");
    }
  }

  async function startShare(path: string, kind: TicketType) {
    setReceivers(0);
    setMessage("");
    setTicketNote("");
    setCopied(false);
    setLinkCopied(false);
    setShowQr(false);
    setStatus("preparing");
    try {
      const info = await sendFile(path, kind, handleEvent);
      setShare(info);
      setStatus("sharing");
    } catch (err) {
      setStatus("error");
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  async function sharePath(path: string) {
    // Drops land on the hidden panel too — ignore them unless visible.
    if (!activeRef.current) return;
    // Delivered no longer blocks: the backend auto-stopped the share,
    // so the slot is free and a new share can start immediately.
    if (
      statusRef.current === "sharing" ||
      statusRef.current === "preparing"
    ) {
      return;
    }
    setShare(null);
    setFilePath(path);
    await startShare(path, ticketTypeRef.current);
  }

  async function handleChoose() {
    try {
      const picked = await pickFile();
      if (!picked) return;
      await sharePath(picked);
    } catch (err) {
      setStatus("error");
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    onFileDrop(
      (paths) => {
        if (paths[0]) void sharePath(paths[0]);
      },
      setDragging,
    )
      .then((fn) => {
        unlisten = fn;
      })
      .catch(() => {});
    return () => unlisten?.();
    // sharePath reads statusRef + setState, so one registration is enough.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function handleStop() {
    try {
      await stopSharing();
    } catch (err) {
      setStatus("error");
      setMessage(err instanceof Error ? err.message : String(err));
      return;
    }
    // Auto-clear: the ticket is dead — drop it from the display.
    setShare(null);
    setShowQr(false);
    setCopied(false);
    setLinkCopied(false);
    setTicketNote("");
    setStatus("stopped");
    setMessage("Sharing stopped.");
  }

  async function handleCopy() {
    if (!share) return;
    try {
      await copyText(share.ticket);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleCopyLink() {
    if (!share) return;
    try {
      await copyText(buildShareLink(share.ticket));
      setLinkCopied(true);
      window.setTimeout(() => setLinkCopied(false), 1500);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : String(err));
    }
  }

  const busy = status === "sharing" || status === "preparing";

  return (
    <div className="view">
      <h1 className="view-title">Send a file</h1>
      <p className="view-subtitle">
        Share directly with another device. No cloud storage.
      </p>

      <div className="card file-card">
        <span className="file-icon">
          <FileIcon size={26} />
        </span>
        <div className="file-meta">
          <div className="file-name">{filePath ? fileNameOf(filePath) : "No file chosen"}</div>
          <div className="file-sub">
            {filePath ? (
              <span className="pill pill-green">
                <CheckIcon size={14} /> Selected
              </span>
            ) : (
              "Pick a file — or drop one anywhere on this window."
            )}
          </div>
        </div>
        <button className="btn-primary" onClick={handleChoose} disabled={busy}>
          {status === "preparing"
            ? "Preparing…"
            : filePath
              ? "Choose another file"
              : "Choose a file"}
        </button>
      </div>

      {!busy && (
        <div className="ticket-type-row">
          <span className="ticket-type-label">Ticket type</span>
          <div className="segmented" role="group" aria-label="Ticket type">
            <button
              className={ticketType === "id" ? "segmented-active" : ""}
              onClick={() => setTicketType("id")}
              title="Short ticket. Needs working discovery on the receiver's network."
            >
              Short
            </button>
            <button
              className={ticketType === "full" ? "segmented-active" : ""}
              onClick={() => setTicketType("full")}
              title="Longer ticket. Use this if the short one can't connect."
            >
              Full
            </button>
          </div>
          <span className="ticket-type-hint">
            {ticketType === "id"
              ? "Short ticket — try the full one if it can't connect."
              : "Full ticket — works even where discovery is filtered."}
          </span>
        </div>
      )}

      {status === "preparing" && (
        <div className="banner banner-info" role="status">
          Preparing the file… (hashing, one moment)
        </div>
      )}

      {(share || status === "delivered" || status === "stopped") && (
        <div className="share-grid">
          {share && (
            <div className="card">
              <h2 className="card-title">Share this ticket</h2>
              <p className="card-sub">Only share it with the person receiving this file.</p>
              <div className="ticket-row">
                <code className="ticket-code" title={share.ticket}>
                  {truncateMiddle(share.ticket)}
                </code>
                <button className="btn-ghost" onClick={handleCopy}>
                  {copied ? <CheckIcon size={16} /> : <CopyIcon size={16} />}
                  {copied ? "Copied" : "Copy"}
                </button>
              </div>
              <div className="link-row">
                <code className="ticket-code link-code" title={buildShareLink(share.ticket)}>
                  {truncateMiddle(buildShareLink(share.ticket))}
                </code>
                <button className="btn-ghost btn-small" onClick={handleCopyLink}>
                  {linkCopied ? <CheckIcon size={16} /> : <CopyIcon size={16} />}
                  {linkCopied ? "Copied" : "Copy share link"}
                </button>
              </div>
              <button
                className="btn-ghost btn-small qr-toggle"
                onClick={() => setShowQr((visible) => !visible)}
                aria-expanded={showQr}
              >
                {showQr ? "Hide QR code" : "Show QR code"}
              </button>
              {showQr && qrSvg && (
                <div className="qr-wrap">
                  <div
                    className="qr-code"
                    role="img"
                    aria-label="QR code for the receive link"
                    dangerouslySetInnerHTML={{ __html: qrSvg }}
                  />
                  <div className="qr-hint">
                    Scan with any phone or QR reader to open the receive link.
                  </div>
                </div>
              )}
              {ticketNote && (
                <div className="banner banner-info" role="status">
                  {ticketNote}
                </div>
              )}
              <Fingerprint value={share.fingerprint} />
              <div className="banner banner-warn">
                <AlertIcon size={16} />
                Anyone with this ticket can download this file.
              </div>
              <button
                className="btn-danger-outline btn-block"
                onClick={handleStop}
              >
                Stop sharing
              </button>
            </div>
          )}

          <div className="card status-card">
            <h2 className="status-heading">
              {status === "sharing" && (receivers > 0 ? "Sending…" : "Waiting for receiver")}
              {status === "delivered" && "Download complete"}
              {status === "stopped" && "Sharing stopped"}
              {status === "error" && "Something went wrong"}
              {status === "idle" && "Ready"}
            </h2>
            <p className="card-sub">
              {status === "sharing" &&
                (receivers > 0
                  ? `${receivers} receiver connected — keep this app open.`
                  : "Send the ticket to begin the transfer.")}
              {status === "delivered" &&
                "The receiver got the file. Share another whenever you're ready."}
              {status === "stopped" && message}
            </p>
            {status === "delivered" && (
              <div className="banner banner-ok deliver-banner" role="status">
                <span className="deliver-pop" aria-hidden="true">
                  <CheckIcon size={20} />
                </span>
                Delivered — sharing stopped automatically.
              </div>
            )}
            <div className="note-row">
              <LockIcon size={16} />
              <div>
                <div className="note-title">Encrypted connection</div>
                <div className="note-sub">
                  Direct connection when possible, relay used if needed.
                </div>
              </div>
            </div>
            {status === "sharing" && (
              <div className="banner banner-info">
                This app must stay open while the file is being downloaded.
              </div>
            )}
          </div>
        </div>
      )}

      {status === "error" && message && (
        <div className="banner banner-error" role="alert">
          <AlertIcon size={16} />
          {message}
        </div>
      )}

      {dragging && (
        <div className="drop-overlay" aria-hidden="true">
          <div className="drop-overlay-inner">
            <FileIcon size={30} />
            <div className="drop-overlay-title">Drop to send this file</div>
            <div className="drop-overlay-sub">One file at a time in v1.</div>
          </div>
        </div>
      )}
    </div>
  );
}
