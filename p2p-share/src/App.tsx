import { useEffect, useRef, useState } from "react";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { APP_NAME, onFlyOpenUrl, takePendingTickets } from "./api";
import Diagnostics from "./components/Diagnostics";
import ReceivePanel from "./components/ReceivePanel";
import SendPanel from "./components/SendPanel";
import { ArrowDownIcon, ArrowUpIcon, LogoMark } from "./components/icons";
import "./App.css";

type View = "send" | "receive";

/** A ticket pushed by a fly:// deeplink. The nonce re-triggers the
    ReceivePanel effect even when the same link arrives twice. */
export interface IncomingTicket {
  ticket: string;
  nonce: number;
}

export default function App() {
  const [view, setView] = useState<View>("send");
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(false);
  const [incomingTicket, setIncomingTicket] = useState<IncomingTicket | null>(null);
  // What is busy right now (panels report in). The ref feeds the close
  // handler (no stale closures); the snapshot drives the confirm dialog.
  const busyRef = useRef({ send: false, receive: false });
  const quittingRef = useRef(false);
  const [quitConfirm, setQuitConfirm] = useState<{
    send: boolean;
    receive: boolean;
  } | null>(null);

  // Deeplinks open straight into Receive with the ticket prefilled:
  // launch-time links are drained once, runtime links arrive as events.
  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let cancelled = false;
    const deliver = (ticket: string) => {
      if (cancelled) return;
      setView("receive");
      setIncomingTicket((prev) => ({ ticket, nonce: (prev?.nonce ?? 0) + 1 }));
    };
    takePendingTickets()
      .then((tickets) => tickets.forEach(deliver))
      .catch(() => {});
    onFlyOpenUrl(deliver)
      .then((fn) => {
        unlisten = fn;
      })
      .catch(() => {});
    return () => {
      cancelled = true;
      unlisten?.();
    };
  }, []);

  // Catch window-close clicks while a transfer or share is live: stop
  // the close and ask instead of dropping the transfer silently.
  // (The backend defers every close to this listener's destroy() call,
  // so core:window:allow-destroy must stay in the capabilities.)
  useEffect(() => {
    let unlisten: (() => void) | undefined;
    getCurrentWindow()
      .onCloseRequested((event) => {
        // Confirmed quits must not be re-intercepted: destroy() raises
        // close-requested again while the transfer is still winding down.
        if (quittingRef.current) return;
        const busy = busyRef.current;
        if (busy.send || busy.receive) {
          event.preventDefault();
          setQuitConfirm({ ...busy });
        }
      })
      .then((fn) => {
        unlisten = fn;
      })
      .catch(() => {});
    return () => unlisten?.();
  }, []);

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <LogoMark />
          <span className="brand-name">{APP_NAME}</span>
        </div>
        <nav className="nav" aria-label="Main">
          <button
            className={view === "send" ? "nav-item active" : "nav-item"}
            onClick={() => setView("send")}
          >
            <ArrowUpIcon size={17} />
            Send
          </button>
          <button
            className={view === "receive" ? "nav-item active" : "nav-item"}
            onClick={() => setView("receive")}
          >
            <ArrowDownIcon size={17} />
            Receive
          </button>
        </nav>
        <div className="sidebar-footer">
          <div className="status-line">
            <span className="status-dot" aria-hidden="true" />
            <div>
              <div className="status-title">Ready</div>
              <div className="status-sub">Direct device-to-device</div>
            </div>
          </div>
          <button
            className="btn-ghost btn-small btn-block"
            onClick={() => setDiagnosticsOpen(true)}
          >
            Diagnostics
          </button>
        </div>
      </aside>
      <main className="content">
        {/* Both panels stay mounted so tab switches never lose state
            (ticket, progress, in-flight transfers). The inactive one is
            hidden with CSS; each panel gates tab-specific listeners on
            its `active` prop. */}
        <div className={view === "send" ? "" : "view-hidden"}>
          <SendPanel
            active={view === "send"}
            onBusyChange={(busy) => {
              busyRef.current.send = busy;
            }}
          />
        </div>
        <div className={view === "receive" ? "" : "view-hidden"}>
          <ReceivePanel
            active={view === "receive"}
            incomingTicket={incomingTicket}
            onBusyChange={(busy) => {
              busyRef.current.receive = busy;
            }}
          />
        </div>
      </main>
      <Diagnostics
        open={diagnosticsOpen}
        onClose={() => setDiagnosticsOpen(false)}
      />
      {quitConfirm && (
        <div className="modal-overlay" onClick={() => setQuitConfirm(null)}>
          <div
            className="card modal-card"
            role="alertdialog"
            aria-modal="true"
            aria-label="Confirm quit"
            onClick={(e) => e.stopPropagation()}
          >
            <h2 className="card-title">Quit fly?</h2>
            <p className="card-sub">
              {quitConfirm.receive && quitConfirm.send
                ? "A file is downloading and a file is being shared. Quitting stops both."
                : quitConfirm.receive
                  ? "A file is still downloading. Quitting stops it (progress stays saved for Resume)."
                  : "A file is still being shared. Quitting stops the share."}
            </p>
            <div className="quit-actions">
              <button
                className="btn-danger-outline"
                onClick={() => {
                  quittingRef.current = true;
                  void getCurrentWindow().destroy();
                }}
              >
                Quit anyway
              </button>
              <button className="btn-ghost" onClick={() => setQuitConfirm(null)}>
                Stay
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
