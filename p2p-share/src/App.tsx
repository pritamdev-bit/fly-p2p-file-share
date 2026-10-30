import { useEffect, useState } from "react";
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
          <SendPanel active={view === "send"} />
        </div>
        <div className={view === "receive" ? "" : "view-hidden"}>
          <ReceivePanel active={view === "receive"} incomingTicket={incomingTicket} />
        </div>
      </main>
      <Diagnostics
        open={diagnosticsOpen}
        onClose={() => setDiagnosticsOpen(false)}
      />
    </div>
  );
}
