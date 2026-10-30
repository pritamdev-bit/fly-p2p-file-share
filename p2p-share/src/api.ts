import { Channel, invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type { UnlistenFn } from "@tauri-apps/api/event";
import { downloadDir } from "@tauri-apps/api/path";
import { writeText } from "@tauri-apps/plugin-clipboard-manager";
import { open } from "@tauri-apps/plugin-dialog";

/** Brand shown in the UI. */
export const APP_NAME = "fly";

/** Mirrors the Rust `TransferEvent` (adjacently tagged serde enum). */
export type TransferEvent =
  | { event: "Progress"; data: { done: number; total: number } }
  | { event: "Sending"; data: { receivers: number } }
  | { event: "Finished" }
  | { event: "Delivered" }
  | { event: "TicketUpdated"; data: { ticket: string } };

/** Returned by `send_file`: what the UI needs once a share starts. */
export interface ShareInfo {
  ticket: string;
  fingerprint: string;
}

/** `id`: short, needs working discovery. `full`: longer, works anywhere. */
export type TicketType = "id" | "full";

/** Returned by `preview_ticket`: the real name + size inside the ticket. */
export interface FilePreview {
  name: string;
  size: number;
}

/** Returned by `inspect_ticket`: offline parse, never connects. */
export interface TicketInfo {
  fingerprint: string;
  hash: string;
  kind: "id" | "full";
  relays: string[];
  addrs: string[];
}

/** Returned by `get_diagnostics`. Never contains tickets or paths. */
export interface DiagnosticsInfo {
  nodeId: string;
  relayReachable: boolean;
  natType: string;
}

/** True when running inside the Tauri webview (backend bridge present). */
function isTauri(): boolean {
  return (
    typeof window !== "undefined" &&
    ("__TAURI_INTERNALS__" in window || "__TAURI__" in window)
  );
}

/**
 * Fail fast with a friendly message when the UI is opened in a plain
 * browser, where the Tauri bridge (and every backend call) is missing.
 */
function requireTauri(): void {
  if (!isTauri()) {
    throw new Error(
      "Desktop only: run the app with `npm run tauri dev` (or open the built .exe) instead of a plain browser.",
    );
  }
}

function firstPath(selection: string | string[] | null): string | null {
  if (typeof selection === "string") return selection;
  if (Array.isArray(selection) && selection.length > 0) return selection[0];
  return null;
}

export async function pickFile(): Promise<string | null> {
  requireTauri();
  return firstPath(await open({ multiple: false, title: "Choose a file to send" }));
}

export async function pickSaveDir(): Promise<string | null> {
  requireTauri();
  return firstPath(await open({ directory: true, title: "Choose a save folder" }));
}

const SAVE_DIR_KEY = "fly.saveDir";

/** Last save folder, or the OS downloads folder on first run. Persists
    across restarts (webview storage); null when neither resolves. */
export async function loadSaveDir(): Promise<string | null> {
  try {
    const stored = window.localStorage.getItem(SAVE_DIR_KEY);
    if (stored) return stored;
  } catch {
    // Storage unavailable (shouldn't happen in-app) — fall through.
  }
  if (!isTauri()) return null;
  try {
    return await downloadDir();
  } catch {
    return null;
  }
}

export function storeSaveDir(dir: string): void {
  try {
    window.localStorage.setItem(SAVE_DIR_KEY, dir);
  } catch {
    // Best effort — the folder still works for this session.
  }
}

export async function pickQrImage(): Promise<string | null> {
  requireTauri();
  return firstPath(
    await open({
      multiple: false,
      title: "Choose a QR code image",
      filters: [{ name: "Images", extensions: ["png", "jpg", "jpeg", "gif"] }],
    }),
  );
}

export function decodeQrTicket(path: string): Promise<string> {
  requireTauri();
  return invoke<string>("decode_qr_ticket", { path });
}

export function sendFile(
  path: string,
  ticketType: TicketType,
  onMessage: (event: TransferEvent) => void,
): Promise<ShareInfo> {
  requireTauri();
  const onEvent = new Channel<TransferEvent>();
  onEvent.onmessage = onMessage;
  return invoke<ShareInfo>("send_file", { path, ticketType, onEvent });
}

export function previewTicket(ticket: string): Promise<FilePreview> {
  requireTauri();
  return invoke<FilePreview>("preview_ticket", { ticket });
}

export function inspectTicket(ticket: string): Promise<TicketInfo> {
  requireTauri();
  return invoke<TicketInfo>("inspect_ticket", { ticket });
}

export function openReceived(): Promise<string> {
  requireTauri();
  return invoke<string>("open_received");
}

export function revealReceived(): Promise<string> {
  requireTauri();
  return invoke<string>("reveal_received");
}

/**
 * Bridge page base for share links (the ticket rides in the URL fragment,
 * so it never reaches the server). Single place to change when the domain
 * moves off the temporary subdomain.
 */
export const SHARE_LINK_BASE = "https://fly.pritam.codes/r";

/** Linkifiable share link for a ticket (what the sender's QR encodes). */
export function buildShareLink(ticket: string): string {
  return `${SHARE_LINK_BASE}#${ticket}`;
}

/** Drain launch-time deeplink tickets (cold-start links). */
export async function takePendingTickets(): Promise<string[]> {
  if (!isTauri()) return [];
  return invoke<string[]>("take_pending_tickets");
}

/** Listen for deeplink tickets arriving while the app runs. */
export async function onFlyOpenUrl(
  handler: (ticket: string) => void,
): Promise<UnlistenFn> {
  if (!isTauri()) return () => {};
  return listen<string>("fly-open-url", (event) => handler(event.payload));
}

/** Extensions that can run code when opened (checked before Open file). */
const EXECUTABLE_EXTENSIONS = new Set([
  "exe", "msi", "bat", "cmd", "com", "scr", "ps1", "vbs", "reg", "jar",
  "sh", "bash", "run", "appimage", "desktop",
]);

export function isExecutableName(name: string): boolean {
  const dot = name.lastIndexOf(".");
  if (dot < 0) return false;
  return EXECUTABLE_EXTENSIONS.has(name.slice(dot + 1).toLowerCase());
}

/** Tickets are one base32 word; chat apps may wrap them across lines. */
export function normalizeTicket(ticket: string): string {
  return ticket.replace(/\s+/g, "");
}

/** Returned by `receive_file`: the saved name plus cache honesty. */
export interface ReceiveInfo {
  name: string;
  fromCache: boolean;
}

export function receiveFile(
  ticket: string,
  saveDir: string,
  onMessage: (event: TransferEvent) => void,
): Promise<ReceiveInfo> {
  requireTauri();
  const onEvent = new Channel<TransferEvent>();
  onEvent.onmessage = onMessage;
  return invoke<ReceiveInfo>("receive_file", { ticket, saveDir, onEvent });
}

export function stopSharing(): Promise<string> {
  requireTauri();
  return invoke<string>("stop_sharing");
}

export function pauseReceive(): Promise<string> {
  requireTauri();
  return invoke<string>("pause_receive");
}

export function cancelReceive(): Promise<string> {
  requireTauri();
  return invoke<string>("cancel_receive");
}

export function formatSpeed(bytesPerSecond: number): string {
  if (!Number.isFinite(bytesPerSecond) || bytesPerSecond < 0) return "—";
  return `${formatBytes(Math.round(bytesPerSecond))}/s`;
}

export function formatEta(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  const total = Math.round(seconds);
  if (total < 60) return `${total}s left`;
  const minutes = Math.floor(total / 60);
  if (minutes < 60) {
    const rest = total % 60;
    return rest === 0 ? `${minutes}m left` : `${minutes}m ${rest}s left`;
  }
  const hours = Math.floor(minutes / 60);
  const restMin = minutes % 60;
  return restMin === 0 ? `${hours}h left` : `${hours}h ${restMin}m left`;
}

export function fetchDiagnostics(): Promise<DiagnosticsInfo> {
  requireTauri();
  return invoke<DiagnosticsInfo>("get_diagnostics");
}

export function copyText(text: string): Promise<void> {
  requireTauri();
  return writeText(text);
}

/** OS file-drop payload (structural subset; position ignored). */
export interface FileDropPayload {
  paths: string[];
}

/**
 * Passive file-drop listener. Outside Tauri (plain browser) it resolves
 * to a no-op: missing drag-drop there is normal, not an error.
 */
export async function onFileDrop(
  onDrop: (paths: string[]) => void,
  onHover?: (hovering: boolean) => void,
): Promise<UnlistenFn> {
  if (!isTauri()) return () => {};
  const unlistens = await Promise.all([
    listen<FileDropPayload>("tauri://drag-drop", (event) => {
      onHover?.(false);
      if (Array.isArray(event.payload.paths)) onDrop(event.payload.paths);
    }),
    listen("tauri://drag-over", () => onHover?.(true)),
    listen("tauri://drag-leave", () => onHover?.(false)),
  ]);
  return () => unlistens.forEach((fn) => fn());
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const rounded = value >= 100 || unit === 0 ? Math.round(value) : value.toFixed(1);
  return `${rounded} ${units[unit]}`;
}

export function fileNameOf(path: string): string {
  const parts = path.split(/[/\\]/);
  return parts[parts.length - 1] || path;
}
