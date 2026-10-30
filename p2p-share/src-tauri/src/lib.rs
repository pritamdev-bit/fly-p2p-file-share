mod deeplink;
mod diagnostics;
mod download;
mod events;
mod export;
mod inspect;
mod launch;
mod prune;
mod share;
mod transfer;

use tauri::Manager;

/// Live-transfer tests serialize on this lock: each spins up endpoints
/// against shared public relays + loopback, and running them in
/// parallel flakes (stalled fetches, wedged shutdowns). One lock for
/// every live-transfer test, whichever module it lives in.
#[cfg(test)]
pub(crate) static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        // Single-instance first (required): a second launch — e.g. a
        // fly:// click while the app runs — forwards its URL to this
        // instance through the deep-link handler, then exits. The
        // callback only brings the window forward.
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .manage(transfer::AppState::default())
        .manage(deeplink::PendingTickets::default())
        .setup(|_app| {
            use tauri_plugin_deep_link::DeepLinkExt;
            // Register the fly:// handler with the OS (covers dev runs
            // and portable exes; installed copies also register through
            // the plugin config in tauri.conf.json). Best effort: a
            // failure here must never stop the app from starting.
            #[cfg(any(target_os = "windows", target_os = "linux"))]
            if let Err(error) = _app.deep_link().register_all() {
                eprintln!("fly: deep-link registration failed: {error}");
            }
            // Runtime links (including second-launch forwards): parse,
            // stash, and emit to the UI.
            let handle = _app.handle().clone();
            _app.deep_link().on_open_url(move |event| {
                deeplink::handle_urls(&handle, event.urls());
            });
            // Cold start: the plugin emitted launch URLs during its own
            // setup, before any listener existed — drain them directly.
            if let Ok(Some(urls)) = _app.deep_link().get_current() {
                deeplink::handle_urls(_app.handle(), urls);
            }
            // Real OS blur (Windows acrylic) so the desktop stays visible
            // but unreadable behind the transparent window. Linux has no
            // equivalent here and falls back to the CSS frost layer.
            #[cfg(target_os = "windows")]
            {
                use tauri::window::{Color, Effect, EffectState, EffectsBuilder};
                let window = _app.get_webview_window("main").unwrap();
                window.set_effects(
                    EffectsBuilder::new()
                        .effect(Effect::Acrylic)
                        .state(EffectState::Active)
                        .color(Color(13, 20, 34, 160))
                        .build(),
                )?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            deeplink::decode_qr_ticket,
            deeplink::take_pending_tickets,
            transfer::send_file,
            transfer::receive_file,
            transfer::preview_ticket,
            transfer::pause_receive,
            transfer::cancel_receive,
            transfer::inspect_ticket,
            transfer::open_received,
            transfer::reveal_received,
            transfer::stop_sharing,
            diagnostics::get_diagnostics,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
