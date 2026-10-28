mod diagnostics;
mod download;
mod events;
mod export;
mod prune;
mod share;
mod transfer;

/// Live-transfer tests serialize on this lock: each spins up endpoints
/// against shared public relays + loopback, and running them in
/// parallel flakes (stalled fetches, wedged shutdowns). One lock for
/// every live-transfer test, whichever module it lives in.
#[cfg(test)]
pub(crate) static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .manage(transfer::AppState::default())
        .setup(|_app| {
            // Real OS blur (Windows acrylic) so the desktop stays visible
            // but unreadable behind the transparent window. Linux has no
            // equivalent here and falls back to the CSS frost layer.
            #[cfg(target_os = "windows")]
            {
                use tauri::{
                    Manager,
                    window::{Color, Effect, EffectState, EffectsBuilder},
                };
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
            transfer::send_file,
            transfer::receive_file,
            transfer::preview_ticket,
            transfer::pause_receive,
            transfer::cancel_receive,
            transfer::stop_sharing,
            diagnostics::get_diagnostics,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
