mod diagnostics;
mod download;
mod events;
mod export;
mod share;
mod transfer;

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
