//! Open / reveal the last received file with the OS default handlers.
//!
//! Deliberately not the Tauri opener plugin (the plan keeps that
//! surface closed): two narrow Rust commands that only ever act on the
//! backend-recorded receive path. The UI passes no path, so the webview
//! cannot make the backend open anything else.

use anyhow::{Result, anyhow, bail};
use std::path::{Path, PathBuf};

/// Resolve the recorded receive path, or explain why there is nothing
/// to open. Pure over the slot value so it stays unit-testable without
/// a Tauri context.
pub fn resolve_recorded(slot: &Option<PathBuf>) -> Result<PathBuf> {
    let path = slot
        .clone()
        .ok_or_else(|| anyhow!("Nothing received yet in this session."))?;
    if !path.is_file() {
        bail!("That file is no longer there (moved, renamed, or deleted).");
    }
    Ok(path)
}

/// Open the file with its default application.
pub fn open_path(path: &Path) -> Result<()> {
    open::that(path).map_err(|error| anyhow!("Couldn't open the file ({error})."))
}

/// Show the file in its folder: highlight-select where the OS offers a
/// one-shot primitive (Windows/macOS), otherwise open the folder.
pub fn reveal_path(path: &Path) -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        // Flag and path MUST be separate argv entries: as one arg
        // (`/select,<path>`) the whole thing gets quoted whenever the
        // path contains spaces, Explorer's switch parser then misses
        // the flag and opens Documents instead. Verified empirically.
        std::process::Command::new("explorer")
            .arg("/select,")
            .arg(path)
            .spawn()
            .map_err(|error| anyhow!("Couldn't open the folder ({error})."))?;
    }
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open")
            .arg("-R")
            .arg(path)
            .spawn()
            .map_err(|error| anyhow!("Couldn't open the folder ({error})."))?;
    }
    // Linux has no portable select-in-folder primitive (the DBus
    // FileManager1 call needs a desktop-portal bus name most distros do
    // not run by default), so open the containing folder instead.
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let dir = path
            .parent()
            .ok_or_else(|| anyhow!("Couldn't locate the save folder."))?;
        open::that(dir).map_err(|error| anyhow!("Couldn't open the folder ({error})."))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn nothing_recorded_is_an_error() {
        assert!(resolve_recorded(&None).is_err());
    }

    #[test]
    fn missing_file_is_an_error() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let ghost = std::env::temp_dir().join(format!("fly-ghost-{nanos}"));
        assert!(!ghost.exists());
        let error = format!("{:#}", resolve_recorded(&Some(ghost)).unwrap_err());
        assert!(error.contains("no longer there"), "got: {error}");
    }

    #[test]
    fn recorded_file_resolves() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let file = std::env::temp_dir().join(format!("fly-resolve-{nanos}.bin"));
        std::fs::write(&file, b"x").unwrap();
        assert_eq!(resolve_recorded(&Some(file.clone())).unwrap(), file);
        std::fs::remove_file(&file).ok();
    }
}
