//! Best-effort pruning of stale transfer workspaces.
//!
//! Cancel deletes its own ticket cache, and Stop sharing removes the
//! sender temp dir — but crashes, kills, and pause-forever runs leave
//! orphans behind. Anything older than [`CACHE_TTL`] is fair game.

use std::{
    path::Path,
    time::{Duration, SystemTime},
};

pub const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Remove `prefix-*` entries directly under `root` older than `max_age`.
/// Only true directories are removed; files and symlinks are never
/// touched. Failures are logged, never propagated — pruning must not
/// fail a transfer.
pub fn prune_old_dirs(root: &Path, prefix: &str, max_age: Duration) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with(prefix) {
            continue;
        }
        // file_type() never follows symlinks: only true dirs qualify.
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .map(|age| age > max_age)
            .unwrap_or(false);
        if stale {
            // Active dirs stay young (writes refresh mtime), so this
            // cannot catch a live transfer.
            if let Err(error) = std::fs::remove_dir_all(entry.path()) {
                eprintln!("fly: pruning {} failed: {error}", entry.path().display());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, time::UNIX_EPOCH};

    fn unique_root(case: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("fly-prune-test-{}-{}-{nanos}", std::process::id(), case))
    }

    #[test]
    fn stale_prefix_dirs_go_everything_else_stays() {
        let root = unique_root("stale");
        let stale = root.join("ticket-old");
        let fresh = root.join("ticket-fresh");
        let other = root.join("other-old");
        let file = root.join("ticket-not-a-dir");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::create_dir_all(&fresh).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(&file, b"x").unwrap();

        // Zero TTL: every existing dir is older than that.
        prune_old_dirs(&root, "ticket-", Duration::ZERO);
        assert!(!stale.exists(), "stale ticket dir should be pruned");
        assert!(!fresh.exists(), "fresh is also older than a zero TTL");
        assert!(other.exists(), "non-matching prefix must survive");
        assert!(file.exists(), "files must never be pruned");

        // Normal TTL: a fresh dir survives.
        std::fs::create_dir_all(&fresh).unwrap();
        prune_old_dirs(&root, "ticket-", CACHE_TTL);
        assert!(fresh.exists(), "fresh ticket dir must survive the real TTL");

        std::fs::remove_dir_all(&root).ok();
    }
}
