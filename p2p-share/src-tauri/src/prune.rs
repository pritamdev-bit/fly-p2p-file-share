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
/// Cap for all ticket caches together: completed transfers keep a full
/// copy for instant re-receive, so time-based pruning alone cannot bound
/// disk use. Oldest-first eviction enforces this.
pub const CACHE_SIZE_CAP: u64 = 2 * 1024 * 1024 * 1024;
/// Dirs younger than this are never evicted for size. A running transfer
/// writes constantly (worst stall without writes is minutes), so an
/// hour of grace can only protect live work — never stale weight.
pub const CACHE_EVICT_GRACE: Duration = Duration::from_secs(60 * 60);

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

/// Total logical bytes and dir count of `prefix-*` dirs under `root`.
/// Symlinks are never followed; unreadable entries count what they can.
pub fn cache_usage(root: &Path, prefix: &str) -> (u64, u32) {
    let mut bytes = 0u64;
    let mut dirs = 0u32;
    let Ok(entries) = std::fs::read_dir(root) else {
        return (0, 0);
    };
    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with(prefix) {
            continue;
        }
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        dirs += 1;
        bytes = bytes.saturating_add(dir_size(&entry.path()));
    }
    (bytes, dirs)
}

/// Recursive logical size of a dir. Never follows symlinks; errors
/// yield a partial sum rather than failing the caller.
fn dir_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                stack.push(entry.path());
            } else if kind.is_file() {
                total = total.saturating_add(entry.metadata().map(|m| m.len()).unwrap_or(0));
            }
        }
    }
    total
}

/// Pick which cache dirs to evict: oldest first by mtime (name breaks
/// ties), skipping anything younger than `min_age`, until the survivors
/// fit `max_bytes`. Pure over (name, bytes, age) so the policy is
/// unit-testable without a filesystem.
fn pick_evictions(
    mut entries: Vec<(String, u64, Duration)>,
    max_bytes: u64,
    min_age: Duration,
) -> Vec<String> {
    entries.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    let mut total: u64 = entries.iter().map(|entry| entry.1).fold(0, u64::saturating_add);
    let mut evict = Vec::new();
    for (name, bytes, age) in &entries {
        if total <= max_bytes {
            break;
        }
        if *age < min_age {
            continue;
        }
        total = total.saturating_sub(*bytes);
        evict.push(name.clone());
    }
    evict
}

/// Enforce the size cap on `prefix-*` dirs under `root`: delete
/// oldest-first until the survivors fit `max_bytes`, never touching
/// dirs younger than `min_age`. Best-effort like TTL pruning — failures
/// are logged, never propagated.
pub fn enforce_cache_cap(root: &Path, prefix: &str, max_bytes: u64, min_age: Duration) {
    let now = SystemTime::now();
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut surveyed = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(prefix) {
            continue;
        }
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let age = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .unwrap_or(Duration::ZERO);
        surveyed.push((name, dir_size(&entry.path()), age));
    }
    for name in pick_evictions(surveyed, max_bytes, min_age) {
        if let Err(error) = std::fs::remove_dir_all(root.join(&name)) {
            eprintln!("fly: cache eviction of {name} failed: {error}");
        }
    }
}

/// Delete every `prefix-*` dir under `root` regardless of age. The
/// caller must hold off live transfers first (manual clear refuses
/// while a download runs). Returns freed bytes and removed dir count.
pub fn clear_caches(root: &Path, prefix: &str) -> (u64, u32) {
    let mut freed = 0u64;
    let mut removed = 0u32;
    let Ok(entries) = std::fs::read_dir(root) else {
        return (0, 0);
    };
    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with(prefix) {
            continue;
        }
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let size = dir_size(&entry.path());
        if std::fs::remove_dir_all(entry.path()).is_ok() {
            freed = freed.saturating_add(size);
            removed += 1;
        } else {
            eprintln!("fly: cache clear of {} failed", entry.path().display());
        }
    }
    (freed, removed)
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

    #[test]
    fn eviction_is_oldest_first_with_grace() {
        let old = Duration::from_secs(7200);
        let young = Duration::from_secs(60);
        let grace = Duration::from_secs(3600);
        // Under the cap: nothing goes, whatever the ages.
        let entries = vec![
            ("a".to_string(), 40, old),
            ("b".to_string(), 40, young),
        ];
        assert!(pick_evictions(entries, 100, grace).is_empty());
        // Over the cap: oldest goes first, young is skipped even when
        // it alone would settle the bill.
        let entries = vec![
            ("young".to_string(), 90, young),
            ("mid".to_string(), 30, old),
            ("oldest".to_string(), 30, old + Duration::from_secs(1)),
        ];
        assert_eq!(
            pick_evictions(entries, 100, grace),
            vec!["oldest".to_string(), "mid".to_string()]
        );
        // Everything young and over the cap: evict nothing rather than
        // a live transfer; the cap bends until the work ages out.
        let entries = vec![("live".to_string(), 500, young)];
        assert!(pick_evictions(entries, 100, grace).is_empty());
    }

    #[test]
    fn cap_and_usage_and_clear_agree_on_disk() {
        let root = unique_root("capusage");
        for name in ["ticket-a", "ticket-b", "other"] {
            let dir = root.join(name);
            std::fs::create_dir_all(dir.join("nested")).unwrap();
            std::fs::write(dir.join("nested/blob"), vec![7u8; 60]).unwrap();
        }
        let (bytes, dirs) = cache_usage(&root, "ticket-");
        assert_eq!((bytes, dirs), (120, 2));
        // Zero grace + tiny cap: ticket-a goes first (created first,
        // and alphabetically first if mtimes tie on coarse filesystems).
        enforce_cache_cap(&root, "ticket-", 100, Duration::ZERO);
        assert!(!root.join("ticket-a").exists());
        assert!(root.join("ticket-b").exists());
        assert!(root.join("other").exists(), "prefix still scopes eviction");
        let (bytes, dirs) = cache_usage(&root, "ticket-");
        assert_eq!((bytes, dirs), (60, 1));
        // Fresh dirs are immune under the real grace even over the cap.
        enforce_cache_cap(&root, "ticket-", 10, CACHE_EVICT_GRACE);
        assert!(root.join("ticket-b").exists());
        // Manual clear takes everything with the prefix and reports it.
        let (freed, removed) = clear_caches(&root, "ticket-");
        assert_eq!((freed, removed), (60, 1));
        assert!(root.join("other").exists(), "prefix still scopes clearing");
        let (bytes, dirs) = cache_usage(&root, "ticket-");
        assert_eq!((bytes, dirs), (0, 0));

        std::fs::remove_dir_all(&root).ok();
    }
}
