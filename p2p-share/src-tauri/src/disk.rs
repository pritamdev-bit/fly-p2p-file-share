//! Free-space preflight for downloads: refuse before transferring a
//! byte when the save folder or the ticket cache cannot hold the file.
//! Written fresh against libc (unix) + windows-sys, like export's
//! no-replace moves — no new dependency for one syscall each.

use anyhow::{Context, Result, anyhow};
use std::path::Path;

/// Nearest existing ancestor of `path` (itself when it exists). Free
/// space is a property of the volume, so measuring the ancestor answers
/// for a not-yet-created save folder too.
fn existing_ancestor(path: &Path) -> &Path {
    let mut current = path;
    loop {
        if std::fs::symlink_metadata(current).is_ok() {
            return current;
        }
        match current.parent() {
            Some(parent) => current = parent,
            None => return current,
        }
    }
}

/// Free bytes available to the caller on `path`'s volume.
#[cfg(windows)]
pub fn available_space(path: &Path) -> Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let wide: Vec<u16> = existing_ancestor(path)
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut free: u64 = 0;
    // SAFETY: NUL-terminated path; the out-pointer is valid, the rest null.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        Err(std::io::Error::last_os_error()).context("cannot read free disk space")
    } else {
        Ok(free)
    }
}

/// Free bytes available to the caller on `path`'s volume.
#[cfg(unix)]
pub fn available_space(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;

    let c_path = std::ffi::CString::new(existing_ancestor(path).as_os_str().as_bytes()).map_err(
        |_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path contains an interior NUL byte",
            )
        },
    );
    let c_path = c_path.context("cannot read free disk space")?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: valid path pointer and out-struct.
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } != 0 {
        return Err(std::io::Error::last_os_error()).context("cannot read free disk space");
    }
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(not(any(unix, windows)))]
pub fn available_space(_path: &Path) -> Result<u64> {
    // No release target takes this path; never fail a transfer for an
    // unmeasurable volume.
    Ok(u64::MAX)
}

/// Bytes that must be free for a `total`-byte file with `cached` bytes
/// already in the ticket cache: (save folder, cache dir). The save
/// folder briefly holds the staged copy plus the final file during the
/// atomic commit; the cache needs the remaining download plus the ~6%
/// hash-tree outboard.
pub fn space_needs(total: u64, cached: u64) -> (u64, u64) {
    let cached = cached.min(total);
    let save = total.saturating_mul(2);
    let cache = total.saturating_sub(cached).saturating_add(total / 16);
    (save, cache)
}

pub fn human_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * KB;
    const GB: f64 = 1024.0 * MB;
    let value = bytes as f64;
    if value >= GB {
        format!("{:.1} GB", value / GB)
    } else if value >= MB {
        format!("{:.1} MB", value / MB)
    } else if value >= KB {
        format!("{:.1} KB", value / KB)
    } else {
        format!("{bytes} B")
    }
}

/// Refuse when `free` bytes cannot cover `need` in `dir`.
pub fn ensure_space(free: u64, need: u64, dir: &Path, what: &str) -> Result<()> {
    if free >= need {
        Ok(())
    } else {
        Err(anyhow!(
            "Not enough free space {} ({}): need about {}, only {} available.",
            what,
            dir.display(),
            human_bytes(need),
            human_bytes(free)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_cover_staged_copy_and_outboard() {
        // Fresh 1000-byte file: save holds staged + final, cache the
        // download plus ~6% outboard.
        assert_eq!(space_needs(1000, 0), (2000, 1062));
        // Resume only downloads what is missing; the save need is unchanged.
        assert_eq!(space_needs(1000, 500), (2000, 562));
        assert_eq!(space_needs(1000, 1000), (2000, 62));
        // Clamped cache, empty file, hostile sizes: never panic, never wrap.
        assert_eq!(space_needs(1000, 9999), (2000, 62));
        assert_eq!(space_needs(0, 0), (0, 0));
        assert_eq!(space_needs(u64::MAX, 0), (u64::MAX, u64::MAX));
    }

    #[test]
    fn bytes_read_as_kb_mb_gb() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KB");
        assert_eq!(human_bytes(1536), "1.5 KB");
        assert_eq!(human_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn shortfall_names_dir_and_amounts() {
        let dir = Path::new("/save/here");
        assert!(ensure_space(2000, 2000, dir, "in the save folder").is_ok());
        let error = ensure_space(1000, 2000, dir, "in the save folder").unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("Not enough free space")
                && message.contains("/save/here")
                && message.contains("2.0 KB")
                && message.contains("1000 B"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn measures_real_volumes_including_missing_dirs() {
        let live = available_space(&std::env::temp_dir()).expect("temp dir is measurable");
        assert!(live > 0, "temp volume must report free space");
        // A not-yet-created save folder measures through its parent.
        let missing = std::env::temp_dir().join("fly-no-such-dir-271828/nested");
        assert!(!missing.exists());
        let via_parent = available_space(&missing).expect("missing dirs measure via parent");
        assert!(via_parent > 0);
    }
}
