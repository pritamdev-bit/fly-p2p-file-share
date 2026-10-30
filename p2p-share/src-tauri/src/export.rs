//! Validated, atomic export of a downloaded blob to its final path.
//!
//! Pipeline: validate the remote name, preflight the target, export into a
//! staging dir, re-check the target, then commit with an atomic no-replace
//! move. Written fresh against std + libc/windows-sys.

use anyhow::{Context, Result, anyhow, bail};
use iroh_blobs::{
    Hash,
    api::{
        Store,
        blobs::{ExportMode, ExportOptions, ExportProgressItem},
    },
};
use n0_future::StreamExt;
use std::{
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// A remote-provided name must be exactly one plain relative segment:
/// no `..`, no absolute paths, no prefixes, no nesting.
pub fn validate_single_segment(name: &str) -> Result<()> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(anyhow!("sender provided an unsafe file name")),
    }
}

/// Windows device names, case-insensitive, with or without an extension
/// (`NUL.txt` is as reserved as `NUL`). Verified against Microsoft's
/// "Naming Files, Paths, and Namespaces" (Win32 FileIO docs).
fn is_reserved_stem(stem: &str) -> bool {
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "COM1" | "COM2" | "COM3" | "COM4" | "COM5"
            | "COM6" | "COM7" | "COM8" | "COM9" | "LPT1" | "LPT2" | "LPT3" | "LPT4"
            | "LPT5" | "LPT6" | "LPT7" | "LPT8" | "LPT9"
    )
}

/// Make a validated remote name safe to create on every release
/// platform (a Linux sender can legally share `a:b` or `CON`, which
/// Windows cannot create). Runs AFTER [`validate_single_segment`]:
/// traversal is rejected, never silently rewritten. Idempotent, so
/// calling it at describe time and again at export time is safe.
/// Rules (same MS doc): strip trailing dots/spaces, replace
/// `<>:"/\|?*` + control chars with `_`, prefix reserved stems with
/// `_`, fall back to `file` when nothing remains.
pub fn sanitize_filename(name: &str) -> String {
    let trimmed = name.trim_end_matches(['.', ' ']);
    let mut out: String = trimmed
        .chars()
        .map(|c| {
            if matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
                || c.is_control()
            {
                '_'
            } else {
                c
            }
        })
        .collect();
    let stem = out.split('.').next().unwrap_or("").to_owned();
    if is_reserved_stem(&stem) {
        out.insert(0, '_');
    }
    if out.is_empty() {
        out.push_str("file");
    }
    out
}

fn target_conflict(target: &Path) -> anyhow::Error {
    anyhow!(
        "'{}' already exists. Rename or remove it and try again",
        target.display()
    )
}

/// Fail if anything (file, dir, or dangling symlink) already exists at
/// `target`. Uses `symlink_metadata` so a symlink at the target can never
/// redirect the commit.
fn ensure_absent(target: &Path) -> Result<()> {
    match std::fs::symlink_metadata(target) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("cannot inspect save destination"),
        Ok(_) => Err(target_conflict(target)),
    }
}

fn staging_dir(output_dir: &Path) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    for attempt in 0..100u32 {
        let dir = output_dir.join(format!(
            ".fly-stage-{}-{}-{}",
            std::process::id(),
            nanos,
            attempt
        ));
        if std::fs::symlink_metadata(&dir).is_err() {
            std::fs::create_dir_all(&dir).context("cannot create staging dir")?;
            return Ok(dir);
        }
    }
    Err(anyhow!("cannot create a unique staging dir"))
}

fn cleanup_staging(dir: &Path) {
    if let Err(error) = std::fs::remove_dir_all(dir) {
        if error.kind() != std::io::ErrorKind::NotFound {
            eprintln!("fly: staging cleanup failed: {error}");
        }
    }
}

/// Remove staging dirs left behind by a cancelled receive. Only true
/// directories with our prefix are touched; anything else is left alone.
/// Safe because only one receive runs at a time (receive_lock).
fn cleanup_stale_staging(output_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(output_dir) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with(".fly-stage-") {
            continue;
        }
        // file_type() never follows symlinks: a symlink with our prefix
        // is not our dir, so it is never removed.
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            cleanup_staging(&entry.path());
        }
    }
}

async fn export_to_staging(db: &Store, hash: Hash, staged_target: &Path) -> Result<()> {
    let export = db.export_with_opts(ExportOptions {
        hash,
        target: staged_target.to_path_buf(),
        mode: ExportMode::Copy,
    });
    let mut stream = export.stream().await;
    while let Some(item) = stream.next().await {
        match item {
            ExportProgressItem::Done => return Ok(()),
            ExportProgressItem::Error(cause) => bail!("export failed: {cause}"),
            _ => {}
        }
    }
    Err(anyhow!("export ended without completion"))
}

/// Export one blob through staging + a validated atomic commit.
/// Returns the final path. Never merges, renames, skips, or overwrites.
pub async fn export_file(
    db: &Store,
    hash: Hash,
    file_name: &str,
    output_dir: &Path,
) -> Result<PathBuf> {
    validate_single_segment(file_name)?;
    // Defense in depth: the receive path pre-sanitizes at describe time
    // (so preview shows the real name), and this is idempotent anyway.
    let safe_name = sanitize_filename(file_name);
    let file_name = safe_name.as_str();
    std::fs::create_dir_all(output_dir).context("cannot create save folder")?;
    if !output_dir.is_dir() {
        bail!("save path is not a folder");
    }
    cleanup_stale_staging(output_dir);
    let target = output_dir.join(file_name);
    ensure_absent(&target)?;
    let staging = staging_dir(output_dir)?;
    let staged_target = staging.join(file_name);
    if let Err(error) = export_to_staging(db, hash, &staged_target).await {
        cleanup_staging(&staging);
        return Err(error);
    }
    // Re-check: another process may have created the target meanwhile.
    if let Err(error) = ensure_absent(&target) {
        cleanup_staging(&staging);
        return Err(error);
    }
    if let Err(error) = move_without_replacing(&staged_target, &target) {
        cleanup_staging(&staging);
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(target_conflict(&target));
        }
        return Err(error.into());
    }
    cleanup_staging(&staging);
    Ok(target)
}

/// Atomically move the staged file into place without replacing an existing
/// target. Every release platform has a native no-replace primitive.
#[cfg(target_os = "linux")]
fn move_without_replacing(source: &Path, target: &Path) -> std::io::Result<()> {
    let source = c_path(source)?;
    let target = c_path(target)?;
    // SAFETY: NUL-terminated paths; relative paths resolve via AT_FDCWD.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn move_without_replacing(source: &Path, target: &Path) -> std::io::Result<()> {
    let source = c_path(source)?;
    let target = c_path(target)?;
    // SAFETY: NUL-terminated paths; RENAME_EXCL refuses to overwrite.
    let rc = unsafe { libc::renamex_np(source.as_ptr(), target.as_ptr(), libc::RENAME_EXCL) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn c_path(path: &Path) -> std::io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;

    std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path contains an interior NUL byte",
        )
    })
}

#[cfg(windows)]
fn move_without_replacing(source: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::MoveFileExW;

    let to_wide = |path: &Path| {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>()
    };
    let source = to_wide(source);
    let target = to_wide(target);
    // SAFETY: NUL-terminated UTF-16 paths; flags 0 means never replace.
    if unsafe { MoveFileExW(source.as_ptr(), target.as_ptr(), 0) } != 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn move_without_replacing(source: &Path, target: &Path) -> std::io::Result<()> {
    // No native primitive here; the preflight check is racy by nature.
    // None of our release targets take this path.
    if std::fs::symlink_metadata(target).is_ok() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "target already exists",
        ));
    }
    std::fs::rename(source, target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_hostile_names() {
        assert!(validate_single_segment("report.pdf").is_ok());
        assert!(validate_single_segment("..").is_err());
        assert!(validate_single_segment("../escape.txt").is_err());
        assert!(validate_single_segment("sub/dir/file.txt").is_err());
        assert!(validate_single_segment("").is_err());
        #[cfg(windows)]
        {
            assert!(validate_single_segment("C:\\Windows\\x.txt").is_err());
            assert!(validate_single_segment("\\abs.txt").is_err());
        }
        #[cfg(unix)]
        {
            assert!(validate_single_segment("/abs.txt").is_err());
        }
    }

    #[test]
    fn sanitize_keeps_ordinary_names() {
        for name in [
            "report.pdf",
            ".bashrc",
            "my file.txt",
            "résumé.pdf",
            "archive.tar.gz",
            "console",
            "COM10",
            "my CON file.txt",
        ] {
            assert_eq!(sanitize_filename(name), name, "must pass through: {name}");
        }
    }

    #[test]
    fn sanitize_neutralizes_platform_illegal_names() {
        // Reserved device stems (any case, with or without extension).
        assert_eq!(sanitize_filename("CON"), "_CON");
        assert_eq!(sanitize_filename("nul.txt"), "_nul.txt");
        assert_eq!(sanitize_filename("COM1"), "_COM1");
        assert_eq!(sanitize_filename("lpt9.dat"), "_lpt9.dat");
        // Trailing dots/spaces (leading dots stay: ".bashrc" is legal).
        assert_eq!(sanitize_filename("notes. "), "notes");
        assert_eq!(sanitize_filename("notes..."), "notes");
        // Reserved characters + controls become underscores.
        assert_eq!(sanitize_filename("a:b"), "a_b");
        assert_eq!(sanitize_filename("a?b*c"), "a_b_c");
        assert_eq!(sanitize_filename("a<b>c"), "a_b_c");
        assert_eq!(sanitize_filename("a|b"), "a_b");
        assert_eq!(sanitize_filename("a\"b"), "a_b");
        assert_eq!(sanitize_filename("a\x07b"), "a_b");
        // Nothing usable left: fall back instead of failing.
        assert_eq!(sanitize_filename("..."), "file");
        assert_eq!(sanitize_filename("   "), "file");
        // Idempotent: describe-time + export-time passes agree.
        for name in ["CON", "a:b", "notes. ", "report.pdf", "..."] {
            let once = sanitize_filename(name);
            assert_eq!(sanitize_filename(&once), once, "not idempotent: {name}");
        }
    }
}
