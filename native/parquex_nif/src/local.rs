use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Category, NativeFailure};
use crate::object::{
    ByteRange, CancellationToken, FlushPolicy, ObjectLocation, ObjectMetadata, ObjectStore,
    StagedWrite, SyncPolicy, WriteOptions,
};
use crate::Operation;

static ACTIVE_WRITERS: AtomicUsize = AtomicUsize::new(0);
static BYTES_READ: AtomicU64 = AtomicU64::new(0);
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Default)]
pub(crate) struct LocalStore;

impl ObjectStore for LocalStore {
    type Writer = LocalWriter;

    fn head(
        &self,
        location: &ObjectLocation,
        cancellation: &CancellationToken,
    ) -> Result<ObjectMetadata, NativeFailure> {
        cancellation.check(Operation::Head)?;
        let path = resolve_existing(location, Operation::Head)?;
        metadata_for(&path, Operation::Head)
    }

    fn read_range(
        &self,
        location: &ObjectLocation,
        range: ByteRange,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, NativeFailure> {
        cancellation.check(Operation::ReadRange)?;
        let path = resolve_existing(location, Operation::ReadRange)?;
        let metadata = fs::metadata(&path)
            .map_err(|error| NativeFailure::from_io(Operation::ReadRange, &error))?;

        if !metadata.is_file() {
            return Err(NativeFailure::invalid(
                Operation::ReadRange,
                "range reads require a regular file",
            ));
        }

        if range.offset > metadata.len() {
            return Err(NativeFailure::invalid(
                Operation::ReadRange,
                "range offset is beyond end of object",
            ));
        }

        let available = metadata.len() - range.offset;
        let read_length = range.length.min(available);
        let capacity = usize::try_from(read_length).map_err(|_| {
            NativeFailure::invalid(Operation::ReadRange, "requested range is too large")
        })?;

        let mut file = File::open(&path)
            .map_err(|error| NativeFailure::from_io(Operation::ReadRange, &error))?;
        file.seek(SeekFrom::Start(range.offset))
            .map_err(|error| NativeFailure::from_io(Operation::ReadRange, &error))?;

        let mut bytes = Vec::with_capacity(capacity);
        file.take(read_length)
            .read_to_end(&mut bytes)
            .map_err(|error| NativeFailure::from_io(Operation::ReadRange, &error))?;
        BYTES_READ.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        cancellation.check(Operation::ReadRange)?;
        Ok(bytes)
    }

    fn list(
        &self,
        location: &ObjectLocation,
        prefix: &str,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ObjectMetadata>, NativeFailure> {
        cancellation.check(Operation::List)?;
        validate_prefix(prefix)?;

        let root = resolve_existing(location, Operation::List)?;
        if !root.is_dir() {
            return Err(NativeFailure::invalid(
                Operation::List,
                "listing requires a directory location",
            ));
        }

        let allowed_root = canonical_allowed_root(location, Operation::List)?;
        let mut entries = Vec::new();
        visit_directory(
            &root,
            &root,
            allowed_root.as_deref(),
            prefix,
            cancellation,
            &mut entries,
        )?;
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    fn stage(
        &self,
        location: &ObjectLocation,
        options: WriteOptions,
        cancellation: Arc<CancellationToken>,
    ) -> Result<Self::Writer, NativeFailure> {
        let destination = resolve_destination(location, Operation::WriterOpen)?;
        LocalWriter::open(destination, options, cancellation)
    }

    fn delete(
        &self,
        location: &ObjectLocation,
        cancellation: &CancellationToken,
    ) -> Result<(), NativeFailure> {
        cancellation.check(Operation::Delete)?;
        let path = resolve_existing(location, Operation::Delete)?;
        fs::remove_file(path).map_err(|error| NativeFailure::from_io(Operation::Delete, &error))
    }
}

pub(crate) struct LocalWriter {
    file: Option<File>,
    stage_path: PathBuf,
    destination: PathBuf,
    flush: FlushPolicy,
    sync: SyncPolicy,
    cancellation: Arc<CancellationToken>,
    active: bool,
}

impl LocalWriter {
    fn open(
        destination: PathBuf,
        options: WriteOptions,
        cancellation: Arc<CancellationToken>,
    ) -> Result<Self, NativeFailure> {
        let parent = destination.parent().ok_or_else(|| {
            NativeFailure::invalid(
                Operation::WriterOpen,
                "destination requires a parent directory",
            )
        })?;
        let file_name = destination.file_name().ok_or_else(|| {
            NativeFailure::invalid(Operation::WriterOpen, "destination requires a file name")
        })?;

        for _attempt in 0..64 {
            let sequence = STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let stage_name = stage_name(file_name, sequence);
            let stage_path = parent.join(stage_name);

            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&stage_path)
            {
                Ok(file) => {
                    ACTIVE_WRITERS.fetch_add(1, Ordering::Relaxed);
                    return Ok(Self {
                        file: Some(file),
                        stage_path,
                        destination,
                        flush: options.flush,
                        sync: options.sync,
                        cancellation,
                        active: true,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(NativeFailure::from_io(Operation::WriterOpen, &error)),
            }
        }

        Err(NativeFailure::new(
            Category::Conflict,
            Operation::WriterOpen,
            "could not allocate a unique staging file",
        ))
    }

    fn finish(&mut self) {
        if self.active {
            self.active = false;
            ACTIVE_WRITERS.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn cleanup_stage(&mut self) {
        self.file.take();
        let _ignored = fs::remove_file(&self.stage_path);
        self.finish();
    }

    fn ensure_open(&self, operation: Operation) -> Result<(), NativeFailure> {
        self.cancellation.check(operation)?;
        if self.active && self.file.is_some() {
            Ok(())
        } else {
            Err(NativeFailure::cancelled(operation))
        }
    }
}

impl StagedWrite for LocalWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<usize, NativeFailure> {
        self.ensure_open(Operation::WriterWrite)?;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| NativeFailure::cancelled(Operation::WriterWrite))?;
        file.write_all(bytes)
            .map_err(|error| NativeFailure::from_io(Operation::WriterWrite, &error))?;
        if self.flush == FlushPolicy::EachChunk {
            file.flush()
                .map_err(|error| NativeFailure::from_io(Operation::WriterWrite, &error))?;
        }
        Ok(bytes.len())
    }

    fn publish(&mut self) -> Result<ObjectMetadata, NativeFailure> {
        self.ensure_open(Operation::WriterPublish)?;

        let prepare_result = (|| {
            let file = self
                .file
                .as_mut()
                .ok_or_else(|| NativeFailure::cancelled(Operation::WriterPublish))?;
            if self.flush != FlushPolicy::None {
                file.flush()
                    .map_err(|error| NativeFailure::from_io(Operation::WriterPublish, &error))?;
            }
            match self.sync {
                SyncPolicy::None => Ok(()),
                SyncPolicy::Data => file
                    .sync_data()
                    .map_err(|error| NativeFailure::from_io(Operation::WriterPublish, &error)),
                SyncPolicy::All => file
                    .sync_all()
                    .map_err(|error| NativeFailure::from_io(Operation::WriterPublish, &error)),
            }
        })();

        if let Err(error) = prepare_result {
            self.cleanup_stage();
            return Err(error);
        }

        let mut metadata = match metadata_for(&self.stage_path, Operation::WriterPublish) {
            Ok(metadata) => metadata,
            Err(error) => {
                self.cleanup_stage();
                return Err(error);
            }
        };

        self.file.take();
        if let Err(error) = fs::hard_link(&self.stage_path, &self.destination) {
            self.cleanup_stage();
            return Err(NativeFailure::from_io(Operation::WriterPublish, &error));
        }

        if self.sync == SyncPolicy::All {
            let parent_sync = self
                .destination
                .parent()
                .and_then(|parent| File::open(parent).ok())
                .and_then(|directory| directory.sync_all().ok());
            if parent_sync.is_none() {
                let _ignored = fs::remove_file(&self.destination);
                self.cleanup_stage();
                return Err(NativeFailure::new(
                    Category::NativeFailure,
                    Operation::WriterPublish,
                    "destination directory sync failed",
                ));
            }
        }

        if let Err(error) = fs::remove_file(&self.stage_path) {
            let _ignored = fs::remove_file(&self.destination);
            self.cleanup_stage();
            return Err(NativeFailure::from_io(Operation::WriterPublish, &error));
        }

        self.finish();
        metadata.path = self.destination.to_string_lossy().into_owned();
        Ok(metadata)
    }

    fn abort(&mut self) -> Result<bool, NativeFailure> {
        if !self.active {
            return Ok(false);
        }
        self.cancellation.cancel();
        self.file.take();
        let remove_result = fs::remove_file(&self.stage_path);
        self.finish();

        match remove_result {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(NativeFailure::from_io(Operation::WriterAbort, &error)),
        }
    }
}

impl Write for LocalWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        StagedWrite::write(self, buffer)
            .map_err(|_| std::io::Error::other("staged object write failed"))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.ensure_open(Operation::WriterWrite)
            .map_err(|_| std::io::Error::other("staged object write failed"))?;
        self.file
            .as_mut()
            .ok_or_else(|| std::io::Error::other("staged object writer is closed"))?
            .flush()
    }
}

impl Drop for LocalWriter {
    fn drop(&mut self) {
        if self.active {
            self.cancellation.cancel();
            self.cleanup_stage();
        }
    }
}

pub(crate) fn resource_snapshot() -> (usize, u64) {
    (
        ACTIVE_WRITERS.load(Ordering::Relaxed),
        BYTES_READ.load(Ordering::Relaxed),
    )
}

fn resolve_existing(
    location: &ObjectLocation,
    operation: Operation,
) -> Result<PathBuf, NativeFailure> {
    let canonical = fs::canonicalize(&location.key)
        .map_err(|error| NativeFailure::from_io(operation, &error))?;
    enforce_allowed_root(location, &canonical, operation)?;
    Ok(canonical)
}

fn resolve_destination(
    location: &ObjectLocation,
    operation: Operation,
) -> Result<PathBuf, NativeFailure> {
    let requested = Path::new(&location.key);
    let parent = requested.parent().ok_or_else(|| {
        NativeFailure::invalid(operation, "destination requires a parent directory")
    })?;
    let file_name = requested
        .file_name()
        .ok_or_else(|| NativeFailure::invalid(operation, "destination requires a file name"))?;
    let canonical_parent =
        fs::canonicalize(parent).map_err(|error| NativeFailure::from_io(operation, &error))?;
    enforce_allowed_root(location, &canonical_parent, operation)?;
    Ok(canonical_parent.join(file_name))
}

fn canonical_allowed_root(
    location: &ObjectLocation,
    operation: Operation,
) -> Result<Option<PathBuf>, NativeFailure> {
    location
        .allowed_root
        .as_ref()
        .map(|root| {
            fs::canonicalize(root).map_err(|error| NativeFailure::from_io(operation, &error))
        })
        .transpose()
}

fn enforce_allowed_root(
    location: &ObjectLocation,
    canonical_path: &Path,
    operation: Operation,
) -> Result<(), NativeFailure> {
    if let Some(root) = canonical_allowed_root(location, operation)? {
        if !canonical_path.starts_with(&root) {
            return Err(NativeFailure::new(
                Category::PermissionDenied,
                operation,
                "object is outside the configured allowed root",
            ));
        }
    }
    Ok(())
}

fn metadata_for(path: &Path, operation: Operation) -> Result<ObjectMetadata, NativeFailure> {
    let metadata = fs::metadata(path).map_err(|error| NativeFailure::from_io(operation, &error))?;
    if !metadata.is_file() {
        return Err(NativeFailure::invalid(
            operation,
            "object operation requires a regular file",
        ));
    }
    let modified_unix_ns = metadata.modified().ok().and_then(system_time_ns);
    Ok(ObjectMetadata {
        path: path.to_string_lossy().into_owned(),
        size: metadata.len(),
        modified_unix_ns,
    })
}

fn system_time_ns(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
}

fn validate_prefix(prefix: &str) -> Result<(), NativeFailure> {
    let path = Path::new(prefix);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(NativeFailure::invalid(
            Operation::List,
            "list prefix must be relative and cannot traverse parents",
        ));
    }
    Ok(())
}

fn visit_directory(
    directory: &Path,
    listing_root: &Path,
    allowed_root: Option<&Path>,
    prefix: &str,
    cancellation: &CancellationToken,
    output: &mut Vec<ObjectMetadata>,
) -> Result<(), NativeFailure> {
    cancellation.check(Operation::List)?;
    let entries =
        fs::read_dir(directory).map_err(|error| NativeFailure::from_io(Operation::List, &error))?;

    for entry in entries {
        cancellation.check(Operation::List)?;
        let entry = entry.map_err(|error| NativeFailure::from_io(Operation::List, &error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| NativeFailure::from_io(Operation::List, &error))?;
        let raw_path = entry.path();

        if file_type.is_symlink() {
            let target = fs::canonicalize(&raw_path)
                .map_err(|error| NativeFailure::from_io(Operation::List, &error))?;
            if let Some(root) = allowed_root {
                if !target.starts_with(root) {
                    return Err(NativeFailure::new(
                        Category::PermissionDenied,
                        Operation::List,
                        "listed symlink escapes the configured allowed root",
                    ));
                }
            }
            continue;
        }

        if file_type.is_dir() {
            visit_directory(
                &raw_path,
                listing_root,
                allowed_root,
                prefix,
                cancellation,
                output,
            )?;
        } else if file_type.is_file() {
            let relative = raw_path
                .strip_prefix(listing_root)
                .map_err(|_| NativeFailure::invalid(Operation::List, "invalid listed path"))?;
            let relative = relative
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if relative.starts_with(prefix) {
                output.push(metadata_for(&raw_path, Operation::List)?);
            }
        }
    }
    Ok(())
}

fn stage_name(file_name: &OsStr, sequence: u64) -> String {
    format!(
        ".{}.parquex-{}-{}.tmp",
        file_name.to_string_lossy(),
        std::process::id(),
        sequence
    )
}
