use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::NativeFailure;
use crate::local::{LocalStore, LocalWriter};
use crate::object::{
    ByteRange, CancellationToken, FlushPolicy, ObjectLocation, ObjectStore, StagedWrite,
    SyncPolicy, WriteOptions,
};
use crate::s3::{RemoteMultipartWriter, RemoteStore, S3Config};
use crate::Operation;

static STORE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static ACTIVE_STORES: AtomicUsize = AtomicUsize::new(0);
static STORES_CREATED: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, rustler::NifMap)]
pub(crate) struct StoreMetadata {
    pub(crate) path: String,
    pub(crate) size: u64,
    pub(crate) modified_unix_ns: Option<u64>,
}

#[derive(Debug)]
pub(crate) enum StoreBackend {
    Local { root: PathBuf },
    S3(Box<RemoteStore>),
}

#[derive(Debug)]
pub(crate) struct StoreResource {
    id: u64,
    backend: StoreBackend,
}

#[rustler::resource_impl]
impl rustler::Resource for StoreResource {}

impl Drop for StoreResource {
    fn drop(&mut self) {
        ACTIVE_STORES.fetch_sub(1, Ordering::Relaxed);
    }
}

impl StoreResource {
    pub(crate) fn open_local(root: String) -> Result<Self, NativeFailure> {
        let root = fs::canonicalize(root)
            .map_err(|error| NativeFailure::from_io(Operation::StoreOpen, &error))?;
        if !root.is_dir() {
            return Err(NativeFailure::invalid(
                Operation::StoreOpen,
                "local store root must be a directory",
            ));
        }
        Ok(Self::new(StoreBackend::Local { root }))
    }

    pub(crate) fn open_s3(config: S3Config) -> Result<Self, NativeFailure> {
        Ok(Self::new(StoreBackend::S3(Box::new(RemoteStore::new(
            config,
        )?))))
    }

    fn new(backend: StoreBackend) -> Self {
        let id = STORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        STORES_CREATED.fetch_add(1, Ordering::Relaxed);
        ACTIVE_STORES.fetch_add(1, Ordering::Relaxed);
        Self { id, backend }
    }

    pub(crate) fn identity(&self) -> u64 {
        self.id
    }

    pub(crate) fn head(&self, key: &str) -> Result<StoreMetadata, NativeFailure> {
        validate_key(key, false)?;
        let cancellation = CancellationToken::default();
        match &self.backend {
            StoreBackend::Local { root } => {
                let metadata = LocalStore.head(&local_location(root, key), &cancellation)?;
                local_metadata(root, metadata)
            }
            StoreBackend::S3(store) => {
                let metadata = store
                    .object(key)?
                    .head(&cancellation, Operation::StoreHead)?;
                Ok(StoreMetadata {
                    path: key.to_owned(),
                    size: metadata.size,
                    modified_unix_ns: metadata.modified_unix_ns,
                })
            }
        }
    }

    pub(crate) fn read_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, NativeFailure> {
        validate_key(key, false)?;
        let cancellation = CancellationToken::default();
        match &self.backend {
            StoreBackend::Local { root } => LocalStore.read_range(
                &local_location(root, key),
                ByteRange { offset, length },
                &cancellation,
            ),
            StoreBackend::S3(store) => {
                let length = usize::try_from(length).map_err(|_| {
                    NativeFailure::invalid(Operation::StoreReadRange, "range length is too large")
                })?;
                store.object(key)?.read_range(
                    offset,
                    length,
                    &cancellation,
                    Operation::StoreReadRange,
                )
            }
        }
    }

    pub(crate) fn list(&self, prefix: &str) -> Result<Vec<StoreMetadata>, NativeFailure> {
        validate_key(prefix, true)?;
        let cancellation = CancellationToken::default();
        match &self.backend {
            StoreBackend::Local { root } => LocalStore
                .list(&local_location(root, ""), prefix, &cancellation)?
                .into_iter()
                .map(|metadata| local_metadata(root, metadata))
                .collect(),
            StoreBackend::S3(store) => Ok(store
                .list(prefix, &cancellation)?
                .into_iter()
                .map(|metadata| StoreMetadata {
                    path: metadata.key,
                    size: metadata.size,
                    modified_unix_ns: metadata.modified_unix_ns,
                })
                .collect()),
        }
    }

    pub(crate) fn delete(&self, key: &str) -> Result<(), NativeFailure> {
        validate_key(key, false)?;
        let cancellation = CancellationToken::default();
        match &self.backend {
            StoreBackend::Local { root } => {
                LocalStore.delete(&local_location(root, key), &cancellation)
            }
            StoreBackend::S3(store) => store.object(key)?.delete(&cancellation),
        }
    }

    pub(crate) fn open_writer(
        &self,
        key: &str,
        flush: FlushPolicy,
        sync: SyncPolicy,
        cancellation: Arc<CancellationToken>,
    ) -> Result<StoreWriter, NativeFailure> {
        validate_key(key, false)?;
        match &self.backend {
            StoreBackend::Local { root } => {
                ensure_local_parent(root, key)?;
                let writer = LocalStore.stage(
                    &local_location(root, key),
                    WriteOptions { flush, sync },
                    cancellation.clone(),
                )?;
                Ok(StoreWriter::Local {
                    writer,
                    root: root.clone(),
                })
            }
            StoreBackend::S3(store) => Ok(StoreWriter::S3 {
                writer: store.object(key)?.open_multipart(cancellation.clone())?,
                key: key.to_owned(),
            }),
        }
    }
}

pub(crate) enum StoreWriter {
    Local {
        writer: LocalWriter,
        root: PathBuf,
    },
    S3 {
        writer: RemoteMultipartWriter,
        key: String,
    },
}

impl StoreWriter {
    pub(crate) fn write(&mut self, bytes: &[u8]) -> Result<usize, NativeFailure> {
        match self {
            Self::Local { writer, .. } => StagedWrite::write(writer, bytes),
            Self::S3 { writer, .. } => Write::write(writer, bytes)
                .map_err(|_| NativeFailure::expected(Operation::StoreWriterWrite, "write failed")),
        }
    }

    pub(crate) fn publish(&mut self) -> Result<StoreMetadata, NativeFailure> {
        match self {
            Self::Local { writer, root } => local_metadata(root, writer.publish()?),
            Self::S3 { writer, key } => {
                let metadata = writer.publish()?;
                Ok(StoreMetadata {
                    path: key.clone(),
                    size: metadata.size,
                    modified_unix_ns: metadata.modified_unix_ns,
                })
            }
        }
    }

    pub(crate) fn abort(&mut self) -> Result<bool, NativeFailure> {
        match self {
            Self::Local { writer, .. } => writer.abort(),
            Self::S3 { writer, .. } => writer.abort(),
        }
    }
}

pub(crate) fn resource_snapshot() -> (usize, u64) {
    (
        ACTIVE_STORES.load(Ordering::Relaxed),
        STORES_CREATED.load(Ordering::Relaxed),
    )
}

fn validate_key(key: &str, allow_empty: bool) -> Result<(), NativeFailure> {
    if (key.is_empty() && allow_empty)
        || (!key.is_empty()
            && !key.starts_with(['/', '\\'])
            && !key.contains('\\')
            && Path::new(key)
                .components()
                .all(|part| matches!(part, Component::Normal(_))))
    {
        Ok(())
    } else {
        Err(NativeFailure::invalid(
            Operation::StoreOpen,
            "object key must be a normalized relative path",
        ))
    }
}

fn local_location(root: &Path, key: &str) -> ObjectLocation {
    ObjectLocation {
        key: root.join(key).to_string_lossy().into_owned(),
        allowed_root: Some(root.to_string_lossy().into_owned()),
    }
}

fn local_metadata(
    root: &Path,
    metadata: crate::object::ObjectMetadata,
) -> Result<StoreMetadata, NativeFailure> {
    let path = Path::new(&metadata.path)
        .strip_prefix(root)
        .map_err(|_| NativeFailure::invalid(Operation::StoreHead, "invalid local object path"))?
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    Ok(StoreMetadata {
        path,
        size: metadata.size,
        modified_unix_ns: metadata.modified_unix_ns,
    })
}

fn ensure_local_parent(root: &Path, key: &str) -> Result<(), NativeFailure> {
    let parent = Path::new(key).parent().unwrap_or_else(|| Path::new(""));
    let mut current = root.to_path_buf();
    for part in parent.components() {
        let Component::Normal(name) = part else {
            continue;
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(NativeFailure::invalid(
                    Operation::StoreWriterOpen,
                    "local object parent cannot contain symlinks",
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(NativeFailure::invalid(
                    Operation::StoreWriterOpen,
                    "local object parent must be a directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .map_err(|error| NativeFailure::from_io(Operation::StoreWriterOpen, &error))?;
            }
            Err(error) => {
                return Err(NativeFailure::from_io(Operation::StoreWriterOpen, &error));
            }
        }
    }
    Ok(())
}
