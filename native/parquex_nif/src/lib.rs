use std::io::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

use rustler::{Binary, Encoder, Env, LocalPid, Monitor, OwnedBinary, Resource, ResourceArc, Term};

mod error;
mod local;
mod object;
mod reader;
mod s3;
mod store;
mod writer;

use error::NativeFailure;
use local::{LocalStore, LocalWriter};
use object::{
    ByteRange, CancellationToken, FlushPolicy, ObjectLocation, ObjectMetadata, ObjectStore,
    StagedWrite, SyncPolicy, WriteOptions,
};
use s3::{RemoteMultipartWriter, RemoteObject, S3Config};
use store::{StoreResource, StoreWriter};

pub(crate) mod atoms {
    rustler::atoms! {
        aborted,
        all,
        before_publish,
        batch,
        binary,
        boolean,
        cancelled,
        closed,
        chunk,
        conflict,
        data,
        deleted,
        each_chunk,
        error,
        invalid_argument,
        malformed_data,
        native_failure,
        native_smoke,
        native_smoke_error,
        not_found,
        none,
        ok,
        permission_denied,
        timeout,
        unsupported,
        local_delete,
        local_head,
        local_list,
        local_read_range,
        local_writer_abort,
        local_writer_open,
        local_writer_publish,
        local_writer_write,
        resource_snapshot,
        reader_open,
        reader_next,
        reader_close,
        reader_stats,
        eof,
        null,
        integer,
        float,
        utf8,
        fixed_binary,
        date32,
        date64,
        time,
        timestamp,
        duration,
        decimal,
        list,
        large_list,
        fixed_list,
        struct_type = "struct",
        second,
        millisecond,
        microsecond,
        nanosecond,
        nil_atom = "nil",
        uncompressed,
        snappy,
        zstd,
        gzip,
        lz4_raw,
        parquet_writer_open,
        parquet_writer_write,
        parquet_writer_close,
        parquet_writer_abort,
        parquet_writer_stats,
        standard,
        explicit,
        gt,
        gte,
        lt,
        lte,
        eq,
        s3_head,
        s3_read_range,
        s3_list,
        s3_delete,
        s3_writer_open,
        s3_writer_write,
        s3_writer_publish,
        s3_writer_abort,
        store_open,
        store_identity,
        store_head,
        store_read_range,
        store_list,
        store_delete,
        store_writer_open,
        store_writer_write,
        store_writer_publish,
        store_writer_abort
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Operation {
    Smoke,
    SmokeError,
    Head,
    ReadRange,
    List,
    Delete,
    WriterOpen,
    WriterWrite,
    WriterPublish,
    WriterAbort,
    ResourceSnapshot,
    ReaderOpen,
    ReaderNext,
    ReaderClose,
    ReaderStats,
    ParquetWriterOpen,
    ParquetWriterWrite,
    ParquetWriterClose,
    ParquetWriterAbort,
    ParquetWriterStats,
    S3Head,
    S3ReadRange,
    S3List,
    S3Delete,
    S3WriterOpen,
    S3WriterWrite,
    S3WriterPublish,
    S3WriterAbort,
    StoreOpen,
    StoreIdentity,
    StoreHead,
    StoreReadRange,
    StoreList,
    StoreDelete,
    StoreWriterOpen,
    StoreWriterWrite,
    StoreWriterPublish,
    StoreWriterAbort,
}

impl Operation {
    pub(crate) fn atom(self) -> rustler::Atom {
        match self {
            Self::Smoke => atoms::native_smoke(),
            Self::SmokeError => atoms::native_smoke_error(),
            Self::Head => atoms::local_head(),
            Self::ReadRange => atoms::local_read_range(),
            Self::List => atoms::local_list(),
            Self::Delete => atoms::local_delete(),
            Self::WriterOpen => atoms::local_writer_open(),
            Self::WriterWrite => atoms::local_writer_write(),
            Self::WriterPublish => atoms::local_writer_publish(),
            Self::WriterAbort => atoms::local_writer_abort(),
            Self::ResourceSnapshot => atoms::resource_snapshot(),
            Self::ReaderOpen => atoms::reader_open(),
            Self::ReaderNext => atoms::reader_next(),
            Self::ReaderClose => atoms::reader_close(),
            Self::ReaderStats => atoms::reader_stats(),
            Self::ParquetWriterOpen => atoms::parquet_writer_open(),
            Self::ParquetWriterWrite => atoms::parquet_writer_write(),
            Self::ParquetWriterClose => atoms::parquet_writer_close(),
            Self::ParquetWriterAbort => atoms::parquet_writer_abort(),
            Self::ParquetWriterStats => atoms::parquet_writer_stats(),
            Self::S3Head => atoms::s3_head(),
            Self::S3ReadRange => atoms::s3_read_range(),
            Self::S3List => atoms::s3_list(),
            Self::S3Delete => atoms::s3_delete(),
            Self::S3WriterOpen => atoms::s3_writer_open(),
            Self::S3WriterWrite => atoms::s3_writer_write(),
            Self::S3WriterPublish => atoms::s3_writer_publish(),
            Self::S3WriterAbort => atoms::s3_writer_abort(),
            Self::StoreOpen => atoms::store_open(),
            Self::StoreIdentity => atoms::store_identity(),
            Self::StoreHead => atoms::store_head(),
            Self::StoreReadRange => atoms::store_read_range(),
            Self::StoreList => atoms::store_list(),
            Self::StoreDelete => atoms::store_delete(),
            Self::StoreWriterOpen => atoms::store_writer_open(),
            Self::StoreWriterWrite => atoms::store_writer_write(),
            Self::StoreWriterPublish => atoms::store_writer_publish(),
            Self::StoreWriterAbort => atoms::store_writer_abort(),
        }
    }
}

fn guarded<T, F>(operation: Operation, work: F) -> Result<T, NativeFailure>
where
    F: FnOnce() -> Result<T, NativeFailure>,
{
    match catch_unwind(AssertUnwindSafe(work)) {
        Ok(result) => result,
        Err(_) => Err(NativeFailure::panic(operation)),
    }
}

fn encode_guarded<'a, T, F>(env: Env<'a>, operation: Operation, work: F) -> Term<'a>
where
    T: Encoder,
    F: FnOnce() -> Result<T, NativeFailure>,
{
    match guarded(operation, work) {
        Ok(value) => (atoms::ok(), value).encode(env),
        Err(failure) => (atoms::error(), failure.payload()).encode(env),
    }
}

#[derive(rustler::NifMap)]
struct NativeMetadata {
    path: String,
    size: u64,
    modified_unix_ns: Option<u64>,
}

impl From<ObjectMetadata> for NativeMetadata {
    fn from(metadata: ObjectMetadata) -> Self {
        Self {
            path: metadata.path,
            size: metadata.size,
            modified_unix_ns: metadata.modified_unix_ns,
        }
    }
}

impl From<s3::RemoteMetadata> for NativeMetadata {
    fn from(metadata: s3::RemoteMetadata) -> Self {
        Self {
            path: metadata.key,
            size: metadata.size,
            modified_unix_ns: metadata.modified_unix_ns,
        }
    }
}

#[derive(rustler::NifMap)]
struct ResourceSnapshot {
    active_writers: usize,
    active_readers: usize,
    bytes_read: u64,
    active_s3_requests: usize,
    peak_s3_requests: usize,
    active_multipart_uploads: usize,
    s3_range_requests: u64,
    s3_range_bytes: u64,
    active_stores: usize,
    stores_created: u64,
    s3_clients_created: u64,
}

#[derive(rustler::NifMap)]
struct NativeReaderOptions {
    max_range_bytes: usize,
    batch_size: usize,
    prefetch_depth: usize,
    columns: Vec<String>,
    predicate: Option<reader::NativePredicate>,
}

struct WriterResource {
    writer: Mutex<LocalWriter>,
}

struct S3WriterResource {
    writer: Mutex<RemoteMultipartWriter>,
    cancellation: std::sync::Arc<CancellationToken>,
}

struct StoreWriterResource {
    writer: Mutex<StoreWriter>,
    cancellation: std::sync::Arc<CancellationToken>,
}

#[rustler::resource_impl]
impl Resource for StoreWriterResource {
    fn down(&self, _env: Env<'_>, _pid: LocalPid, _monitor: Monitor) {
        self.cancellation.cancel();
        if let Ok(mut writer) = self.writer.lock() {
            let _ignored = writer.abort();
        }
    }
}

#[rustler::resource_impl]
impl Resource for S3WriterResource {
    fn down(&self, _env: Env<'_>, _pid: LocalPid, _monitor: Monitor) {
        self.cancellation.cancel();
        if let Ok(mut writer) = self.writer.lock() {
            let _ignored = writer.abort();
        }
    }
}

#[rustler::resource_impl]
impl Resource for WriterResource {
    fn down(&self, _env: Env<'_>, _pid: LocalPid, _monitor: Monitor) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ignored = writer.abort();
        }
    }
}

fn location(path: String, allowed_root: Option<String>) -> ObjectLocation {
    ObjectLocation {
        key: path,
        allowed_root,
    }
}

fn lock_writer(
    writer: &ResourceArc<WriterResource>,
    operation: Operation,
) -> Result<std::sync::MutexGuard<'_, LocalWriter>, NativeFailure> {
    writer
        .writer
        .lock()
        .map_err(|_| NativeFailure::expected(operation, "native writer state is unavailable"))
}

fn lock_s3_writer(
    writer: &ResourceArc<S3WriterResource>,
    operation: Operation,
) -> Result<std::sync::MutexGuard<'_, RemoteMultipartWriter>, NativeFailure> {
    writer
        .writer
        .lock()
        .map_err(|_| NativeFailure::expected(operation, "native S3 writer state is unavailable"))
}

fn lock_store_writer(
    writer: &ResourceArc<StoreWriterResource>,
    operation: Operation,
) -> Result<std::sync::MutexGuard<'_, StoreWriter>, NativeFailure> {
    writer
        .writer
        .lock()
        .map_err(|_| NativeFailure::expected(operation, "native store writer is unavailable"))
}

fn write_policies(
    flush: rustler::Atom,
    sync: rustler::Atom,
    operation: Operation,
) -> Result<(FlushPolicy, SyncPolicy), NativeFailure> {
    let flush = if flush == atoms::none() {
        FlushPolicy::None
    } else if flush == atoms::each_chunk() {
        FlushPolicy::EachChunk
    } else if flush == atoms::before_publish() {
        FlushPolicy::BeforePublish
    } else {
        return Err(NativeFailure::invalid(operation, "invalid flush policy"));
    };
    let sync = if sync == atoms::none() {
        SyncPolicy::None
    } else if sync == atoms::data() {
        SyncPolicy::Data
    } else if sync == atoms::all() {
        SyncPolicy::All
    } else {
        return Err(NativeFailure::invalid(operation, "invalid sync policy"));
    };
    Ok((flush, sync))
}

#[rustler::nif]
fn smoke(env: Env<'_>) -> Term<'_> {
    encode_guarded(env, Operation::Smoke, || Ok(1_u32))
}

#[rustler::nif]
fn smoke_error(env: Env<'_>) -> Term<'_> {
    encode_guarded(env, Operation::SmokeError, || {
        Err::<u32, NativeFailure>(NativeFailure::expected(
            Operation::SmokeError,
            "native smoke error",
        ))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_open_local(env: Env<'_>, root: String) -> Term<'_> {
    encode_guarded(env, Operation::StoreOpen, || {
        Ok(ResourceArc::new(StoreResource::open_local(root)?))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_open_s3(env: Env<'_>, config: S3Config) -> Term<'_> {
    encode_guarded(env, Operation::StoreOpen, || {
        Ok(ResourceArc::new(StoreResource::open_s3(config)?))
    })
}

#[rustler::nif]
fn store_identity(env: Env<'_>, store: ResourceArc<StoreResource>) -> Term<'_> {
    encode_guarded(env, Operation::StoreIdentity, || Ok(store.identity()))
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_head(env: Env<'_>, store: ResourceArc<StoreResource>, key: String) -> Term<'_> {
    encode_guarded(env, Operation::StoreHead, || store.head(&key))
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_read_range<'a>(
    env: Env<'a>,
    store: ResourceArc<StoreResource>,
    key: String,
    offset: u64,
    length: u64,
) -> Term<'a> {
    match guarded(Operation::StoreReadRange, || {
        store.read_range(&key, offset, length)
    }) {
        Ok(bytes) => match OwnedBinary::new(bytes.len()) {
            Some(mut binary) => {
                binary.as_mut_slice().copy_from_slice(&bytes);
                (atoms::ok(), binary.release(env)).encode(env)
            }
            None => (
                atoms::error(),
                NativeFailure::expected(Operation::StoreReadRange, "binary allocation failed")
                    .payload(),
            )
                .encode(env),
        },
        Err(failure) => (atoms::error(), failure.payload()).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_list(env: Env<'_>, store: ResourceArc<StoreResource>, prefix: String) -> Term<'_> {
    encode_guarded(env, Operation::StoreList, || store.list(&prefix))
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_delete(env: Env<'_>, store: ResourceArc<StoreResource>, key: String) -> Term<'_> {
    encode_guarded(env, Operation::StoreDelete, || {
        store.delete(&key)?;
        Ok(atoms::deleted())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_writer_open(
    env: Env<'_>,
    store: ResourceArc<StoreResource>,
    key: String,
    flush: rustler::Atom,
    sync: rustler::Atom,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::StoreWriterOpen, || {
        let (flush, sync) = write_policies(flush, sync, Operation::StoreWriterOpen)?;
        let cancellation = std::sync::Arc::new(CancellationToken::default());
        let writer = store.open_writer(&key, flush, sync, cancellation.clone())?;
        let resource = ResourceArc::new(StoreWriterResource {
            writer: Mutex::new(writer),
            cancellation,
        });
        if env.monitor(&resource, &owner).is_none() {
            lock_store_writer(&resource, Operation::StoreWriterOpen)?.abort()?;
            return Err(NativeFailure::expected(
                Operation::StoreWriterOpen,
                "could not monitor store writer owner",
            ));
        }
        Ok(resource)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_writer_write<'a>(
    env: Env<'a>,
    writer: ResourceArc<StoreWriterResource>,
    data: Binary<'a>,
) -> Term<'a> {
    encode_guarded(env, Operation::StoreWriterWrite, || {
        lock_store_writer(&writer, Operation::StoreWriterWrite)?.write(data.as_slice())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_writer_publish(env: Env<'_>, writer: ResourceArc<StoreWriterResource>) -> Term<'_> {
    encode_guarded(env, Operation::StoreWriterPublish, || {
        lock_store_writer(&writer, Operation::StoreWriterPublish)?.publish()
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn store_writer_abort(env: Env<'_>, writer: ResourceArc<StoreWriterResource>) -> Term<'_> {
    encode_guarded(env, Operation::StoreWriterAbort, || {
        let aborted = lock_store_writer(&writer, Operation::StoreWriterAbort)?.abort()?;
        Ok(if aborted {
            atoms::aborted()
        } else {
            atoms::closed()
        })
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_head(env: Env<'_>, path: String, allowed_root: Option<String>) -> Term<'_> {
    encode_guarded(env, Operation::Head, || {
        let cancellation = CancellationToken::default();
        LocalStore
            .head(&location(path, allowed_root), &cancellation)
            .map(NativeMetadata::from)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_read_range<'a>(
    env: Env<'a>,
    path: String,
    allowed_root: Option<String>,
    offset: u64,
    length: u64,
) -> Term<'a> {
    match guarded(Operation::ReadRange, || {
        let cancellation = CancellationToken::default();
        LocalStore.read_range(
            &location(path, allowed_root),
            ByteRange { offset, length },
            &cancellation,
        )
    }) {
        Ok(bytes) => match OwnedBinary::new(bytes.len()) {
            Some(mut binary) => {
                binary.as_mut_slice().copy_from_slice(&bytes);
                (atoms::ok(), binary.release(env)).encode(env)
            }
            None => (
                atoms::error(),
                NativeFailure::expected(Operation::ReadRange, "binary allocation failed").payload(),
            )
                .encode(env),
        },
        Err(failure) => (atoms::error(), failure.payload()).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_list(
    env: Env<'_>,
    path: String,
    allowed_root: Option<String>,
    prefix: String,
) -> Term<'_> {
    encode_guarded(env, Operation::List, || {
        let cancellation = CancellationToken::default();
        LocalStore
            .list(&location(path, allowed_root), &prefix, &cancellation)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(NativeMetadata::from)
                    .collect::<Vec<_>>()
            })
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_delete(env: Env<'_>, path: String, allowed_root: Option<String>) -> Term<'_> {
    encode_guarded(env, Operation::Delete, || {
        let cancellation = CancellationToken::default();
        LocalStore.delete(&location(path, allowed_root), &cancellation)?;
        Ok(atoms::deleted())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_writer_open(
    env: Env<'_>,
    path: String,
    allowed_root: Option<String>,
    flush: rustler::Atom,
    sync: rustler::Atom,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::WriterOpen, || {
        let flush = if flush == atoms::none() {
            FlushPolicy::None
        } else if flush == atoms::each_chunk() {
            FlushPolicy::EachChunk
        } else if flush == atoms::before_publish() {
            FlushPolicy::BeforePublish
        } else {
            return Err(NativeFailure::invalid(
                Operation::WriterOpen,
                "invalid flush policy",
            ));
        };
        let sync = if sync == atoms::none() {
            SyncPolicy::None
        } else if sync == atoms::data() {
            SyncPolicy::Data
        } else if sync == atoms::all() {
            SyncPolicy::All
        } else {
            return Err(NativeFailure::invalid(
                Operation::WriterOpen,
                "invalid sync policy",
            ));
        };
        let writer = LocalStore.stage(
            &location(path, allowed_root),
            WriteOptions { flush, sync },
            std::sync::Arc::new(CancellationToken::default()),
        )?;
        let resource = ResourceArc::new(WriterResource {
            writer: Mutex::new(writer),
        });
        if env.monitor(&resource, &owner).is_none() {
            lock_writer(&resource, Operation::WriterOpen)?.abort()?;
            return Err(NativeFailure::expected(
                Operation::WriterOpen,
                "could not monitor writer owner",
            ));
        }
        Ok(resource)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_writer_write<'a>(
    env: Env<'a>,
    writer: ResourceArc<WriterResource>,
    data: Binary<'a>,
) -> Term<'a> {
    encode_guarded(env, Operation::WriterWrite, || {
        StagedWrite::write(
            &mut *lock_writer(&writer, Operation::WriterWrite)?,
            data.as_slice(),
        )
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_writer_publish(env: Env<'_>, writer: ResourceArc<WriterResource>) -> Term<'_> {
    encode_guarded(env, Operation::WriterPublish, || {
        lock_writer(&writer, Operation::WriterPublish)?
            .publish()
            .map(NativeMetadata::from)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_writer_abort(env: Env<'_>, writer: ResourceArc<WriterResource>) -> Term<'_> {
    encode_guarded(env, Operation::WriterAbort, || {
        let aborted = lock_writer(&writer, Operation::WriterAbort)?.abort()?;
        Ok(if aborted {
            atoms::aborted()
        } else {
            atoms::closed()
        })
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_head(env: Env<'_>, config: S3Config) -> Term<'_> {
    encode_guarded(env, Operation::S3Head, || {
        let cancellation = CancellationToken::default();
        RemoteObject::new(config)?
            .head(&cancellation, Operation::S3Head)
            .map(NativeMetadata::from)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_read_range<'a>(env: Env<'a>, config: S3Config, offset: u64, length: u64) -> Term<'a> {
    match guarded(Operation::S3ReadRange, || {
        let length = usize::try_from(length).map_err(|_| {
            NativeFailure::invalid(Operation::S3ReadRange, "S3 range length is too large")
        })?;
        let cancellation = CancellationToken::default();
        RemoteObject::new(config)?.read_range(offset, length, &cancellation, Operation::S3ReadRange)
    }) {
        Ok(bytes) => match OwnedBinary::new(bytes.len()) {
            Some(mut binary) => {
                binary.as_mut_slice().copy_from_slice(&bytes);
                (atoms::ok(), binary.release(env)).encode(env)
            }
            None => (
                atoms::error(),
                NativeFailure::expected(Operation::S3ReadRange, "binary allocation failed")
                    .payload(),
            )
                .encode(env),
        },
        Err(failure) => (atoms::error(), failure.payload()).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_list(env: Env<'_>, config: S3Config, prefix: String) -> Term<'_> {
    encode_guarded(env, Operation::S3List, || {
        let cancellation = CancellationToken::default();
        RemoteObject::new(config)?
            .list(&prefix, &cancellation)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(NativeMetadata::from)
                    .collect::<Vec<_>>()
            })
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_delete(env: Env<'_>, config: S3Config) -> Term<'_> {
    encode_guarded(env, Operation::S3Delete, || {
        let cancellation = CancellationToken::default();
        RemoteObject::new(config)?.delete(&cancellation)?;
        Ok(atoms::deleted())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_writer_open(env: Env<'_>, config: S3Config, owner: LocalPid) -> Term<'_> {
    encode_guarded(env, Operation::S3WriterOpen, || {
        let cancellation = std::sync::Arc::new(CancellationToken::default());
        let writer = RemoteObject::new(config)?.open_multipart(cancellation.clone())?;
        let resource = ResourceArc::new(S3WriterResource {
            writer: Mutex::new(writer),
            cancellation,
        });
        if env.monitor(&resource, &owner).is_none() {
            lock_s3_writer(&resource, Operation::S3WriterOpen)?.abort()?;
            return Err(NativeFailure::expected(
                Operation::S3WriterOpen,
                "could not monitor S3 writer owner",
            ));
        }
        Ok(resource)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_writer_write<'a>(
    env: Env<'a>,
    writer: ResourceArc<S3WriterResource>,
    data: Binary<'a>,
) -> Term<'a> {
    encode_guarded(env, Operation::S3WriterWrite, || {
        let written = Write::write(
            &mut *lock_s3_writer(&writer, Operation::S3WriterWrite)?,
            data.as_slice(),
        )
        .map_err(|_| NativeFailure::expected(Operation::S3WriterWrite, "S3 write failed"))?;
        Ok(written)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_writer_publish(env: Env<'_>, writer: ResourceArc<S3WriterResource>) -> Term<'_> {
    encode_guarded(env, Operation::S3WriterPublish, || {
        lock_s3_writer(&writer, Operation::S3WriterPublish)?
            .publish()
            .map(NativeMetadata::from)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_writer_abort(env: Env<'_>, writer: ResourceArc<S3WriterResource>) -> Term<'_> {
    encode_guarded(env, Operation::S3WriterAbort, || {
        let aborted = lock_s3_writer(&writer, Operation::S3WriterAbort)?.abort()?;
        Ok(if aborted {
            atoms::aborted()
        } else {
            atoms::closed()
        })
    })
}

#[rustler::nif]
fn resource_snapshot(env: Env<'_>) -> Term<'_> {
    encode_guarded(env, Operation::ResourceSnapshot, || {
        let (active_writers, bytes_read) = local::resource_snapshot();
        let (
            active_s3_requests,
            peak_s3_requests,
            active_multipart_uploads,
            s3_range_requests,
            s3_range_bytes,
            s3_clients_created,
        ) = s3::resource_snapshot();
        let (active_stores, stores_created) = store::resource_snapshot();
        Ok(ResourceSnapshot {
            active_writers,
            active_readers: reader::active_readers(),
            bytes_read,
            active_s3_requests,
            peak_s3_requests,
            active_multipart_uploads,
            s3_range_requests,
            s3_range_bytes,
            active_stores,
            stores_created,
            s3_clients_created,
        })
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn reader_open(
    env: Env<'_>,
    path: String,
    allowed_root: Option<String>,
    options: NativeReaderOptions,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::ReaderOpen, || {
        let (reader, fields) = reader::open(
            location(path, allowed_root),
            options.max_range_bytes,
            options.batch_size,
            options.prefetch_depth,
            options.columns,
            options.predicate,
        )?;
        let resource = ResourceArc::new(reader);
        if env.monitor(&resource, &owner).is_none() {
            resource.close()?;
            return Err(NativeFailure::expected(
                Operation::ReaderOpen,
                "could not monitor reader owner",
            ));
        }
        Ok((resource, fields))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn reader_open_s3(
    env: Env<'_>,
    config: S3Config,
    options: NativeReaderOptions,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::ReaderOpen, || {
        let (reader, fields) = reader::open_s3(
            config,
            options.max_range_bytes,
            options.batch_size,
            options.prefetch_depth,
            options.columns,
            options.predicate,
        )?;
        let resource = ResourceArc::new(reader);
        if env.monitor(&resource, &owner).is_none() {
            resource.close()?;
            return Err(NativeFailure::expected(
                Operation::ReaderOpen,
                "could not monitor reader owner",
            ));
        }
        Ok((resource, fields))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn reader_open_store(
    env: Env<'_>,
    store: ResourceArc<StoreResource>,
    key: String,
    options: NativeReaderOptions,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::ReaderOpen, || {
        let (reader, fields) = reader::open_store(
            store,
            key,
            options.max_range_bytes,
            options.batch_size,
            options.prefetch_depth,
            options.columns,
            options.predicate,
        )?;
        let resource = ResourceArc::new(reader);
        if env.monitor(&resource, &owner).is_none() {
            resource.close()?;
            return Err(NativeFailure::expected(
                Operation::ReaderOpen,
                "could not monitor reader owner",
            ));
        }
        Ok((resource, fields))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn reader_next(env: Env<'_>, resource: ResourceArc<reader::ReaderResource>) -> Term<'_> {
    match guarded(Operation::ReaderNext, || resource.next_batch()) {
        Ok(Some(batch)) => {
            match guarded(Operation::ReaderNext, || reader::encode_batch(env, &batch)) {
                Ok(encoded) => (atoms::ok(), encoded).encode(env),
                Err(failure) => (atoms::error(), failure.payload()).encode(env),
            }
        }
        Ok(None) => (atoms::ok(), atoms::eof()).encode(env),
        Err(failure) => (atoms::error(), failure.payload()).encode(env),
    }
}

#[rustler::nif]
fn reader_close(env: Env<'_>, resource: ResourceArc<reader::ReaderResource>) -> Term<'_> {
    encode_guarded(env, Operation::ReaderClose, || {
        resource.close()?;
        Ok(atoms::closed())
    })
}

#[rustler::nif]
fn reader_stats(env: Env<'_>, resource: ResourceArc<reader::ReaderResource>) -> Term<'_> {
    encode_guarded(env, Operation::ReaderStats, || resource.stats())
}

#[rustler::nif(schedule = "DirtyIo")]
fn parquet_writer_open(
    env: Env<'_>,
    path: String,
    allowed_root: Option<String>,
    schema: Vec<writer::InputField>,
    options: writer::NativeWriterOptions,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::ParquetWriterOpen, || {
        let resource =
            ResourceArc::new(writer::open(location(path, allowed_root), schema, options)?);
        if env.monitor(&resource, &owner).is_none() {
            resource.abort()?;
            return Err(NativeFailure::expected(
                Operation::ParquetWriterOpen,
                "could not monitor Parquet writer owner",
            ));
        }
        Ok(resource)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn parquet_writer_open_s3(
    env: Env<'_>,
    config: S3Config,
    schema: Vec<writer::InputField>,
    options: writer::NativeWriterOptions,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::ParquetWriterOpen, || {
        let resource = ResourceArc::new(writer::open_s3(config, schema, options)?);
        if env.monitor(&resource, &owner).is_none() {
            resource.abort()?;
            return Err(NativeFailure::expected(
                Operation::ParquetWriterOpen,
                "could not monitor Parquet writer owner",
            ));
        }
        Ok(resource)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn parquet_writer_open_store(
    env: Env<'_>,
    store: ResourceArc<StoreResource>,
    key: String,
    schema: Vec<writer::InputField>,
    options: writer::NativeWriterOptions,
    owner: LocalPid,
) -> Term<'_> {
    encode_guarded(env, Operation::ParquetWriterOpen, || {
        let resource = ResourceArc::new(writer::open_store(&store, &key, schema, options)?);
        if env.monitor(&resource, &owner).is_none() {
            resource.abort()?;
            return Err(NativeFailure::expected(
                Operation::ParquetWriterOpen,
                "could not monitor Parquet writer owner",
            ));
        }
        Ok(resource)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn parquet_writer_write<'a>(
    env: Env<'a>,
    resource: ResourceArc<writer::ParquetWriterResource>,
    batch: Term<'a>,
) -> Term<'a> {
    encode_guarded(env, Operation::ParquetWriterWrite, || {
        resource.write_batch(batch)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn parquet_writer_close(
    env: Env<'_>,
    resource: ResourceArc<writer::ParquetWriterResource>,
) -> Term<'_> {
    encode_guarded(env, Operation::ParquetWriterClose, || {
        resource.close().map(NativeMetadata::from)
    })
}

#[rustler::nif]
fn parquet_writer_abort(
    env: Env<'_>,
    resource: ResourceArc<writer::ParquetWriterResource>,
) -> Term<'_> {
    encode_guarded(env, Operation::ParquetWriterAbort, || {
        Ok(if resource.abort()? {
            atoms::aborted()
        } else {
            atoms::closed()
        })
    })
}

#[rustler::nif]
fn parquet_writer_stats(
    env: Env<'_>,
    resource: ResourceArc<writer::ParquetWriterResource>,
) -> Term<'_> {
    encode_guarded(env, Operation::ParquetWriterStats, || resource.stats())
}

rustler::init!("Elixir.Parquex.Native");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guarded_returns_success() {
        assert_eq!(guarded(Operation::Smoke, || Ok(7_u32)), Ok(7));
    }

    #[test]
    fn guarded_preserves_expected_failure() {
        let failure = NativeFailure::expected(Operation::SmokeError, "expected");

        assert_eq!(
            guarded::<(), _>(Operation::SmokeError, || Err(failure)),
            Err(NativeFailure::expected(Operation::SmokeError, "expected"))
        );
    }

    #[test]
    fn guarded_contains_panics() {
        let result = guarded::<(), _>(Operation::Smoke, || panic!("contained test panic"));

        assert_eq!(result, Err(NativeFailure::panic(Operation::Smoke)));
    }
}
