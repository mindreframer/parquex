use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

use rustler::{Binary, Encoder, Env, LocalPid, Monitor, OwnedBinary, Resource, ResourceArc, Term};

mod error;
mod local;
mod object;

use error::NativeFailure;
use local::{LocalStore, LocalWriter};
use object::{
    ByteRange, CancellationToken, FlushPolicy, ObjectLocation, ObjectMetadata, ObjectStore,
    StagedWrite, SyncPolicy, WriteOptions,
};

pub(crate) mod atoms {
    rustler::atoms! {
        aborted,
        all,
        before_publish,
        cancelled,
        closed,
        conflict,
        data,
        deleted,
        each_chunk,
        error,
        invalid_argument,
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
        resource_snapshot
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

#[derive(rustler::NifMap)]
struct ResourceSnapshot {
    active_writers: usize,
    bytes_read: u64,
}

struct WriterResource {
    writer: Mutex<LocalWriter>,
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
        lock_writer(&writer, Operation::WriterWrite)?.write(data.as_slice())
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

#[rustler::nif]
fn resource_snapshot(env: Env<'_>) -> Term<'_> {
    encode_guarded(env, Operation::ResourceSnapshot, || {
        let (active_writers, bytes_read) = local::resource_snapshot();
        Ok(ResourceSnapshot {
            active_writers,
            bytes_read,
        })
    })
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
