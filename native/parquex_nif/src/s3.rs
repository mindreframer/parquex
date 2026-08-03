use std::fmt::Debug;
use std::future::Future;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures_util::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3CopyIfNotExists};
use object_store::client::ClientOptions;
use object_store::limit::LimitStore;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, RetryConfig, WriteMultipart};
use rustler::Atom;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use crate::error::{Category, NativeFailure};
use crate::object::CancellationToken;
use crate::{atoms, Operation};

static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
static ACTIVE_REQUESTS: AtomicUsize = AtomicUsize::new(0);
static PEAK_REQUESTS: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_MULTIPART: AtomicUsize = AtomicUsize::new(0);
static RANGE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static RANGE_BYTES: AtomicU64 = AtomicU64::new(0);
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static CLIENTS_CREATED: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, rustler::NifMap)]
pub(crate) struct S3Config {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) region: String,
    pub(crate) path_style: bool,
    pub(crate) tls: bool,
    pub(crate) request_timeout_ms: u64,
    pub(crate) max_retries: usize,
    pub(crate) credential_provider: Atom,
    pub(crate) access_key_id: Option<String>,
    pub(crate) secret_access_key: Option<String>,
    pub(crate) session_token: Option<String>,
    pub(crate) max_request_concurrency: usize,
    pub(crate) multipart_part_size: usize,
    pub(crate) max_in_flight_parts: usize,
}

#[derive(Clone, rustler::NifMap)]
pub(crate) struct RemoteMetadata {
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) modified_unix_ns: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct RemoteObject {
    store: Arc<dyn ObjectStore>,
    path: Path,
    config: S3Config,
}

#[derive(Clone)]
pub(crate) struct RemoteStore {
    store: Arc<dyn ObjectStore>,
    config: S3Config,
}

impl Debug for RemoteStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteStore")
            .field("bucket", &self.config.bucket)
            .field("base_prefix", &self.config.key)
            .finish_non_exhaustive()
    }
}

impl RemoteStore {
    pub(crate) fn new(config: S3Config) -> Result<Self, NativeFailure> {
        let store = build_store(&config)?;
        Ok(Self { store, config })
    }

    pub(crate) fn object(&self, key: &str) -> Result<RemoteObject, NativeFailure> {
        let combined = join_key(&self.config.key, key);
        let path = Path::parse(&combined)
            .map_err(|_| NativeFailure::invalid(Operation::StoreOpen, "object key is invalid"))?;
        let mut config = self.config.clone();
        config.key = combined;
        Ok(RemoteObject {
            store: self.store.clone(),
            path,
            config,
        })
    }

    pub(crate) fn base_prefix(&self) -> &str {
        &self.config.key
    }

    pub(crate) fn multipart_buffer_limit_bytes(&self) -> usize {
        self.config
            .max_in_flight_parts
            .saturating_add(1)
            .saturating_mul(self.config.multipart_part_size)
    }

    pub(crate) fn list(
        &self,
        prefix: &str,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RemoteMetadata>, NativeFailure> {
        let mut entries = self.object("")?.list(prefix, cancellation)?;
        let base = self.base_prefix().trim_matches('/');
        if !base.is_empty() {
            let marker = format!("{base}/");
            for entry in &mut entries {
                entry.key = entry
                    .key
                    .strip_prefix(&marker)
                    .unwrap_or(&entry.key)
                    .to_owned();
            }
        }
        Ok(entries)
    }
}

impl Debug for RemoteObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteObject")
            .field("bucket", &self.config.bucket)
            .field("key", &self.config.key)
            .finish_non_exhaustive()
    }
}

impl RemoteObject {
    pub(crate) fn head(
        &self,
        cancellation: &CancellationToken,
        operation: Operation,
    ) -> Result<RemoteMetadata, NativeFailure> {
        cancellation.check(operation)?;
        let metadata = remote_call(operation, Some(cancellation), || {
            self.store.head(&self.path)
        })?;
        cancellation.check(operation)?;
        Ok(remote_metadata(metadata))
    }

    pub(crate) fn read_range(
        &self,
        offset: u64,
        length: usize,
        cancellation: &CancellationToken,
        operation: Operation,
    ) -> Result<Vec<u8>, NativeFailure> {
        cancellation.check(operation)?;
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| NativeFailure::invalid(operation, "S3 range overflow"))?;
        if length == 0 {
            return Ok(Vec::new());
        }
        let bytes = remote_call(operation, Some(cancellation), || {
            self.store.get_range(&self.path, offset..end)
        })?;
        cancellation.check(operation)?;
        RANGE_REQUESTS.fetch_add(1, Ordering::Relaxed);
        RANGE_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes.to_vec())
    }

    pub(crate) fn list(
        &self,
        explicit_prefix: &str,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RemoteMetadata>, NativeFailure> {
        cancellation.check(Operation::S3List)?;
        let combined = join_key(&self.config.key, explicit_prefix);
        let prefix = if combined.is_empty() {
            None
        } else {
            Some(Path::parse(combined).map_err(|_| {
                NativeFailure::invalid(Operation::S3List, "S3 list prefix is invalid")
            })?)
        };
        let mut entries = remote_call(Operation::S3List, Some(cancellation), || {
            self.store
                .list(prefix.as_ref())
                .map_ok(remote_metadata)
                .try_collect::<Vec<_>>()
        })?;
        cancellation.check(Operation::S3List)?;
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(entries)
    }

    pub(crate) fn delete(&self, cancellation: &CancellationToken) -> Result<(), NativeFailure> {
        cancellation.check(Operation::S3Delete)?;
        remote_call(Operation::S3Delete, Some(cancellation), || {
            self.store.delete(&self.path)
        })?;
        cancellation.check(Operation::S3Delete)
    }

    pub(crate) fn open_multipart(
        &self,
        cancellation: Arc<CancellationToken>,
    ) -> Result<RemoteMultipartWriter, NativeFailure> {
        let sequence = STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let (parent, name) = self
            .config
            .key
            .rsplit_once('/')
            .map_or(("", self.config.key.as_str()), |(parent, name)| {
                (parent, name)
            });
        let stage_name = format!(".{name}.parquex-{}-{sequence}.tmp", std::process::id());
        let stage_key = if parent.is_empty() {
            stage_name
        } else {
            format!("{parent}/{stage_name}")
        };
        let stage_path = Path::parse(stage_key).map_err(|_| {
            NativeFailure::invalid(Operation::S3WriterOpen, "S3 staging key is invalid")
        })?;
        let upload = remote_call(Operation::S3WriterOpen, Some(&cancellation), || {
            self.store.put_multipart(&stage_path)
        })?;
        ACTIVE_MULTIPART.fetch_add(1, Ordering::Relaxed);
        Ok(RemoteMultipartWriter {
            store: self.store.clone(),
            destination: self.path.clone(),
            stage: stage_path,
            upload: Some(WriteMultipart::new_with_chunk_size(
                upload,
                self.config.multipart_part_size,
            )),
            cancellation,
            max_in_flight_parts: self.config.max_in_flight_parts,
            active: true,
            bytes_written: 0,
        })
    }
}

pub(crate) struct RemoteMultipartWriter {
    store: Arc<dyn ObjectStore>,
    destination: Path,
    stage: Path,
    upload: Option<WriteMultipart>,
    cancellation: Arc<CancellationToken>,
    max_in_flight_parts: usize,
    active: bool,
    bytes_written: u64,
}

impl RemoteMultipartWriter {
    pub(crate) fn publish(&mut self) -> Result<RemoteMetadata, NativeFailure> {
        self.cancellation.check(Operation::S3WriterPublish)?;
        let upload = self
            .upload
            .take()
            .ok_or_else(|| NativeFailure::cancelled(Operation::S3WriterPublish))?;
        let completed = remote_call(Operation::S3WriterPublish, Some(&self.cancellation), || {
            upload.finish()
        });
        if let Err(error) = completed {
            let _cleanup = remote_call(Operation::S3WriterAbort, None, || {
                self.store.delete(&self.stage)
            });
            self.finish_active();
            return Err(error);
        }

        let copy_result = remote_call(Operation::S3WriterPublish, Some(&self.cancellation), || {
            self.store
                .copy_if_not_exists(&self.stage, &self.destination)
        });
        let cleanup_result = remote_call(Operation::S3WriterAbort, None, || {
            self.store.delete(&self.stage)
        });
        self.finish_active();

        match publication_result(copy_result, cleanup_result) {
            Ok(()) => {
                let metadata =
                    remote_call(Operation::S3WriterPublish, Some(&self.cancellation), || {
                        self.store.head(&self.destination)
                    })?;
                Ok(remote_metadata(metadata))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn abort(&mut self) -> Result<bool, NativeFailure> {
        if !self.active {
            return Ok(false);
        }
        self.cancellation.cancel();
        let abort_result = match self.upload.take() {
            Some(upload) => remote_call(Operation::S3WriterAbort, None, || upload.abort()),
            None => Ok(()),
        };
        let _cleanup = remote_call(Operation::S3WriterAbort, None, || {
            self.store.delete(&self.stage)
        });
        self.finish_active();
        abort_result.map(|()| true)
    }

    fn finish_active(&mut self) {
        if self.active {
            self.active = false;
            ACTIVE_MULTIPART.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

fn publication_result(
    copy_result: Result<(), NativeFailure>,
    cleanup_result: Result<(), NativeFailure>,
) -> Result<(), NativeFailure> {
    match (copy_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), _cleanup) => Err(primary),
        (Ok(()), Err(_cleanup)) => Err(NativeFailure::expected(
            Operation::S3WriterPublish,
            "S3 destination published but staging cleanup failed",
        )),
    }
}

impl Write for RemoteMultipartWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.cancellation
            .check(Operation::S3WriterWrite)
            .map_err(|_| io::Error::other("S3 multipart write was cancelled"))?;
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| io::Error::other("S3 multipart writer is closed"))?;
        runtime()
            .map_err(|_| io::Error::other("S3 runtime is unavailable"))?
            .block_on(async {
                tokio::select! {
                    result = upload.wait_for_capacity(self.max_in_flight_parts) => {
                        result.map_err(|_| io::Error::other("S3 multipart capacity failed"))
                    }
                    () = self.cancellation.cancelled() => {
                        Err(io::Error::other("S3 multipart write was cancelled"))
                    }
                }
            })?;
        {
            let _runtime_guard = runtime()
                .map_err(|_| io::Error::other("S3 runtime is unavailable"))?
                .enter();
            upload.write(buffer);
        }
        self.bytes_written += buffer.len() as u64;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.cancellation
            .check(Operation::S3WriterWrite)
            .map_err(|_| io::Error::other("S3 multipart write was cancelled"))
    }
}

impl Drop for RemoteMultipartWriter {
    fn drop(&mut self) {
        if self.active {
            let _ignored = self.abort();
        }
    }
}

pub(crate) fn resource_snapshot() -> (usize, usize, usize, u64, u64, u64) {
    (
        ACTIVE_REQUESTS.load(Ordering::Relaxed),
        PEAK_REQUESTS.load(Ordering::Relaxed),
        ACTIVE_MULTIPART.load(Ordering::Relaxed),
        RANGE_REQUESTS.load(Ordering::Relaxed),
        RANGE_BYTES.load(Ordering::Relaxed),
        CLIENTS_CREATED.load(Ordering::Relaxed),
    )
}

fn build_store(config: &S3Config) -> Result<Arc<dyn ObjectStore>, NativeFailure> {
    let mut builder = if config.credential_provider == atoms::standard() {
        AmazonS3Builder::from_env()
    } else if config.credential_provider == atoms::explicit() {
        let access_key = config.access_key_id.clone().ok_or_else(|| {
            NativeFailure::invalid(
                Operation::StoreOpen,
                "explicit S3 credentials are incomplete",
            )
        })?;
        let secret_key = config.secret_access_key.clone().ok_or_else(|| {
            NativeFailure::invalid(
                Operation::StoreOpen,
                "explicit S3 credentials are incomplete",
            )
        })?;
        let mut builder = AmazonS3Builder::new()
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key);
        if let Some(token) = &config.session_token {
            builder = builder.with_token(token);
        }
        builder
    } else {
        return Err(NativeFailure::invalid(
            Operation::S3Head,
            "S3 credential provider is invalid",
        ));
    };

    let timeout = Duration::from_millis(config.request_timeout_ms);
    builder = builder
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_allow_http(!config.tls)
        .with_virtual_hosted_style_request(!config.path_style)
        .with_retry(RetryConfig {
            max_retries: config.max_retries,
            retry_timeout: timeout,
            ..Default::default()
        })
        .with_client_options(
            ClientOptions::new()
                .with_timeout(timeout)
                .with_allow_http(!config.tls),
        )
        .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    let store = builder.build().map_err(|_| {
        NativeFailure::invalid(Operation::S3Head, "S3 client configuration is invalid")
    })?;
    CLIENTS_CREATED.fetch_add(1, Ordering::Relaxed);
    Ok(Arc::new(LimitStore::new(
        store,
        config.max_request_concurrency,
    )))
}

fn runtime() -> Result<&'static Runtime, NativeFailure> {
    match RUNTIME.get_or_init(|| {
        RuntimeBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("parquex-s3")
            .build()
            .map_err(|_| "runtime initialization failed".to_owned())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(_message) => Err(NativeFailure::expected(
            Operation::S3Head,
            "S3 runtime is unavailable",
        )),
    }
}

fn remote_call<T, F>(
    operation: Operation,
    cancellation: Option<&CancellationToken>,
    work: impl FnOnce() -> F,
) -> Result<T, NativeFailure>
where
    F: Future<Output = Result<T, object_store::Error>>,
{
    let runtime = runtime()?;
    let active = ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed) + 1;
    PEAK_REQUESTS.fetch_max(active, Ordering::Relaxed);
    let result = runtime.block_on(async {
        match cancellation {
            Some(cancellation) => {
                tokio::select! {
                    result = work() => result.map_err(|error| remote_error(operation, error)),
                    () = cancellation.cancelled() => Err(NativeFailure::cancelled(operation)),
                }
            }
            None => work().await.map_err(|error| remote_error(operation, error)),
        }
    });
    ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);
    result
}

fn remote_error(operation: Operation, error: object_store::Error) -> NativeFailure {
    use object_store::Error;
    let (category, message, retryable) = match error {
        Error::NotFound { .. } => (Category::NotFound, "S3 object not found", false),
        Error::AlreadyExists { .. } | Error::Precondition { .. } => {
            (Category::Conflict, "S3 destination already exists", false)
        }
        Error::PermissionDenied { .. } | Error::Unauthenticated { .. } => (
            Category::PermissionDenied,
            "S3 object access was denied",
            false,
        ),
        Error::NotSupported { .. } | Error::NotImplemented { .. } => {
            (Category::Unsupported, "S3 operation is unsupported", false)
        }
        Error::InvalidPath { .. } | Error::UnknownConfigurationKey { .. } => {
            (Category::InvalidArgument, "S3 request is invalid", false)
        }
        _ => (
            Category::NativeFailure,
            "S3 operation failed after bounded retries",
            true,
        ),
    };
    NativeFailure {
        category,
        operation,
        message: message.to_owned(),
        retryable,
    }
}

fn remote_metadata(metadata: object_store::ObjectMeta) -> RemoteMetadata {
    RemoteMetadata {
        key: metadata.location.to_string(),
        size: metadata.size,
        modified_unix_ns: metadata
            .last_modified
            .timestamp_nanos_opt()
            .and_then(|value| u64::try_from(value).ok()),
    }
}

fn join_key(base: &str, suffix: &str) -> String {
    match (base.trim_matches('/'), suffix.trim_matches('/')) {
        ("", suffix) => suffix.to_owned(),
        (base, "") => base.to_owned(),
        (base, suffix) => format!("{base}/{suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_cleanup_never_hides_the_primary_failure() {
        let primary = NativeFailure::expected(Operation::S3WriterPublish, "primary");
        let cleanup = NativeFailure::expected(Operation::S3WriterAbort, "cleanup");

        assert_eq!(
            publication_result(Err(primary), Err(cleanup)),
            Err(NativeFailure::expected(
                Operation::S3WriterPublish,
                "primary"
            ))
        );
    }

    #[test]
    fn publication_reports_cleanup_failure_after_success() {
        let cleanup = NativeFailure::expected(Operation::S3WriterAbort, "cleanup");
        let error = publication_result(Ok(()), Err(cleanup)).unwrap_err();

        assert_eq!(error.operation, Operation::S3WriterPublish);
        assert_eq!(
            error.message,
            "S3 destination published but staging cleanup failed"
        );
    }
}
