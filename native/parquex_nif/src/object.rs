use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::NativeFailure;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObjectLocation {
    pub(crate) key: String,
    pub(crate) allowed_root: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObjectMetadata {
    pub(crate) path: String,
    pub(crate) size: u64,
    pub(crate) modified_unix_ns: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ByteRange {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlushPolicy {
    None,
    EachChunk,
    BeforePublish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncPolicy {
    None,
    Data,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WriteOptions {
    pub(crate) flush: FlushPolicy,
    pub(crate) sync: SyncPolicy,
}

#[derive(Debug, Default)]
pub(crate) struct CancellationToken {
    cancelled: AtomicBool,
    notification: tokio::sync::Notify,
}

impl CancellationToken {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notification.notify_waiters();
    }

    pub(crate) fn check(&self, operation: crate::Operation) -> Result<(), NativeFailure> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(NativeFailure::cancelled(operation))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn cancelled(&self) {
        if self.cancelled.load(Ordering::Acquire) {
            return;
        }
        self.notification.notified().await;
    }
}

pub(crate) trait StagedWrite {
    fn write(&mut self, bytes: &[u8]) -> Result<usize, NativeFailure>;
    fn publish(&mut self) -> Result<ObjectMetadata, NativeFailure>;
    fn abort(&mut self) -> Result<bool, NativeFailure>;
}

pub(crate) trait ObjectStore {
    type Writer: StagedWrite;

    fn head(
        &self,
        location: &ObjectLocation,
        cancellation: &CancellationToken,
    ) -> Result<ObjectMetadata, NativeFailure>;

    fn read_range(
        &self,
        location: &ObjectLocation,
        range: ByteRange,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, NativeFailure>;

    fn list(
        &self,
        location: &ObjectLocation,
        prefix: &str,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ObjectMetadata>, NativeFailure>;

    fn stage(
        &self,
        location: &ObjectLocation,
        options: WriteOptions,
        cancellation: Arc<CancellationToken>,
    ) -> Result<Self::Writer, NativeFailure>;

    fn delete(
        &self,
        location: &ObjectLocation,
        cancellation: &CancellationToken,
    ) -> Result<(), NativeFailure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_observable_without_waiting() {
        let cancellation = CancellationToken::default();
        assert!(cancellation.check(crate::Operation::ReadRange).is_ok());

        cancellation.cancel();

        assert_eq!(
            cancellation
                .check(crate::Operation::ReadRange)
                .unwrap_err()
                .category,
            crate::error::Category::Cancelled
        );
    }
}
