use std::io;

use crate::{atoms, Operation};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Category {
    Cancelled,
    Conflict,
    InvalidArgument,
    MalformedData,
    NativeFailure,
    NotFound,
    PermissionDenied,
    Timeout,
    Unsupported,
}

impl Category {
    fn atom(self) -> rustler::Atom {
        match self {
            Self::Cancelled => atoms::cancelled(),
            Self::Conflict => atoms::conflict(),
            Self::InvalidArgument => atoms::invalid_argument(),
            Self::MalformedData => atoms::malformed_data(),
            Self::NativeFailure => atoms::native_failure(),
            Self::NotFound => atoms::not_found(),
            Self::PermissionDenied => atoms::permission_denied(),
            Self::Timeout => atoms::timeout(),
            Self::Unsupported => atoms::unsupported(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct NativeFailure {
    pub(crate) category: Category,
    pub(crate) operation: Operation,
    pub(crate) message: String,
    pub(crate) retryable: bool,
}

impl NativeFailure {
    pub(crate) fn new(
        category: Category,
        operation: Operation,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category,
            operation,
            message: message.into(),
            retryable: false,
        }
    }

    pub(crate) fn expected(operation: Operation, message: impl Into<String>) -> Self {
        Self::new(Category::NativeFailure, operation, message)
    }

    pub(crate) fn invalid(operation: Operation, message: impl Into<String>) -> Self {
        Self::new(Category::InvalidArgument, operation, message)
    }

    pub(crate) fn cancelled(operation: Operation) -> Self {
        Self::new(Category::Cancelled, operation, "operation was cancelled")
    }

    pub(crate) fn panic(operation: Operation) -> Self {
        Self::new(
            Category::NativeFailure,
            operation,
            "native operation failed safely",
        )
    }

    pub(crate) fn from_io(operation: Operation, error: &io::Error) -> Self {
        let (category, message, retryable) = match error.kind() {
            io::ErrorKind::NotFound => (Category::NotFound, "object not found", false),
            io::ErrorKind::PermissionDenied => (
                Category::PermissionDenied,
                "object access was denied",
                false,
            ),
            io::ErrorKind::AlreadyExists => {
                (Category::Conflict, "destination already exists", false)
            }
            io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => (
                Category::InvalidArgument,
                "object request is invalid",
                false,
            ),
            io::ErrorKind::TimedOut => (Category::Timeout, "object operation timed out", true),
            io::ErrorKind::Unsupported => (
                Category::Unsupported,
                "object operation is unsupported",
                false,
            ),
            _ => (
                Category::NativeFailure,
                "local object operation failed",
                false,
            ),
        };

        Self {
            category,
            operation,
            message: message.to_owned(),
            retryable,
        }
    }

    pub(crate) fn payload(self) -> NativeErrorPayload {
        NativeErrorPayload {
            category: self.category.atom(),
            operation: self.operation.atom(),
            message: self.message,
            retryable: self.retryable,
        }
    }
}

#[derive(rustler::NifMap)]
pub(crate) struct NativeErrorPayload {
    category: rustler::Atom,
    operation: rustler::Atom,
    message: String,
    retryable: bool,
}
