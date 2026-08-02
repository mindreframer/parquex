use std::panic::{catch_unwind, UnwindSafe};

use rustler::{Encoder, Env, Term};

mod atoms {
    rustler::atoms! {
        error,
        native_failure,
        native_smoke,
        native_smoke_error,
        ok
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Smoke,
    SmokeError,
}

impl Operation {
    fn atom(self) -> rustler::Atom {
        match self {
            Self::Smoke => atoms::native_smoke(),
            Self::SmokeError => atoms::native_smoke_error(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct NativeFailure {
    operation: Operation,
    message: &'static str,
}

impl NativeFailure {
    fn expected(operation: Operation, message: &'static str) -> Self {
        Self { operation, message }
    }

    fn panic(operation: Operation) -> Self {
        Self {
            operation,
            message: "native operation failed safely",
        }
    }

    fn payload(self) -> NativeErrorPayload {
        NativeErrorPayload {
            category: atoms::native_failure(),
            operation: self.operation.atom(),
            message: self.message.to_owned(),
            retryable: false,
        }
    }
}

#[derive(rustler::NifMap)]
struct NativeErrorPayload {
    category: rustler::Atom,
    operation: rustler::Atom,
    message: String,
    retryable: bool,
}

fn guarded<T, F>(operation: Operation, work: F) -> Result<T, NativeFailure>
where
    F: FnOnce() -> Result<T, NativeFailure> + UnwindSafe,
{
    match catch_unwind(work) {
        Ok(result) => result,
        Err(_) => Err(NativeFailure::panic(operation)),
    }
}

fn encode_guarded<'a, T, F>(env: Env<'a>, operation: Operation, work: F) -> Term<'a>
where
    T: Encoder,
    F: FnOnce() -> Result<T, NativeFailure> + UnwindSafe,
{
    match guarded(operation, work) {
        Ok(value) => (atoms::ok(), value).encode(env),
        Err(failure) => (atoms::error(), failure.payload()).encode(env),
    }
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
