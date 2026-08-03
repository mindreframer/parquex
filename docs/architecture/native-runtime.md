# Native lifecycle design

Native resources keep cancellation tokens and owner monitors. Completion, explicit cancellation, owner exit, early enumeration halt, and resource drop release the same underlying state.

Short validation and state transitions run as ordinary NIF calls. Parquet encoding and decoding use dirty CPU schedulers, local file work uses dirty I/O schedulers, and remote I/O uses a bounded managed runtime.

Each exported native entry point contains panics and returns a small error payload with category, operation, message, and retryability. The Elixir boundary converts that payload into `Parquex.Error`.
