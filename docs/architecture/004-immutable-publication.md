# ADR 004: Immutable publication

- Status: accepted
- Context: partially written Parquet files lack a valid footer, and replacing a
  completed object is unsafe for append-oriented workflows.
- Decision: writers stage a new file or object, finish and close Parquet output,
  then publish with create-only semantics. Failure or cancellation removes local
  staging and aborts multipart work where possible.
- Consequence: Parquex never appends bytes to or silently replaces a completed
  destination. Append-oriented use creates uniquely named immutable objects.
