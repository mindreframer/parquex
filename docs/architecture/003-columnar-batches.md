# ADR 003: Columnar batch boundary

- Status: accepted
- Context: expanding complete results into Elixir row maps defeats Parquet's
  columnar layout and multiplies allocation pressure.
- Decision: the native boundary keeps batches columnar for as long as practical,
  with an explicit schema and bounded row count. Projection happens before
  decoding when possible. Row access or conversion is explicit and limited to a
  batch.
- Consequence: schema and batch types remain narrow enough for a later query
  adapter, but Parquex does not introduce a data-frame or SQL abstraction.
