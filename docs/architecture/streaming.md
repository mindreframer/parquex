# Streaming design

Parquet readers are pull-based enumerables of bounded `Parquex.Batch` values. Demand triggers native range reads and decoding. Projection is applied before decoding, and supported predicates use row-group statistics plus exact row filtering.

Parquet writers accept an explicit schema and bounded batches. Encoded bytes flow incrementally into the store writer. Closing the writer finishes the Parquet footer and completes the storage write.

Finite helpers convert rows and columns at the API boundary. `Parquex.read/3` collects rows, while `Parquex.stream/3` keeps the columnar batch boundary visible to the caller.

Dataset writers route rows to bounded active partition writers. Dataset readers plan UTC partition prefixes and open one part stream at a time.

