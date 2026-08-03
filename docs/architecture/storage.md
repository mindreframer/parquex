# Storage design

One `Parquex.Store` owns backend configuration and one reusable native handle. Public operations address objects with normalized relative keys.

Local stores constrain resolved paths beneath a canonical root. Writers build a complete sibling temporary file and replace the destination on successful publication.

S3-compatible stores reuse one client for metadata, ranges, listing, deletion, multipart writes, and Parquet access. Multipart uploads target the requested final key directly. Completion replaces the value currently stored at that key.

Object access supplies Parquet with metadata, bounded ranges, prefix listing, sequential writes, completion, cancellation, and deletion. Backend paths and clients remain inside the storage implementation.

