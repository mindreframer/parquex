use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io::{self, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::types::*;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, NullArray, PrimitiveArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::basic::Compression;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use rustler::{Encoder, Env, OwnedBinary, Resource, Term};

use crate::error::{Category, NativeFailure};
use crate::local::LocalStore;
use crate::object::{ByteRange, CancellationToken, ObjectLocation, ObjectStore};
use crate::s3::{RemoteObject, S3Config};
use crate::{atoms, Operation};

static ACTIVE_READERS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Default)]
struct RangeMetrics {
    requests: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    max_request_bytes: std::sync::atomic::AtomicU64,
}

impl RangeMetrics {
    fn record(&self, bytes: usize) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.max_request_bytes
            .fetch_max(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug)]
enum ChunkBackend {
    Local(ObjectLocation),
    S3(Box<RemoteObject>),
}

impl ChunkBackend {
    fn read_range(
        &self,
        offset: u64,
        length: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, NativeFailure> {
        match self {
            Self::Local(location) => LocalStore.read_range(
                location,
                ByteRange {
                    offset,
                    length: length as u64,
                },
                cancellation,
            ),
            Self::S3(object) => {
                object.read_range(offset, length, cancellation, Operation::ReaderNext)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct ObjectChunkReader {
    backend: ChunkBackend,
    size: u64,
    max_range_bytes: usize,
    cancellation: Arc<CancellationToken>,
    metrics: Arc<RangeMetrics>,
}

impl Length for ObjectChunkReader {
    fn len(&self) -> u64 {
        self.size
    }
}

impl ChunkReader for ObjectChunkReader {
    type T = ObjectCursor;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.size {
            return Err(ParquetError::EOF(
                "range begins beyond the object".to_owned(),
            ));
        }
        Ok(ObjectCursor {
            reader: self.clone(),
            position: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if length > self.max_range_bytes {
            return Err(ParquetError::General(
                "Parquet page or metadata exceeds the configured range bound".to_owned(),
            ));
        }
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ParquetError::EOF("range overflow".to_owned()))?;
        if end > self.size {
            return Err(ParquetError::EOF("range exceeds the object".to_owned()));
        }
        let bytes = self
            .backend
            .read_range(start, length, &self.cancellation)
            .map_err(parquet_object_error)?;
        if bytes.len() != length {
            return Err(ParquetError::EOF("range was truncated".to_owned()));
        }
        self.metrics.record(bytes.len());
        Ok(bytes.into())
    }
}

#[derive(Debug)]
struct ObjectCursor {
    reader: ObjectChunkReader,
    position: u64,
}

impl Read for ObjectCursor {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || self.position == self.reader.size {
            return Ok(0);
        }
        let available = self.reader.size - self.position;
        let length = buffer
            .len()
            .min(self.reader.max_range_bytes)
            .min(usize::try_from(available).unwrap_or(usize::MAX));
        let bytes = self
            .reader
            .backend
            .read_range(self.position, length, &self.reader.cancellation)
            .map_err(object_io_error)?;
        buffer[..bytes.len()].copy_from_slice(&bytes);
        self.position += bytes.len() as u64;
        self.reader.metrics.record(bytes.len());
        Ok(bytes.len())
    }
}

fn parquet_object_error(_error: NativeFailure) -> ParquetError {
    ParquetError::General("bounded object range failed".to_owned())
}

fn object_io_error(_error: NativeFailure) -> io::Error {
    io::Error::other("bounded object range failed")
}

pub(crate) struct ReaderResource {
    state: Mutex<ReaderState>,
    cancellation: Arc<CancellationToken>,
}

#[rustler::resource_impl]
impl Resource for ReaderResource {
    fn down(&self, _env: Env<'_>, _pid: rustler::LocalPid, _monitor: rustler::Monitor) {
        self.cancellation.cancel();
        if let Ok(mut state) = self.state.lock() {
            state.close();
        }
    }
}

impl ReaderResource {
    fn lock(&self, operation: Operation) -> Result<MutexGuard<'_, ReaderState>, NativeFailure> {
        self.state
            .lock()
            .map_err(|_| NativeFailure::expected(operation, "native reader state is unavailable"))
    }

    pub(crate) fn next_batch(&self) -> Result<Option<RecordBatch>, NativeFailure> {
        self.lock(Operation::ReaderNext)?.next_batch()
    }

    pub(crate) fn close(&self) -> Result<bool, NativeFailure> {
        Ok(self.lock(Operation::ReaderClose)?.close())
    }

    pub(crate) fn stats(&self) -> Result<ReaderStats, NativeFailure> {
        Ok(self.lock(Operation::ReaderStats)?.stats())
    }
}

struct ReaderState {
    reader: Option<ParquetRecordBatchReader>,
    queue: VecDeque<RecordBatch>,
    cancellation: Arc<CancellationToken>,
    range_metrics: Arc<RangeMetrics>,
    prefetch_depth: usize,
    current_buffered_bytes: usize,
    peak_buffered_bytes: usize,
    peak_buffered_batches: usize,
    active: bool,
    eof: bool,
    row_groups: usize,
    compressions: Vec<String>,
    writer_options: HashMap<String, String>,
}

impl ReaderState {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, NativeFailure> {
        self.cancellation.check(Operation::ReaderNext)?;
        if !self.active {
            return Err(NativeFailure::cancelled(Operation::ReaderNext));
        }

        while self.queue.len() < self.prefetch_depth && !self.eof {
            let next = self
                .reader
                .as_mut()
                .ok_or_else(|| NativeFailure::cancelled(Operation::ReaderNext))?
                .next();
            match next {
                Some(Ok(batch)) => {
                    self.current_buffered_bytes += batch.get_array_memory_size();
                    self.queue.push_back(batch);
                    self.peak_buffered_bytes =
                        self.peak_buffered_bytes.max(self.current_buffered_bytes);
                    self.peak_buffered_batches = self.peak_buffered_batches.max(self.queue.len());
                }
                Some(Err(_error)) => {
                    return Err(NativeFailure::new(
                        Category::MalformedData,
                        Operation::ReaderNext,
                        "Parquet batch decoding failed",
                    ));
                }
                None => self.eof = true,
            }
        }

        let batch = self.queue.pop_front();
        if let Some(batch) = &batch {
            self.current_buffered_bytes = self
                .current_buffered_bytes
                .saturating_sub(batch.get_array_memory_size());
        }
        Ok(batch)
    }

    fn close(&mut self) -> bool {
        if !self.active {
            return false;
        }
        self.cancellation.cancel();
        self.reader.take();
        self.queue.clear();
        self.current_buffered_bytes = 0;
        self.active = false;
        ACTIVE_READERS.fetch_sub(1, Ordering::Relaxed);
        true
    }

    fn stats(&self) -> ReaderStats {
        ReaderStats {
            active: self.active,
            buffered_batches: self.queue.len(),
            buffered_bytes: self.current_buffered_bytes,
            peak_buffered_batches: self.peak_buffered_batches,
            peak_buffered_bytes: self.peak_buffered_bytes,
            range_requests: self.range_metrics.requests.load(Ordering::Relaxed),
            range_bytes: self.range_metrics.bytes.load(Ordering::Relaxed),
            max_range_bytes: self.range_metrics.max_request_bytes.load(Ordering::Relaxed),
            row_groups: self.row_groups,
            compressions: self.compressions.clone(),
            writer_options: self.writer_options.clone(),
        }
    }
}

impl Drop for ReaderState {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(rustler::NifMap)]
pub(crate) struct ReaderStats {
    active: bool,
    buffered_batches: usize,
    buffered_bytes: usize,
    peak_buffered_batches: usize,
    peak_buffered_bytes: usize,
    range_requests: u64,
    range_bytes: u64,
    max_range_bytes: u64,
    row_groups: usize,
    compressions: Vec<String>,
    writer_options: HashMap<String, String>,
}

#[derive(rustler::NifMap)]
pub(crate) struct NativeField {
    name: String,
    nullable: bool,
    data_type: NativeDataType,
}

#[derive(rustler::NifMap)]
struct NativeDataType {
    kind: rustler::Atom,
    bit_width: Option<u16>,
    signed: Option<bool>,
    unit: Option<rustler::Atom>,
    timezone: Option<String>,
    precision: Option<u8>,
    scale: Option<i8>,
    length: Option<i32>,
    children: Vec<NativeField>,
}

impl NativeDataType {
    fn simple(kind: rustler::Atom) -> Self {
        Self {
            kind,
            bit_width: None,
            signed: None,
            unit: None,
            timezone: None,
            precision: None,
            scale: None,
            length: None,
            children: Vec::new(),
        }
    }
}

pub(crate) fn open(
    location: ObjectLocation,
    max_range_bytes: usize,
    batch_size: usize,
    prefetch_depth: usize,
    columns: Vec<String>,
) -> Result<(ReaderResource, Vec<NativeField>), NativeFailure> {
    let cancellation = Arc::new(CancellationToken::default());
    let metadata = LocalStore
        .head(&location, &cancellation)
        .map_err(|error| NativeFailure {
            operation: Operation::ReaderOpen,
            ..error
        })?;
    open_backend(
        ChunkBackend::Local(location),
        metadata.size,
        cancellation,
        max_range_bytes,
        batch_size,
        prefetch_depth,
        columns,
    )
}

pub(crate) fn open_s3(
    config: S3Config,
    max_range_bytes: usize,
    batch_size: usize,
    prefetch_depth: usize,
    columns: Vec<String>,
) -> Result<(ReaderResource, Vec<NativeField>), NativeFailure> {
    let cancellation = Arc::new(CancellationToken::default());
    let object = RemoteObject::new(config)?;
    let metadata = object
        .head(&cancellation, Operation::ReaderOpen)
        .map_err(|error| NativeFailure {
            operation: Operation::ReaderOpen,
            ..error
        })?;
    open_backend(
        ChunkBackend::S3(Box::new(object)),
        metadata.size,
        cancellation,
        max_range_bytes,
        batch_size,
        prefetch_depth,
        columns,
    )
}

fn open_backend(
    backend: ChunkBackend,
    size: u64,
    cancellation: Arc<CancellationToken>,
    max_range_bytes: usize,
    batch_size: usize,
    prefetch_depth: usize,
    columns: Vec<String>,
) -> Result<(ReaderResource, Vec<NativeField>), NativeFailure> {
    let metrics = Arc::new(RangeMetrics::default());
    let chunk_reader = ObjectChunkReader {
        backend,
        size,
        max_range_bytes,
        cancellation: cancellation.clone(),
        metrics: metrics.clone(),
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
        .map_err(|_error| malformed_open())?;

    let all_fields = builder.schema().fields();
    let row_groups = builder.metadata().num_row_groups();
    let mut compressions = builder
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .map(|column| compression_label(column.compression()).to_owned())
        .collect::<Vec<_>>();
    compressions.sort_unstable();
    compressions.dedup();
    let writer_options = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            entry
                .value
                .as_ref()
                .map(|value| (entry.key.clone(), value.clone()))
        })
        .filter(|(key, _value)| key.starts_with("parquex."))
        .collect();
    let native_all_fields = all_fields
        .iter()
        .map(|field| native_field(field))
        .collect::<Result<Vec<_>, _>>()?;
    let projection_indices = projection_indices(all_fields, &columns)?;
    let projection = if columns.is_empty() {
        ProjectionMask::all()
    } else {
        ProjectionMask::roots(builder.parquet_schema(), projection_indices.clone())
    };
    let projected_fields = if columns.is_empty() {
        native_all_fields
    } else {
        projection_indices
            .iter()
            .map(|index| native_field(&all_fields[*index]))
            .collect::<Result<Vec<_>, _>>()?
    };
    let reader = builder
        .with_batch_size(batch_size)
        .with_projection(projection)
        .build()
        .map_err(|_error| malformed_open())?;

    ACTIVE_READERS.fetch_add(1, Ordering::Relaxed);
    Ok((
        ReaderResource {
            cancellation: cancellation.clone(),
            state: Mutex::new(ReaderState {
                reader: Some(reader),
                queue: VecDeque::with_capacity(prefetch_depth),
                cancellation,
                range_metrics: metrics,
                prefetch_depth,
                current_buffered_bytes: 0,
                peak_buffered_bytes: 0,
                peak_buffered_batches: 0,
                active: true,
                eof: false,
                row_groups,
                compressions,
                writer_options,
            }),
        },
        projected_fields,
    ))
}

pub(crate) fn active_readers() -> usize {
    ACTIVE_READERS.load(Ordering::Relaxed)
}

fn compression_label(compression: Compression) -> &'static str {
    match compression {
        Compression::UNCOMPRESSED => "uncompressed",
        Compression::SNAPPY => "snappy",
        Compression::GZIP(_) => "gzip",
        Compression::ZSTD(_) => "zstd",
        Compression::LZ4_RAW => "lz4_raw",
        Compression::LZO => "lzo",
        Compression::BROTLI(_) => "brotli",
        Compression::LZ4 => "lz4",
    }
}

fn malformed_open() -> NativeFailure {
    NativeFailure::new(
        Category::MalformedData,
        Operation::ReaderOpen,
        "Parquet metadata is malformed",
    )
}

fn projection_indices(
    fields: &arrow_schema::Fields,
    columns: &[String],
) -> Result<Vec<usize>, NativeFailure> {
    if columns.is_empty() {
        return Ok((0..fields.len()).collect());
    }
    let mut indices = Vec::with_capacity(columns.len());
    for column in columns {
        let index = fields
            .iter()
            .position(|field| field.name() == column)
            .ok_or_else(|| {
                NativeFailure::invalid(Operation::ReaderOpen, "projected column does not exist")
            })?;
        indices.push(index);
    }
    indices.sort_unstable();
    Ok(indices)
}

fn native_field(field: &Field) -> Result<NativeField, NativeFailure> {
    Ok(NativeField {
        name: field.name().to_owned(),
        nullable: field.is_nullable(),
        data_type: native_data_type(field.data_type())?,
    })
}

fn native_data_type(data_type: &DataType) -> Result<NativeDataType, NativeFailure> {
    use DataType::*;
    let result = match data_type {
        Null => NativeDataType::simple(atoms::null()),
        Boolean => NativeDataType::simple(atoms::boolean()),
        Int8 => integer_type(8, true),
        Int16 => integer_type(16, true),
        Int32 => integer_type(32, true),
        Int64 => integer_type(64, true),
        UInt8 => integer_type(8, false),
        UInt16 => integer_type(16, false),
        UInt32 => integer_type(32, false),
        UInt64 => integer_type(64, false),
        Float32 => float_type(32),
        Float64 => float_type(64),
        Utf8 | LargeUtf8 => NativeDataType::simple(atoms::utf8()),
        Binary | LargeBinary => NativeDataType::simple(atoms::binary()),
        FixedSizeBinary(length) if *length > 0 => {
            let mut descriptor = NativeDataType::simple(atoms::fixed_binary());
            descriptor.length = Some(*length);
            descriptor
        }
        Date32 => NativeDataType::simple(atoms::date32()),
        Date64 => NativeDataType::simple(atoms::date64()),
        Time32(unit) if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) => {
            time_type(unit, 32)
        }
        Time64(unit) if matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            time_type(unit, 64)
        }
        Timestamp(unit, timezone) => {
            let mut descriptor = NativeDataType::simple(atoms::timestamp());
            descriptor.unit = Some(time_unit_atom(unit));
            descriptor.timezone = timezone.as_ref().map(|value| value.to_string());
            descriptor
        }
        Duration(unit) => {
            let mut descriptor = NativeDataType::simple(atoms::duration());
            descriptor.unit = Some(time_unit_atom(unit));
            descriptor
        }
        Decimal32(precision, scale) => decimal_type(32, *precision, *scale),
        Decimal64(precision, scale) => decimal_type(64, *precision, *scale),
        Decimal128(precision, scale) => decimal_type(128, *precision, *scale),
        Decimal256(precision, scale) => decimal_type(256, *precision, *scale),
        List(field) => nested_type(atoms::list(), vec![native_field(field)?]),
        LargeList(field) => nested_type(atoms::large_list(), vec![native_field(field)?]),
        FixedSizeList(field, length) if *length > 0 => {
            let mut descriptor = nested_type(atoms::fixed_list(), vec![native_field(field)?]);
            descriptor.length = Some(*length);
            descriptor
        }
        Struct(fields) => nested_type(
            atoms::struct_type(),
            fields
                .iter()
                .map(|field| native_field(field))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => {
            return Err(NativeFailure::new(
                Category::Unsupported,
                Operation::ReaderOpen,
                "Parquet schema contains an unsupported logical type",
            ));
        }
    };
    Ok(result)
}

fn integer_type(bit_width: u16, signed: bool) -> NativeDataType {
    let mut descriptor = NativeDataType::simple(atoms::integer());
    descriptor.bit_width = Some(bit_width);
    descriptor.signed = Some(signed);
    descriptor
}

fn float_type(bit_width: u16) -> NativeDataType {
    let mut descriptor = NativeDataType::simple(atoms::float());
    descriptor.bit_width = Some(bit_width);
    descriptor
}

fn time_type(unit: &TimeUnit, bit_width: u16) -> NativeDataType {
    let mut descriptor = NativeDataType::simple(atoms::time());
    descriptor.unit = Some(time_unit_atom(unit));
    descriptor.bit_width = Some(bit_width);
    descriptor
}

fn decimal_type(bit_width: u16, precision: u8, scale: i8) -> NativeDataType {
    let mut descriptor = NativeDataType::simple(atoms::decimal());
    descriptor.bit_width = Some(bit_width);
    descriptor.precision = Some(precision);
    descriptor.scale = Some(scale);
    descriptor
}

fn nested_type(kind: rustler::Atom, children: Vec<NativeField>) -> NativeDataType {
    let mut descriptor = NativeDataType::simple(kind);
    descriptor.children = children;
    descriptor
}

fn time_unit_atom(unit: &TimeUnit) -> rustler::Atom {
    match unit {
        TimeUnit::Second => atoms::second(),
        TimeUnit::Millisecond => atoms::millisecond(),
        TimeUnit::Microsecond => atoms::microsecond(),
        TimeUnit::Nanosecond => atoms::nanosecond(),
    }
}

pub(crate) fn encode_batch<'a>(
    env: Env<'a>,
    batch: &RecordBatch,
) -> Result<Term<'a>, NativeFailure> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let values = encode_array(env, array)?;
        columns.push((field.name().to_owned(), values));
    }
    Ok((atoms::batch(), batch.num_rows(), columns).encode(env))
}

fn encode_array<'a>(env: Env<'a>, array: &ArrayRef) -> Result<Term<'a>, NativeFailure> {
    let values = (0..array.len())
        .map(|index| encode_value(env, array, index))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(values.encode(env))
}

fn encode_value<'a>(
    env: Env<'a>,
    array: &ArrayRef,
    index: usize,
) -> Result<Term<'a>, NativeFailure> {
    if array.is_null(index) {
        return Ok(atoms::nil_atom().encode(env));
    }
    use DataType::*;
    match array.data_type() {
        Null => downcast::<NullArray>(array, Operation::ReaderNext)
            .map(|_| atoms::nil_atom().encode(env)),
        Boolean => encode_boolean(env, array, index),
        Int8 => encode_primitive::<Int8Type>(env, array, index),
        Int16 => encode_primitive::<Int16Type>(env, array, index),
        Int32 => encode_primitive::<Int32Type>(env, array, index),
        Int64 => encode_primitive::<Int64Type>(env, array, index),
        UInt8 => encode_primitive::<UInt8Type>(env, array, index),
        UInt16 => encode_primitive::<UInt16Type>(env, array, index),
        UInt32 => encode_primitive::<UInt32Type>(env, array, index),
        UInt64 => encode_primitive::<UInt64Type>(env, array, index),
        Float32 => encode_primitive::<Float32Type>(env, array, index),
        Float64 => encode_primitive::<Float64Type>(env, array, index),
        Date32 => encode_primitive::<Date32Type>(env, array, index),
        Date64 => encode_primitive::<Date64Type>(env, array, index),
        Time32(TimeUnit::Second) => encode_primitive::<Time32SecondType>(env, array, index),
        Time32(TimeUnit::Millisecond) => {
            encode_primitive::<Time32MillisecondType>(env, array, index)
        }
        Time64(TimeUnit::Microsecond) => {
            encode_primitive::<Time64MicrosecondType>(env, array, index)
        }
        Time64(TimeUnit::Nanosecond) => encode_primitive::<Time64NanosecondType>(env, array, index),
        Timestamp(TimeUnit::Second, _) => {
            encode_primitive::<TimestampSecondType>(env, array, index)
        }
        Timestamp(TimeUnit::Millisecond, _) => {
            encode_primitive::<TimestampMillisecondType>(env, array, index)
        }
        Timestamp(TimeUnit::Microsecond, _) => {
            encode_primitive::<TimestampMicrosecondType>(env, array, index)
        }
        Timestamp(TimeUnit::Nanosecond, _) => {
            encode_primitive::<TimestampNanosecondType>(env, array, index)
        }
        Duration(TimeUnit::Second) => encode_primitive::<DurationSecondType>(env, array, index),
        Duration(TimeUnit::Millisecond) => {
            encode_primitive::<DurationMillisecondType>(env, array, index)
        }
        Duration(TimeUnit::Microsecond) => {
            encode_primitive::<DurationMicrosecondType>(env, array, index)
        }
        Duration(TimeUnit::Nanosecond) => {
            encode_primitive::<DurationNanosecondType>(env, array, index)
        }
        Decimal32(_, _) => encode_decimal::<Decimal32Type>(env, array, index),
        Decimal64(_, _) => encode_decimal::<Decimal64Type>(env, array, index),
        Decimal128(_, _) => {
            let array = downcast::<Decimal128Array>(array, Operation::ReaderNext)?;
            Ok(array.value(index).to_string().encode(env))
        }
        Decimal256(_, _) => {
            let array = downcast::<Decimal256Array>(array, Operation::ReaderNext)?;
            Ok(array.value(index).to_string().encode(env))
        }
        Utf8 => encode_string::<StringArray>(env, array, index),
        LargeUtf8 => encode_string::<LargeStringArray>(env, array, index),
        Binary => encode_binary::<BinaryArray>(env, array, index),
        LargeBinary => encode_binary::<LargeBinaryArray>(env, array, index),
        FixedSizeBinary(_) => {
            let array = downcast::<FixedSizeBinaryArray>(array, Operation::ReaderNext)?;
            encode_bytes(env, array.value(index))
        }
        List(_) => encode_list::<ListArray>(env, array, index),
        LargeList(_) => encode_list::<LargeListArray>(env, array, index),
        FixedSizeList(_, _) => {
            let array = downcast::<FixedSizeListArray>(array, Operation::ReaderNext)?;
            encode_array(env, &array.value(index))
        }
        Struct(fields) => {
            let array = downcast::<StructArray>(array, Operation::ReaderNext)?;
            let mut map = Term::map_new(env);
            for (field, child) in fields.iter().zip(array.columns()) {
                map = map
                    .map_put(field.name(), encode_value(env, child, index)?)
                    .map_err(|_| {
                        NativeFailure::expected(Operation::ReaderNext, "map encoding failed")
                    })?;
            }
            Ok(map)
        }
        _ => Err(NativeFailure::new(
            Category::Unsupported,
            Operation::ReaderNext,
            "Parquet batch contains an unsupported logical type",
        )),
    }
}

fn encode_boolean<'a>(
    env: Env<'a>,
    array: &ArrayRef,
    index: usize,
) -> Result<Term<'a>, NativeFailure> {
    let array = downcast::<BooleanArray>(array, Operation::ReaderNext)?;
    Ok(array.value(index).encode(env))
}

fn encode_primitive<'a, T>(
    env: Env<'a>,
    array: &ArrayRef,
    index: usize,
) -> Result<Term<'a>, NativeFailure>
where
    T: ArrowPrimitiveType,
    T::Native: Encoder,
{
    let array = downcast::<PrimitiveArray<T>>(array, Operation::ReaderNext)?;
    Ok(array.value(index).encode(env))
}

fn encode_decimal<'a, T>(
    env: Env<'a>,
    array: &ArrayRef,
    index: usize,
) -> Result<Term<'a>, NativeFailure>
where
    T: ArrowPrimitiveType,
    T::Native: std::fmt::Display,
{
    let array = downcast::<PrimitiveArray<T>>(array, Operation::ReaderNext)?;
    Ok(array.value(index).to_string().encode(env))
}

trait StringValueArray: Array {
    fn string_value(&self, index: usize) -> &str;
}

impl StringValueArray for StringArray {
    fn string_value(&self, index: usize) -> &str {
        self.value(index)
    }
}

impl StringValueArray for LargeStringArray {
    fn string_value(&self, index: usize) -> &str {
        self.value(index)
    }
}

fn encode_string<'a, T>(
    env: Env<'a>,
    array: &ArrayRef,
    index: usize,
) -> Result<Term<'a>, NativeFailure>
where
    T: StringValueArray + 'static,
{
    let array = downcast::<T>(array, Operation::ReaderNext)?;
    Ok(array.string_value(index).encode(env))
}

trait BinaryValueArray: Array {
    fn binary_value(&self, index: usize) -> &[u8];
}

impl BinaryValueArray for BinaryArray {
    fn binary_value(&self, index: usize) -> &[u8] {
        self.value(index)
    }
}

impl BinaryValueArray for LargeBinaryArray {
    fn binary_value(&self, index: usize) -> &[u8] {
        self.value(index)
    }
}

fn encode_binary<'a, T>(
    env: Env<'a>,
    array: &ArrayRef,
    index: usize,
) -> Result<Term<'a>, NativeFailure>
where
    T: BinaryValueArray + 'static,
{
    let array = downcast::<T>(array, Operation::ReaderNext)?;
    encode_bytes(env, array.binary_value(index))
}

fn encode_bytes<'a>(env: Env<'a>, bytes: &[u8]) -> Result<Term<'a>, NativeFailure> {
    let mut binary = OwnedBinary::new(bytes.len()).ok_or_else(|| {
        NativeFailure::expected(Operation::ReaderNext, "binary allocation failed")
    })?;
    binary.as_mut_slice().copy_from_slice(bytes);
    Ok(binary.release(env).encode(env))
}

trait ListValueArray: Array {
    fn list_value(&self, index: usize) -> ArrayRef;
}

impl ListValueArray for ListArray {
    fn list_value(&self, index: usize) -> ArrayRef {
        self.value(index)
    }
}

impl ListValueArray for LargeListArray {
    fn list_value(&self, index: usize) -> ArrayRef {
        self.value(index)
    }
}

fn encode_list<'a, T>(
    env: Env<'a>,
    array: &ArrayRef,
    index: usize,
) -> Result<Term<'a>, NativeFailure>
where
    T: ListValueArray + 'static,
{
    let array = downcast::<T>(array, Operation::ReaderNext)?;
    encode_array(env, &array.list_value(index))
}

fn downcast<T: 'static>(array: &ArrayRef, operation: Operation) -> Result<&T, NativeFailure> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        NativeFailure::expected(operation, "Arrow array did not match its declared type")
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn projection_uses_file_order_regardless_of_request_order() {
        let fields = vec![
            Arc::new(Field::new("first", DataType::Int64, false)),
            Arc::new(Field::new("second", DataType::Utf8, true)),
            Arc::new(Field::new("third", DataType::Boolean, false)),
        ]
        .into();

        let indices = projection_indices(&fields, &["third".to_owned(), "first".to_owned()])
            .expect("projection should resolve");

        assert_eq!(indices, vec![0, 2]);
    }

    #[test]
    fn projection_rejects_missing_columns() {
        let fields = vec![Arc::new(Field::new("present", DataType::Int64, false))].into();

        let error = projection_indices(&fields, &["missing".to_owned()])
            .expect_err("missing columns must fail during open");

        assert_eq!(error.category, Category::InvalidArgument);
        assert_eq!(error.operation, Operation::ReaderOpen);
    }
}
