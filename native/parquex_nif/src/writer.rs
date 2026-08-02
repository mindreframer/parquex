use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::builder::*;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rustler::{Atom, Binary, Env, LocalPid, Resource, Term};

use crate::error::{Category, NativeFailure};
use crate::local::{LocalStore, LocalWriter};
use crate::object::{
    CancellationToken, FlushPolicy, ObjectLocation, ObjectMetadata, ObjectStore, StagedWrite,
    SyncPolicy, WriteOptions,
};
use crate::s3::{RemoteMultipartWriter, RemoteObject, S3Config};
use crate::{atoms, Operation};

#[derive(rustler::NifMap)]
pub(crate) struct InputField {
    name: String,
    nullable: bool,
    data_type: InputDataType,
}

#[derive(rustler::NifMap)]
pub(crate) struct InputDataType {
    kind: Atom,
    bit_width: Option<u16>,
    signed: Option<bool>,
    unit: Option<Atom>,
    timezone: Option<String>,
    precision: Option<u8>,
    scale: Option<i8>,
    length: Option<i32>,
    children: Vec<InputField>,
}

#[derive(rustler::NifMap)]
pub(crate) struct NativeWriterOptions {
    compression: Atom,
    max_batch_rows: usize,
    max_row_group_rows: usize,
    data_page_size_limit: usize,
    flush: Atom,
    sync: Atom,
    statistics: Atom,
}

#[derive(rustler::NifMap, Clone, Copy)]
pub(crate) struct WriterStats {
    active: bool,
    batches: usize,
    rows: usize,
    peak_batch_bytes: usize,
    peak_encoder_bytes: usize,
    multipart_buffer_limit_bytes: usize,
}

pub(crate) struct ParquetWriterResource {
    state: Mutex<WriterState>,
    cancellation: Arc<CancellationToken>,
}

#[rustler::resource_impl]
impl Resource for ParquetWriterResource {
    fn down(&self, _env: Env<'_>, _pid: LocalPid, _monitor: rustler::Monitor) {
        self.cancellation.cancel();
        if let Ok(mut state) = self.state.lock() {
            state.abort();
        }
    }
}

impl ParquetWriterResource {
    fn lock(&self, operation: Operation) -> Result<MutexGuard<'_, WriterState>, NativeFailure> {
        self.state
            .lock()
            .map_err(|_| NativeFailure::expected(operation, "native Parquet writer is unavailable"))
    }

    pub(crate) fn write_batch(&self, batch: Term<'_>) -> Result<WriterStats, NativeFailure> {
        self.lock(Operation::ParquetWriterWrite)?.write_batch(batch)
    }

    pub(crate) fn close(&self) -> Result<ObjectMetadata, NativeFailure> {
        self.lock(Operation::ParquetWriterClose)?.close()
    }

    pub(crate) fn abort(&self) -> Result<bool, NativeFailure> {
        Ok(self.lock(Operation::ParquetWriterAbort)?.abort())
    }

    pub(crate) fn stats(&self) -> Result<WriterStats, NativeFailure> {
        Ok(self.lock(Operation::ParquetWriterStats)?.stats())
    }
}

struct WriterState {
    writer: Option<ArrowWriter<StagedOutput>>,
    schema: SchemaRef,
    cancellation: Arc<CancellationToken>,
    max_batch_rows: usize,
    stats: WriterStats,
}

enum StagedOutput {
    Local(LocalWriter),
    S3(RemoteMultipartWriter),
}

impl StagedOutput {
    fn publish(&mut self) -> Result<ObjectMetadata, NativeFailure> {
        match self {
            Self::Local(writer) => writer.publish(),
            Self::S3(writer) => writer.publish().map(|metadata| ObjectMetadata {
                path: metadata.key,
                size: metadata.size,
                modified_unix_ns: metadata.modified_unix_ns,
            }),
        }
    }
}

impl Write for StagedOutput {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Local(writer) => Write::write(writer, buffer),
            Self::S3(writer) => writer.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Local(writer) => writer.flush(),
            Self::S3(writer) => writer.flush(),
        }
    }
}

impl WriterState {
    fn write_batch(&mut self, batch: Term<'_>) -> Result<WriterStats, NativeFailure> {
        self.cancellation.check(Operation::ParquetWriterWrite)?;
        if !self.stats.active {
            return Err(NativeFailure::cancelled(Operation::ParquetWriterWrite));
        }
        let record_batch = decode_batch(batch, self.schema.clone(), self.max_batch_rows)?;
        let batch_bytes = record_batch.get_array_memory_size();
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| NativeFailure::cancelled(Operation::ParquetWriterWrite))?;
        writer
            .write(&record_batch)
            .map_err(|_| encode_failure(Operation::ParquetWriterWrite))?;
        self.stats.batches += 1;
        self.stats.rows += record_batch.num_rows();
        self.stats.peak_batch_bytes = self.stats.peak_batch_bytes.max(batch_bytes);
        self.stats.peak_encoder_bytes =
            self.stats.peak_encoder_bytes.max(writer.in_progress_size());
        Ok(self.stats())
    }

    fn close(&mut self) -> Result<ObjectMetadata, NativeFailure> {
        self.cancellation.check(Operation::ParquetWriterClose)?;
        let writer = self
            .writer
            .take()
            .ok_or_else(|| NativeFailure::cancelled(Operation::ParquetWriterClose))?;
        let mut staged = match writer.into_inner() {
            Ok(staged) => staged,
            Err(_error) => {
                self.stats.active = false;
                return Err(encode_failure(Operation::ParquetWriterClose));
            }
        };
        match staged.publish() {
            Ok(metadata) => {
                self.stats.active = false;
                Ok(metadata)
            }
            Err(error) => {
                self.stats.active = false;
                Err(NativeFailure {
                    operation: Operation::ParquetWriterClose,
                    ..error
                })
            }
        }
    }

    fn abort(&mut self) -> bool {
        if !self.stats.active {
            return false;
        }
        self.cancellation.cancel();
        self.writer.take();
        self.stats.active = false;
        true
    }

    fn stats(&self) -> WriterStats {
        WriterStats {
            active: self.stats.active,
            ..self.stats
        }
    }
}

impl Drop for WriterState {
    fn drop(&mut self) {
        self.abort();
    }
}

pub(crate) fn open(
    location: ObjectLocation,
    fields: Vec<InputField>,
    options: NativeWriterOptions,
) -> Result<ParquetWriterResource, NativeFailure> {
    let cancellation = Arc::new(CancellationToken::default());
    let flush = flush_policy(options.flush)?;
    let sync = sync_policy(options.sync)?;
    let staged = LocalStore.stage(
        &location,
        WriteOptions { flush, sync },
        cancellation.clone(),
    )?;
    open_output(
        StagedOutput::Local(staged),
        fields,
        options,
        cancellation,
        0,
    )
}

pub(crate) fn open_s3(
    config: S3Config,
    fields: Vec<InputField>,
    options: NativeWriterOptions,
) -> Result<ParquetWriterResource, NativeFailure> {
    // These local durability controls remain validated for a backend-neutral API,
    // but S3 durability is governed by successful multipart completion.
    flush_policy(options.flush)?;
    sync_policy(options.sync)?;
    let multipart_buffer_limit_bytes = config
        .max_in_flight_parts
        .saturating_add(1)
        .saturating_mul(config.multipart_part_size);
    let cancellation = Arc::new(CancellationToken::default());
    let staged = RemoteObject::new(config)?.open_multipart(cancellation.clone())?;
    open_output(
        StagedOutput::S3(staged),
        fields,
        options,
        cancellation,
        multipart_buffer_limit_bytes,
    )
}

fn open_output(
    staged: StagedOutput,
    fields: Vec<InputField>,
    options: NativeWriterOptions,
    cancellation: Arc<CancellationToken>,
    multipart_buffer_limit_bytes: usize,
) -> Result<ParquetWriterResource, NativeFailure> {
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(arrow_field)
            .collect::<Result<Vec<_>, _>>()?,
    ));
    let compression = compression(options.compression)?;
    let compression_name = compression_name(options.compression)?;
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(options.max_row_group_rows))
        .set_data_page_size_limit(options.data_page_size_limit)
        .set_write_batch_size(options.max_batch_rows.min(options.max_row_group_rows))
        .set_statistics_enabled(statistics_policy(options.statistics)?)
        .set_key_value_metadata(Some(vec![
            KeyValue {
                key: "parquex.compression".to_owned(),
                value: Some(compression_name.to_owned()),
            },
            KeyValue {
                key: "parquex.max_row_group_rows".to_owned(),
                value: Some(options.max_row_group_rows.to_string()),
            },
            KeyValue {
                key: "parquex.data_page_size_limit".to_owned(),
                value: Some(options.data_page_size_limit.to_string()),
            },
        ]))
        .build();
    let writer = ArrowWriter::try_new(staged, schema.clone(), Some(properties))
        .map_err(|_| encode_failure(Operation::ParquetWriterOpen))?;

    Ok(ParquetWriterResource {
        cancellation: cancellation.clone(),
        state: Mutex::new(WriterState {
            writer: Some(writer),
            schema,
            cancellation,
            max_batch_rows: options.max_batch_rows,
            stats: WriterStats {
                active: true,
                batches: 0,
                rows: 0,
                peak_batch_bytes: 0,
                peak_encoder_bytes: 0,
                multipart_buffer_limit_bytes,
            },
        }),
    })
}

fn compression(value: Atom) -> Result<Compression, NativeFailure> {
    if value == atoms::uncompressed() {
        Ok(Compression::UNCOMPRESSED)
    } else if value == atoms::snappy() {
        Ok(Compression::SNAPPY)
    } else if value == atoms::zstd() {
        Ok(Compression::ZSTD(ZstdLevel::default()))
    } else if value == atoms::gzip() {
        Ok(Compression::GZIP(GzipLevel::default()))
    } else if value == atoms::lz4_raw() {
        Ok(Compression::LZ4_RAW)
    } else {
        Err(NativeFailure::invalid(
            Operation::ParquetWriterOpen,
            "unsupported compression",
        ))
    }
}

fn compression_name(value: Atom) -> Result<&'static str, NativeFailure> {
    if value == atoms::uncompressed() {
        Ok("uncompressed")
    } else if value == atoms::snappy() {
        Ok("snappy")
    } else if value == atoms::zstd() {
        Ok("zstd")
    } else if value == atoms::gzip() {
        Ok("gzip")
    } else if value == atoms::lz4_raw() {
        Ok("lz4_raw")
    } else {
        Err(NativeFailure::invalid(
            Operation::ParquetWriterOpen,
            "unsupported compression",
        ))
    }
}

fn flush_policy(value: Atom) -> Result<FlushPolicy, NativeFailure> {
    if value == atoms::none() {
        Ok(FlushPolicy::None)
    } else if value == atoms::each_chunk() {
        Ok(FlushPolicy::EachChunk)
    } else if value == atoms::before_publish() {
        Ok(FlushPolicy::BeforePublish)
    } else {
        Err(NativeFailure::invalid(
            Operation::ParquetWriterOpen,
            "invalid flush policy",
        ))
    }
}

fn sync_policy(value: Atom) -> Result<SyncPolicy, NativeFailure> {
    if value == atoms::none() {
        Ok(SyncPolicy::None)
    } else if value == atoms::data() {
        Ok(SyncPolicy::Data)
    } else if value == atoms::all() {
        Ok(SyncPolicy::All)
    } else {
        Err(NativeFailure::invalid(
            Operation::ParquetWriterOpen,
            "invalid sync policy",
        ))
    }
}

fn statistics_policy(value: Atom) -> Result<EnabledStatistics, NativeFailure> {
    if value == atoms::chunk() {
        Ok(EnabledStatistics::Chunk)
    } else if value == atoms::none() {
        Ok(EnabledStatistics::None)
    } else {
        Err(NativeFailure::invalid(
            Operation::ParquetWriterOpen,
            "invalid statistics policy",
        ))
    }
}

fn arrow_field(field: &InputField) -> Result<Field, NativeFailure> {
    Ok(Field::new(
        &field.name,
        arrow_data_type(&field.data_type)?,
        field.nullable,
    ))
}

fn arrow_data_type(input: &InputDataType) -> Result<DataType, NativeFailure> {
    let unsupported = || {
        NativeFailure::new(
            Category::Unsupported,
            Operation::ParquetWriterOpen,
            "schema contains an unsupported write type",
        )
    };
    let kind = input.kind;
    if kind == atoms::null() {
        Ok(DataType::Null)
    } else if kind == atoms::boolean() {
        Ok(DataType::Boolean)
    } else if kind == atoms::integer() {
        match (input.bit_width, input.signed) {
            (Some(8), Some(true)) => Ok(DataType::Int8),
            (Some(16), Some(true)) => Ok(DataType::Int16),
            (Some(32), Some(true)) => Ok(DataType::Int32),
            (Some(64), Some(true)) => Ok(DataType::Int64),
            (Some(8), Some(false)) => Ok(DataType::UInt8),
            (Some(16), Some(false)) => Ok(DataType::UInt16),
            (Some(32), Some(false)) => Ok(DataType::UInt32),
            (Some(64), Some(false)) => Ok(DataType::UInt64),
            _ => Err(unsupported()),
        }
    } else if kind == atoms::float() {
        match input.bit_width {
            Some(32) => Ok(DataType::Float32),
            Some(64) => Ok(DataType::Float64),
            _ => Err(unsupported()),
        }
    } else if kind == atoms::utf8() {
        Ok(DataType::Utf8)
    } else if kind == atoms::binary() {
        Ok(DataType::Binary)
    } else if kind == atoms::fixed_binary() {
        input
            .length
            .map(DataType::FixedSizeBinary)
            .ok_or_else(unsupported)
    } else if kind == atoms::date32() {
        Ok(DataType::Date32)
    } else if kind == atoms::date64() {
        Ok(DataType::Date64)
    } else if kind == atoms::time() {
        match (input.bit_width, input.unit.and_then(time_unit)) {
            (Some(32), Some(unit @ (TimeUnit::Second | TimeUnit::Millisecond))) => {
                Ok(DataType::Time32(unit))
            }
            (Some(64), Some(unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond))) => {
                Ok(DataType::Time64(unit))
            }
            _ => Err(unsupported()),
        }
    } else if kind == atoms::timestamp() {
        input
            .unit
            .and_then(time_unit)
            .map(|unit| DataType::Timestamp(unit, input.timezone.clone().map(Into::into)))
            .ok_or_else(unsupported)
    } else if kind == atoms::duration() {
        input
            .unit
            .and_then(time_unit)
            .map(DataType::Duration)
            .ok_or_else(unsupported)
    } else if kind == atoms::decimal() {
        match (input.bit_width, input.precision, input.scale) {
            (Some(32), Some(p), Some(s)) => Ok(DataType::Decimal32(p, s)),
            (Some(64), Some(p), Some(s)) => Ok(DataType::Decimal64(p, s)),
            (Some(128), Some(p), Some(s)) => Ok(DataType::Decimal128(p, s)),
            (Some(256), Some(p), Some(s)) => Ok(DataType::Decimal256(p, s)),
            _ => Err(unsupported()),
        }
    } else if kind == atoms::list() || kind == atoms::large_list() || kind == atoms::fixed_list() {
        let child = input.children.first().ok_or_else(unsupported)?;
        let field = Arc::new(arrow_field(child)?);
        if kind == atoms::list() {
            Ok(DataType::List(field))
        } else if kind == atoms::large_list() {
            Ok(DataType::LargeList(field))
        } else {
            input
                .length
                .map(|length| DataType::FixedSizeList(field, length))
                .ok_or_else(unsupported)
        }
    } else if kind == atoms::struct_type() {
        Ok(DataType::Struct(
            input
                .children
                .iter()
                .map(arrow_field)
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ))
    } else {
        Err(unsupported())
    }
}

fn time_unit(atom: Atom) -> Option<TimeUnit> {
    if atom == atoms::second() {
        Some(TimeUnit::Second)
    } else if atom == atoms::millisecond() {
        Some(TimeUnit::Millisecond)
    } else if atom == atoms::microsecond() {
        Some(TimeUnit::Microsecond)
    } else if atom == atoms::nanosecond() {
        Some(TimeUnit::Nanosecond)
    } else {
        None
    }
}

fn decode_batch(
    term: Term<'_>,
    schema: SchemaRef,
    max_rows: usize,
) -> Result<RecordBatch, NativeFailure> {
    let (tag, row_count, columns): (Atom, usize, Vec<(String, Term<'_>)>) = term
        .decode()
        .map_err(|_| invalid_batch("invalid batch boundary"))?;
    if tag != atoms::batch() || row_count > max_rows || columns.len() != schema.fields().len() {
        return Err(invalid_batch("batch shape or row bound is invalid"));
    }
    let mut arrays = Vec::with_capacity(columns.len());
    for ((name, values), field) in columns.into_iter().zip(schema.fields()) {
        if name.as_str() != field.name() {
            return Err(invalid_batch("batch schema does not match writer schema"));
        }
        let terms: Vec<Term<'_>> = values
            .decode()
            .map_err(|_| invalid_batch("batch column is not a list"))?;
        if terms.len() != row_count {
            return Err(invalid_batch("batch column lengths do not match"));
        }
        arrays.push(build_array(field, terms)?);
    }
    RecordBatch::try_new(schema, arrays).map_err(|_| invalid_batch("batch arrays are invalid"))
}

fn build_array(field: &Field, terms: Vec<Term<'_>>) -> Result<ArrayRef, NativeFailure> {
    let mut builder = make_builder(field.data_type(), terms.len());
    for term in terms {
        append_value(
            builder.as_mut(),
            field.data_type(),
            field.is_nullable(),
            term,
        )?;
    }
    Ok(builder.finish())
}

fn is_nil(term: Term<'_>) -> bool {
    matches!(term.decode::<Atom>(), Ok(atom) if atom == atoms::nil_atom())
}

macro_rules! primitive {
    ($builder:expr, $term:expr, $builder_type:ty, $native_type:ty) => {{
        let builder = downcast_builder::<$builder_type>($builder)?;
        if is_nil($term) {
            builder.append_null();
        } else {
            let value: $native_type = $term
                .decode()
                .map_err(|_| invalid_batch("batch value has the wrong type"))?;
            builder.append_value(value);
        }
        Ok(())
    }};
}

macro_rules! decimal {
    ($builder:expr, $term:expr, $builder_type:ty, $native_type:ty) => {{
        let builder = downcast_builder::<$builder_type>($builder)?;
        if is_nil($term) {
            builder.append_null();
        } else {
            let text: String = $term
                .decode()
                .map_err(|_| invalid_batch("decimal values must be unscaled integer strings"))?;
            let value: $native_type = text
                .parse()
                .map_err(|_| invalid_batch("decimal value is outside its declared width"))?;
            builder.append_value(value);
        }
        Ok(())
    }};
}

fn append_value(
    builder: &mut dyn ArrayBuilder,
    data_type: &DataType,
    nullable: bool,
    term: Term<'_>,
) -> Result<(), NativeFailure> {
    if is_nil(term) && !nullable {
        return Err(invalid_batch("null value violates a non-nullable field"));
    }
    use DataType::*;
    match data_type {
        Null => {
            if !is_nil(term) {
                return Err(invalid_batch("null column contains a value"));
            }
            downcast_builder::<NullBuilder>(builder)?.append_null();
            Ok(())
        }
        Boolean => primitive!(builder, term, BooleanBuilder, bool),
        Int8 => primitive!(builder, term, Int8Builder, i8),
        Int16 => primitive!(builder, term, Int16Builder, i16),
        Int32 => primitive!(builder, term, Int32Builder, i32),
        Int64 => primitive!(builder, term, Int64Builder, i64),
        UInt8 => primitive!(builder, term, UInt8Builder, u8),
        UInt16 => primitive!(builder, term, UInt16Builder, u16),
        UInt32 => primitive!(builder, term, UInt32Builder, u32),
        UInt64 => primitive!(builder, term, UInt64Builder, u64),
        Float32 => primitive!(builder, term, Float32Builder, f32),
        Float64 => primitive!(builder, term, Float64Builder, f64),
        Date32 => primitive!(builder, term, Date32Builder, i32),
        Date64 => primitive!(builder, term, Date64Builder, i64),
        Time32(TimeUnit::Second) => primitive!(builder, term, Time32SecondBuilder, i32),
        Time32(TimeUnit::Millisecond) => primitive!(builder, term, Time32MillisecondBuilder, i32),
        Time64(TimeUnit::Microsecond) => primitive!(builder, term, Time64MicrosecondBuilder, i64),
        Time64(TimeUnit::Nanosecond) => primitive!(builder, term, Time64NanosecondBuilder, i64),
        Timestamp(TimeUnit::Second, _) => primitive!(builder, term, TimestampSecondBuilder, i64),
        Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(builder, term, TimestampMillisecondBuilder, i64)
        }
        Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(builder, term, TimestampMicrosecondBuilder, i64)
        }
        Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(builder, term, TimestampNanosecondBuilder, i64)
        }
        Duration(TimeUnit::Second) => primitive!(builder, term, DurationSecondBuilder, i64),
        Duration(TimeUnit::Millisecond) => {
            primitive!(builder, term, DurationMillisecondBuilder, i64)
        }
        Duration(TimeUnit::Microsecond) => {
            primitive!(builder, term, DurationMicrosecondBuilder, i64)
        }
        Duration(TimeUnit::Nanosecond) => {
            primitive!(builder, term, DurationNanosecondBuilder, i64)
        }
        Decimal32(_, _) => decimal!(builder, term, Decimal32Builder, i32),
        Decimal64(_, _) => decimal!(builder, term, Decimal64Builder, i64),
        Decimal128(_, _) => decimal!(builder, term, Decimal128Builder, i128),
        Decimal256(_, _) => decimal!(builder, term, Decimal256Builder, i256),
        Utf8 => string_value::<StringBuilder>(builder, term),
        LargeUtf8 => string_value::<LargeStringBuilder>(builder, term),
        Binary => binary_value::<BinaryBuilder>(builder, term),
        LargeBinary => binary_value::<LargeBinaryBuilder>(builder, term),
        FixedSizeBinary(_) => fixed_binary_value(builder, term),
        List(field) => list_value::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, field, term),
        LargeList(field) => {
            list_value::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder, field, term)
        }
        FixedSizeList(field, length) => fixed_list_value(builder, field, *length, term),
        Struct(fields) => struct_value(builder, fields, term),
        _ => Err(invalid_batch("batch type is unsupported for writing")),
    }
}

fn string_value<T>(builder: &mut dyn ArrayBuilder, term: Term<'_>) -> Result<(), NativeFailure>
where
    T: ArrayBuilder + 'static,
    T: StringAppend,
{
    let builder = downcast_builder::<T>(builder)?;
    if is_nil(term) {
        builder.append_null_value();
    } else {
        let value: String = term
            .decode()
            .map_err(|_| invalid_batch("expected a string"))?;
        builder.append_string(&value);
    }
    Ok(())
}

trait StringAppend {
    fn append_string(&mut self, value: &str);
    fn append_null_value(&mut self);
}

impl StringAppend for StringBuilder {
    fn append_string(&mut self, value: &str) {
        self.append_value(value);
    }
    fn append_null_value(&mut self) {
        self.append_null();
    }
}

impl StringAppend for LargeStringBuilder {
    fn append_string(&mut self, value: &str) {
        self.append_value(value);
    }
    fn append_null_value(&mut self) {
        self.append_null();
    }
}

fn binary_value<T>(builder: &mut dyn ArrayBuilder, term: Term<'_>) -> Result<(), NativeFailure>
where
    T: ArrayBuilder + BinaryAppend + 'static,
{
    let builder = downcast_builder::<T>(builder)?;
    if is_nil(term) {
        builder.append_null_value();
    } else {
        let value: Binary<'_> = term
            .decode()
            .map_err(|_| invalid_batch("expected a binary"))?;
        builder.append_binary(value.as_slice());
    }
    Ok(())
}

trait BinaryAppend {
    fn append_binary(&mut self, value: &[u8]);
    fn append_null_value(&mut self);
}

impl BinaryAppend for BinaryBuilder {
    fn append_binary(&mut self, value: &[u8]) {
        self.append_value(value);
    }
    fn append_null_value(&mut self) {
        self.append_null();
    }
}

impl BinaryAppend for LargeBinaryBuilder {
    fn append_binary(&mut self, value: &[u8]) {
        self.append_value(value);
    }
    fn append_null_value(&mut self) {
        self.append_null();
    }
}

fn fixed_binary_value(builder: &mut dyn ArrayBuilder, term: Term<'_>) -> Result<(), NativeFailure> {
    let builder = downcast_builder::<FixedSizeBinaryBuilder>(builder)?;
    if is_nil(term) {
        builder.append_null();
        Ok(())
    } else {
        let value: Binary<'_> = term
            .decode()
            .map_err(|_| invalid_batch("expected a binary"))?;
        builder
            .append_value(value.as_slice())
            .map_err(|_| invalid_batch("fixed binary has the wrong length"))
    }
}

trait DynamicListBuilder: ArrayBuilder {
    fn values_mut(&mut self) -> &mut dyn ArrayBuilder;
    fn append_list(&mut self, valid: bool);
}

impl DynamicListBuilder for ListBuilder<Box<dyn ArrayBuilder>> {
    fn values_mut(&mut self) -> &mut dyn ArrayBuilder {
        self.values().as_mut()
    }
    fn append_list(&mut self, valid: bool) {
        self.append(valid);
    }
}

impl DynamicListBuilder for LargeListBuilder<Box<dyn ArrayBuilder>> {
    fn values_mut(&mut self) -> &mut dyn ArrayBuilder {
        self.values().as_mut()
    }
    fn append_list(&mut self, valid: bool) {
        self.append(valid);
    }
}

fn list_value<T>(
    builder: &mut dyn ArrayBuilder,
    field: &Arc<Field>,
    term: Term<'_>,
) -> Result<(), NativeFailure>
where
    T: DynamicListBuilder + 'static,
{
    let builder = downcast_builder::<T>(builder)?;
    if is_nil(term) {
        builder.append_list(false);
        return Ok(());
    }
    let values: Vec<Term<'_>> = term
        .decode()
        .map_err(|_| invalid_batch("expected a list"))?;
    for value in values {
        append_value(
            builder.values_mut(),
            field.data_type(),
            field.is_nullable(),
            value,
        )?;
    }
    builder.append_list(true);
    Ok(())
}

fn fixed_list_value(
    builder: &mut dyn ArrayBuilder,
    field: &Arc<Field>,
    length: i32,
    term: Term<'_>,
) -> Result<(), NativeFailure> {
    let builder = downcast_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(builder)?;
    if is_nil(term) {
        for _ in 0..length {
            append_null(builder.values().as_mut(), field.data_type())?;
        }
        builder.append(false);
        return Ok(());
    }
    let values: Vec<Term<'_>> = term
        .decode()
        .map_err(|_| invalid_batch("expected a list"))?;
    if values.len() != length as usize {
        return Err(invalid_batch("fixed list has the wrong length"));
    }
    for value in values {
        append_value(
            builder.values().as_mut(),
            field.data_type(),
            field.is_nullable(),
            value,
        )?;
    }
    builder.append(true);
    Ok(())
}

fn struct_value(
    builder: &mut dyn ArrayBuilder,
    fields: &Fields,
    term: Term<'_>,
) -> Result<(), NativeFailure> {
    let builder = downcast_builder::<StructBuilder>(builder)?;
    if is_nil(term) {
        for (child, field) in builder.field_builders_mut().iter_mut().zip(fields) {
            append_null(child.as_mut(), field.data_type())?;
        }
        builder.append(false);
        return Ok(());
    }
    for (child, field) in builder.field_builders_mut().iter_mut().zip(fields) {
        let value = term
            .map_get(field.name().as_str())
            .map_err(|_| invalid_batch("struct field is missing"))?;
        append_value(
            child.as_mut(),
            field.data_type(),
            field.is_nullable(),
            value,
        )?;
    }
    builder.append(true);
    Ok(())
}

fn append_null(builder: &mut dyn ArrayBuilder, data_type: &DataType) -> Result<(), NativeFailure> {
    use DataType::*;
    match data_type {
        Null => downcast_builder::<NullBuilder>(builder)?.append_null(),
        Boolean => downcast_builder::<BooleanBuilder>(builder)?.append_null(),
        Int8 => downcast_builder::<Int8Builder>(builder)?.append_null(),
        Int16 => downcast_builder::<Int16Builder>(builder)?.append_null(),
        Int32 => downcast_builder::<Int32Builder>(builder)?.append_null(),
        Int64 => downcast_builder::<Int64Builder>(builder)?.append_null(),
        UInt8 => downcast_builder::<UInt8Builder>(builder)?.append_null(),
        UInt16 => downcast_builder::<UInt16Builder>(builder)?.append_null(),
        UInt32 => downcast_builder::<UInt32Builder>(builder)?.append_null(),
        UInt64 => downcast_builder::<UInt64Builder>(builder)?.append_null(),
        Float32 => downcast_builder::<Float32Builder>(builder)?.append_null(),
        Float64 => downcast_builder::<Float64Builder>(builder)?.append_null(),
        Utf8 => downcast_builder::<StringBuilder>(builder)?.append_null(),
        LargeUtf8 => downcast_builder::<LargeStringBuilder>(builder)?.append_null(),
        Binary => downcast_builder::<BinaryBuilder>(builder)?.append_null(),
        LargeBinary => downcast_builder::<LargeBinaryBuilder>(builder)?.append_null(),
        FixedSizeBinary(_) => downcast_builder::<FixedSizeBinaryBuilder>(builder)?.append_null(),
        Date32 => downcast_builder::<Date32Builder>(builder)?.append_null(),
        Date64 => downcast_builder::<Date64Builder>(builder)?.append_null(),
        Time32(TimeUnit::Second) => downcast_builder::<Time32SecondBuilder>(builder)?.append_null(),
        Time32(TimeUnit::Millisecond) => {
            downcast_builder::<Time32MillisecondBuilder>(builder)?.append_null()
        }
        Time64(TimeUnit::Microsecond) => {
            downcast_builder::<Time64MicrosecondBuilder>(builder)?.append_null()
        }
        Time64(TimeUnit::Nanosecond) => {
            downcast_builder::<Time64NanosecondBuilder>(builder)?.append_null()
        }
        Timestamp(TimeUnit::Second, _) => {
            downcast_builder::<TimestampSecondBuilder>(builder)?.append_null()
        }
        Timestamp(TimeUnit::Millisecond, _) => {
            downcast_builder::<TimestampMillisecondBuilder>(builder)?.append_null()
        }
        Timestamp(TimeUnit::Microsecond, _) => {
            downcast_builder::<TimestampMicrosecondBuilder>(builder)?.append_null()
        }
        Timestamp(TimeUnit::Nanosecond, _) => {
            downcast_builder::<TimestampNanosecondBuilder>(builder)?.append_null()
        }
        Duration(TimeUnit::Second) => {
            downcast_builder::<DurationSecondBuilder>(builder)?.append_null()
        }
        Duration(TimeUnit::Millisecond) => {
            downcast_builder::<DurationMillisecondBuilder>(builder)?.append_null()
        }
        Duration(TimeUnit::Microsecond) => {
            downcast_builder::<DurationMicrosecondBuilder>(builder)?.append_null()
        }
        Duration(TimeUnit::Nanosecond) => {
            downcast_builder::<DurationNanosecondBuilder>(builder)?.append_null()
        }
        Decimal32(_, _) => downcast_builder::<Decimal32Builder>(builder)?.append_null(),
        Decimal64(_, _) => downcast_builder::<Decimal64Builder>(builder)?.append_null(),
        Decimal128(_, _) => downcast_builder::<Decimal128Builder>(builder)?.append_null(),
        Decimal256(_, _) => downcast_builder::<Decimal256Builder>(builder)?.append_null(),
        List(_) => downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)?.append(false),
        LargeList(_) => {
            downcast_builder::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder)?.append(false)
        }
        FixedSizeList(field, length) => {
            let builder = downcast_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(builder)?;
            for _ in 0..*length {
                append_null(builder.values().as_mut(), field.data_type())?;
            }
            builder.append(false);
        }
        Struct(fields) => {
            let builder = downcast_builder::<StructBuilder>(builder)?;
            for (child, field) in builder.field_builders_mut().iter_mut().zip(fields) {
                append_null(child.as_mut(), field.data_type())?;
            }
            builder.append(false);
        }
        _ => {
            return Err(invalid_batch(
                "nested null padding is unsupported for this type",
            ))
        }
    }
    Ok(())
}

fn downcast_builder<T: 'static>(builder: &mut dyn ArrayBuilder) -> Result<&mut T, NativeFailure> {
    builder.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
        NativeFailure::expected(Operation::ParquetWriterWrite, "Arrow builder mismatch")
    })
}

fn invalid_batch(message: &'static str) -> NativeFailure {
    NativeFailure::invalid(Operation::ParquetWriterWrite, message)
}

fn encode_failure(operation: Operation) -> NativeFailure {
    NativeFailure::new(
        Category::MalformedData,
        operation,
        "Parquet batch encoding failed",
    )
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, RecordBatch};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    #[test]
    fn advertised_codecs_are_readable_through_an_independent_reader() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .expect("valid batch");
        let codecs = [
            Compression::UNCOMPRESSED,
            Compression::SNAPPY,
            Compression::ZSTD(ZstdLevel::default()),
            Compression::GZIP(GzipLevel::default()),
            Compression::LZ4_RAW,
        ];

        for codec in codecs {
            let properties = WriterProperties::builder()
                .set_compression(codec)
                .set_max_row_group_row_count(Some(2))
                .set_data_page_size_limit(32)
                .build();
            let mut writer =
                ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties)).unwrap();
            writer.write(&batch).unwrap();
            let encoded = writer.into_inner().unwrap();

            let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(encoded)).unwrap();
            assert_eq!(builder.metadata().num_row_groups(), 2);
            assert!(builder
                .metadata()
                .row_groups()
                .iter()
                .flat_map(|row_group| row_group.columns())
                .all(|column| column.compression() == codec));
            let rows = builder
                .with_batch_size(2)
                .build()
                .unwrap()
                .map(|batch| batch.unwrap().num_rows())
                .sum::<usize>();
            assert_eq!(rows, 4);
        }
    }
}
