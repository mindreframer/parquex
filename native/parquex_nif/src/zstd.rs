use std::io::Read;

use crate::error::{Category, NativeFailure};
use crate::Operation;

pub(crate) fn compress(data: &[u8], level: i32) -> Result<Vec<u8>, NativeFailure> {
    if !zstd::compression_level_range().contains(&level) {
        return Err(NativeFailure::invalid(
            Operation::ZstdCompress,
            "zstd compression level is outside the supported range",
        ));
    }

    zstd::bulk::compress(data, level).map_err(|_error| {
        NativeFailure::expected(Operation::ZstdCompress, "zstd compression failed")
    })
}

pub(crate) fn decompress(data: &[u8], max_output_size: usize) -> Result<Vec<u8>, NativeFailure> {
    let mut decoder = zstd::stream::Decoder::new(data).map_err(|_error| malformed_data())?;
    decoder
        .window_log_max(window_log_max(max_output_size))
        .map_err(|_error| malformed_data())?;

    let read_limit = u64::try_from(max_output_size)
        .ok()
        .and_then(|limit| limit.checked_add(1))
        .ok_or_else(|| {
            NativeFailure::invalid(
                Operation::ZstdDecompress,
                "max_output_size is outside the supported range",
            )
        })?;
    let mut output = Vec::new();
    decoder
        .take(read_limit)
        .read_to_end(&mut output)
        .map_err(|_error| malformed_data())?;

    if output.len() > max_output_size {
        return Err(NativeFailure::invalid(
            Operation::ZstdDecompress,
            "decompressed data exceeds max_output_size",
        ));
    }

    Ok(output)
}

fn window_log_max(max_output_size: usize) -> u32 {
    let required_bits = usize::BITS - max_output_size.saturating_sub(1).leading_zeros();
    required_bits.clamp(10, 31)
}

fn malformed_data() -> NativeFailure {
    NativeFailure::new(
        Category::MalformedData,
        Operation::ZstdDecompress,
        "input is not a valid zstd frame",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_uses_standard_frames() {
        let input = b"parquex-zstd".repeat(1_024);
        let compressed = compress(&input, 0).expect("compress");

        assert_eq!(&compressed[..4], &[0x28, 0xb5, 0x2f, 0xfd]);
        assert_eq!(
            decompress(&compressed, input.len()).expect("decompress"),
            input
        );
    }

    #[test]
    fn decompresses_concatenated_frames() {
        let mut compressed = compress(b"first", 3).expect("first frame");
        compressed.extend(compress(b"second", 3).expect("second frame"));

        assert_eq!(
            decompress(&compressed, 11).expect("concatenated frames"),
            b"firstsecond"
        );
    }

    #[test]
    fn decompression_enforces_output_bound() {
        let compressed = compress(&vec![b'x'; 4_096], 3).expect("compress");
        let failure = decompress(&compressed, 4_095).expect_err("bound must fail");

        assert_eq!(failure.category, Category::InvalidArgument);
        assert_eq!(failure.operation, Operation::ZstdDecompress);
    }

    #[test]
    fn malformed_input_has_stable_error() {
        let failure = decompress(b"not-zstd", 1_024).expect_err("malformed input must fail");

        assert_eq!(failure.category, Category::MalformedData);
        assert_eq!(failure.operation, Operation::ZstdDecompress);
        assert_eq!(failure.message, "input is not a valid zstd frame");
    }

    #[test]
    fn compression_rejects_unsupported_level() {
        let level = *zstd::compression_level_range().end() + 1;
        let failure = compress(b"data", level).expect_err("level must fail");

        assert_eq!(failure.category, Category::InvalidArgument);
        assert_eq!(failure.operation, Operation::ZstdCompress);
    }
}
