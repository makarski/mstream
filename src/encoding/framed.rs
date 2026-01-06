//! Framed encoding support for batch processing.
//!
//! This module provides utilities for encoding and decoding batches of messages
//! using a length-prefixed framed format. This format is optimized for
//! zero-copy processing and efficient network transmission.
//!
//! # Format Structure
//!
//! The format consists of a header followed by a sequence of items:
//!
//! ```text
//! +----------------+--------------+----------------+----------------+
//! | Count (4 bytes)| Type (1 byte)| Item 1 Length  | Item 1 Payload | ...
//! +----------------+--------------+----------------+----------------+
//! |    u32 LE      |      u8      |    u32 LE      |    bytes       |
//! +----------------+--------------+----------------+----------------+
//! ```
//!
//! - **Count**: Total number of items in the batch (Little Endian u32).
//! - **Type**: Content type of the items (u8 enum value).
//! - **Item Length**: Length of the following payload (Little Endian u32).
//! - **Item Payload**: The actual data bytes.

use std::convert::TryFrom;
use std::convert::TryInto;

use crate::config::Encoding;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum BatchContentType {
    Raw = 0,
    Json = 1,
    Bson = 2,
    Avro = 3,
}

impl TryFrom<u8> for BatchContentType {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(BatchContentType::Raw),
            1 => Ok(BatchContentType::Json),
            2 => Ok(BatchContentType::Bson),
            3 => Ok(BatchContentType::Avro),
            _ => Err(anyhow::anyhow!("unknown batch content type: {}", v)),
        }
    }
}

impl From<&Encoding> for BatchContentType {
    fn from(enc: &Encoding) -> Self {
        match enc {
            Encoding::Json => BatchContentType::Json,
            Encoding::Bson => BatchContentType::Bson,
            Encoding::Avro => BatchContentType::Avro,
        }
    }
}

/// A wrapper struct for converting an iterator of byte vectors into a framed message.
///
/// This struct is primarily used with `TryFrom` to encode a batch of items into
/// the framed format.
pub struct FramedBytes<I> {
    pub iter: I,
    pub content_type: BatchContentType,
}

impl<I> FramedBytes<I> {
    pub fn new(iter: I, content_type: BatchContentType) -> Self {
        Self { iter, content_type }
    }
}

impl<I> TryFrom<FramedBytes<I>> for Vec<u8>
where
    I: IntoIterator<Item = Vec<u8>>,
{
    type Error = anyhow::Error;

    /// Encodes the iterator of items into a single framed byte vector.
    ///
    /// This process iterates over all items, writing them to a `FramedWriter`
    /// with the specified content type.
    fn try_from(value: FramedBytes<I>) -> Result<Self, Self::Error> {
        let iter = value.iter.into_iter();
        let (lower, _) = iter.size_hint();
        // Heuristic: assume 256 bytes per item if we have a count, otherwise default
        let capacity = if lower > 0 { lower * 256 } else { 1024 };

        let mut writer = FramedWriter::new(value.content_type, capacity);

        for event_bytes in iter {
            writer.add_item(&event_bytes)?;
        }

        Ok(writer.finish())
    }
}

/// Decodes a framed byte slice into a vector of items and their content type.
///
/// This function validates the header (count and content type) and iterates
/// through the length-prefixed items, extracting them into separate vectors.
///
/// # Returns
///
/// Returns a tuple containing:
/// - `Vec<Vec<u8>>`: The list of decoded items.
/// - `BatchContentType`: The type of content stored in the items.
///
/// # Errors
///
/// Returns an error if:
/// - The input slice is too short to contain a header.
/// - The content type is invalid.
/// - An item length prefix indicates a size larger than the remaining data.
pub fn decode(bytes: &[u8]) -> anyhow::Result<(Vec<Vec<u8>>, BatchContentType)> {
    if bytes.len() < 5 {
        return Err(anyhow::anyhow!("invalid FramedBytes: too short"));
    }

    let count = u32::from_le_bytes(bytes[0..4].try_into()?);
    let content_type = BatchContentType::try_from(bytes[4])?;

    let mut offset = 5;
    let count_usize = usize::try_from(count).map_err(|_| anyhow::anyhow!("count exceeds usize"))?;
    let mut items = Vec::with_capacity(count_usize);

    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err(anyhow::anyhow!(
                "invalid FramedBytes: unexpected end of data"
            ));
        }
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into()?) as usize;
        offset += 4;

        if offset + len > bytes.len() {
            return Err(anyhow::anyhow!(
                "invalid FramedBytes: item length exceeds data"
            ));
        }
        items.push(bytes[offset..offset + len].to_vec());
        offset += len;
    }

    Ok((items, content_type))
}

/// A helper struct for constructing framed batch messages.
///
/// This writer manages a buffer where items are serialized with length prefixes,
/// allowing for efficient batch processing and zero-copy decoding where supported.
///
/// # Format Specification
///
/// The framed format consists of a header followed by a sequence of items:
///
/// ```text
/// +----------------+--------------+----------------+----------------+
/// | Count (4 bytes)| Type (1 byte)| Item 1 Length  | Item 1 Payload | ...
/// +----------------+--------------+----------------+----------------+
/// |    u32 LE      |      u8      |    u32 LE      |    bytes       |
/// +----------------+--------------+----------------+----------------+
/// ```
pub struct FramedWriter {
    pub buffer: Vec<u8>,
    count: u32,
}

impl FramedWriter {
    pub fn new(content_type: BatchContentType, capacity_hint: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity_hint);
        // Placeholder for count
        buffer.extend_from_slice(&0u32.to_le_bytes());
        // Content Type
        buffer.push(content_type as u8);

        Self { buffer, count: 0 }
    }

    /// Adds a raw byte slice as an item to the framed message.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mstream::encoding::framed::{FramedWriter, BatchContentType};
    ///
    /// let mut writer = FramedWriter::new(BatchContentType::Json, 1024);
    /// writer.add_item(b"{\"key\": \"value\"}").unwrap();
    /// ```
    pub fn add_item(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.count += 1;
        let len: u32 = bytes
            .len()
            .try_into()
            .map_err(|_| anyhow::anyhow!("item length exceeds u32::MAX"))?;
        self.buffer.extend_from_slice(&len.to_le_bytes());
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    /// Adds an item by invoking a closure that writes directly to the buffer.
    ///
    /// This method reserves space for the length prefix, calls the closure to write the payload,
    /// and then patches the length prefix with the actual written size.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::io::Write;
    /// use mstream::encoding::framed::{FramedWriter, BatchContentType};
    ///
    /// let mut writer = FramedWriter::new(BatchContentType::Json, 1024);
    /// writer.add_item_with(|buf| {
    ///     write!(buf, "{{\"key\": \"{}\"}}", "value").map_err(anyhow::Error::from)
    /// }).unwrap();
    /// ```
    pub fn add_item_with<F, E>(&mut self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
        E: Into<anyhow::Error>,
    {
        self.count += 1;
        let start_len = self.buffer.len();
        // Placeholder for length
        self.buffer.extend_from_slice(&0u32.to_le_bytes());

        // Write payload
        f(&mut self.buffer).map_err(Into::into)?;

        let end_len = self.buffer.len();
        let item_len: u32 = (end_len - start_len - 4).try_into()?;

        // Patch length
        let len_bytes = item_len.to_le_bytes();
        self.buffer[start_len..start_len + 4].copy_from_slice(&len_bytes);

        Ok(())
    }

    /// Finalizes the framed message and returns the underlying buffer.
    ///
    /// This method updates the count field in the header (first 4 bytes) with the
    /// actual number of items added to the writer.
    pub fn finish(mut self) -> Vec<u8> {
        // Patch count
        let count_bytes = self.count.to_le_bytes();
        self.buffer[0..4].copy_from_slice(&count_bytes);
        self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Write;

    // =========================================================================
    // Group 1: BatchContentType conversions
    // =========================================================================

    #[test]
    fn batch_content_type_try_from_valid_values() {
        assert_eq!(
            BatchContentType::try_from(0).unwrap(),
            BatchContentType::Raw
        );
        assert_eq!(
            BatchContentType::try_from(1).unwrap(),
            BatchContentType::Json
        );
        assert_eq!(
            BatchContentType::try_from(2).unwrap(),
            BatchContentType::Bson
        );
        assert_eq!(
            BatchContentType::try_from(3).unwrap(),
            BatchContentType::Avro
        );
    }

    #[test]
    fn batch_content_type_try_from_invalid_value() {
        let err = BatchContentType::try_from(255).unwrap_err();
        assert!(err.to_string().contains("unknown batch content type: 255"));

        let err = BatchContentType::try_from(4).unwrap_err();
        assert!(err.to_string().contains("unknown batch content type: 4"));
    }

    #[test]
    fn batch_content_type_from_encoding() {
        assert_eq!(
            BatchContentType::from(&Encoding::Json),
            BatchContentType::Json
        );
        assert_eq!(
            BatchContentType::from(&Encoding::Bson),
            BatchContentType::Bson
        );
        assert_eq!(
            BatchContentType::from(&Encoding::Avro),
            BatchContentType::Avro
        );
    }

    #[test]
    fn batch_content_type_repr_values() {
        // Verify repr(u8) values match expected discriminants
        assert_eq!(BatchContentType::Raw as u8, 0);
        assert_eq!(BatchContentType::Json as u8, 1);
        assert_eq!(BatchContentType::Bson as u8, 2);
        assert_eq!(BatchContentType::Avro as u8, 3);
    }

    // =========================================================================
    // Group 2: decode() error paths
    // =========================================================================

    #[test]
    fn decode_error_too_short_empty() {
        let err = decode(&[]).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn decode_error_too_short_partial_header() {
        // Only 4 bytes (missing content type)
        let err = decode(&[0, 0, 0, 0]).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn decode_error_invalid_content_type() {
        // Valid header length but invalid content type (255)
        let bytes = [0, 0, 0, 0, 255]; // count=0, type=255
        let err = decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("unknown batch content type: 255"));
    }

    #[test]
    fn decode_error_truncated_item_length() {
        // Header says 1 item, but no item length bytes follow
        let mut bytes = vec![];
        bytes.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        bytes.push(0); // content type = Raw
        // Missing item length and payload

        let err = decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("unexpected end of data"));
    }

    #[test]
    fn decode_error_truncated_item_length_partial() {
        // Header says 1 item, only 2 bytes of item length (need 4)
        let mut bytes = vec![];
        bytes.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        bytes.push(0); // content type = Raw
        bytes.extend_from_slice(&[0, 0]); // partial length (only 2 bytes)

        let err = decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("unexpected end of data"));
    }

    #[test]
    fn decode_error_item_length_exceeds_data() {
        // Header says 1 item, item length says 100 bytes, but only 5 bytes present
        let mut bytes = vec![];
        bytes.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        bytes.push(0); // content type = Raw
        bytes.extend_from_slice(&100u32.to_le_bytes()); // item length = 100
        bytes.extend_from_slice(b"hello"); // only 5 bytes of payload

        let err = decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("item length exceeds data"));
    }

    #[test]
    fn decode_error_second_item_truncated() {
        // Two items declared, first is valid, second is truncated
        let mut bytes = vec![];
        bytes.extend_from_slice(&2u32.to_le_bytes()); // count = 2
        bytes.push(1); // content type = Json
        // First item: valid
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.extend_from_slice(b"hello");
        // Second item: missing

        let err = decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("unexpected end of data"));
    }

    // =========================================================================
    // Group 3: FramedWriter basic operations
    // =========================================================================

    #[test]
    fn framed_writer_new_initializes_header() {
        let writer = FramedWriter::new(BatchContentType::Json, 1024);

        // Buffer should have 5 bytes: 4 for count placeholder + 1 for content type
        assert_eq!(writer.buffer.len(), 5);
        // Count placeholder is 0
        assert_eq!(&writer.buffer[0..4], &[0, 0, 0, 0]);
        // Content type byte
        assert_eq!(writer.buffer[4], BatchContentType::Json as u8);
    }

    #[test]
    fn framed_writer_new_different_content_types() {
        for (ct, expected_byte) in [
            (BatchContentType::Raw, 0),
            (BatchContentType::Json, 1),
            (BatchContentType::Bson, 2),
            (BatchContentType::Avro, 3),
        ] {
            let writer = FramedWriter::new(ct, 64);
            assert_eq!(writer.buffer[4], expected_byte);
        }
    }

    #[test]
    fn framed_writer_add_item_appends_correctly() {
        let mut writer = FramedWriter::new(BatchContentType::Raw, 1024);
        writer.add_item(b"hello").unwrap();

        // Header (5) + length prefix (4) + payload (5) = 14
        assert_eq!(writer.buffer.len(), 14);

        // Check length prefix at offset 5
        let len = u32::from_le_bytes(writer.buffer[5..9].try_into().unwrap());
        assert_eq!(len, 5);

        // Check payload
        assert_eq!(&writer.buffer[9..14], b"hello");
    }

    #[test]
    fn framed_writer_add_item_multiple() {
        let mut writer = FramedWriter::new(BatchContentType::Json, 1024);
        writer.add_item(b"one").unwrap();
        writer.add_item(b"two").unwrap();
        writer.add_item(b"three").unwrap();

        let result = writer.finish();

        // Verify count is patched to 3
        let count = u32::from_le_bytes(result[0..4].try_into().unwrap());
        assert_eq!(count, 3);
    }

    #[test]
    fn framed_writer_add_item_with_closure() {
        let mut writer = FramedWriter::new(BatchContentType::Json, 1024);

        writer
            .add_item_with(|buf| write!(buf, "{{\"key\": \"value\"}}").map_err(anyhow::Error::from))
            .unwrap();

        let result = writer.finish();

        // Decode and verify
        let (items, ct) = decode(&result).unwrap();
        assert_eq!(ct, BatchContentType::Json);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], b"{\"key\": \"value\"}");
    }

    #[test]
    fn framed_writer_add_item_with_error_propagates() {
        let mut writer = FramedWriter::new(BatchContentType::Raw, 64);

        let result = writer.add_item_with(|_buf| -> Result<(), anyhow::Error> {
            Err(anyhow::anyhow!("test error"))
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test error"));
    }

    #[test]
    fn framed_writer_finish_patches_count() {
        let mut writer = FramedWriter::new(BatchContentType::Bson, 1024);

        // Count should be 0 before adding items
        assert_eq!(&writer.buffer[0..4], &[0, 0, 0, 0]);

        writer.add_item(b"item1").unwrap();
        writer.add_item(b"item2").unwrap();

        let result = writer.finish();

        // Count should now be 2
        let count = u32::from_le_bytes(result[0..4].try_into().unwrap());
        assert_eq!(count, 2);
    }

    // =========================================================================
    // Group 4: Round-trip (encode → decode)
    // =========================================================================

    #[test]
    fn roundtrip_empty_batch() {
        let items: Vec<Vec<u8>> = vec![];
        let framed = FramedBytes::new(items.clone(), BatchContentType::Json);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, decoded_ct) = decode(&encoded).unwrap();

        assert_eq!(decoded_items, items);
        assert_eq!(decoded_ct, BatchContentType::Json);
    }

    #[test]
    fn roundtrip_single_item() {
        let items = vec![b"hello world".to_vec()];
        let framed = FramedBytes::new(items.clone(), BatchContentType::Raw);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, decoded_ct) = decode(&encoded).unwrap();

        assert_eq!(decoded_items, items);
        assert_eq!(decoded_ct, BatchContentType::Raw);
    }

    #[test]
    fn roundtrip_multiple_items() {
        let items = vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()];
        let framed = FramedBytes::new(items.clone(), BatchContentType::Avro);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, decoded_ct) = decode(&encoded).unwrap();

        assert_eq!(decoded_items, items);
        assert_eq!(decoded_ct, BatchContentType::Avro);
    }

    #[test]
    fn roundtrip_all_content_types() {
        let items = vec![b"test data".to_vec()];

        for ct in [
            BatchContentType::Raw,
            BatchContentType::Json,
            BatchContentType::Bson,
            BatchContentType::Avro,
        ] {
            let framed = FramedBytes::new(items.clone(), ct);
            let encoded: Vec<u8> = framed.try_into().unwrap();
            let (decoded_items, decoded_ct) = decode(&encoded).unwrap();

            assert_eq!(decoded_items, items);
            assert_eq!(decoded_ct, ct);
        }
    }

    #[test]
    fn roundtrip_with_framed_writer() {
        let mut writer = FramedWriter::new(BatchContentType::Json, 1024);
        writer.add_item(b"{\"a\": 1}").unwrap();
        writer.add_item(b"{\"b\": 2}").unwrap();

        let encoded = writer.finish();
        let (items, ct) = decode(&encoded).unwrap();

        assert_eq!(ct, BatchContentType::Json);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], b"{\"a\": 1}");
        assert_eq!(items[1], b"{\"b\": 2}");
    }

    #[test]
    fn roundtrip_mixed_add_methods() {
        let mut writer = FramedWriter::new(BatchContentType::Raw, 1024);

        writer.add_item(b"direct").unwrap();
        writer
            .add_item_with(|buf| {
                buf.extend_from_slice(b"via closure");
                Ok::<_, anyhow::Error>(())
            })
            .unwrap();
        writer.add_item(b"direct again").unwrap();

        let encoded = writer.finish();
        let (items, _) = decode(&encoded).unwrap();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0], b"direct");
        assert_eq!(items[1], b"via closure");
        assert_eq!(items[2], b"direct again");
    }

    // =========================================================================
    // Group 5: Edge cases
    // =========================================================================

    #[test]
    fn edge_case_zero_length_payload() {
        let items = vec![vec![], vec![], vec![]];
        let framed = FramedBytes::new(items.clone(), BatchContentType::Raw);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, _) = decode(&encoded).unwrap();

        assert_eq!(decoded_items.len(), 3);
        assert!(decoded_items.iter().all(|item| item.is_empty()));
    }

    #[test]
    fn edge_case_mixed_empty_and_nonempty() {
        let items = vec![
            b"has content".to_vec(),
            vec![],
            b"also has content".to_vec(),
            vec![],
        ];
        let framed = FramedBytes::new(items.clone(), BatchContentType::Json);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, _) = decode(&encoded).unwrap();

        assert_eq!(decoded_items, items);
    }

    #[test]
    fn edge_case_large_item_count() {
        let item_count = 1000;
        let items: Vec<Vec<u8>> = (0..item_count)
            .map(|i| format!("item_{}", i).into_bytes())
            .collect();

        let framed = FramedBytes::new(items.clone(), BatchContentType::Raw);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, _) = decode(&encoded).unwrap();

        assert_eq!(decoded_items.len(), item_count);
        for (i, item) in decoded_items.iter().enumerate() {
            assert_eq!(item, &format!("item_{}", i).into_bytes());
        }
    }

    #[test]
    fn edge_case_large_payload() {
        // Single item with 64KB payload
        let large_payload = vec![0xABu8; 64 * 1024];
        let items = vec![large_payload.clone()];

        let framed = FramedBytes::new(items, BatchContentType::Raw);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, _) = decode(&encoded).unwrap();

        assert_eq!(decoded_items.len(), 1);
        assert_eq!(decoded_items[0], large_payload);
    }

    #[test]
    fn edge_case_binary_data() {
        // Items with arbitrary binary data including null bytes
        let items = vec![
            vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD],
            vec![0x00, 0x00, 0x00, 0x00],
            (0u8..=255).collect::<Vec<u8>>(),
        ];

        let framed = FramedBytes::new(items.clone(), BatchContentType::Bson);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, _) = decode(&encoded).unwrap();

        assert_eq!(decoded_items, items);
    }

    #[test]
    fn edge_case_framed_bytes_from_encoding_integration() {
        // Test integration with Encoding enum
        let items = vec![b"{\"test\": true}".to_vec()];
        let content_type = BatchContentType::from(&Encoding::Json);

        let framed = FramedBytes::new(items.clone(), content_type);
        let encoded: Vec<u8> = framed.try_into().unwrap();

        let (decoded_items, decoded_ct) = decode(&encoded).unwrap();

        assert_eq!(decoded_items, items);
        assert_eq!(decoded_ct, BatchContentType::Json);
    }

    #[test]
    fn edge_case_exact_header_size() {
        // Minimum valid frame: 0 items
        let mut bytes = vec![];
        bytes.extend_from_slice(&0u32.to_le_bytes()); // count = 0
        bytes.push(1); // content type = Json

        let (items, ct) = decode(&bytes).unwrap();

        assert!(items.is_empty());
        assert_eq!(ct, BatchContentType::Json);
    }

    #[test]
    fn edge_case_capacity_hint_used() {
        // Verify that capacity hint doesn't affect correctness
        let writer_small = FramedWriter::new(BatchContentType::Raw, 1);
        let writer_large = FramedWriter::new(BatchContentType::Raw, 1_000_000);

        // Both should produce valid empty frames
        let result_small = writer_small.finish();
        let result_large = writer_large.finish();

        // Content should be identical (just header)
        assert_eq!(result_small, result_large);
    }

    #[test]
    fn decode_valid_frame_manually_constructed() {
        // Manually construct a valid frame to verify decode logic
        let mut bytes = vec![];
        bytes.extend_from_slice(&2u32.to_le_bytes()); // count = 2
        bytes.push(1); // content type = Json

        // Item 1: "ab"
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(b"ab");

        // Item 2: "cde"
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(b"cde");

        let (items, ct) = decode(&bytes).unwrap();

        assert_eq!(ct, BatchContentType::Json);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], b"ab");
        assert_eq!(items[1], b"cde");
    }

    // =========================================================================
    // Property-based tests (proptest)
    // =========================================================================

    fn arb_content_type() -> impl Strategy<Value = BatchContentType> {
        prop_oneof![
            Just(BatchContentType::Raw),
            Just(BatchContentType::Json),
            Just(BatchContentType::Bson),
            Just(BatchContentType::Avro),
        ]
    }

    proptest! {
        #[test]
        fn proptest_roundtrip_framed_bytes(
            items in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..256), 0..50),
            content_type in arb_content_type()
        ) {
            let framed = FramedBytes::new(items.clone(), content_type);
            let encoded: Vec<u8> = framed.try_into().unwrap();

            let (decoded_items, decoded_ct) = decode(&encoded).unwrap();

            prop_assert_eq!(decoded_items, items);
            prop_assert_eq!(decoded_ct, content_type);
        }

        #[test]
        fn proptest_roundtrip_framed_writer(
            items in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..256), 0..50),
            content_type in arb_content_type()
        ) {
            let mut writer = FramedWriter::new(content_type, 1024);
            for item in &items {
                writer.add_item(item).unwrap();
            }
            let encoded = writer.finish();

            let (decoded_items, decoded_ct) = decode(&encoded).unwrap();

            prop_assert_eq!(decoded_items, items);
            prop_assert_eq!(decoded_ct, content_type);
        }

        #[test]
        fn proptest_count_header_matches_items(
            items in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..128), 0..100)
        ) {
            let framed = FramedBytes::new(items.clone(), BatchContentType::Raw);
            let encoded: Vec<u8> = framed.try_into().unwrap();

            // Read count directly from header
            let count = u32::from_le_bytes(encoded[0..4].try_into().unwrap());

            prop_assert_eq!(count as usize, items.len());
        }

        #[test]
        fn proptest_content_type_preserved(
            content_type in arb_content_type()
        ) {
            let items: Vec<Vec<u8>> = vec![b"test".to_vec()];
            let framed = FramedBytes::new(items, content_type);
            let encoded: Vec<u8> = framed.try_into().unwrap();

            // Read content type directly from header byte 4
            let ct_byte = encoded[4];
            let decoded_ct = BatchContentType::try_from(ct_byte).unwrap();

            prop_assert_eq!(decoded_ct, content_type);
        }

        #[test]
        fn proptest_encoded_length_correct(
            items in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..128), 0..20)
        ) {
            let framed = FramedBytes::new(items.clone(), BatchContentType::Json);
            let encoded: Vec<u8> = framed.try_into().unwrap();

            // Expected length: header (5) + sum of (4 + item.len()) for each item
            let expected_len = 5 + items.iter().map(|i| 4 + i.len()).sum::<usize>();

            prop_assert_eq!(encoded.len(), expected_len);
        }
    }
}
