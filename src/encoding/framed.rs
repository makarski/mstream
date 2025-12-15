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
