use std::convert::TryFrom;

pub struct OtherBytes(pub Vec<Vec<u8>>);

impl TryFrom<OtherBytes> for Vec<u8> {
    type Error = anyhow::Error;

    /// For batch, use a a length-prefixed format
    /// Format:
    /// - 4 bytes: length of the batch (u32 in little-endian)
    /// - For each event in the batch:
    ///     - 4 bytes: length of the event (u32 in little-endian)
    ///     - N bytes: the event itself in original encoding
    fn try_from(value: OtherBytes) -> Result<Self, Self::Error> {
        let batch = &value.0;
        let message_count = batch.len() as u32;
        let overhead_size = 4 + 4 * message_count;
        let payload_size = batch.iter().map(|p| p.len()).sum::<usize>();
        let total_size = overhead_size as usize + payload_size;

        let mut result = Vec::with_capacity(total_size);

        // Write 4 bytes: message count
        result.extend(message_count.to_le_bytes());

        for event_bytes in batch {
            let event_len = event_bytes.len() as u32;
            // Write 4 bytes: event length
            result.extend(event_len.to_le_bytes());
            // Write N bytes: the event itself
            result.extend(event_bytes);
        }

        Ok(result)
    }
}
