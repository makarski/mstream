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

    fn try_from(value: FramedBytes<I>) -> Result<Self, Self::Error> {
        let mut result = Vec::new();
        // 1. Count Placeholder (4 bytes)
        result.extend(0u32.to_le_bytes());

        // 2. Content Type (1 byte)
        result.push(value.content_type as u8);

        let mut count = 0u32;
        for event_bytes in value.iter {
            count += 1;
            // 3. Item Length (4 bytes)
            let event_len = event_bytes.len() as u32;
            result.extend(event_len.to_le_bytes());
            // 4. Item Payload
            result.extend(event_bytes);
        }

        // Update count at the beginning
        let count_bytes = count.to_le_bytes();
        result[0..4].copy_from_slice(&count_bytes);

        Ok(result)
    }
}

pub fn decode(bytes: &[u8]) -> anyhow::Result<(Vec<Vec<u8>>, BatchContentType)> {
    if bytes.len() < 5 {
        return Err(anyhow::anyhow!("invalid FramedBytes: too short"));
    }

    let count = u32::from_le_bytes(bytes[0..4].try_into()?);
    let content_type = BatchContentType::try_from(bytes[4])?;

    let mut offset = 5;
    let mut items = Vec::with_capacity(count as usize);

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

    pub fn add_item(&mut self, bytes: &[u8]) {
        self.count += 1;
        let len = bytes.len() as u32;
        self.buffer.extend_from_slice(&len.to_le_bytes());
        self.buffer.extend_from_slice(bytes);
    }

    pub fn add_item_with<F, E>(&mut self, f: F) -> Result<(), E>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
    {
        self.count += 1;
        let start_len = self.buffer.len();
        // Placeholder for length
        self.buffer.extend_from_slice(&0u32.to_le_bytes());

        // Write payload
        f(&mut self.buffer)?;

        let end_len = self.buffer.len();
        let item_len = (end_len - start_len - 4) as u32;

        // Patch length
        let len_bytes = item_len.to_le_bytes();
        self.buffer[start_len..start_len + 4].copy_from_slice(&len_bytes);

        Ok(())
    }

    pub fn finish(mut self) -> Vec<u8> {
        // Patch count
        let count_bytes = self.count.to_le_bytes();
        self.buffer[0..4].copy_from_slice(&count_bytes);
        self.buffer
    }
}
