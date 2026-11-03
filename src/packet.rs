//! The packet format shared by every plugin.
//!
//! All integers are big-endian:
//!
//! ```text
//! [header length: u16][total length: u32][sender id: u32][plugin id: u16]
//! [target ids: u32..., ended by NOBODY_ID]
//! [payload]
//! ```

use crate::error::{Error, Result};
use crate::types::{NodeId, PluginId, NOBODY_ID};
use std::collections::BTreeSet;
use std::io::{ErrorKind, Read};

/// Largest packet accepted, in bytes.
pub const MAX_PACKET_SIZE: usize = 16 * 1024 * 1024;

const PREFIX_LEN: usize = 6;
const TARGETS_OFFSET: usize = 12;
const MIN_HEADER_LEN: usize = TARGETS_OFFSET + 4;

/// A decoded packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Packet {
    pub from: NodeId,
    pub plugin: PluginId,
    pub targets: BTreeSet<NodeId>,
    pub payload: Vec<u8>,
}

impl Packet {
    /// Encodes to the wire format.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let targets: Vec<NodeId> = self
            .targets
            .iter()
            .copied()
            .filter(|&t| t != NOBODY_ID)
            .collect();
        let header_len = TARGETS_OFFSET + (targets.len() + 1) * 4;
        let total = header_len + self.payload.len();
        if header_len > u16::MAX as usize || total > MAX_PACKET_SIZE {
            return Err(Error::PacketTooLarge(total));
        }

        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(header_len as u16).to_be_bytes());
        out.extend_from_slice(&(total as u32).to_be_bytes());
        out.extend_from_slice(&self.from.to_be_bytes());
        out.extend_from_slice(&self.plugin.to_be_bytes());
        for target in targets {
            out.extend_from_slice(&target.to_be_bytes());
        }
        out.extend_from_slice(&NOBODY_ID.to_be_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    /// Decodes a whole packet, checking every length against the buffer.
    pub fn decode(frame: &[u8]) -> Result<Packet> {
        if frame.len() < MIN_HEADER_LEN {
            return Err(Error::InvalidPacket("shorter than the minimum header"));
        }
        let (header_len, total) = parse_prefix(frame)?;
        if total != frame.len() {
            return Err(Error::InvalidPacket("length mismatch"));
        }

        let header = &frame[..header_len];
        let mut targets = BTreeSet::new();
        let mut terminated = false;
        for chunk in header[TARGETS_OFFSET..].as_chunks::<4>().0 {
            let id = u32::from_be_bytes(*chunk);
            if id == NOBODY_ID {
                terminated = true;
                break;
            }
            targets.insert(id);
        }
        if !terminated {
            return Err(Error::InvalidPacket("unterminated target list"));
        }

        Ok(Packet {
            from: u32::from_be_bytes(frame[6..10].try_into().unwrap()),
            plugin: u16::from_be_bytes(frame[10..12].try_into().unwrap()),
            targets,
            payload: frame[header_len..].to_vec(),
        })
    }

    /// Reads one packet. Returns `None` if the stream ended between packets.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Option<Packet>> {
        let mut prefix = [0u8; PREFIX_LEN];
        loop {
            match reader.read(&mut prefix[..1]) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }
        }
        reader.read_exact(&mut prefix[1..])?;

        let (_, total) = parse_prefix(&prefix)?;
        let mut frame = vec![0u8; total];
        frame[..PREFIX_LEN].copy_from_slice(&prefix);
        reader.read_exact(&mut frame[PREFIX_LEN..])?;
        Packet::decode(&frame).map(Some)
    }
}

/// Validates the lengths in a packet prefix. They come from the network, so
/// they must be checked before being used to allocate or slice.
fn parse_prefix(bytes: &[u8]) -> Result<(usize, usize)> {
    let header_len = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
    let total = u32::from_be_bytes(bytes[2..6].try_into().unwrap()) as usize;
    if header_len < MIN_HEADER_LEN {
        return Err(Error::InvalidPacket("header too short"));
    }
    if total < header_len {
        return Err(Error::InvalidPacket("total length smaller than header"));
    }
    if total > MAX_PACKET_SIZE {
        return Err(Error::PacketTooLarge(total));
    }
    Ok((header_len, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn sample() -> Packet {
        Packet {
            from: 0x10001,
            plugin: 14,
            targets: BTreeSet::from([3, 1]),
            payload: vec![0xAA, 0xBB],
        }
    }

    /// Bytes produced by following `Packet::encode` in the C++ `packet.cpp`.
    const SAMPLE_BYTES: [u8; 26] = [
        0x00, 0x18, // header length: 24
        0x00, 0x00, 0x00, 0x1A, // total length: 26
        0x00, 0x01, 0x00, 0x01, // sender
        0x00, 0x0E, // plugin
        0x00, 0x00, 0x00, 0x01, // target (sorted)
        0x00, 0x00, 0x00, 0x03, // target
        0x00, 0x00, 0x00, 0x00, // terminator
        0xAA, 0xBB, // payload
    ];

    #[test]
    fn encodes_the_cpp_wire_format() {
        assert_eq!(sample().encode().unwrap(), SAMPLE_BYTES);
    }

    #[test]
    fn decodes_the_cpp_wire_format() {
        assert_eq!(Packet::decode(&SAMPLE_BYTES).unwrap(), sample());
    }

    #[test]
    fn encode_skips_nobody_in_targets() {
        let p = Packet {
            targets: BTreeSet::from([NOBODY_ID, 7]),
            ..Packet::default()
        };
        let decoded = Packet::decode(&p.encode().unwrap()).unwrap();
        assert_eq!(decoded.targets, BTreeSet::from([7]));
    }

    #[test]
    fn empty_payload_and_targets_round_trip() {
        let p = Packet::default();
        let bytes = p.encode().unwrap();
        assert_eq!(bytes.len(), MIN_HEADER_LEN);
        assert_eq!(Packet::decode(&bytes).unwrap(), p);
    }

    #[test]
    fn read_from_returns_none_at_clean_eof() {
        let mut empty = Cursor::new(Vec::new());
        assert!(Packet::read_from(&mut empty).unwrap().is_none());
    }

    #[test]
    fn read_from_reads_consecutive_packets() {
        let mut bytes = sample().encode().unwrap();
        bytes.extend(Packet::default().encode().unwrap());
        let mut cursor = Cursor::new(bytes);
        assert_eq!(Packet::read_from(&mut cursor).unwrap(), Some(sample()));
        assert_eq!(
            Packet::read_from(&mut cursor).unwrap(),
            Some(Packet::default())
        );
        assert!(Packet::read_from(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn read_from_fails_on_truncated_packet() {
        let bytes = sample().encode().unwrap();
        let mut cursor = Cursor::new(bytes[..10].to_vec());
        assert!(matches!(Packet::read_from(&mut cursor), Err(Error::Io(_))));
    }

    #[test]
    fn rejects_total_smaller_than_header() {
        // In the C++ code this makes `length - headerlen` underflow.
        let mut bytes = sample().encode().unwrap();
        bytes[2..6].copy_from_slice(&8u32.to_be_bytes());
        let mut cursor = Cursor::new(bytes);
        assert!(matches!(
            Packet::read_from(&mut cursor),
            Err(Error::InvalidPacket(_))
        ));
    }

    #[test]
    fn rejects_header_shorter_than_minimum() {
        // In the C++ code this makes `headerlen - 6` underflow.
        let mut bytes = sample().encode().unwrap();
        bytes[0..2].copy_from_slice(&2u16.to_be_bytes());
        let mut cursor = Cursor::new(bytes);
        assert!(matches!(
            Packet::read_from(&mut cursor),
            Err(Error::InvalidPacket(_))
        ));
    }

    #[test]
    fn rejects_oversized_packet_before_allocating() {
        let mut bytes = sample().encode().unwrap();
        bytes[2..6].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut cursor = Cursor::new(bytes);
        assert!(matches!(
            Packet::read_from(&mut cursor),
            Err(Error::PacketTooLarge(_))
        ));
    }

    #[test]
    fn rejects_unterminated_target_list() {
        let mut bytes = sample().encode().unwrap();
        bytes[20..24].copy_from_slice(&9u32.to_be_bytes());
        assert!(matches!(
            Packet::decode(&bytes),
            Err(Error::InvalidPacket(_))
        ));
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        let p = Packet {
            payload: vec![0; MAX_PACKET_SIZE],
            ..Packet::default()
        };
        assert!(matches!(p.encode(), Err(Error::PacketTooLarge(_))));
    }
}
