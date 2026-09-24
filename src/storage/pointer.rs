//! The destination pointer (ADR-0006 "Destination artifacts"): the record, kept beside the final file,
//! that names the transfer a stage belongs to. It is backend-neutral; a backend adds only an opaque
//! extension (an S3 upload's id, say).
//!
//! Layout, little-endian, exactly `116 + E` bytes:
//!
//! | Offset | Bytes | Field |
//! |---|---|---|
//! | 0 | 8 | magic `DMDPTR01` (the version is part of it) |
//! | 8 | 1 | flags: bit 0 = durable prefix present; every other bit 0 |
//! | 9 | 1 | reserved, 0 |
//! | 10 | 2 | extension length E |
//! | 12 | 32 | recovery binding (v3) |
//! | 44 | 32 | transfer identity |
//! | 76 | 8 | durable prefix (0 when absent) |
//! | 84 | E | extension |
//! | 84 + E | 32 | `blake3` of every byte before it |

use std::ops::Range;

use bytes::Bytes;

const MAGIC: &[u8; 8] = b"DMDPTR01";
const HAS_PREFIX: u8 = 0b1;
const HEADER_BYTES: usize = 84;
const CHECKSUM_BYTES: usize = 32;
/// Largest backend extension a pointer carries.
const MAX_EXTENSION_BYTES: usize = 4096;
/// Largest encoded pointer; a reader never needs more.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read bound for backends moving in ADR-0006 C8")
)]
pub(crate) const MAX_POINTER_BYTES: usize = HEADER_BYTES + MAX_EXTENSION_BYTES + CHECKSUM_BYTES;

/// A pointer that does not decode: wrong length, magic, flags, checksum, or an oversized extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PointerCorrupt;

/// What the destination records about the stage beside a final file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DestinationPointer {
    /// The recovery binding the stage was written under; resume requires it to be equal.
    pub(crate) binding: [u8; 32],
    /// The transfer identity, kept only to tell another transfer from a changed source.
    pub(crate) transfer_identity: [u8; 32],
    /// The durable prefix, for backends whose pointer is their checkpoint record; others
    /// re-observe it from the stage.
    pub(crate) durable_prefix: Option<u64>,
    /// Opaque backend state, at most 4 KiB.
    pub(crate) extension: Bytes,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "written by backends moving in ADR-0006 C8")
)]
impl DestinationPointer {
    /// Encodes the pointer.
    ///
    /// # Errors
    /// Returns [`PointerCorrupt`] for an extension over 4 KiB — a caller error, refused because such
    /// a pointer could never be read back.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, PointerCorrupt> {
        let extension_len = u16::try_from(self.extension.len())
            .ok()
            .filter(|len| usize::from(*len) <= MAX_EXTENSION_BYTES)
            .ok_or(PointerCorrupt)?;
        let mut out = Vec::with_capacity(HEADER_BYTES + self.extension.len() + CHECKSUM_BYTES);
        out.extend_from_slice(MAGIC);
        out.push(if self.durable_prefix.is_some() {
            HAS_PREFIX
        } else {
            0
        });
        out.push(0);
        out.extend_from_slice(&extension_len.to_le_bytes());
        out.extend_from_slice(&self.binding);
        out.extend_from_slice(&self.transfer_identity);
        out.extend_from_slice(&self.durable_prefix.unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&self.extension);
        let checksum = *blake3::hash(&out).as_bytes();
        out.extend_from_slice(&checksum);
        Ok(out)
    }

    /// Decodes a pointer, accepting only the exact layout above.
    ///
    /// # Errors
    /// Returns [`PointerCorrupt`] for anything else.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, PointerCorrupt> {
        if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES || bytes.len() > MAX_POINTER_BYTES {
            return Err(PointerCorrupt);
        }
        let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM_BYTES);
        let extension_len = usize::from(u16::from_le_bytes([body[10], body[11]]));
        let flags = body[8];
        if &body[..8] != MAGIC
            || flags & !HAS_PREFIX != 0
            || body[9] != 0
            || body.len() != HEADER_BYTES + extension_len
            || blake3::hash(body).as_bytes() != checksum
        {
            return Err(PointerCorrupt);
        }
        let field = |range: Range<usize>| -> Result<[u8; 32], PointerCorrupt> {
            body[range].try_into().map_err(|_| PointerCorrupt)
        };
        let prefix = u64::from_le_bytes(body[76..84].try_into().map_err(|_| PointerCorrupt)?);
        // Only one encoding is accepted: an absent prefix is written as zero.
        if flags & HAS_PREFIX == 0 && prefix != 0 {
            return Err(PointerCorrupt);
        }
        Ok(Self {
            binding: field(12..44)?,
            transfer_identity: field(44..76)?,
            durable_prefix: (flags & HAS_PREFIX != 0).then_some(prefix),
            extension: Bytes::copy_from_slice(&body[HEADER_BYTES..]),
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{DestinationPointer, MAX_POINTER_BYTES, PointerCorrupt};

    fn pointer(prefix: Option<u64>, extension: &'static [u8]) -> DestinationPointer {
        DestinationPointer {
            binding: [0x11; 32],
            transfer_identity: [0x22; 32],
            durable_prefix: prefix,
            extension: Bytes::from_static(extension),
        }
    }

    #[test]
    fn a_pointer_round_trips() -> Result<(), PointerCorrupt> {
        for value in [
            pointer(Some(64 << 20), b""),
            pointer(None, b""),
            pointer(Some(0), b"upload-id\0part-size"),
        ] {
            let bytes = value.encode()?;
            assert_eq!(bytes.len(), 116 + value.extension.len());
            assert_eq!(DestinationPointer::decode(&bytes)?, value);
        }
        Ok(())
    }

    /// Pins the layout: the expected bytes were computed independently (Python `blake3`).
    #[test]
    fn the_layout_matches_the_frozen_vector() -> Result<(), PointerCorrupt> {
        let bytes = pointer(Some(0x0102_0304), b"ext").encode()?;
        let hex = bytes.iter().fold(String::new(), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        });
        assert_eq!(hex, FROZEN);
        Ok(())
    }

    const FROZEN: &str = "444d44505452303101000300111111111111111111111111111111111111111111111111111111111111111122222222222222222222222222222222222222222222222222222222222222220403020100000000657874bad60b48fd8ac578e72e24d689ee010a7f817f0e58ed2a2176d7e8175fc8f635";

    #[test]
    fn every_single_bit_flip_is_rejected() -> Result<(), PointerCorrupt> {
        let bytes = pointer(Some(7), b"x").encode()?;
        for index in 0..bytes.len() {
            for bit in 0..8 {
                let mut flipped = bytes.clone();
                flipped[index] ^= 1 << bit;
                assert_eq!(
                    DestinationPointer::decode(&flipped),
                    Err(PointerCorrupt),
                    "byte {index} bit {bit}"
                );
            }
        }
        Ok(())
    }

    /// Rewrites one header byte and recomputes the checksum, so the check under test — not the
    /// checksum — is what must refuse it.
    fn resealed(mut bytes: Vec<u8>, index: usize, value: u8) -> Vec<u8> {
        bytes[index] = value;
        let body = bytes.len() - 32;
        let checksum = *blake3::hash(&bytes[..body]).as_bytes();
        bytes[body..].copy_from_slice(&checksum);
        bytes
    }

    #[test]
    fn each_header_check_refuses_on_its_own() -> Result<(), PointerCorrupt> {
        let with_prefix = pointer(Some(7), b"x").encode()?;
        let without_prefix = pointer(None, b"x").encode()?;
        for (name, bytes) in [
            ("magic", resealed(with_prefix.clone(), 7, b'2')),
            ("unknown flag", resealed(with_prefix.clone(), 8, 0b11)),
            ("reserved", resealed(with_prefix.clone(), 9, 1)),
            ("extension length", resealed(with_prefix.clone(), 10, 2)),
            (
                "prefix without its flag",
                resealed(without_prefix.clone(), 76, 1),
            ),
        ] {
            assert_eq!(
                DestinationPointer::decode(&bytes),
                Err(PointerCorrupt),
                "{name}"
            );
        }
        // The control: resealing without a change still decodes.
        assert!(DestinationPointer::decode(&resealed(without_prefix, 76, 0)).is_ok());
        Ok(())
    }

    #[test]
    fn truncated_padded_and_oversized_pointers_are_rejected() -> Result<(), PointerCorrupt> {
        let bytes = pointer(None, b"").encode()?;
        assert!(DestinationPointer::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut padded = bytes.clone();
        padded.push(0);
        assert!(DestinationPointer::decode(&padded).is_err());
        assert!(DestinationPointer::decode(&[]).is_err());
        assert!(DestinationPointer::decode(&vec![0; MAX_POINTER_BYTES + 1]).is_err());
        let largest = DestinationPointer {
            extension: Bytes::from(vec![7; 4096]),
            ..pointer(None, b"")
        };
        let encoded = largest.encode()?;
        assert_eq!(encoded.len(), MAX_POINTER_BYTES);
        assert_eq!(DestinationPointer::decode(&encoded)?, largest);
        let oversized = DestinationPointer {
            extension: Bytes::from(vec![0; 4097]),
            ..pointer(None, b"")
        };
        assert_eq!(oversized.encode(), Err(PointerCorrupt));
        Ok(())
    }
}
