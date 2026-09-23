use std::io::Write;

use crate::codec::bit_set::BitSet;
use crate::codec::var_int::VarInt;
use crate::ser::{NetworkReadExt, NetworkWriteExt, ReadingError, WritingError};
use crate::{ClientPacket, ServerPacket};
use pumpkin_data::packet::clientbound::play::LIGHT_UPDATE;
use pumpkin_macros::java_packet;
use pumpkin_util::version::JavaMinecraftVersion;

/// Sent by the server to update light levels (block light and sky light) for a chunk.
///
/// This packet updates lighting data for a specific chunk without sending the full chunk data.
/// It was introduced in Minecraft 1.14 (protocol version 477).
#[derive(Debug, PartialEq, Eq, Clone)]
#[java_packet(LIGHT_UPDATE)]
pub struct CLightUpdate {
    pub chunk_x: VarInt,
    pub chunk_z: VarInt,
    pub light_data: LightData,
}

pub type CUpdateLight = CLightUpdate;

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct LightData {
    pub trust_edges: bool,
    pub sky_light_mask: BitSet,
    pub block_light_mask: BitSet,
    pub empty_sky_light_mask: BitSet,
    pub empty_block_light_mask: BitSet,
    pub sky_light_arrays: Vec<Vec<u8>>,
    pub block_light_arrays: Vec<Vec<u8>>,
}

impl CLightUpdate {
    #[must_use]
    pub const fn new(chunk_x: VarInt, chunk_z: VarInt, light_data: LightData) -> Self {
        Self {
            chunk_x,
            chunk_z,
            light_data,
        }
    }
}

impl LightData {
    #[must_use]
    pub const fn new(
        trust_edges: bool,
        sky_light_mask: BitSet,
        block_light_mask: BitSet,
        empty_sky_light_mask: BitSet,
        empty_block_light_mask: BitSet,
        sky_light_arrays: Vec<Vec<u8>>,
        block_light_arrays: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            trust_edges,
            sky_light_mask,
            block_light_mask,
            empty_sky_light_mask,
            empty_block_light_mask,
            sky_light_arrays,
            block_light_arrays,
        }
    }

    fn trim_to_max_words(&mut self, max_words: usize) {
        fn trim_mask(mask: &BitSet, max_words: usize) -> BitSet {
            BitSet::from_longs(mask.0.iter().copied().take(max_words).collect())
        }

        fn trim_arrays(mask: &BitSet, arrays: &[Vec<u8>], max_words: usize) -> Vec<Vec<u8>> {
            let mut kept = Vec::new();
            let mut array_index = 0;
            for bit in 0..mask.0.len() * 64 {
                if mask.get_bit(bit) {
                    if bit / 64 < max_words {
                        if let Some(array) = arrays.get(array_index) {
                            kept.push(array.clone());
                        }
                    }
                    array_index += 1;
                }
            }
            kept
        }

        self.sky_light_arrays = trim_arrays(
            &self.sky_light_mask,
            &self.sky_light_arrays,
            max_words,
        );
        self.block_light_arrays = trim_arrays(
            &self.block_light_mask,
            &self.block_light_arrays,
            max_words,
        );
        self.sky_light_mask = trim_mask(&self.sky_light_mask, max_words);
        self.block_light_mask = trim_mask(&self.block_light_mask, max_words);
        self.empty_sky_light_mask = trim_mask(&self.empty_sky_light_mask, max_words);
        self.empty_block_light_mask = trim_mask(&self.empty_block_light_mask, max_words);
    }

    pub fn write(
        &self,
        mut write: impl Write,
        version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        if *version == JavaMinecraftVersion::V_26_2 {
            let mut trimmed = self.clone();
            // Vanilla 26.2 rejects masks longer than 12 longs. The imported
            // Paper world can expose a much taller synthetic light array.
            trimmed.trim_to_max_words(12);
            return trimmed.write_internal(&mut write, version);
        }
        self.write_internal(&mut write, version)
    }

    fn write_internal(
        &self,
        mut write: impl Write,
        version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        // Trust edges (1.16 - 1.19.4; added in 1.16, removed in 1.20)
        if *version >= JavaMinecraftVersion::V_1_16 && *version <= JavaMinecraftVersion::V_1_19_4 {
            write.write_bool(self.trust_edges)?;
        }

        // Chunk bitmasks
        if *version >= JavaMinecraftVersion::V_1_17 {
            self.sky_light_mask
                .encode_with_version(&mut write, version)?;
            self.block_light_mask
                .encode_with_version(&mut write, version)?;
            self.empty_sky_light_mask
                .encode_with_version(&mut write, version)?;
            self.empty_block_light_mask
                .encode_with_version(&mut write, version)?;
        } else {
            write.write_var_int(&VarInt(self.sky_light_mask.as_u64() as i32))?;
            write.write_var_int(&VarInt(self.block_light_mask.as_u64() as i32))?;
            write.write_var_int(&VarInt(self.empty_sky_light_mask.as_u64() as i32))?;
            write.write_var_int(&VarInt(self.empty_block_light_mask.as_u64() as i32))?;
        }

        // Sky light arrays
        if *version >= JavaMinecraftVersion::V_1_17 {
            write.write_var_int(&VarInt(self.sky_light_arrays.len() as i32))?;
            for array in &self.sky_light_arrays {
                write.write_var_int(&VarInt(array.len() as i32))?;
                write.write_slice(array)?;
            }
        } else {
            let mut array_idx = 0;
            for i in 0..18 {
                if self.sky_light_mask.get_bit(i)
                    && let Some(array) = self.sky_light_arrays.get(array_idx)
                {
                    write.write_var_int(&VarInt(array.len() as i32))?;
                    write.write_slice(array)?;
                    array_idx += 1;
                }
            }
        }

        // Block light arrays
        if *version >= JavaMinecraftVersion::V_1_17 {
            write.write_var_int(&VarInt(self.block_light_arrays.len() as i32))?;
            for array in &self.block_light_arrays {
                write.write_var_int(&VarInt(array.len() as i32))?;
                write.write_slice(array)?;
            }
        } else {
            let mut array_idx = 0;
            for i in 0..18 {
                if self.block_light_mask.get_bit(i)
                    && let Some(array) = self.block_light_arrays.get(array_idx)
                {
                    write.write_var_int(&VarInt(array.len() as i32))?;
                    write.write_slice(array)?;
                    array_idx += 1;
                }
            }
        }

        Ok(())
    }

    pub fn read(bytebuf: &mut &[u8], version: &JavaMinecraftVersion) -> Result<Self, ReadingError> {
        let trust_edges = if *version >= JavaMinecraftVersion::V_1_16
            && *version <= JavaMinecraftVersion::V_1_19_4
        {
            bytebuf.get_bool()?
        } else {
            false
        };

        let (sky_light_mask, block_light_mask, empty_sky_light_mask, empty_block_light_mask) =
            if *version >= JavaMinecraftVersion::V_1_17 {
                (
                    BitSet::decode_with_version(bytebuf, version)?,
                    BitSet::decode_with_version(bytebuf, version)?,
                    BitSet::decode_with_version(bytebuf, version)?,
                    BitSet::decode_with_version(bytebuf, version)?,
                )
            } else {
                (
                    BitSet::from_u64(bytebuf.get_var_int()?.0 as u64),
                    BitSet::from_u64(bytebuf.get_var_int()?.0 as u64),
                    BitSet::from_u64(bytebuf.get_var_int()?.0 as u64),
                    BitSet::from_u64(bytebuf.get_var_int()?.0 as u64),
                )
            };

        let sky_light_arrays = if *version >= JavaMinecraftVersion::V_1_17 {
            let count = bytebuf.get_var_int()?.0 as usize;
            let mut arrays = Vec::with_capacity(count);
            for _ in 0..count {
                let len = bytebuf.get_var_int()?.0 as usize;
                let mut buf = vec![0u8; len];
                bytebuf.read_bytes_to_buf(&mut buf)?;
                arrays.push(buf);
            }
            arrays
        } else {
            let mut arrays = Vec::new();
            for i in 0..18 {
                if sky_light_mask.get_bit(i) {
                    let len = bytebuf.get_var_int()?.0 as usize;
                    let mut buf = vec![0u8; len];
                    bytebuf.read_bytes_to_buf(&mut buf)?;
                    arrays.push(buf);
                }
            }
            arrays
        };

        let block_light_arrays = if *version >= JavaMinecraftVersion::V_1_17 {
            let count = bytebuf.get_var_int()?.0 as usize;
            let mut arrays = Vec::with_capacity(count);
            for _ in 0..count {
                let len = bytebuf.get_var_int()?.0 as usize;
                let mut buf = vec![0u8; len];
                bytebuf.read_bytes_to_buf(&mut buf)?;
                arrays.push(buf);
            }
            arrays
        } else {
            let mut arrays = Vec::new();
            for i in 0..18 {
                if block_light_mask.get_bit(i) {
                    let len = bytebuf.get_var_int()?.0 as usize;
                    let mut buf = vec![0u8; len];
                    bytebuf.read_bytes_to_buf(&mut buf)?;
                    arrays.push(buf);
                }
            }
            arrays
        };

        Ok(Self {
            trust_edges,
            sky_light_mask,
            block_light_mask,
            empty_sky_light_mask,
            empty_block_light_mask,
            sky_light_arrays,
            block_light_arrays,
        })
    }
}

impl ClientPacket for CLightUpdate {
    fn write_packet_data(
        &self,
        mut write: impl Write,
        version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        write.write_var_int(&self.chunk_x)?;
        write.write_var_int(&self.chunk_z)?;
        self.light_data.write(&mut write, version)
    }
}

impl<'a> ServerPacket<'a> for CLightUpdate {
    fn read(bytebuf: &mut &'a [u8], version: &JavaMinecraftVersion) -> Result<Self, ReadingError> {
        let chunk_x = bytebuf.get_var_int()?;
        let chunk_z = bytebuf.get_var_int()?;
        let light_data = LightData::read(bytebuf, version)?;
        Ok(Self {
            chunk_x,
            chunk_z,
            light_data,
        })
    }
}
