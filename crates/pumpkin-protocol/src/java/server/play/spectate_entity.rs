use pumpkin_data::packet::serverbound::play::SPECTATE_ENTITY;
use pumpkin_macros::java_packet;

use crate::{
    ServerPacket,
    ser::{NetworkReadExt, ReadingError},
};
use pumpkin_util::version::JavaMinecraftVersion;

#[java_packet(SPECTATE_ENTITY)]
pub struct SSpectateEntity {
    /// Spectated entity id, or `None` when the client clears its target.
    /// Vanilla encodes this as an optional var int: zero means absent,
    /// otherwise the value is the entity id plus one.
    pub target: Option<i32>,
}

impl<'a> ServerPacket<'a> for SSpectateEntity {
    fn read(bytebuf: &mut &'a [u8], _version: &JavaMinecraftVersion) -> Result<Self, ReadingError> {
        let raw = bytebuf.get_var_int()?.0;
        Ok(Self {
            target: (raw > 0).then_some(raw - 1),
        })
    }
}

impl crate::ClientPacket for SSpectateEntity {
    fn write_packet_data(
        &self,
        mut write: impl std::io::Write,
        _version: &JavaMinecraftVersion,
    ) -> Result<(), crate::ser::WritingError> {
        use crate::{ser::NetworkWriteExt, codec::var_int::VarInt};
        write.write_var_int(&VarInt(self.target.map_or(0, |id| id + 1)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientPacket;

    #[test]
    fn spectate_action_round_trips_vanilla_bytes() {
        let empty =
            SSpectateEntity::read(&mut &[0x00][..], &JavaMinecraftVersion::V_26_3).unwrap();
        assert_eq!(empty.target, None);

        let targeted =
            SSpectateEntity::read(&mut &[0x06][..], &JavaMinecraftVersion::V_26_2).unwrap();
        assert_eq!(targeted.target, Some(5));

        let mut buf = Vec::new();
        empty.write_packet_data(&mut buf, &JavaMinecraftVersion::V_26_3).unwrap();
        assert_eq!(buf, vec![0x00]);
        buf.clear();
        targeted
            .write_packet_data(&mut buf, &JavaMinecraftVersion::V_26_2)
            .unwrap();
        assert_eq!(buf, vec![0x06]);
    }
}
