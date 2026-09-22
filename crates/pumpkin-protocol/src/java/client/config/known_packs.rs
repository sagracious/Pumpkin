use pumpkin_data::packet::clientbound::config::SELECT_KNOWN_PACKS;

use crate::{ClientPacket, MultiVersionJavaPacket};
use crate::KnownPack;
use crate::ser::NetworkWriteExt;
use pumpkin_util::version::JavaMinecraftVersion;

pub struct CKnownPacks<'a> {
    pub known_packs: &'a [KnownPack<'a>],
}

impl MultiVersionJavaPacket for CKnownPacks<'_> {
    fn to_id(version: JavaMinecraftVersion) -> i32 {
        if version == JavaMinecraftVersion::V_26_2 { 7 } else { SELECT_KNOWN_PACKS.to_id(version) }
    }
}

impl<'a> CKnownPacks<'a> {
    #[must_use]
    pub const fn new(known_packs: &'a [KnownPack]) -> Self {
        Self { known_packs }
    }
}

impl ClientPacket for CKnownPacks<'_> {
    fn write_packet_data(
        &self,
        mut write: impl std::io::Write,
        _version: &JavaMinecraftVersion,
    ) -> Result<(), crate::ser::WritingError> {
        write.write_var_int(&crate::VarInt(self.known_packs.len() as i32))?;
        for pack in self.known_packs {
            pack.write(&mut write)?;
        }
        Ok(())
    }
}
