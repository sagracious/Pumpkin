use crate::Link;
use pumpkin_data::packet::clientbound::config::SERVER_LINKS;

use crate::{ClientPacket, MultiVersionJavaPacket};
use crate::ser::NetworkWriteExt;
use pumpkin_util::version::JavaMinecraftVersion;

pub struct CConfigServerLinks<'a> {
    pub links: &'a [Link<'a>],
}

impl MultiVersionJavaPacket for CConfigServerLinks<'_> {
    fn to_id(version: JavaMinecraftVersion) -> i32 {
        if version == JavaMinecraftVersion::V_26_2 { 16 } else { SERVER_LINKS.to_id(version) }
    }
}

impl<'a> CConfigServerLinks<'a> {
    #[must_use]
    pub const fn new(links: &'a [Link<'a>]) -> Self {
        Self { links }
    }
}

impl ClientPacket for CConfigServerLinks<'_> {
    fn write_packet_data(
        &self,
        mut write: impl std::io::Write,
        _version: &JavaMinecraftVersion,
    ) -> Result<(), crate::ser::WritingError> {
        write.write_var_int(&crate::VarInt(self.links.len() as i32))?;
        for link in self.links {
            link.write(&mut write)?;
        }
        Ok(())
    }
}
