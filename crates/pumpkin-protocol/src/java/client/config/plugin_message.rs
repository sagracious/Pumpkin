use std::io::Write;

use pumpkin_data::packet::clientbound::config::CUSTOM_PAYLOAD;
use pumpkin_util::version::JavaMinecraftVersion;

use crate::{
    ClientPacket, MultiVersionJavaPacket,
    ser::{NetworkWriteExt, WritingError},
};

pub struct CPluginMessage<'a> {
    pub channel: &'a str,
    pub data: &'a [u8],
}

impl MultiVersionJavaPacket for CPluginMessage<'_> {
    fn to_id(version: JavaMinecraftVersion) -> i32 {
        if version == JavaMinecraftVersion::V_26_2 { 2 } else { CUSTOM_PAYLOAD.to_id(version) }
    }
}

impl<'a> CPluginMessage<'a> {
    #[must_use]
    pub const fn new(channel: &'a str, data: &'a [u8]) -> Self {
        Self { channel, data }
    }
}

impl ClientPacket for CPluginMessage<'_> {
    fn write_packet_data(
        &self,
        write: impl Write,
        _version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        let mut write = write;

        write.write_string(self.channel)?;

        write.write_all(self.data).map_err(WritingError::IoError)
    }
}
