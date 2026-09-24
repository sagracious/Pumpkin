use std::sync::Arc;

use bytes::Bytes;
use pumpkin_macros::{Event, cancellable};

use crate::entity::player::Player;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketDirection {
    Clientbound,
    Serverbound,
}

#[derive(Clone, Debug)]
pub struct PacketTranslationOutput {
    pub packet_id: i32,
    pub payload: Bytes,
}

/// A raw packet at the connection boundary, including pre-Player login states.
#[cancellable]
#[derive(Event, Clone)]
pub struct ProtocolPacketEvent {
    pub connection_id: u64,
    pub player: Option<Arc<Player>>,
    pub direction: PacketDirection,
    pub packet_id: i32,
    pub payload: Bytes,
    pub protocol_version: i32,
    pub connection_state: u8,
    pub translated: bool,
    pub clientbound_packets: Vec<PacketTranslationOutput>,
}

impl ProtocolPacketEvent {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection_id: u64,
        player: Option<Arc<Player>>,
        direction: PacketDirection,
        packet_id: i32,
        payload: Bytes,
        protocol_version: i32,
        connection_state: u8,
    ) -> Self {
        Self {
            connection_id,
            player,
            direction,
            packet_id,
            payload,
            protocol_version,
            connection_state,
            translated: false,
            clientbound_packets: Vec::new(),
            cancelled: false,
        }
    }
}
