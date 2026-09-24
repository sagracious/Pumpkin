use super::FromIntoEvent;
use crate::wit::pumpkin::plugin::event::{
    Event, EventType, PacketReceivedEventData, PacketSentEventData, ProtocolPacketEventData,
};

/// An event fired when a packet is received from a client
pub struct PacketReceivedEvent;

impl FromIntoEvent for PacketReceivedEvent {
    const EVENT_TYPE: EventType = EventType::PacketReceivedEvent;
    type Data = PacketReceivedEventData;

    fn data_from_event(event: Event) -> Self::Data {
        match event {
            Event::PacketReceivedEvent(data) => data,
            _ => panic!("unexpected event"),
        }
    }

    fn data_into_event(data: Self::Data) -> Event {
        Event::PacketReceivedEvent(data)
    }
}

/// An event fired when a packet is sent to a client
pub struct PacketSentEvent;

impl FromIntoEvent for PacketSentEvent {
    const EVENT_TYPE: EventType = EventType::PacketSentEvent;
    type Data = PacketSentEventData;

    fn data_from_event(event: Event) -> Self::Data {
        match event {
            Event::PacketSentEvent(data) => data,
            _ => panic!("unexpected event"),
        }
    }

    fn data_into_event(data: Self::Data) -> Event {
        Event::PacketSentEvent(data)
    }
}

/// A raw protocol packet at the connection boundary, available before a Player exists.
pub struct ProtocolPacketEvent;

impl FromIntoEvent for ProtocolPacketEvent {
    const EVENT_TYPE: EventType = EventType::ProtocolPacketEvent;
    type Data = ProtocolPacketEventData;

    fn data_from_event(event: Event) -> Self::Data {
        match event {
            Event::ProtocolPacketEvent(data) => data,
            _ => panic!("unexpected event"),
        }
    }

    fn data_into_event(data: Self::Data) -> Event {
        Event::ProtocolPacketEvent(data)
    }
}
