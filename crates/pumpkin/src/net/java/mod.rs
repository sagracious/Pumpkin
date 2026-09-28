use pumpkin_protocol::java::client::play::{
    CChunkBatchEnd, CChunkBatchStart, CLightUpdate, CPlayDisconnect,
};
use pumpkin_world::level::SyncChunk;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{collections::VecDeque, io::Write, sync::Arc};

use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use pumpkin_data::{packet::CURRENT_MC_VERSION, translation};
use pumpkin_protocol::java::server::play::{
    SAttack, SBlockEntityTagQuery, SBundleItemSelected, SChangeDifficulty, SChangeGameMode,
    SChatAck, SChatCommand, SChatCommandSigned, SChatMessage, SChunkBatch, SClickSlot,
    SClientCommand, SClientInformationPlay, SClientTickEnd, SCloseContainer, SCommandSuggestion,
    SConfigurationAcknowledged, SConfirmTeleport, SContainerButtonClick,
    SContainerSlotStateChanged, SCookieResponse as SPCookieResponse, SCustomPayload,
    SDebugSampleSubscription, SDebugSubscriptionRequest, SEditBook, SEntityTagQuery, SInteract,
    SJigsawGenerate, SLockDifficulty, SMoveVehicle, SPaddleBoat, SPickItemFromBlock, SPlaceRecipe,
    SPlayPingRequest, SPlayPong, SPlayResourcePack, SPlayerAbilities, SPlayerAction,
    SPlayerCommand, SPlayerInput, SPlayerLoaded, SPlayerPosition, SPlayerPositionRotation,
    SPlayerRotation, SPlayerSession, SRecipeBookChangeSettings, SRecipeBookSeenRecipe, SRenameItem,
    SSeenAdvancement, SSelectTrade, SSetBeacon, SSetCommandBlock, SSetCommandMinecart,
    SSetCreativeSlot, SSetGameRule, SSetHeldItem, SSetJigsawBlock, SSetPlayerGround,
    SSetStructureBlock, SSetTestBlock, SSpectateEntity, SSwingArm, STeleportToEntity,
    STestInstanceBlockAction, SUpdateSign, SUseItem, SUseItemOn,
};
use pumpkin_protocol::packet::MultiVersionJavaPacket;
use pumpkin_protocol::{
    ClientPacket, ConnectionState, MAX_PACKET_SIZE, PacketDecodeError, PacketEncodeError,
    RawPacket, ServerPacket,
    codec::var_int::VarInt,
    java::{
        client::{config::CConfigDisconnect, login::CLoginDisconnect},
        packet_decoder::TCPNetworkDecoder,
        packet_encoder::TCPNetworkEncoder,
    },
    ser::{NetworkReadExt, NetworkWriteExt, WritingError},
};
use pumpkin_util::text::TextComponent;
use pumpkin_util::version::JavaMinecraftVersion;
use tokio::{
    io::{BufReader, BufWriter},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::oneshot,
};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, error::TryRecvError},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

pub mod chunk_data;
pub mod handshake;
pub mod login;
pub mod pending;
pub mod play;
pub mod recipe_helper;
pub mod status;

pub use chunk_data::{CChunkData, ChunkLightExt};

/// Max wait for the disconnect flush: queued packets are still written and
/// flushed after `close()`, bounded by this timeout.
const DISCONNECT_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

use arc_swap::ArcSwap;
use pending::PendingConnection;

use crate::entity::player::Player;
use crate::net::{
    ClientPlatform, GameProfile, MAX_PENDING_BYTES, PacketRateLimiter, PlayerConfig,
    decrement_pending_bytes,
};
use crate::plugin::api::events::server::protocol_packet::{
    PacketDirection, PacketTranslationOutput, ProtocolPacketEvent,
};
use crate::plugin::api::events::world::chunk_send::ChunkSend;
use crate::plugin::player::player_custom_payload::PlayerCustomPayloadEvent;
use crate::plugin::server::packet::PacketSentEvent;
use crate::{error::PumpkinError, server::Server};

const MAX_TRANSLATED_FOLLOW_UP_PACKETS: usize = 64;
const MAX_DIAGNOSTIC_TRACE_PACKETS: usize = 24;

fn should_trace_java_diagnostic(version: JavaMinecraftVersion) -> bool {
    matches!(
        version,
        JavaMinecraftVersion::V_1_16_2
            | JavaMinecraftVersion::V_26_2
            | JavaMinecraftVersion::V_26_3
    )
}

pub(crate) struct ProtocolPacketEventOutput {
    pub clientbound_packets: Vec<PacketTranslationOutput>,
    pub serverbound_packets: Vec<PacketTranslationOutput>,
}

pub struct JavaClient {
    pub id: u64,
    /// The protocol the client speaks. Play packets are always encoded/decoded as
    /// `CURRENT_MC_VERSION`. Older clients are not admitted; the packet events are the hook
    /// for a plugin that converts them.
    pub version: AtomicCell<JavaMinecraftVersion>,
    protocol_translator_active: bool,
    clientbound_diagnostic_trace_packets: AtomicUsize,
    serverbound_diagnostic_trace_packets: AtomicUsize,
    raw_serverbound_diagnostic_trace_packets: AtomicUsize,
    flushed_clientbound_diagnostic_trace_packets: AtomicUsize,
    /// The client's game profile information. Direct field (lock-free).
    pub gameprofile: GameProfile,
    /// The client's configuration settings. Lock-free `ArcSwap`.
    pub config: ArcSwap<PlayerConfig>,
    /// The Address used to connect to the Server, Sent in the Handshake. Direct field.
    pub server_address: String,
    /// The current connection state of the client (e.g., Handshaking, Status, Play).
    pub connection_state: AtomicCell<ConnectionState>,
    /// The client's IP address. Direct field (lock-free).
    pub address: SocketAddr,
    /// The client's brand or modpack information. Lock-free `ArcSwap`.
    pub brand: ArcSwap<Option<String>>,
    /// Associated player reference. Lock-free `ArcSwap`.
    pub player: Arc<ArcSwap<Option<Arc<Player>>>>,
    /// A collection of tasks associated with this client. The tasks await completion when removing the client.
    tasks: TaskTracker,
    rt_handle: tokio::runtime::Handle,
    /// An notifier that is triggered when this client is closed.
    close_token: CancellationToken,
    /// Per-connection FIFO of serialized packets (vanilla Netty eventLoop).
    /// Unbounded like vanilla; `MAX_PENDING_BYTES` is the limit.
    outgoing_packet_queue_send: UnboundedSender<OutgoingPacket>,
    outgoing_packet_queue_recv: Option<UnboundedReceiver<OutgoingPacket>>,
    /// A high-priority queue of serialized packets to send to the network.
    outgoing_packet_priority_send: UnboundedSender<OutgoingPacket>,
    /// A high-priority queue of serialized packets to send to the network.
    outgoing_packet_priority_recv: Option<UnboundedReceiver<OutgoingPacket>>,
    /// Tracks total buffered payload bytes in the outgoing queue.
    pub pending_bytes: Arc<AtomicUsize>,
    /// The packet encoder for outgoing packets.
    network_writer: std::sync::Mutex<Option<TCPNetworkEncoder<BufWriter<OwnedWriteHalf>>>>,
    /// The packet decoder for incoming packets.
    network_reader: std::sync::Mutex<Option<TCPNetworkDecoder<BufReader<OwnedReadHalf>>>>,
    /// Keep Alive:
    ///
    /// Whether we are waiting for a response after sending a keep alive packet.
    pub wait_for_keep_alive: AtomicBool,
    /// Set to `true` when any movement packet is received this tick.
    /// On `SClientTickEnd` (≥1.21.4), if still `false`, the player's known
    /// movement is zeroed (they stood still). Matches vanilla's `receivedMovementThisTick`.
    pub received_movement_this_tick: AtomicBool,
    /// The keep alive packet payload we send. The client should respond with the same id.
    pub keep_alive_id: AtomicCell<i64>,
    /// The last time we sent a keep alive packet.
    pub last_keep_alive_time: AtomicCell<Instant>,
    /// The last time any packet was received from the client.
    pub last_packet_time: AtomicCell<Instant>,
    /// Recent in-flight keep alive IDs with their sent timestamps.
    pub pending_keep_alives: std::sync::Mutex<Vec<(i64, Instant)>>,

    pub packet_sequence: AtomicI32,
    /// Packet rate limiter for incoming client packets.
    pub packet_limiter: PacketRateLimiter,
    /// Vanilla `suspendFlushingOnServerThread`: the tick holds TCP flushes
    /// until `flush_channel`, so block and entity updates share one tick.
    suspend_flushing: Arc<AtomicBool>,
}

pub enum OutgoingPacketType {
    Normal,
    HighPriority,
}

struct OutgoingPacket {
    data: Bytes,
    completion: Option<oneshot::Sender<()>>,
    translation_applied: bool,
    /// `flush_channel` barrier: consumed by the writer, never framed.
    force_flush: bool,
}

const MAX_FRAME_BATCH_DATA_SIZE: usize = MAX_PACKET_SIZE as usize;

#[must_use]
fn packet_serialization_version(
    client_version: JavaMinecraftVersion,
    protocol_translator_active: bool,
) -> JavaMinecraftVersion {
    if protocol_translator_active {
        CURRENT_MC_VERSION
    } else {
        client_version
    }
}

#[cfg(test)]
mod packet_serialization_version_tests {
    use super::{JavaClient, packet_serialization_version};
    use pumpkin_data::dimension::Dimension;
    use pumpkin_data::packet::CURRENT_MC_VERSION;
    use pumpkin_protocol::ClientPacket;
    use pumpkin_protocol::codec::var_int::VarInt;
    use pumpkin_protocol::java::client::play::{CLogin, CRespawn, PlayerSpawnData};
    use pumpkin_protocol::packet::MultiVersionJavaPacket;
    use pumpkin_util::resource_location::ResourceLocation;
    use pumpkin_util::version::JavaMinecraftVersion;

    #[test]
    fn translator_receives_native_layout_while_normal_clients_keep_their_version() {
        assert_eq!(
            packet_serialization_version(JavaMinecraftVersion::V_1_16_2, true),
            CURRENT_MC_VERSION
        );
        assert_eq!(
            packet_serialization_version(JavaMinecraftVersion::V_1_16_2, false),
            JavaMinecraftVersion::V_1_16_2
        );
    }

    #[test]
    fn legacy_login_keeps_native_packet_id_and_inline_registry_payload() {
        let version = JavaMinecraftVersion::V_1_16_2;
        let dimensions = [ResourceLocation::from("minecraft:overworld")];
        let spawn_data = PlayerSpawnData::new(
            Dimension::OVERWORLD,
            0,
            0,
            -1,
            false,
            true,
            None,
            VarInt(0),
            VarInt(63),
        );
        let packet = CLogin::new(
            1,
            false,
            &dimensions,
            VarInt(50),
            VarInt(8),
            VarInt(8),
            false,
            true,
            false,
            spawn_data.clone(),
            false,
            false,
        );

        let encoded =
            JavaClient::serialize_packet_with_versions(&packet, CURRENT_MC_VERSION, version)
                .unwrap();
        let mut header = std::io::Cursor::new(encoded.as_ref());
        assert_eq!(
            VarInt::decode(&mut header).unwrap().0,
            CLogin::to_id(CURRENT_MC_VERSION)
        );
        let payload_start = header.position() as usize;

        let mut expected_payload = Vec::new();
        packet
            .write_packet_data(&mut expected_payload, &version)
            .unwrap();
        assert_eq!(encoded.len() - payload_start, expected_payload.len());
        assert!(
            encoded.len() - payload_start > 10_000,
            "legacy inline registry codec is present"
        );

        let packet = CRespawn::new(spawn_data, CRespawn::KEEP_ALL_DATA);
        let encoded =
            JavaClient::serialize_packet_with_versions(&packet, CURRENT_MC_VERSION, version)
                .unwrap();
        let mut header = std::io::Cursor::new(encoded.as_ref());
        assert_eq!(
            VarInt::decode(&mut header).unwrap().0,
            CRespawn::to_id(CURRENT_MC_VERSION)
        );
        let payload_start = header.position() as usize;

        let mut expected_payload = Vec::new();
        packet
            .write_packet_data(&mut expected_payload, &version)
            .unwrap();
        assert_eq!(encoded.len() - payload_start, expected_payload.len());
    }
}

fn take_frame_batch(packets: &mut VecDeque<OutgoingPacket>) -> Vec<OutgoingPacket> {
    let mut batch = Vec::new();
    let mut data_len = 0usize;

    while let Some(packet) = packets.pop_front() {
        let next_len = data_len.saturating_add(packet.data.len());
        if !batch.is_empty() && next_len > MAX_FRAME_BATCH_DATA_SIZE {
            packets.push_front(packet);
            break;
        }

        data_len = next_len;
        batch.push(packet);
    }

    batch
}

fn frame_packet_batch(
    mut writer: TCPNetworkEncoder<BufWriter<OwnedWriteHalf>>,
    batch: &[OutgoingPacket],
) -> (
    TCPNetworkEncoder<BufWriter<OwnedWriteHalf>>,
    Vec<u8>,
    Option<PacketEncodeError>,
) {
    let mut frame = Vec::new();
    let mut frame_err = None;
    for packet in batch {
        if let Err(err) = writer.frame_packet(&packet.data, &mut frame) {
            frame_err = Some(err);
            break;
        }
    }
    (writer, frame, frame_err)
}

async fn frame_batch_maybe_offload(
    writer: TCPNetworkEncoder<BufWriter<OwnedWriteHalf>>,
    packet_batch: Vec<OutgoingPacket>,
) -> Result<
    (
        TCPNetworkEncoder<BufWriter<OwnedWriteHalf>>,
        Vec<OutgoingPacket>,
        Vec<u8>,
        Option<PacketEncodeError>,
    ),
    tokio::task::JoinError,
> {
    let needs_offload = packet_batch
        .iter()
        .any(|packet| writer.is_compressing_packet(&packet.data));

    if needs_offload {
        tokio::task::spawn_blocking(move || {
            let (writer, frame, frame_err) = frame_packet_batch(writer, &packet_batch);
            (writer, packet_batch, frame, frame_err)
        })
        .await
    } else {
        let (writer, frame, frame_err) = frame_packet_batch(writer, &packet_batch);
        Ok((writer, packet_batch, frame, frame_err))
    }
}

pub(crate) const fn connection_state_code(state: ConnectionState) -> u8 {
    match state {
        ConnectionState::HandShake => 0,
        ConnectionState::Status => 1,
        ConnectionState::Login => 2,
        ConnectionState::Transfer => 3,
        ConnectionState::Config => 4,
        ConnectionState::Play => 5,
    }
}

/// Runs the raw packet hook shared by pending and player-backed connections.
#[expect(clippy::too_many_arguments)]
pub(crate) fn apply_protocol_packet_event(
    server: &Arc<Server>,
    connection_id: u64,
    player: Option<Arc<Player>>,
    direction: PacketDirection,
    version: JavaMinecraftVersion,
    state: ConnectionState,
    packet_id: &mut i32,
    payload: &mut Bytes,
    translated: &mut bool,
    cancelled: &mut bool,
) -> ProtocolPacketEventOutput {
    if !server.plugin_manager.has_handlers::<ProtocolPacketEvent>() {
        return ProtocolPacketEventOutput {
            clientbound_packets: Vec::new(),
            serverbound_packets: Vec::new(),
        };
    }

    let mut event = ProtocolPacketEvent::new(
        connection_id,
        player,
        direction,
        *packet_id,
        payload.clone(),
        version.protocol_version(),
        connection_state_code(state),
    );
    server.plugin_manager.fire_blocking(server, &mut event);
    *packet_id = event.packet_id;
    *payload = event.payload;
    *translated = event.translated;
    *cancelled = event.cancelled;
    ProtocolPacketEventOutput {
        clientbound_packets: event.clientbound_packets,
        serverbound_packets: event.serverbound_packets,
    }
}

fn encode_protocol_packet(packet_id: i32, payload: &[u8]) -> Option<Bytes> {
    if packet_id < 0 || payload.len() > MAX_PACKET_SIZE as usize {
        return None;
    }
    let mut encoded = Vec::with_capacity(payload.len() + 5);
    encoded.write_var_int(&VarInt(packet_id)).ok()?;
    encoded.extend_from_slice(payload);
    Some(Bytes::from(encoded))
}

async fn apply_packet_sent_events(
    packets: Vec<OutgoingPacket>,
    player_store: &Arc<ArcSwap<Option<Arc<Player>>>>,
    pending_bytes: &Arc<AtomicUsize>,
    version: JavaMinecraftVersion,
    connection_id: u64,
    state: ConnectionState,
    server: &Arc<Server>,
    close_token: &CancellationToken,
) -> Vec<OutgoingPacket> {
    let player = player_store.load_full();
    let player = player.as_ref().clone();
    let mut translated_packets = Vec::with_capacity(packets.len());
    for mut packet in packets {
        if packet.translation_applied {
            if should_trace_java_diagnostic(version)
                && let Some(player) = player.as_ref()
                && let ClientPlatform::Java(client) = player.client.as_ref()
                && client.protocol_translator_active
                && client.connection_state.load() == ConnectionState::Play
            {
                let sequence = client
                    .clientbound_diagnostic_trace_packets
                    .fetch_add(1, Ordering::Relaxed);
                if sequence < MAX_DIAGNOSTIC_TRACE_PACKETS {
                    let mut encoded = packet.data.as_ref();
                    if let Ok(packet_id) = encoded.get_var_int() {
                        debug!(
                            connection_id,
                            sequence = sequence + 1,
                            client_protocol = version.protocol_version(),
                            packet_id_to_client = packet_id.0,
                            payload_len_to_client = encoded.len(),
                            already_translated = true,
                            "PJM clientbound packet trace"
                        );
                    }
                }
            }
            translated_packets.push(packet);
            continue;
        }
        let original_len = packet.data.len();
        let mut encoded = packet.data.as_ref();
        let Ok(mut packet_id) = encoded.get_var_int().map(|id| id.0) else {
            translated_packets.push(packet);
            continue;
        };
        let mut payload = Bytes::copy_from_slice(encoded);
        let packet_state = player
            .as_ref()
            .and_then(|player| match player.client.as_ref() {
                ClientPlatform::Java(client) => Some(client.connection_state.load()),
                ClientPlatform::Bedrock(_) => None,
            })
            .unwrap_or(state);
        let source_packet_id = packet_id;
        let source_payload_len = payload.len();
        let trace_sequence =
            if should_trace_java_diagnostic(version) && packet_state == ConnectionState::Play {
                player
                    .as_ref()
                    .and_then(|player| match player.client.as_ref() {
                        ClientPlatform::Java(client) if client.protocol_translator_active => {
                            let sequence = client
                                .clientbound_diagnostic_trace_packets
                                .fetch_add(1, Ordering::Relaxed);
                            (sequence < MAX_DIAGNOSTIC_TRACE_PACKETS).then_some(sequence + 1)
                        }
                        _ => None,
                    })
            } else {
                None
            };

        // Preserve the public PacketSentEvent behavior for plugins that still use it.
        if let Some(player) = player.as_ref() {
            let event = player
                .fire_packet_sent_event_no_obj(packet_id, payload.clone())
                .await;
            if event.cancelled {
                if let Some(sequence) = trace_sequence {
                    debug!(
                        connection_id,
                        sequence,
                        state = ?packet_state,
                        client_protocol = version.protocol_version(),
                        packet_id_before_packet_sent = source_packet_id,
                        payload_len_before_packet_sent = source_payload_len,
                        packet_id_after_packet_sent = event.packet_id,
                        payload_len_after_packet_sent = event.payload.len(),
                        cancelled = true,
                        "PJM clientbound packet trace"
                    );
                }
                decrement_pending_bytes(pending_bytes, original_len);
                continue;
            }
            packet_id = event.packet_id;
            payload = event.payload;
        }

        let mut native_layout = false;
        let mut cancelled = false;
        let packet_id_before_translation = packet_id;
        let payload_len_before_translation = payload.len();
        let event_output = apply_protocol_packet_event(
            server,
            connection_id,
            player.clone(),
            PacketDirection::Clientbound,
            version,
            packet_state,
            &mut packet_id,
            &mut payload,
            &mut native_layout,
            &mut cancelled,
        );
        if let Some(sequence) = trace_sequence {
            debug!(
                connection_id,
                sequence,
                state = ?packet_state,
                client_protocol = version.protocol_version(),
                packet_id_before_packet_sent = source_packet_id,
                payload_len_before_packet_sent = source_payload_len,
                packet_id_before_translation,
                payload_len_before_translation,
                packet_id_after_translation = packet_id,
                payload_len_after_translation = payload.len(),
                translated = native_layout,
                cancelled,
                clientbound_follow_ups = event_output.clientbound_packets.len(),
                serverbound_follow_ups = event_output.serverbound_packets.len(),
                "PJM clientbound packet trace"
            );
        }
        let clientbound_packets = event_output.clientbound_packets;
        let serverbound_packets = event_output.serverbound_packets;
        let follow_up_bytes = serverbound_packets.iter().fold(0usize, |total, packet| {
            total.saturating_add(packet.payload.len())
        });
        let java_client = player
            .as_ref()
            .and_then(|player| match player.client.as_ref() {
                ClientPlatform::Java(client) => Some((player, client)),
                ClientPlatform::Bedrock(_) => None,
            });
        if (!serverbound_packets.is_empty() && !native_layout)
            || serverbound_packets.len() > MAX_TRANSLATED_FOLLOW_UP_PACKETS
            || follow_up_bytes > MAX_PENDING_BYTES
            || serverbound_packets.iter().any(|packet| {
                packet.packet_id < 0 || packet.payload.len() > MAX_PACKET_SIZE as usize
            })
            || (!serverbound_packets.is_empty() && java_client.is_none())
        {
            warn!("Invalid native serverbound follow-up output from clientbound packet event");
            close_token.cancel();
            decrement_pending_bytes(pending_bytes, original_len);
            continue;
        }
        if let Some((player, client)) = java_client {
            for follow_up in serverbound_packets {
                let packet = RawPacket {
                    id: follow_up.packet_id,
                    payload: follow_up.payload,
                };
                if let Err(error) = client.handle_play_packet_inner(player, server, &packet, true) {
                    warn!("Failed to dispatch native serverbound follow-up: {error}");
                    client.close();
                    break;
                }
            }
        }
        if close_token.is_cancelled() {
            decrement_pending_bytes(pending_bytes, original_len);
            continue;
        }
        let mut follow_up = Vec::with_capacity(clientbound_packets.len());
        let mut invalid_output = clientbound_packets.len() > MAX_TRANSLATED_FOLLOW_UP_PACKETS;
        for follow_up_packet in clientbound_packets {
            let Some(encoded_follow_up) =
                encode_protocol_packet(follow_up_packet.packet_id, &follow_up_packet.payload)
            else {
                invalid_output = true;
                break;
            };
            follow_up.push(encoded_follow_up);
        }
        if invalid_output {
            close_token.cancel();
            decrement_pending_bytes(pending_bytes, original_len);
            continue;
        }

        let follow_up_bytes = follow_up.iter().map(Bytes::len).sum::<usize>();
        if follow_up_bytes > 0 {
            let previous = pending_bytes.fetch_add(follow_up_bytes, Ordering::AcqRel);
            if previous.saturating_add(follow_up_bytes) > MAX_PENDING_BYTES {
                decrement_pending_bytes(pending_bytes, follow_up_bytes);
                decrement_pending_bytes(pending_bytes, original_len);
                close_token.cancel();
                continue;
            }
        }

        if cancelled {
            decrement_pending_bytes(pending_bytes, original_len);
        } else {
            let Some(encoded_main) = encode_protocol_packet(packet_id, &payload) else {
                if follow_up_bytes > 0 {
                    decrement_pending_bytes(pending_bytes, follow_up_bytes);
                }
                decrement_pending_bytes(pending_bytes, original_len);
                close_token.cancel();
                continue;
            };
            if encoded_main.len() > original_len {
                pending_bytes.fetch_add(encoded_main.len() - original_len, Ordering::AcqRel);
            } else {
                decrement_pending_bytes(pending_bytes, original_len - encoded_main.len());
            }
            packet.data = encoded_main;
            packet.translation_applied = true;
            translated_packets.push(packet);
        }

        translated_packets.extend(follow_up.into_iter().map(OutgoingPacket::normal_translated));
    }
    translated_packets
}
impl OutgoingPacket {
    const fn normal(data: Bytes) -> Self {
        Self {
            data,
            completion: None,
            translation_applied: false,
            force_flush: false,
        }
    }

    const fn normal_translated(data: Bytes) -> Self {
        Self {
            data,
            completion: None,
            translation_applied: true,
            force_flush: false,
        }
    }

    const fn high_priority(data: Bytes, completion: oneshot::Sender<()>) -> Self {
        Self {
            data,
            completion: Some(completion),
            translation_applied: false,
            force_flush: false,
        }
    }

    /// Kick barrier: the writer signals completion after the flush attempt,
    /// so a kick awaited inside the tick does not wait on its own barrier.
    /// Forces a flush even while `suspend_flushing` holds.
    const fn flushed(data: Bytes, completion: oneshot::Sender<()>) -> Self {
        Self {
            data,
            completion: Some(completion),
            translation_applied: false,
            force_flush: true,
        }
    }

    /// `flush_channel` barrier: consumed by the writer task, never framed.
    const fn flush_barrier() -> Self {
        Self {
            data: Bytes::new(),
            completion: None,
            translation_applied: true,
            force_flush: true,
        }
    }
}

impl JavaClient {
    #[must_use]
    pub fn from_pending(
        pending: PendingConnection,
        gameprofile: GameProfile,
        config: PlayerConfig,
    ) -> Self {
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();
        let (priority_send, priority_recv) = tokio::sync::mpsc::unbounded_channel();

        Self {
            id: pending.id,
            protocol_translator_active: false,
            clientbound_diagnostic_trace_packets: AtomicUsize::new(0),
            serverbound_diagnostic_trace_packets: AtomicUsize::new(0),
            raw_serverbound_diagnostic_trace_packets: AtomicUsize::new(0),
            flushed_clientbound_diagnostic_trace_packets: AtomicUsize::new(0),
            gameprofile,
            config: ArcSwap::from_pointee(config),
            server_address: pending.server_address,
            address: pending.address,
            connection_state: pending.connection_state,
            close_token: pending.close_token,
            tasks: TaskTracker::new(),
            rt_handle: tokio::runtime::Handle::current(),
            outgoing_packet_queue_send: send,
            outgoing_packet_queue_recv: Some(recv),
            outgoing_packet_priority_send: priority_send,
            outgoing_packet_priority_recv: Some(priority_recv),
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            version: pending.version,
            network_writer: std::sync::Mutex::new(Some(pending.network_writer)),
            network_reader: std::sync::Mutex::new(Some(pending.network_reader)),
            brand: ArcSwap::from_pointee(pending.brand),
            player: Arc::new(ArcSwap::from_pointee(None)),
            wait_for_keep_alive: AtomicBool::new(false),
            received_movement_this_tick: AtomicBool::new(false),
            keep_alive_id: AtomicCell::new(0),
            last_keep_alive_time: AtomicCell::new(Instant::now()),
            last_packet_time: AtomicCell::new(Instant::now()),
            pending_keep_alives: std::sync::Mutex::new(Vec::new()),
            packet_sequence: AtomicI32::new(-1),
            packet_limiter: pending.packet_limiter,
            suspend_flushing: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Vanilla `ServerCommonPacketListenerImpl.suspendFlushing`.
    pub fn suspend_flushing(&self) {
        self.suspend_flushing.store(true, Ordering::Release);
    }

    /// Vanilla `resumeFlushing`: queue `flushChannel` then lift the hold.
    pub fn resume_flushing(&self) {
        self.flush_channel();
        self.suspend_flushing.store(false, Ordering::Release);
    }

    /// Flushes Channel even while suspended. The barrier is consumed by
    /// the writer task and never reaches the wire.
    pub fn flush_channel(&self) {
        if self
            .outgoing_packet_queue_send
            .send(OutgoingPacket::flush_barrier())
            .is_err()
            && !self.close_token.is_cancelled()
        {
            warn!(
                "Failed to queue flush for client {}: channel closed",
                self.id
            );
            self.close();
        }
    }

    pub fn set_player(&self, player: Arc<Player>) {
        self.player.store(Arc::new(Some(player)));
    }

    pub async fn progress_player_packets(&self, player: &Arc<Player>, server: &Arc<Server>) {
        let Some(mut network_reader) = self
            .network_reader
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };

        let keep_alive_time = server.advanced_config.networking.java.keep_alive_time;
        let mut keep_alive_interval =
            tokio::time::interval(std::time::Duration::from_secs(keep_alive_time.max(1)));
        let timeout_duration =
            std::time::Duration::from_secs(keep_alive_time.saturating_mul(2).max(1));

        // Skip the immediate first tick so we don't send a keep-alive the exact millisecond they join
        keep_alive_interval.tick().await;

        loop {
            tokio::select! {
                // KEEP-ALIVE TIMER
                _ = keep_alive_interval.tick() => {
                    // Check if the client has timed out on keep-alive responses or no packet activity
                    let has_timed_out = {
                        let pending = self
                            .pending_keep_alives
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        pending.iter().any(|(_, send_time)| send_time.elapsed() > timeout_duration)
                    } || (self.wait_for_keep_alive.load(Ordering::Relaxed) && self.last_keep_alive_time.load().elapsed() > timeout_duration)
                      || (self.last_packet_time.load().elapsed() > timeout_duration);

                    if has_timed_out {
                        self.kick(pumpkin_macros::translate_cross!(translation::java::DISCONNECT_TIMEOUT, translation::bedrock::DISCONNECT_TIMEOUT)).await;
                        break;
                    }

                    let keep_alive_id = i64::from(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i32,
                    );

                    self.keep_alive_id.store(keep_alive_id);
                    self.wait_for_keep_alive.store(true, Ordering::Relaxed);
                    self.last_keep_alive_time.store(Instant::now());
                    {
                        let mut pending = self
                            .pending_keep_alives
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        pending.push((keep_alive_id, Instant::now()));
                        if pending.len() > 16 {
                            pending.remove(0);
                        }
                    }
                    let packet = pumpkin_protocol::java::client::play::CKeepAlive::new(keep_alive_id);
                    self.enqueue_client_packet(&packet).await;
                }

                () = self.close_token.cancelled() => {
                    break;
                }

                // INCOMING PACKETS
                packet_opt = self.get_packet_with_reader(&mut network_reader) => {
                    let Some(packet) = packet_opt else {
                        break;
                    };
                    self.last_packet_time.store(Instant::now());

                    if !self.packet_limiter.check_packet() {
                        warn!(
                            "Client {} ({}) exceeded packet rate limit (rate: {}/s)",
                            self.id,
                            self.gameprofile.name,
                            self.packet_limiter.max_rate()
                        );
                        self.kick(TextComponent::text(
                            server
                                .advanced_config
                                .networking
                                .java
                                .packet_limiter
                                .kick_message
                                .clone(),
                        ))
                        .await;
                        break;
                    }

                    player.inbound_packets.push(packet);
                }
            }
        }
    }

    pub async fn await_tasks(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    /// Spawns a task associated with this client. All tasks spawned with this method are awaited
    /// when the client. This means tasks should complete in a reasonable amount of time or select
    /// on `Self::await_close_interrupt` to cancel the task when the client is closed
    ///
    /// Returns an `Option<JoinHandle<F::Output>>`. If the client is closed, this returns `None`.
    pub fn spawn_task<F>(&self, task: F) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if self.close_token.is_cancelled() {
            None
        } else {
            let _guard = self.rt_handle.enter();
            Some(self.tasks.spawn(task))
        }
    }

    pub async fn send_chunks(&self, chunks: &[SyncChunk]) {
        let _ = self.send_chunks_impl(chunks, false, false).await;
    }

    /// Sends chunks already approved by their `ChunkSend` hook.
    pub(crate) async fn send_chunks_reserved_approved(&self, chunks: &[SyncChunk]) -> bool {
        self.send_chunks_impl(chunks, true, true).await
    }

    /// Reserves the acknowledgment slot and gates regular sends before world-transition chunks are scheduled.
    pub(crate) fn reserve_chunk_batch(&self) {
        let track_acknowledgment = self.version.load() >= JavaMinecraftVersion::V_1_20_2;
        let player = self.player.load_full();
        let Some(player) = player.as_ref() else {
            return;
        };
        player
            .chunk_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reserve_out_of_band_batch(track_acknowledgment);
    }

    fn finish_reserved_chunk_batch(&self, abort: bool) {
        let track_acknowledgment = self.version.load() >= JavaMinecraftVersion::V_1_20_2;
        let player = self.player.load_full();
        if let Some(player) = player.as_ref() {
            let mut sender = player
                .chunk_sender
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if abort {
                sender.abort_out_of_band_batch(track_acknowledgment);
            } else {
                sender.finish_out_of_band_batch();
            }
        }
    }

    async fn send_chunks_impl(
        &self,
        chunks: &[SyncChunk],
        batch_reserved: bool,
        events_already_approved: bool,
    ) -> bool {
        let player = self.player.load_full();
        let Some(player) = player.as_ref() else {
            if batch_reserved {
                self.finish_reserved_chunk_batch(true);
            }
            return false;
        };
        let Some(server) = player.world().server.upgrade() else {
            if batch_reserved {
                self.finish_reserved_chunk_batch(true);
            }
            return false;
        };

        let mut valid_chunks = Vec::with_capacity(chunks.len());
        if events_already_approved {
            valid_chunks.extend_from_slice(chunks);
        } else {
            for chunk in chunks {
                let mut event = ChunkSend::new(player.world(), chunk.clone());
                server.plugin_manager.fire(&server, &mut event).await;
                if !event.cancelled {
                    valid_chunks.push(chunk.clone());
                }
            }
        }

        if valid_chunks.is_empty() {
            if batch_reserved {
                self.finish_reserved_chunk_batch(true);
            }
            return false;
        }

        let version = self.version.load();
        let (tx, rx) = oneshot::channel();
        rayon::spawn(move || {
            let mut serialized = Vec::with_capacity(valid_chunks.len());
            for chunk in valid_chunks {
                let mut buf = Vec::with_capacity(32 * 1024);
                if let Err(err) = buf.write_var_int(&VarInt(CChunkData::to_id(version))) {
                    error!("Failed to write chunk data id: {err:?}");
                    continue;
                }
                if let Err(err) = CChunkData(&chunk).write_packet_data(&mut buf, &version) {
                    error!("Failed to write chunk data: {err:?}");
                    continue;
                }

                let light_buf = if version >= JavaMinecraftVersion::V_1_14
                    && version < JavaMinecraftVersion::V_1_18
                {
                    match <CLightUpdate as ChunkLightExt>::from_chunk(&chunk, version) {
                        Ok(light_packet) => {
                            let mut light_buf = Vec::new();
                            if let Err(err) =
                                light_buf.write_var_int(&VarInt(CLightUpdate::to_id(version)))
                            {
                                error!("Failed to write light update id: {err:?}");
                                None
                            } else if let Err(err) =
                                light_packet.write_packet_data(&mut light_buf, &version)
                            {
                                error!("Failed to write light update data: {err:?}");
                                None
                            } else {
                                Some(Bytes::from(light_buf))
                            }
                        }
                        Err(err) => {
                            error!("Failed to create light update packet: {err:?}");
                            None
                        }
                    }
                } else {
                    None
                };

                serialized.push((Bytes::from(buf), light_buf));
            }
            let _ = tx.send(serialized);
        });

        let Ok(serialized) = rx.await else {
            if batch_reserved {
                self.finish_reserved_chunk_batch(true);
            }
            return false;
        };
        let sent_count = serialized.len();
        if sent_count == 0 {
            if batch_reserved {
                self.finish_reserved_chunk_batch(true);
            }
            return false;
        }

        if version >= JavaMinecraftVersion::V_1_20_2 {
            self.send_packet(&CChunkBatchStart).await;
            if self.is_closed() {
                return false;
            }
        }

        // Keep the whole batch on the priority queue. Otherwise the batch end can overtake chunk
        // data queued on the normal channel, leaving the client unable to render those chunks.
        for (chunk_data, light_data) in serialized {
            self.send_packet_now_data(chunk_data).await;
            if self.is_closed() {
                return false;
            }
            if let Some(light_data) = light_data {
                self.send_packet_now_data(light_data).await;
                if self.is_closed() {
                    return false;
                }
            }
        }

        if version >= JavaMinecraftVersion::V_1_20_2 {
            self.send_packet(&CChunkBatchEnd::new(sent_count as u16))
                .await;
            if self.is_closed() {
                return false;
            }
        }

        if batch_reserved {
            self.finish_reserved_chunk_batch(false);
        }
        true
    }

    pub async fn enqueue_packet(&self, packet_data: Bytes) {
        self.enqueue_packet_data(packet_data).await;
    }

    #[allow(clippy::unused_async)]
    pub async fn enqueue_packet_data(&self, packet_data: Bytes) {
        self.try_enqueue_packet_data(packet_data);
    }

    /// Outbound choke point of all enqueue/send paths. `None` when the packet must be dropped.
    fn reserve_pending_bytes(&self, packet_data: Bytes) -> Option<(Bytes, usize)> {
        if self.close_token.is_cancelled() {
            return None;
        }
        let packet_data = self.translate_outgoing(packet_data)?;

        // The outgoing writer applies ProtocolPacketEvent to this queue entry,
        // including priority packets, before it frames the bytes.

        let packet_len = packet_data.len();
        let prev_bytes = self.pending_bytes.fetch_add(packet_len, Ordering::AcqRel);
        let new_bytes = prev_bytes.saturating_add(packet_len);

        if new_bytes > MAX_PENDING_BYTES {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            if !self.close_token.is_cancelled() {
                warn!(
                    "Client {} outbound packet buffer overflow ({} bytes > {} bytes). Closing connection.",
                    self.id, new_bytes, MAX_PENDING_BYTES
                );
                self.close();
            }
            return None;
        }

        Some((packet_data, packet_len))
    }

    /// `PacketSentEvent` for clients the multiversion plugin admitted below
    /// `CURRENT_MC_VERSION`: it gets the 26.3 id + payload and rewrites both.
    /// `None` when cancelled.
    fn translate_outgoing(&self, packet_data: Bytes) -> Option<Bytes> {
        if self.version.load() == CURRENT_MC_VERSION {
            return Some(packet_data);
        }
        // TODO: packets sent before `set_player` (e.g. an `add_player` kick) go out untranslated.
        let player = self.player.load_full();
        let Some(player) = player.as_ref() else {
            return Some(packet_data);
        };
        let Some(server) = player.world().server.upgrade() else {
            return Some(packet_data);
        };
        if !server.plugin_manager.has_handlers::<PacketSentEvent>() {
            return Some(packet_data);
        }

        let mut reader = &packet_data[..];
        let Ok(packet_id) = reader.get_var_int() else {
            return Some(packet_data);
        };
        let payload = packet_data.slice(packet_data.len() - reader.len()..);
        let mut event = PacketSentEvent::new_raw(player.clone(), packet_id.0, payload);
        server.plugin_manager.fire_blocking(&server, &mut event);
        if event.cancelled {
            return None;
        }

        let mut framed = Vec::with_capacity(5 + event.payload.len());
        framed.write_var_int(&VarInt(event.packet_id)).ok()?;
        framed.extend_from_slice(&event.payload);
        Some(framed.into())
    }

    pub fn try_enqueue_packet(&self, packet_data: Bytes) {
        self.try_enqueue_packet_data(packet_data);
    }

    pub fn try_enqueue_packet_data(&self, packet_data: Bytes) {
        let Some((packet_data, packet_len)) = self.reserve_pending_bytes(packet_data) else {
            return;
        };
        self.queue_outgoing(OutgoingPacket::normal(packet_data), packet_len);
    }

    /// `false` once the writer is gone. Then the connection is closed.
    fn queue_outgoing(&self, packet: OutgoingPacket, packet_len: usize) -> bool {
        if self.outgoing_packet_queue_send.send(packet).is_ok() {
            return true;
        }
        decrement_pending_bytes(&self.pending_bytes, packet_len);
        // It is expected that the packet will fail if closed
        if !self.close_token.is_cancelled() {
            warn!(
                "Failed to add packet to the outgoing packet queue for client {}: channel closed",
                self.id
            );
            // Connection to the client closed since the stream is in an unknown state
            self.close();
        }
        false
    }

    /// Queues a clientbound packet already translated for this connection's negotiated version.
    pub(crate) fn try_enqueue_translated_packet(&self, packet_id: i32, payload: &[u8]) {
        if self.close_token.is_cancelled() || packet_id < 0 {
            return;
        }
        let mut data = Vec::with_capacity(payload.len() + 5);
        if data.write_var_int(&VarInt(packet_id)).is_err() {
            return;
        }
        data.extend_from_slice(payload);
        let packet = Bytes::from(data);
        let packet_len = packet.len();
        let prev_bytes = self.pending_bytes.fetch_add(packet_len, Ordering::AcqRel);
        let new_bytes = prev_bytes.saturating_add(packet_len);
        if new_bytes > MAX_PENDING_BYTES {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            self.close();
            return;
        }
        if let Err(err) = self
            .outgoing_packet_priority_send
            .send(OutgoingPacket::normal_translated(packet))
        {
            decrement_pending_bytes(&self.pending_bytes, packet_len);
            if !self.close_token.is_cancelled() {
                warn!(
                    "Failed to queue translated packet for client {}: {}",
                    self.id, err
                );
                self.close();
            }
        }
    }

    pub async fn await_close_interrupt(&self) {
        self.close_token.cancelled().await;
    }

    pub async fn get_packet_with_reader(
        &self,
        network_reader: &mut TCPNetworkDecoder<BufReader<OwnedReadHalf>>,
    ) -> Option<RawPacket> {
        tokio::select! {
            () = self.await_close_interrupt() => {
                if should_trace_java_diagnostic(self.version.load())
                    && self.connection_state.load() == ConnectionState::Play
                    && self.protocol_translator_active
                {
                    debug!(
                        connection_id = self.id,
                        client_protocol = self.version.load().protocol_version(),
                        "PJM raw serverbound packet read cancelled"
                    );
                }
                debug!("Canceling player packet processing");
                None
            },
            packet_result = network_reader.get_raw_packet() => {
                match packet_result {
                    Ok(packet) => {
                        let version = self.version.load();
                        let state = self.connection_state.load();
                        if should_trace_java_diagnostic(version)
                            && state == ConnectionState::Play
                            && self.protocol_translator_active
                        {
                            let sequence = self
                                .raw_serverbound_diagnostic_trace_packets
                                .fetch_add(1, Ordering::Relaxed);
                            if sequence < MAX_DIAGNOSTIC_TRACE_PACKETS {
                                debug!(
                                    connection_id = self.id,
                                    sequence = sequence + 1,
                                    state = ?state,
                                    client_protocol = version.protocol_version(),
                                    packet_id = packet.id,
                                    payload_len = packet.payload.len(),
                                    "PJM raw serverbound packet received"
                                );
                            }
                        }
                        Some(packet)
                    }
                    Err(err) => {
                        let version = self.version.load();
                        let state = self.connection_state.load();
                        if should_trace_java_diagnostic(version)
                            && state == ConnectionState::Play
                            && self.protocol_translator_active
                        {
                            debug!(
                                connection_id = self.id,
                                state = ?state,
                                client_protocol = version.protocol_version(),
                                error = ?err,
                                "PJM raw serverbound packet read ended"
                            );
                        }
                        if !matches!(err, PacketDecodeError::ConnectionClosed) {
                            debug!("Failed to decode packet from client {}: {}", self.id, err);
                            let text = format!("Error while reading incoming packet {err}");
                            self.kick(TextComponent::text(text)).await;
                        }
                        None
                    }
                }
            }
        }
    }

    /// Disconnect packet for the current state. `None` in handshake/status.
    fn serialize_disconnect(&self, reason: &TextComponent) -> Option<Bytes> {
        match self.connection_state.load() {
            ConnectionState::Login => {
                // TextComponent implements Serialize and writes in bytes instead of String
                let packet = CLoginDisconnect::new(
                    serde_json::to_string(&reason.0).unwrap_or_else(|_| String::new()),
                );
                self.serialize_packet(&packet).ok()
            }
            ConnectionState::Config => {
                let reason_text = reason.clone().get_text();
                let packet = CConfigDisconnect::new(&reason_text);
                self.serialize_packet(&packet).ok()
            }
            ConnectionState::Play => {
                let packet = CPlayDisconnect::new(reason);
                self.serialize_packet(&packet).ok()
            }
            _ => None,
        }
    }

    pub fn try_kick(&self, reason: &TextComponent) {
        if let Some(data) = self
            .serialize_disconnect(reason)
            .and_then(|data| self.translate_outgoing(data))
        {
            let packet_len = data.len();
            let _ = self.pending_bytes.fetch_add(packet_len, Ordering::AcqRel);
            // The writer drains and flushes it after `close()`
            if self
                .outgoing_packet_queue_send
                .send(OutgoingPacket::normal(data))
                .is_err()
            {
                decrement_pending_bytes(&self.pending_bytes, packet_len);
                // Expected: the writer task is already gone.
                debug!(
                    "Disconnect packet for client {} dropped: outgoing packet queue closed",
                    self.id
                );
            }
        }
        let reason_text = reason.clone().get_text();
        warn!("Closing connection for {}: {reason_text}", self.id);
        self.close();
    }

    pub async fn kick(&self, reason: TextComponent) {
        self.kick_explicit(&reason, true).await;
    }

    pub async fn kick_explicit(&self, reason: &TextComponent, send_packet: bool) {
        if send_packet && let Some(data) = self.serialize_disconnect(reason) {
            // Stalled peer: never flushes -> Close anyway.
            let _ = tokio::time::timeout(
                DISCONNECT_FLUSH_TIMEOUT,
                self.send_and_wait(data, OutgoingPacket::flushed),
            )
            .await;
        }
        let reason_text = reason.clone().get_text();
        warn!("Closing connection for {}: {reason_text}", self.id);
        self.close();
    }

    pub async fn send_packet_now(&self, packet: Bytes) {
        self.send_packet_now_data(packet).await;
    }

    /// Enqueue on the per-connection FIFO and wait until the writer has
    /// `write_frame`d into the `BufWriter`. Never waits for a TCP flush.
    pub async fn send_packet_now_data(&self, packet: Bytes) {
        self.send_and_wait(packet, OutgoingPacket::high_priority)
            .await;
    }

    /// Enqueue and wait for the writer's completion, `Framed` or `Flushed` per `make`.
    async fn send_and_wait(
        &self,
        packet: Bytes,
        make: fn(Bytes, oneshot::Sender<()>) -> OutgoingPacket,
    ) {
        let Some((packet, packet_len)) = self.reserve_pending_bytes(packet) else {
            return;
        };

        let (completion_tx, completion_rx) = oneshot::channel();
        if !self.queue_outgoing(make(packet, completion_tx), packet_len) {
            return;
        }

        if completion_rx.await.is_err() && !self.close_token.is_cancelled() {
            // The outgoing packet task dropped before confirming the write.
            self.close();
        }
    }

    pub fn write_packet_for_version<P: ClientPacket>(
        packet: &P,
        version: JavaMinecraftVersion,
        write: impl Write,
    ) -> Result<(), WritingError> {
        pumpkin_protocol::java::packet_encoder::write_packet(packet, &version, write)
    }

    /// Serializes using the explicitly requested protocol layout. Per-client
    /// sends should use [`JavaClient::serialize_packet`].
    pub fn serialize_packet_for_version<P: ClientPacket>(
        packet: &P,
        version: JavaMinecraftVersion,
    ) -> Result<Bytes, WritingError> {
        pumpkin_protocol::java::packet_encoder::serialize_packet(packet, &version)
    }

    /// Serializes a packet with an independent packet ID and payload layout.
    ///
    /// The translator uses this for legacy Login/Respawn packets: the Pumpkin
    /// writer supplies the older inline dimension registry codec, while PJM
    /// still needs to recognize the packet by its native 26.3 ID.
    pub(crate) fn serialize_packet_with_versions<P: ClientPacket>(
        packet: &P,
        packet_id_version: JavaMinecraftVersion,
        payload_version: JavaMinecraftVersion,
    ) -> Result<Bytes, WritingError> {
        let packet_id = P::to_id(packet_id_version);
        if packet_id < 0 {
            return Err(WritingError::UnsupportedVersion(packet_id_version));
        }

        let mut packet_buf = Vec::new();
        packet_buf.write_var_int(&VarInt(packet_id))?;
        packet.write_packet_data(&mut packet_buf, &payload_version)?;
        Ok(packet_buf.into())
    }

    pub fn serialize_packet<P: ClientPacket>(&self, packet: &P) -> Result<Bytes, WritingError> {
        Self::serialize_packet_for_version(packet, self.packet_encoding_version())
    }

    #[must_use]
    pub(crate) fn packet_encoding_version(&self) -> JavaMinecraftVersion {
        packet_serialization_version(self.version.load(), self.protocol_translator_active)
    }

    pub fn try_send_packet<P: ClientPacket>(&self, packet: &P) {
        if let Ok(data) = self.serialize_packet(packet) {
            self.try_enqueue_packet(data);
        }
    }

    pub async fn send_packet<P: ClientPacket>(&self, packet: &P) {
        if let Ok(data) = self.serialize_packet(packet) {
            self.send_packet_now(data).await;
        }
    }

    fn serialize_packet_with_compatibility_layout<P: ClientPacket>(
        &self,
        packet: &P,
    ) -> Result<Bytes, WritingError> {
        let client_version = self.version.load();
        let packet_id_version = self.packet_encoding_version();
        let payload_version =
            if self.protocol_translator_active && client_version < JavaMinecraftVersion::V_1_20_2 {
                client_version
            } else {
                packet_id_version
            };

        Self::serialize_packet_with_versions(packet, packet_id_version, payload_version)
    }

    /// Sends Login/Respawn data with Pumpkin's legacy inline-registry layout
    /// for clients that cannot receive a configuration-state registry stream.
    pub(crate) async fn send_packet_with_compatibility_layout<P: ClientPacket>(&self, packet: &P) {
        match self.serialize_packet_with_compatibility_layout(packet) {
            Ok(data) => self.send_packet_now(data).await,
            Err(err) => warn!(
                packet = std::any::type_name::<P>(),
                ?err,
                "Failed to serialize compatibility-layout packet"
            ),
        }
    }

    pub(crate) fn try_send_packet_with_compatibility_layout<P: ClientPacket>(&self, packet: &P) {
        match self.serialize_packet_with_compatibility_layout(packet) {
            Ok(data) => self.try_enqueue_packet(data),
            Err(err) => warn!(
                packet = std::any::type_name::<P>(),
                ?err,
                "Failed to serialize compatibility-layout packet"
            ),
        }
    }

    pub(crate) async fn enqueue_packet_with_compatibility_layout<P: ClientPacket>(
        &self,
        packet: &P,
    ) {
        match self.serialize_packet_with_compatibility_layout(packet) {
            Ok(data) => self.enqueue_packet(data).await,
            Err(err) => warn!(
                packet = std::any::type_name::<P>(),
                ?err,
                "Failed to serialize compatibility-layout packet"
            ),
        }
    }

    pub async fn enqueue_client_packet<P: ClientPacket>(&self, packet: &P) {
        if let Ok(data) = self.serialize_packet(packet) {
            self.enqueue_packet(data).await;
        }
    }

    pub fn write_packet<P: ClientPacket>(
        &self,
        packet: &P,
        write: impl Write,
    ) -> Result<(), WritingError> {
        Self::write_packet_for_version(packet, self.packet_encoding_version(), write)
    }

    /// Handles an incoming packet, routing it to the appropriate handler based on the current connection state.
    ///
    /// This function takes a `RawPacket` and routes it to the corresponding handler based on the current connection state.
    /// It supports the following connection states:
    ///
    /// - **Handshake:** Handles handshake packets.
    /// - **Status:** Handles status request and ping packets.
    /// - **Login/Transfer:** Handles login and transfer packets.
    /// - **Config:** Handles configuration packets.
    #[expect(clippy::too_many_lines)]
    pub fn start_outgoing_packet_task(&mut self, server: &Arc<Server>) {
        const MAX_BATCH_SIZE: usize = 64;

        // Multi-version protocol handlers consume packets in Pumpkin's native
        // layout and rewrite them before they reach the client.
        self.protocol_translator_active =
            server.plugin_manager.has_handlers::<ProtocolPacketEvent>();

        let Some(mut packet_receiver) = self.outgoing_packet_queue_recv.take() else {
            return;
        };
        let Some(mut priority_packet_receiver) = self.outgoing_packet_priority_recv.take() else {
            return;
        };
        let close_token = self.close_token.clone();
        let pending_bytes = self.pending_bytes.clone();
        let player = self.player.clone();
        let version = self.version.load();
        let connection_id = self.id;
        let connection_state = self.connection_state.load();
        let server = server.clone();
        let Some(mut writer) = self
            .network_writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };
        let id = self.id;
        let suspend_flushing = self.suspend_flushing.clone();
        self.spawn_task(async move {
            loop {
                let recv_result = tokio::select! {
                    biased;
                    res = priority_packet_receiver.recv() => res,
                    res = packet_receiver.recv() => res,
                    () = close_token.cancelled() => {
                        priority_packet_receiver
                            .try_recv()
                            .ok()
                            .or_else(|| packet_receiver.try_recv().ok())
                    }
                };

                let Some(packet_data) = recv_result else {
                    break;
                };

                let mut packet_batch = Vec::with_capacity(MAX_BATCH_SIZE);
                packet_batch.push(packet_data);

                while packet_batch.len() < MAX_BATCH_SIZE {
                    match priority_packet_receiver.try_recv() {
                        Ok(packet_data) => {
                            packet_batch.push(packet_data);
                            continue;
                        }
                        Err(TryRecvError::Disconnected | TryRecvError::Empty) => {}
                    }

                    match packet_receiver.try_recv() {
                        Ok(packet_data) => packet_batch.push(packet_data),
                        Err(TryRecvError::Disconnected | TryRecvError::Empty) => break,
                    }
                }

                let packet_batch = apply_packet_sent_events(
                    packet_batch,
                    &player,
                    &pending_bytes,
                    version,
                    connection_id,
                    connection_state,
                    &server,
                    &close_token,
                )
                .await;

                let mut packets_to_frame = VecDeque::from(packet_batch);
                // `flush_channel` barriers force a flush even while the tick
                // holds flushing; they are consumed here, never framed.
                let mut force_flush = false;
                packets_to_frame.retain(|packet| {
                    if packet.force_flush {
                        force_flush = true;
                        false
                    } else {
                        true
                    }
                });
                let suspended = suspend_flushing.load(Ordering::Acquire);
                let mut written_packets = Vec::with_capacity(packets_to_frame.len());
                let mut send_failed = false;

                while !packets_to_frame.is_empty() {
                    let frame_batch = take_frame_batch(&mut packets_to_frame);
                    let (returned_writer, returned_batch, frame, frame_err) =
                        match frame_batch_maybe_offload(writer, frame_batch).await {
                            Ok(result) => result,
                            Err(err) => {
                                if !close_token.is_cancelled() {
                                    warn!("Packet framing task failed for client {id}: {err}");
                                }
                                close_token.cancel();
                                return;
                            }
                        };
                    writer = returned_writer;

                    if let Some(err) = frame_err {
                        if !close_token.is_cancelled() {
                            warn!("Failed to frame packet for client {id}: {err}");
                        }
                        send_failed = true;
                        break;
                    }

                    if let Err(err) = writer.write_frame(&frame).await {
                        let current_player = player.load_full();
                        if !close_token.is_cancelled()
                            && should_trace_java_diagnostic(version)
                            && let Some(active_player) = current_player.as_ref()
                            && let ClientPlatform::Java(client) = active_player.client.as_ref()
                            && client.connection_state.load() == ConnectionState::Play
                            && client.protocol_translator_active
                        {
                            debug!(
                                connection_id,
                                client_protocol = version.protocol_version(),
                                frame_bytes = frame.len(),
                                packets_in_batch = returned_batch.len(),
                                error = ?err,
                                "PJM clientbound frame write failed"
                            );
                        }
                        if !close_token.is_cancelled() {
                            warn!("Failed to send packet batch to client {id}: {err}");
                        }
                        send_failed = true;
                        break;
                    }

                    written_packets.extend(returned_batch);
                }

                // Vanilla tick semantics: while `suspend_flushing` holds,
                // frames accumulate in the socket buffer and go out together
                // at `flush_channel`. A barrier, kick drain, or close still
                // flushes immediately.
                let hold_flush = suspended
                    && !force_flush
                    && !send_failed
                    && !close_token.is_cancelled();
                if !hold_flush && !send_failed && let Err(err) = writer.flush().await {
                    let current_player = player.load_full();
                    if !close_token.is_cancelled()
                        && should_trace_java_diagnostic(version)
                        && let Some(active_player) = current_player.as_ref()
                        && let ClientPlatform::Java(client) = active_player.client.as_ref()
                        && client.connection_state.load() == ConnectionState::Play
                        && client.protocol_translator_active
                    {
                        debug!(
                            connection_id,
                            client_protocol = version.protocol_version(),
                            packets_in_batch = written_packets.len(),
                            payload_bytes_in_batch = written_packets
                                .iter()
                                .map(|packet| packet.data.len())
                                .sum::<usize>(),
                            error = ?err,
                            "PJM clientbound packet flush failed"
                        );
                    }
                    if !close_token.is_cancelled() {
                        warn!("Failed to flush packet batch for client {id}: {err}");
                    }
                    send_failed = true;
                }

                let current_player = player.load_full();
                if !send_failed
                    && let Some(active_player) = current_player.as_ref()
                    && let ClientPlatform::Java(client) = active_player.client.as_ref()
                    && should_trace_java_diagnostic(version)
                    && client.connection_state.load() == ConnectionState::Play
                    && client.protocol_translator_active
                {
                    for packet in &written_packets {
                        let sequence = client
                            .flushed_clientbound_diagnostic_trace_packets
                            .fetch_add(1, Ordering::Relaxed);
                        if sequence >= MAX_DIAGNOSTIC_TRACE_PACKETS {
                            break;
                        }
                        let mut encoded = packet.data.as_ref();
                        if let Ok(packet_id) = encoded.get_var_int() {
                            debug!(
                                connection_id,
                                sequence = sequence + 1,
                                client_protocol = version.protocol_version(),
                                packet_id_to_client = packet_id.0,
                                payload_len_to_client = encoded.len(),
                                "PJM clientbound packet flushed"
                            );
                        }
                    }
                }

                let flushed_bytes: usize = written_packets.iter().map(|p| p.data.len()).sum();
                decrement_pending_bytes(&pending_bytes, flushed_bytes);

                if send_failed {
                    // We now need to close the connection to the client since the stream is in an unknown state.
                    close_token.cancel();
                    break;
                }

                for packet in written_packets {
                    if let Some(completion) = packet.completion {
                        let _ = completion.send(());
                    }
                }
            }
        });
    }

    /// Closes the connection to the client.
    ///
    /// This function marks the connection as closed using an atomic flag. It's generally preferable
    /// to use the `kick` function if you want to send a specific message to the client explaining the reason for the closure.
    /// However, use `close` in scenarios where sending a message is not critical or might not be possible (e.g., sudden connection drop).
    ///
    /// # Notes
    ///
    /// This function does not attempt to send any disconnect packets to the client.
    /// Packets already queued are still written and flushed, bounded by `DISCONNECT_FLUSH_TIMEOUT`.
    pub fn close(&self) {
        self.close_token.cancel();
    }

    pub fn is_closed(&self) -> bool {
        self.close_token.is_cancelled()
    }

    #[expect(clippy::too_many_lines)]
    pub fn handle_play_packet(
        &self,
        player: &Arc<Player>,
        server: &Arc<Server>,
        packet: &RawPacket,
    ) -> Result<(), Box<dyn PumpkinError>> {
        self.handle_play_packet_inner(player, server, packet, false)
    }

    fn handle_play_packet_inner(
        &self,
        player: &Arc<Player>,
        server: &Arc<Server>,
        packet: &RawPacket,
        native_layout: bool,
    ) -> Result<(), Box<dyn PumpkinError>> {
        let client_version = self.version.load();
        let source_packet_id = packet.id;
        let source_payload_len = packet.payload.len();
        let state = self.connection_state.load();
        let trace_sequence = if should_trace_java_diagnostic(client_version)
            && state == ConnectionState::Play
            && self.protocol_translator_active
        {
            let sequence = self
                .serverbound_diagnostic_trace_packets
                .fetch_add(1, Ordering::Relaxed);
            (sequence < MAX_DIAGNOSTIC_TRACE_PACKETS).then_some(sequence + 1)
        } else {
            None
        };
        let (
            packet_id,
            packet_payload,
            translated,
            cancelled,
            clientbound_packets,
            serverbound_packets,
        ) = if native_layout {
            (
                packet.id,
                packet.payload.clone(),
                true,
                false,
                Vec::new(),
                Vec::new(),
            )
        } else {
            let mut event = crate::plugin::server::packet::PacketReceivedEvent::new(
                player.clone(),
                packet.id,
                packet.payload.clone(),
            );
            server.plugin_manager.fire_blocking(server, &mut event);
            if event.cancelled {
                if let Some(sequence) = trace_sequence {
                    debug!(
                        connection_id = self.id,
                        sequence,
                        state = ?state,
                        client_protocol = client_version.protocol_version(),
                        packet_id_before_received_event = source_packet_id,
                        payload_len_before_received_event = source_payload_len,
                        cancelled = true,
                        "PJM serverbound packet trace"
                    );
                }
                return Ok(());
            }

            let mut packet_id = event.packet_id;
            let mut packet_payload = event.payload;
            let packet_id_after_received_event = packet_id;
            let payload_len_after_received_event = packet_payload.len();
            let mut translated = false;
            let mut cancelled = false;
            let event_output = apply_protocol_packet_event(
                server,
                self.id,
                Some(player.clone()),
                PacketDirection::Serverbound,
                client_version,
                self.connection_state.load(),
                &mut packet_id,
                &mut packet_payload,
                &mut translated,
                &mut cancelled,
            );
            if let Some(sequence) = trace_sequence {
                debug!(
                    connection_id = self.id,
                    sequence,
                    state = ?state,
                    client_protocol = client_version.protocol_version(),
                    packet_id_before_received_event = source_packet_id,
                    payload_len_before_received_event = source_payload_len,
                    packet_id_after_received_event,
                    payload_len_after_received_event,
                    packet_id_after_translation = packet_id,
                    payload_len_after_translation = packet_payload.len(),
                    translated,
                    cancelled,
                    clientbound_follow_ups = event_output.clientbound_packets.len(),
                    serverbound_follow_ups = event_output.serverbound_packets.len(),
                    "PJM serverbound packet trace"
                );
            }
            (
                packet_id,
                packet_payload,
                translated,
                cancelled,
                event_output.clientbound_packets,
                event_output.serverbound_packets,
            )
        };

        if native_layout && let Some(sequence) = trace_sequence {
            debug!(
                connection_id = self.id,
                sequence,
                state = ?state,
                client_protocol = client_version.protocol_version(),
                packet_id = source_packet_id,
                payload_len = source_payload_len,
                native_layout,
                translated,
                cancelled,
                clientbound_follow_ups = clientbound_packets.len(),
                serverbound_follow_ups = serverbound_packets.len(),
                "PJM serverbound packet trace"
            );
        }

        for reply in clientbound_packets {
            self.try_enqueue_translated_packet(reply.packet_id, &reply.payload);
        }

        let follow_up_bytes = serverbound_packets.iter().fold(0usize, |total, packet| {
            total.saturating_add(packet.payload.len())
        });
        if (!serverbound_packets.is_empty() && !translated)
            || serverbound_packets.len() > MAX_TRANSLATED_FOLLOW_UP_PACKETS
            || follow_up_bytes > MAX_PENDING_BYTES
            || serverbound_packets.iter().any(|packet| {
                packet.packet_id < 0 || packet.payload.len() > MAX_PACKET_SIZE as usize
            })
        {
            warn!("Invalid native serverbound protocol follow-up output; closing connection");
            self.close();
            return Ok(());
        }
        for follow_up in serverbound_packets {
            let packet = RawPacket {
                id: follow_up.packet_id,
                payload: follow_up.payload,
            };
            // Follow-ups already use Pumpkin's native 26.3 packet layout. Run
            // them through the same play dispatcher without firing client
            // packet hooks or translating them a second time.
            self.handle_play_packet_inner(player, server, &packet, true)?;
        }

        if cancelled {
            return Ok(());
        }

        let version = if translated {
            CURRENT_MC_VERSION
        } else {
            client_version
        };
        let mut payload = &packet_payload[..];
        match packet_id {
            id if id == SConfirmTeleport::to_id(version) => {
                self.handle_confirm_teleport(
                    player,
                    &SConfirmTeleport::read(&mut payload, &version)?,
                );
            }
            id if id == SChangeGameMode::to_id(version) => {
                self.handle_change_game_mode(
                    player,
                    &SChangeGameMode::read(&mut payload, &version)?,
                );
            }
            id if id == SChatAck::to_id(version) => {
                let packet = SChatAck::read(&mut payload, &version)?;
                self.handle_chat_ack(player, &packet);
            }
            id if id == SChatCommand::to_id(version) => {
                let packet = SChatCommand::read(&mut payload, &version)?;
                let cmd = packet.command.to_string();
                let client_platform = player.client.clone();
                let player_c = player.clone();
                let server_c = server.clone();
                server.spawn_task(async move {
                    if let ClientPlatform::Java(client) = client_platform.as_ref() {
                        let packet = SChatCommand { command: &cmd };
                        client
                            .handle_chat_command(&player_c, &server_c, &packet)
                            .await;
                    }
                });
            }
            id if id == SChatCommandSigned::to_id(version) => {
                let mut signed_payload = payload;
                let cmd =
                    if let Ok(signed) = SChatCommandSigned::read(&mut signed_payload, &version) {
                        signed.command.to_string()
                    } else {
                        SChatCommand::read(&mut payload, &version)?
                            .command
                            .to_string()
                    };
                let client_platform = player.client.clone();
                let player_c = player.clone();
                let server_c = server.clone();
                server.spawn_task(async move {
                    if let ClientPlatform::Java(client) = client_platform.as_ref() {
                        let packet = SChatCommand { command: &cmd };
                        client
                            .handle_chat_command(&player_c, &server_c, &packet)
                            .await;
                    }
                });
            }
            id if id == SChatMessage::to_id(version) => {
                let packet = SChatMessage::read(&mut payload, &version)?;
                let msg = packet.message.to_string();
                let signature = packet.signature.map(<[u8]>::to_vec);
                let ack = packet.acknowledged.to_vec();
                let ts = packet.timestamp;
                let salt = packet.salt;
                let count = packet.message_count;
                let checksum = packet.checksum;
                let client_platform = player.client.clone();
                let player_c = player.clone();
                let server_c = server.clone();
                server.spawn_task(async move {
                    if let ClientPlatform::Java(client) = client_platform.as_ref() {
                        let packet = SChatMessage {
                            message: &msg,
                            timestamp: ts,
                            salt,
                            signature: signature.as_deref(),
                            message_count: count,
                            acknowledged: &ack,
                            checksum,
                        };
                        client
                            .handle_chat_message(&server_c, &player_c, packet)
                            .await;
                    }
                });
            }
            id if id == SClientInformationPlay::to_id(version) => {
                self.handle_client_information(
                    server,
                    player,
                    &SClientInformationPlay::read(&mut payload, &version)?,
                );
            }
            id if id == SClientCommand::to_id(version) => {
                self.handle_client_status(player, &SClientCommand::read(&mut payload, &version)?);
            }
            id if id == SPlayerInput::to_id(version) => {
                self.handle_player_input(
                    player,
                    &SPlayerInput::read(&mut payload, &version)?,
                    server,
                );
            }
            id if id == SMoveVehicle::to_id(version) => {
                self.handle_move_vehicle(player, &SMoveVehicle::read(&mut payload, &version)?);
            }
            id if id == SPaddleBoat::to_id(version) => {
                self.handle_paddle_boat(player, &SPaddleBoat::read(&mut payload, &version)?);
            }
            id if id == SInteract::to_id(version) => {
                self.handle_interact(player, &SInteract::read(&mut payload, &version)?, server);
            }
            id if id == SBundleItemSelected::to_id(version) => {
                self.handle_bundle_item_selected(
                    player,
                    &SBundleItemSelected::read(&mut payload, &version)?,
                );
            }
            id if id == SAttack::to_id(version) => {
                self.handle_attack(player, &SAttack::read(&mut payload, &version)?, server);
            }
            id if id == STeleportToEntity::to_id(version) => {
                self.handle_teleport_to_entity(
                    player,
                    &STeleportToEntity::read(&mut payload, &version)?,
                    server,
                );
            }
            id if id == pumpkin_protocol::java::server::play::SKeepAlive::to_id(version) => {
                self.handle_keep_alive(
                    player,
                    &pumpkin_protocol::java::server::play::SKeepAlive::read(
                        &mut payload,
                        &version,
                    )?,
                );
            }
            id if id == SClientTickEnd::to_id(version) => {
                self.handle_client_tick_end(player);
            }
            id if id == STestInstanceBlockAction::to_id(version) => {
                self.handle_test_instance_block_action(
                    player,
                    &STestInstanceBlockAction::read(&mut payload, &version)?,
                );
            }
            id if id == SSetTestBlock::to_id(version) => {
                self.handle_set_test_block(player, &SSetTestBlock::read(&mut payload, &version)?);
            }
            id if id == SDebugSubscriptionRequest::to_id(version) => {
                self.handle_debug_subscription_request(
                    player,
                    &SDebugSubscriptionRequest::read(&mut payload, &version)?,
                );
            }
            id if id == SDebugSampleSubscription::to_id(version) => {
                self.handle_debug_sample_subscription(
                    player,
                    &SDebugSampleSubscription::read(&mut payload, &version)?,
                );
            }
            id if id == SPlayerPosition::to_id(version) => {
                self.handle_position(
                    player,
                    server,
                    &SPlayerPosition::read(&mut payload, &version)?,
                );
            }
            id if id == SPlayerPositionRotation::to_id(version) => {
                self.handle_position_rotation(
                    player,
                    server,
                    &SPlayerPositionRotation::read(&mut payload, &version)?,
                );
            }
            id if id == SPlayerRotation::to_id(version) => {
                self.handle_rotation(player, &SPlayerRotation::read(&mut payload, &version)?);
            }
            id if id == SSetPlayerGround::to_id(version) => {
                self.handle_player_ground(player, &SSetPlayerGround::read(&mut payload, &version)?);
            }
            id if id == SPickItemFromBlock::to_id(version) => {
                self.handle_pick_item_from_block(
                    player,
                    &SPickItemFromBlock::read(&mut payload, &version)?,
                );
            }
            id if id
                == pumpkin_protocol::java::server::play::SPickItemFromEntity::to_id(version) =>
            {
                self.handle_pick_item_from_entity(
                    player,
                    &pumpkin_protocol::java::server::play::SPickItemFromEntity::read(
                        &mut payload,
                        &version,
                    )?,
                );
            }
            id if id == SPlayerAbilities::to_id(version) => {
                self.handle_player_abilities(
                    player,
                    &SPlayerAbilities::read(&mut payload, &version)?,
                    server,
                );
            }
            id if id == SPlayerAction::to_id(version) => {
                self.handle_player_action(
                    player,
                    &SPlayerAction::read(&mut payload, &version)?,
                    server,
                );
            }
            id if id == SSetCommandBlock::to_id(version) => {
                self.handle_set_command_block(
                    player,
                    &SSetCommandBlock::read(&mut payload, &version)?,
                );
            }
            id if id == SSetJigsawBlock::to_id(version) => {
                self.handle_set_jigsaw_block(
                    player,
                    &SSetJigsawBlock::read(&mut payload, &version)?,
                );
            }
            id if id == SJigsawGenerate::to_id(version) => {
                self.handle_jigsaw_generate(
                    player,
                    &SJigsawGenerate::read(&mut payload, &version)?,
                );
            }
            id if id == SPlayerCommand::to_id(version) => {
                self.handle_player_command(
                    player,
                    &SPlayerCommand::read(&mut payload, &version)?,
                    server,
                );
            }
            id if id == SPlayerLoaded::to_id(version) => {
                Self::handle_player_loaded(player);
            }
            id if id == SPlayPingRequest::to_id(version) => {
                self.handle_play_ping_request(&SPlayPingRequest::read(&mut payload, &version)?);
            }
            id if id == SClickSlot::to_id(version) => {
                player.on_slot_click(SClickSlot::read(&mut payload, &version)?, server);
            }
            id if id == SContainerButtonClick::to_id(version) => {
                player.on_container_button_click(&SContainerButtonClick::read(
                    &mut payload,
                    &version,
                )?);
            }
            id if id == SSetHeldItem::to_id(version) => {
                self.handle_set_held_item(
                    server,
                    player,
                    &SSetHeldItem::read(&mut payload, &version)?,
                );
            }
            id if id == SSetCreativeSlot::to_id(version) => {
                self.handle_set_creative_slot(
                    player,
                    SSetCreativeSlot::read(&mut payload, &version)?,
                )?;
            }
            id if id == SSwingArm::to_id(version) => {
                self.handle_swing_arm(server, player, &SSwingArm::read(&mut payload, &version)?);
            }
            id if id == SUpdateSign::to_id(version) => {
                self.handle_sign_update(player, &SUpdateSign::read(&mut payload, &version)?);
            }
            id if id == SEditBook::to_id(version) => {
                self.handle_edit_book(player, &SEditBook::read(&mut payload, &version)?);
            }
            id if id == SUseItemOn::to_id(version) => {
                self.handle_use_item_on(
                    player,
                    &SUseItemOn::read(&mut payload, &version)?,
                    server,
                )?;
            }
            id if id == SUseItem::to_id(version) => {
                self.handle_use_item(player, &SUseItem::read(&mut payload, &version)?, server);
            }
            id if id == SCommandSuggestion::to_id(version) => {
                self.handle_command_suggestion(
                    player,
                    &SCommandSuggestion::read(&mut payload, &version)?,
                    server,
                );
            }
            id if id == SPCookieResponse::to_id(version) => {
                self.handle_cookie_response(&SPCookieResponse::read(&mut payload, &version)?);
            }
            id if id == SCloseContainer::to_id(version) => {
                let _ = SCloseContainer::read(&mut payload, &version)?;
                self.handle_close_container(player);
            }
            id if id == SChunkBatch::to_id(version) => {
                self.handle_chunk_batch(player, &SChunkBatch::read(&mut payload, &version)?);
            }
            id if id == SPlayerSession::to_id(version) => {
                let session = SPlayerSession::read(&mut payload, &version)?;
                let client_platform = player.client.clone();
                let player_c = player.clone();
                let server_c = server.clone();
                server.spawn_task(async move {
                    if let ClientPlatform::Java(client) = client_platform.as_ref() {
                        client
                            .handle_chat_session_update(&player_c, &server_c, session)
                            .await;
                    }
                });
            }
            id if id == SCustomPayload::to_id(version) => {
                let payload = SCustomPayload::read(&mut payload, &version)?;
                let channel_str = payload.channel.to_string();
                let mut event = PlayerCustomPayloadEvent::new(
                    player.clone(),
                    channel_str.clone(),
                    Bytes::copy_from_slice(payload.data),
                );
                server.plugin_manager.fire_blocking(server, &mut event);

                if channel_str == "minecraft:register" {
                    if let Ok(channels_data) = std::str::from_utf8(payload.data) {
                        for ch in channels_data.split('\0') {
                            if !ch.is_empty() {
                                let mut reg_event = crate::plugin::api::events::player::player_register_channel::PlayerRegisterChannelEvent::new(
                                    player.clone(),
                                    ch.to_string(),
                                );
                                server.plugin_manager.fire_blocking(server, &mut reg_event);
                                let mut ch_event = crate::plugin::api::events::player::player_channel::PlayerChannelEvent {
                                    player: player.clone(),
                                    channel: ch.to_string(),
                                    cancelled: false,
                                };
                                server.plugin_manager.fire_blocking(server, &mut ch_event);
                            }
                        }
                    }
                } else if channel_str == "minecraft:unregister"
                    && let Ok(channels_data) = std::str::from_utf8(payload.data)
                {
                    for ch in channels_data.split('\0') {
                        if !ch.is_empty() {
                            let mut unreg_event = crate::plugin::api::events::player::player_unregister_channel::PlayerUnregisterChannelEvent::new(
                                player.clone(),
                                ch.to_string(),
                            );
                            server
                                .plugin_manager
                                .fire_blocking(server, &mut unreg_event);
                        }
                    }
                }
            }
            id if id == SRecipeBookChangeSettings::to_id(version) => {
                self.handle_recipe_book_change_settings(
                    server,
                    player,
                    &SRecipeBookChangeSettings::read(&mut payload, &version)?,
                );
            }
            id if id == SRecipeBookSeenRecipe::to_id(version) => {
                self.handle_recipe_book_seen_recipe(
                    server,
                    player,
                    &SRecipeBookSeenRecipe::read(&mut payload, &version)?,
                );
            }
            id if id == SRenameItem::to_id(version) => {
                player.on_rename_item(&SRenameItem::read(&mut payload, &version)?);
            }
            id if id == SPlaceRecipe::to_id(version) => {
                let packet = SPlaceRecipe::read(&mut payload, &version)?;
                self.handle_place_recipe(server, player, &packet);
            }
            id if id
                == pumpkin_protocol::java::server::play::SCustomClickAction::to_id(version) =>
            {
                let packet = pumpkin_protocol::java::server::play::SCustomClickAction::read(
                    &mut payload,
                    &version,
                )?;
                let mut event = crate::plugin::api::events::dialog::dialog_click_action::DialogClickActionEvent::new(
                    player.clone(),
                    packet.action_id.to_string(),
                    packet.payload.map(Bytes::copy_from_slice),
                );
                server.plugin_manager.fire_blocking(server, &mut event);
            }
            id if id == SSelectTrade::to_id(version) => {
                self.handle_select_trade(player, &SSelectTrade::read(&mut payload, &version)?);
            }
            id if id == SSeenAdvancement::to_id(version) => {
                self.handle_seen_advancement(
                    player,
                    &SSeenAdvancement::read(&mut payload, &version)?,
                );
            }
            id if id == SPlayResourcePack::to_id(version) => {
                self.handle_play_resource_pack_response(
                    server,
                    player,
                    &SPlayResourcePack::read(&mut payload, &version)?,
                );
            }
            id if id == SPlayPong::to_id(version) => {
                self.handle_play_pong(player, &SPlayPong::read(&mut payload, &version)?);
            }
            id if id == SLockDifficulty::to_id(version) => {
                self.handle_lock_difficulty(
                    server,
                    player,
                    &SLockDifficulty::read(&mut payload, &version)?,
                );
            }
            id if id == SChangeDifficulty::to_id(version) => {
                self.handle_change_difficulty(
                    server,
                    player,
                    &SChangeDifficulty::read(&mut payload, &version)?,
                );
            }
            id if id == SSetBeacon::to_id(version) => {
                self.handle_set_beacon(player, &SSetBeacon::read(&mut payload, &version)?);
            }
            id if id == SContainerSlotStateChanged::to_id(version) => {
                self.handle_container_slot_state_changed(
                    player,
                    &SContainerSlotStateChanged::read(&mut payload, &version)?,
                );
            }
            id if id == SSpectateEntity::to_id(version) => {
                self.handle_spectate_entity(
                    player,
                    server,
                    &SSpectateEntity::read(&mut payload, &version)?,
                );
            }
            id if id == SSetCommandMinecart::to_id(version) => {
                self.handle_set_command_minecart(
                    player,
                    &SSetCommandMinecart::read(&mut payload, &version)?,
                );
            }
            id if id == SSetStructureBlock::to_id(version) => {
                self.handle_set_structure_block(
                    player,
                    &SSetStructureBlock::read(&mut payload, &version)?,
                );
            }
            id if id == SSetGameRule::to_id(version) => {
                self.handle_set_game_rule(player, &SSetGameRule::read(&mut payload, &version)?);
            }
            id if id == SBlockEntityTagQuery::to_id(version) => {
                self.handle_block_entity_tag_query(
                    player,
                    &SBlockEntityTagQuery::read(&mut payload, &version)?,
                );
            }
            id if id == SEntityTagQuery::to_id(version) => {
                self.handle_entity_tag_query(
                    player,
                    &SEntityTagQuery::read(&mut payload, &version)?,
                );
            }
            id if id == SConfigurationAcknowledged::to_id(version) => {
                self.handle_configuration_acknowledged(player);
            }
            _ => {
                warn!("Failed to handle player packet id {packet_id}");
            }
        }
        Ok(())
    }
}
