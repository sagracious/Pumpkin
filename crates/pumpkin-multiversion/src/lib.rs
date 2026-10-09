//! Native multiversion translator for older Java clients.
//!
//! Subscribes to the connection-scoped packet events and currently passes
//! every packet through unchanged. Translation tables land in later
//! increments; this stage proves the event path flows end to end: a debug
//! line records the first packet seen per client protocol version.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use pumpkin_core::plugin::api::events::server::packet::{
    ConnectionPacketReceivedEvent, ConnectionPacketSentEvent,
};
use pumpkin_core::plugin::{BoxFuture, EventHandler, EventPriority, PluginManager};
use pumpkin_core::server::Server;
use pumpkin_data::packet::CURRENT_MC_VERSION;
use pumpkin_util::version::JavaMinecraftVersion;

/// Translates packets between 26.3 and the protocol each client speaks.
#[derive(Debug, Default)]
pub struct Translator {
    seen_versions: Mutex<HashSet<i32>>,
}

impl Translator {
    /// Records a client protocol the first time it is seen.
    fn mark_seen(&self, version: JavaMinecraftVersion) -> bool {
        self.seen_versions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(version.protocol_version())
    }
}

/// Whether packets for this client version need translation.
#[must_use]
pub fn needs_translation(version: JavaMinecraftVersion) -> bool {
    version != CURRENT_MC_VERSION
}

/// Registers the translator for both packet directions. Handlers are
/// blocking so the mutable event form runs and rewritten id/payload
/// is what the connection continues with.
pub fn register_translator(plugin_manager: &Arc<PluginManager>) {
    let translator = Arc::new(Translator::default());
    plugin_manager.register::<ConnectionPacketReceivedEvent, Translator>(
        Arc::clone(&translator),
        EventPriority::Normal,
        true,
    );
    plugin_manager.register::<ConnectionPacketSentEvent, Translator>(
        translator,
        EventPriority::Normal,
        true,
    );
}

impl EventHandler<ConnectionPacketReceivedEvent> for Translator {
    fn handle_blocking<'a>(
        &'a self,
        _server: &'a Arc<Server>,
        event: &'a mut ConnectionPacketReceivedEvent,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if needs_translation(event.version) && self.mark_seen(event.version) {
                tracing::debug!(
                    "multiversion translator observing protocol {}",
                    event.version.protocol_version()
                );
            }
        })
    }
}

impl EventHandler<ConnectionPacketSentEvent> for Translator {
    fn handle_blocking<'a>(
        &'a self,
        _server: &'a Arc<Server>,
        event: &'a mut ConnectionPacketSentEvent,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if needs_translation(event.version) && self.mark_seen(event.version) {
                tracing::debug!(
                    "multiversion translator observing protocol {}",
                    event.version.protocol_version()
                );
            }
        })
    }
}
