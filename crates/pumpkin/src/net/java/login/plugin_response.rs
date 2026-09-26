#[allow(clippy::wildcard_imports)]
use super::*;

fn consume_matching_query_id(expected: &mut Option<i32>, received: i32) -> bool {
    if *expected == Some(received) {
        *expected = None;
        true
    } else {
        false
    }
}

impl PendingConnection {
    pub async fn handle_plugin_response(
        &mut self,
        server: &Arc<Server>,
        plugin_response: SLoginPluginResponse,
    ) -> Option<PacketHandlerResult> {
        debug!("Handling plugin");
        let proxy_config = &server.advanced_config.networking.proxy;
        if proxy_config.vine.enabled {
            if !consume_matching_query_id(
                &mut self.vine_message_id,
                plugin_response.message_id.0,
            ) {
                return None;
            }
            let expected_challenge = self.vine_challenge.take();
            match vine::receive_vine_plugin_response(
                self.address.port(),
                &proxy_config.vine,
                plugin_response,
                expected_challenge,
            ) {
                Ok((profile, new_address)) => {
                    self.gameprofile = Some(profile.clone());
                    self.address = new_address;
                    self.finish_login(server, &profile).await
                }
                Err(error) => {
                    self.kick(TextComponent::text(error.to_string())).await;
                    Some(PacketHandlerResult::Stop)
                }
            }
        } else if proxy_config.velocity.enabled {
            if !consume_matching_query_id(
                &mut self.velocity_message_id,
                plugin_response.message_id.0,
            ) {
                return None;
            }
            match velocity::receive_velocity_plugin_response(
                self.address.port(),
                &proxy_config.velocity,
                plugin_response,
            ) {
                Ok((profile, new_address)) => {
                    self.gameprofile = Some(profile.clone());
                    self.address = new_address;
                    self.finish_login(server, &profile).await
                }
                Err(error) => {
                    self.kick(TextComponent::text(error.to_string())).await;
                    Some(PacketHandlerResult::Stop)
                }
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::consume_matching_query_id;

    #[test]
    fn unrelated_plugin_replies_do_not_consume_forwarding_query_ids() {
        let mut expected = Some(42);
        assert!(!consume_matching_query_id(&mut expected, 7));
        assert_eq!(expected, Some(42));
        assert!(consume_matching_query_id(&mut expected, 42));
        assert_eq!(expected, None);
    }
}
