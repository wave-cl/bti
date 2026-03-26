use std::net::SocketAddr;
use std::sync::Arc;

use crate::ktable::KTable;
use crate::msg::{self, EncodeReturn, NodeInfo, RecvMsg};

pub trait Responder: Send + Sync {
    fn respond(&self, msg: &RecvMsg) -> Result<EncodeReturn, Box<dyn std::error::Error + Send + Sync>>;
}

/// Default DHT responder that handles standard KRPC queries.
pub struct DhtResponder {
    ktable: Arc<KTable>,
    node_id: [u8; 20],
}

impl DhtResponder {
    pub fn new(node_id: [u8; 20], ktable: Arc<KTable>) -> Self {
        Self { ktable, node_id }
    }

    fn handle_ping(&self) -> EncodeReturn {
        EncodeReturn {
            id: self.node_id,
            ..Default::default()
        }
    }

    fn handle_find_node(&self, target: &[u8; 20]) -> EncodeReturn {
        let closest = self.ktable.get_closest_nodes(target);
        EncodeReturn {
            id: self.node_id,
            nodes: closest
                .into_iter()
                .map(|n| NodeInfo {
                    id: n.id,
                    addr: n.addr,
                })
                .collect(),
            ..Default::default()
        }
    }

    fn handle_get_peers(&self, info_hash: &[u8; 20]) -> EncodeReturn {
        let result = self.ktable.get_hash_or_closest_nodes(info_hash);
        let token = Self::make_token(info_hash);

        if result.found {
            EncodeReturn {
                id: self.node_id,
                token: Some(token),
                values: result.peers,
                ..Default::default()
            }
        } else {
            EncodeReturn {
                id: self.node_id,
                token: Some(token),
                nodes: result
                    .closest_nodes
                    .into_iter()
                    .map(|n| NodeInfo {
                        id: n.id,
                        addr: n.addr,
                    })
                    .collect(),
                ..Default::default()
            }
        }
    }

    fn handle_announce_peer(
        &self,
        info_hash: &[u8; 20],
        peer: SocketAddr,
    ) -> EncodeReturn {
        self.ktable.put_hash(*info_hash, vec![peer]);
        EncodeReturn {
            id: self.node_id,
            ..Default::default()
        }
    }

    fn handle_sample_infohashes(&self) -> EncodeReturn {
        let result = self.ktable.sample_hashes_and_nodes();
        EncodeReturn {
            id: self.node_id,
            nodes: result
                .nodes
                .into_iter()
                .map(|n| NodeInfo {
                    id: n.id,
                    addr: n.addr,
                })
                .collect(),
            samples: result.hashes,
            num: Some(result.total_hashes as i64),
            interval: Some(60),
            ..Default::default()
        }
    }

    fn make_token(info_hash: &[u8; 20]) -> Vec<u8> {
        // Simple token: first 4 bytes of infohash
        info_hash[..4].to_vec()
    }
}

impl Responder for DhtResponder {
    fn respond(
        &self,
        recv_msg: &RecvMsg,
    ) -> Result<EncodeReturn, Box<dyn std::error::Error + Send + Sync>> {
        let method = recv_msg
            .msg
            .q
            .as_deref()
            .ok_or("missing query method")?;

        let args = recv_msg
            .msg
            .a
            .as_ref()
            .ok_or("missing query arguments")?;

        match method {
            msg::Q_PING => Ok(self.handle_ping()),
            msg::Q_FIND_NODE => {
                let target = args.target.as_ref().ok_or("missing target")?;
                Ok(self.handle_find_node(target))
            }
            msg::Q_GET_PEERS => {
                let ih = args.info_hash.as_ref().ok_or("missing info_hash")?;
                Ok(self.handle_get_peers(ih))
            }
            msg::Q_ANNOUNCE_PEER => {
                let ih = args.info_hash.as_ref().ok_or("missing info_hash")?;
                let port = recv_msg.announce_port();
                let peer = SocketAddr::new(recv_msg.from.ip(), port);
                Ok(self.handle_announce_peer(ih, peer))
            }
            msg::Q_SAMPLE_INFOHASHES => Ok(self.handle_sample_infohashes()),
            _ => Err(format!("unknown method: {}", method).into()),
        }
    }
}
