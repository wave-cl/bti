use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;

use crate::krpc::{QueryError, Server};
use crate::msg::{self, EncodeMsgArgs};

pub struct DhtClient {
    node_id: [u8; 20],
    server: Arc<Server>,
}

#[derive(Debug)]
pub struct FindNodeResult {
    pub id: [u8; 20],
    pub nodes: Vec<NodeEntry>,
}

#[derive(Debug, Clone)]
pub struct NodeEntry {
    pub id: [u8; 20],
    pub addr: SocketAddrV4,
}

#[derive(Debug)]
pub struct GetPeersResult {
    pub id: [u8; 20],
    pub values: Vec<SocketAddr>,
    pub nodes: Vec<NodeEntry>,
}

#[derive(Debug)]
pub struct SampleInfoHashesResult {
    pub id: [u8; 20],
    pub samples: Vec<[u8; 20]>,
    pub nodes: Vec<NodeEntry>,
    pub num: i64,
    pub interval: i64,
}

impl DhtClient {
    pub fn new(node_id: [u8; 20], server: Arc<Server>) -> Self {
        Self { node_id, server }
    }

    pub async fn ping(&self, addr: SocketAddr) -> Result<[u8; 20], QueryError> {
        let resp = self
            .server
            .query(
                addr,
                msg::Q_PING,
                EncodeMsgArgs {
                    id: self.node_id,
                    info_hash: None,
                    target: None,
                    token: None,
                    port: None,
                    implied_port: false,
                },
            )
            .await?;
        Ok(resp.msg.r.unwrap().id)
    }

    pub async fn find_node(
        &self,
        addr: SocketAddr,
        target: [u8; 20],
    ) -> Result<FindNodeResult, QueryError> {
        let resp = self
            .server
            .query(
                addr,
                msg::Q_FIND_NODE,
                EncodeMsgArgs {
                    id: self.node_id,
                    info_hash: None,
                    target: Some(target),
                    token: None,
                    port: None,
                    implied_port: false,
                },
            )
            .await?;

        let r = resp.msg.r.unwrap();
        Ok(FindNodeResult {
            id: r.id,
            nodes: r
                .nodes
                .iter()
                .map(|n| NodeEntry {
                    id: n.id,
                    addr: n.addr,
                })
                .collect(),
        })
    }

    pub async fn get_peers(
        &self,
        addr: SocketAddr,
        info_hash: [u8; 20],
    ) -> Result<GetPeersResult, QueryError> {
        let resp = self
            .server
            .query(
                addr,
                msg::Q_GET_PEERS,
                EncodeMsgArgs {
                    id: self.node_id,
                    info_hash: Some(info_hash),
                    target: None,
                    token: None,
                    port: None,
                    implied_port: false,
                },
            )
            .await?;

        let r = resp.msg.r.unwrap();
        Ok(GetPeersResult {
            id: r.id,
            values: r.values,
            nodes: r
                .nodes
                .iter()
                .map(|n| NodeEntry {
                    id: n.id,
                    addr: n.addr,
                })
                .collect(),
        })
    }

    pub async fn sample_infohashes(
        &self,
        addr: SocketAddr,
        target: [u8; 20],
    ) -> Result<SampleInfoHashesResult, QueryError> {
        let resp = self
            .server
            .query(
                addr,
                msg::Q_SAMPLE_INFOHASHES,
                EncodeMsgArgs {
                    id: self.node_id,
                    info_hash: None,
                    target: Some(target),
                    token: None,
                    port: None,
                    implied_port: false,
                },
            )
            .await?;

        let r = resp.msg.r.unwrap();
        Ok(SampleInfoHashesResult {
            id: r.id,
            samples: r.samples,
            nodes: r
                .nodes
                .iter()
                .map(|n| NodeEntry {
                    id: n.id,
                    addr: n.addr,
                })
                .collect(),
            num: r.num.unwrap_or(0),
            interval: r.interval.unwrap_or(0),
        })
    }
}
