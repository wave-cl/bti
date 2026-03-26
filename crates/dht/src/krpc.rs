use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::{oneshot, RwLock};
use tracing::{debug, trace, warn};

use crate::msg::{self, RecvMsg};
use crate::responder::Responder;

const MAX_UDP_SIZE: usize = 65507;
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(4);

pub struct Server {
    conn: Arc<UdpSocket>,
    queries: Arc<RwLock<HashMap<Vec<u8>, oneshot::Sender<RecvMsg>>>>,
    id_counter: AtomicU32,
    query_timeout: Duration,
}

impl Server {
    pub async fn start(
        port: u16,
        responder: Arc<dyn Responder>,
        on_node_discovered: Option<Arc<dyn Fn([u8; 20], SocketAddr) + Send + Sync>>,
    ) -> std::io::Result<Arc<Self>> {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        let conn = UdpSocket::bind(addr).await?;
        let conn = Arc::new(conn);

        let server = Arc::new(Self {
            conn: conn.clone(),
            queries: Arc::new(RwLock::new(HashMap::new())),
            id_counter: AtomicU32::new(0),
            query_timeout: DEFAULT_QUERY_TIMEOUT,
        });

        // Spawn read loop
        let srv = server.clone();
        let resp = responder;
        let on_disc = on_node_discovered;
        tokio::spawn(async move {
            srv.read_loop(resp, on_disc).await;
        });

        Ok(server)
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.conn.local_addr().unwrap()
    }

    async fn read_loop(
        &self,
        responder: Arc<dyn Responder>,
        on_node_discovered: Option<Arc<dyn Fn([u8; 20], SocketAddr) + Send + Sync>>,
    ) {
        let mut buf = vec![0u8; MAX_UDP_SIZE];
        loop {
            let (n, from) = match self.conn.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("udp recv error: {}", e);
                    continue;
                }
            };

            let data = &buf[..n];
            let parsed = match msg::parse_msg(data) {
                Some(m) => m,
                None => {
                    trace!("unparseable msg from {}", from);
                    continue;
                }
            };

            let recv_msg = RecvMsg {
                msg: parsed,
                from,
            };

            if recv_msg.msg.y == msg::Y_QUERY {
                let resp = responder.clone();
                let conn = self.conn.clone();
                let on_disc = on_node_discovered.clone();
                tokio::spawn(async move {
                    Self::handle_query(conn, resp, recv_msg, on_disc).await;
                });
            } else {
                // Response or error
                let tx_id = recv_msg.msg.t.clone();
                let mut queries = self.queries.write().await;
                if let Some(sender) = queries.remove(&tx_id) {
                    let _ = sender.send(recv_msg);
                }
            }
        }
    }

    async fn handle_query(
        conn: Arc<UdpSocket>,
        responder: Arc<dyn Responder>,
        recv_msg: RecvMsg,
        on_node_discovered: Option<Arc<dyn Fn([u8; 20], SocketAddr) + Send + Sync>>,
    ) {
        let tx_id = recv_msg.msg.t.clone();
        let from = recv_msg.from;

        match responder.respond(&recv_msg) {
            Ok(ret) => {
                let response = msg::encode_response(&tx_id, &ret);
                let _ = conn.send_to(&response, from).await;

                if let Some(ref cb) = on_node_discovered {
                    if let Some(ref a) = recv_msg.msg.a {
                        cb(a.id, from);
                    }
                }
            }
            Err(e) => {
                debug!("responder error for {} from {}: {}",
                    recv_msg.msg.q.as_deref().unwrap_or("?"), from, e);
                let response = msg::encode_error(&tx_id, 202, &e.to_string());
                let _ = conn.send_to(&response, from).await;
            }
        }
    }

    pub async fn query(
        &self,
        addr: SocketAddr,
        method: &str,
        args: msg::EncodeMsgArgs,
    ) -> Result<RecvMsg, QueryError> {
        let tx_id = self.next_tx_id();
        let encoded = msg::encode_query(&tx_id, method, &args);

        let (tx, rx) = oneshot::channel();
        {
            let mut queries = self.queries.write().await;
            queries.insert(tx_id.clone(), tx);
        }

        self.conn.send_to(&encoded, addr).await.map_err(|e| {
            QueryError::Io(e)
        })?;

        let result = tokio::time::timeout(self.query_timeout, rx).await;

        // Clean up pending query
        {
            let mut queries = self.queries.write().await;
            queries.remove(&tx_id);
        }

        match result {
            Ok(Ok(recv_msg)) => {
                if recv_msg.msg.y == msg::Y_ERROR {
                    if let Some(ref e) = recv_msg.msg.e {
                        return Err(QueryError::Remote(e.msg.clone()));
                    }
                    return Err(QueryError::Remote("unknown error".into()));
                }
                if recv_msg.msg.r.is_none() {
                    return Err(QueryError::NoResponse);
                }
                Ok(recv_msg)
            }
            Ok(Err(_)) => Err(QueryError::ChannelClosed),
            Err(_) => Err(QueryError::Timeout),
        }
    }

    fn next_tx_id(&self) -> Vec<u8> {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        (id as u16).to_be_bytes().to_vec()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("timeout")]
    Timeout,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("remote: {0}")]
    Remote(String),
    #[error("no response")]
    NoResponse,
    #[error("channel closed")]
    ChannelClosed,
}
