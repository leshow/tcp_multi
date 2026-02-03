pub const BUF_SIZE: usize = 4096;

use std::{
    collections::HashMap,
    mem,
    net::SocketAddr,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use atomic_time::AtomicInstant;
use socket2::TcpKeepalive;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
// use tokio_util::{bytes::Bytes, task::TaskTracker};
use tracing::{debug, warn};

use crate::msg::SerialMsg;

pub mod msg;
pub mod pool;

#[derive(Clone, Copy, Debug)]
pub struct TcpConnectionConfig {
    pub chan_size: usize,
    pub ka_idle: Option<u64>,
    pub ka_interval: Option<u64>,
    pub max_in_flight: Option<usize>,
}

#[derive(Debug)]
pub struct DnsQuery<T> {
    pub to_send: T,
    pub reply: oneshot::Sender<T>,
}

#[derive(Debug)]
pub struct PendingSend<T> {
    pub to_send: T,
    pub next_id: u16,
    pub reply: oneshot::Sender<T>,
}

impl<T> PendingSend<T> {
    pub fn new(to_send: T, next_id: u16, reply: oneshot::Sender<T>) -> Self {
        Self {
            to_send,
            reply,
            next_id,
        }
    }
}

const MAX_ID: u16 = u16::MAX - 1;
const FRESH_THRESHOLD: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct PendingResponse<T> {
    pub original_id: u16,
    pub reply: oneshot::Sender<T>,
    pub sent_at: Instant,
}

pub struct TcpConnection {
    addr: SocketAddr,
    pending: ResponseMap<PendingResponse<SerialMsg>>,
    queued_tx: mpsc::Sender<PendingSend<SerialMsg>>,
    // @TODO remove individual Arcs and use Arc<TcpInner>?
    next_id: Arc<AtomicU16>,
    is_closing: Arc<AtomicBool>,
    // created_at: Instant,
    last_read: Arc<AtomicInstant>,
    // used to check SO_ERROR on socket
    read_fd: Option<RawFd>,
    max_in_flight: Option<usize>,
    // read/write tasks, dropping JoinSet will abort tasks
    tasks: JoinSet<Result<()>>,
}

type ResponseMap<T> = Arc<Mutex<HashMap<u16, T>>>;

impl TcpConnection {
    pub async fn new(addr: SocketAddr, config: TcpConnectionConfig) -> Result<Arc<Self>> {
        let (read, send) = tcpstream_connect(addr, config.ka_idle, config.ka_interval)
            .await?
            .into_split();
        let read_fd = read.as_ref().as_raw_fd();
        Self::from_split(read, send, addr, config, Some(read_fd))
    }

    // copied from new for testing
    // New generic function to handle both real streams and test streams
    fn from_split<R, W>(
        read: R,
        send: W,
        addr: SocketAddr,
        config: TcpConnectionConfig,
        read_fd: Option<RawFd>,
    ) -> Result<Arc<Self>>
    where
        R: AsyncReadExt + Unpin + Send + 'static,
        W: AsyncWriteExt + Unpin + Send + 'static,
    {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (queued_tx, queued_rx) = mpsc::channel(config.chan_size);

        let (is_closing, mut conn) =
            new_conn(read_fd, addr, config.max_in_flight, &pending, queued_tx);
        conn.tasks.spawn(read_half(
            read,
            addr,
            is_closing,
            conn.last_read.clone(),
            pending.clone(),
        ));
        conn.tasks.spawn(send_half(send, queued_rx, pending));

        Ok(Arc::new(conn))
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub async fn send(&self, query: DnsQuery<SerialMsg>) -> Result<()> {
        // should this be done in the pool and not on send?
        if !self.can_reuse() || self.queued_tx.is_closed() {
            self.set_closing();
            return Err(anyhow::Error::msg("connection unhealthy"));
        }
        if query.to_send.bytes().len() < 12 {
            // notify err? msg too small
            return Err(anyhow::Error::msg("message too small"));
        }
        let next_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if next_id >= MAX_ID {
            self.set_closing();
        }
        let DnsQuery { to_send, reply } = query;
        // note: there is a small race here where we could have reached the max_in_flight
        // but still send because the send_half has not added to the pending map yet
        // but it's inconsequential, the next send will error

        if let Err(err) = self
            .queued_tx
            .send(PendingSend {
                to_send,
                next_id,
                reply,
            })
            .await
        {
            warn!(%err, "TCP pending queue dropped, setting connection to closing");
            self.set_closing();
        }
        Ok(())
    }

    fn set_closing(&self) {
        self.is_closing.swap(true, Ordering::Relaxed);
    }
    fn is_closing(&self) -> bool {
        self.is_closing.load(Ordering::Relaxed)
    }
    fn max_in_flight(&self) -> bool {
        if let Some(max) = self.max_in_flight {
            let pending = { self.pending.lock().unwrap().len() };
            if pending >= max {
                return true;
            }
        }
        false
    }
    pub fn will_be_reusable(&self) -> bool {
        if self.is_closing() {
            return false;
        }
        if self.reached_max_id() {
            return false;
        }
        true
    }
    // same as will_be_reusable but checks max_in_flight
    pub fn can_reuse(&self) -> bool {
        if !self.will_be_reusable() {
            return false;
        }
        if self.max_in_flight() {
            return false;
        }

        true
    }
    /// Check if connection is usable - matches dnsdist's isConnectionUsable
    pub fn is_usable(&self, now: Instant) -> bool {
        if !self.can_reuse() {
            return false;
        }

        // skip socket check for recently-used
        let last_activity = self.last_read();
        if now.duration_since(last_activity) < FRESH_THRESHOLD {
            // used recently
            return true;
        }

        self.is_healthy()
    }

    fn reached_max_id(&self) -> bool {
        self.next_id.load(Ordering::Relaxed) >= MAX_ID
    }
    pub fn is_healthy(&self) -> bool {
        if let Some(read_fd) = self.read_fd {
            return match get_soerror(&read_fd) {
                Ok(_) => true,
                Err(err) => {
                    warn!(%err, "is_healthy returned false-- connection closed by remote");
                    false
                }
            };
        }
        true
    }
    pub fn is_idle(&self) -> bool {
        self.pending.lock().unwrap().is_empty()
    }
    pub fn last_read(&self) -> Instant {
        self.last_read.load(Ordering::Relaxed)
    }
}

// #[derive(Debug)]
// pub struct ConnectionStats {
//     pub addr: SocketAddr,
//     pub queries_sent: u64,
//     pub pending_responses: usize,
//     pub current_query_id: u16,
//     pub age: Duration,
//     pub last_activity: Duration,
// }

fn new_conn(
    read_fd: Option<RawFd>,
    addr: SocketAddr,
    max_in_flight: Option<usize>,
    pending: &Arc<Mutex<HashMap<u16, PendingResponse<SerialMsg>>>>,
    queued_tx: mpsc::Sender<PendingSend<SerialMsg>>,
) -> (Arc<AtomicBool>, TcpConnection) {
    let is_closing = Arc::new(AtomicBool::new(false));
    let now = Instant::now();
    let this = TcpConnection {
        read_fd,
        addr,
        pending: pending.clone(),
        tasks: JoinSet::new(),
        queued_tx,
        next_id: Arc::new(AtomicU16::new(0)),
        is_closing: is_closing.clone(),
        max_in_flight,
        // created_at: now,
        last_read: Arc::new(AtomicInstant::new(now)),
    };
    (is_closing, this)
}

async fn read_half<R: AsyncReadExt + Unpin>(
    mut recv: R,
    addr: SocketAddr,
    is_closing: Arc<AtomicBool>,
    last_activity: Arc<AtomicInstant>,
    pending: ResponseMap<PendingResponse<SerialMsg>>,
) -> Result<()> {
    loop {
        match SerialMsg::read(&mut recv, addr).await {
            Ok(mut msg) => {
                // Extract DNS ID from response header (first 2 bytes)
                if msg.bytes().len() < 12 {
                    // notify_io_error
                    continue;
                }
                last_activity.store(Instant::now(), Ordering::Relaxed);
                let resp_id = msg.msg_id();
                let (r, is_empty) = {
                    let mut lock = pending.lock().unwrap();

                    let entry = lock.remove(&resp_id);
                    let is_empty = lock.is_empty();
                    (entry, is_empty)
                };
                if let Some(resp) = r {
                    let latency = resp.sent_at.elapsed();
                    // restore original id
                    msg.replace_id(u16::to_be_bytes(resp.original_id));

                    if let Err(err) = resp.reply.send(msg) {
                        warn!(
                            id = err.msg_id(),
                            ?latency,
                            "TCP: sending over oneshot failed (likely reason: got response back for an already dropped message)"
                        )
                    }
                }
                // if there's nothing to read and we're closing
                if is_empty && is_closing.load(Ordering::Relaxed) {
                    break;
                }
            }
            Err(err) => {
                warn!(%err, "tcp read half error");
                // notify_io_error
                is_closing.swap(true, Ordering::Relaxed);
                break;
            }
        };
    }
    // Notify all pending requests of the error
    let mut pending = pending.lock().unwrap();
    for (_id, resp) in pending.drain() {
        // Just drop the sender, which will signal an error to the receiver
        drop(resp.reply);
    }
    Ok(())
}

async fn send_half<W: AsyncWriteExt + Unpin>(
    mut conn: W,
    mut queued_msgs: mpsc::Receiver<PendingSend<SerialMsg>>,
    pending: ResponseMap<PendingResponse<SerialMsg>>,
) -> Result<()> {
    while let Some(PendingSend {
        mut to_send,
        reply,
        next_id,
    }) = queued_msgs.recv().await
    {
        // swap id
        let original_id = to_send.msg_id();
        to_send.replace_id(u16::to_be_bytes(next_id));

        match to_send.write(&mut conn).await {
            Ok(_) => {
                {
                    let mut lock = pending.lock().unwrap();
                    // insert next_id and for recv
                    lock.insert(
                        next_id,
                        PendingResponse {
                            original_id,
                            reply,
                            sent_at: Instant::now(),
                        },
                    );
                    drop(lock);
                }
                if let Err(err) = conn.flush().await {
                    warn!(%err, "TCP flush failed");
                    // notify_io_error?
                    break;
                }
            }
            Err(err) => {
                // notify_io_error
                warn!(%err, "send half error tcp");
                break;
            }
        }
    }
    debug!("TCP send half exited");
    // will drop queued_msgs and Sender will fail
    Ok(())
}

async fn tcpstream_connect(
    addr: SocketAddr,
    ka_idle: Option<u64>,
    ka_interval: Option<u64>,
) -> Result<TcpStream> {
    let tfo_on = 1;
    let soc = socket2::Socket::new(
        if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::STREAM,
        None,
    )?;
    // Enable TCP keepalive
    let mut keepalive = TcpKeepalive::new();
    let mut ka_enable = false;
    if let Some(idle) = ka_idle {
        ka_enable = true;
        keepalive = keepalive.with_time(Duration::from_secs(idle)); // Start probing after Xs idle
    }
    if let Some(interval) = ka_interval {
        ka_enable = true;
        keepalive = keepalive.with_interval(Duration::from_secs(interval)); // Probe every Xs
    }
    if ka_enable {
        soc.set_tcp_keepalive(&keepalive)?;
    }

    soc.set_nonblocking(true)?;
    soc.set_tcp_nodelay(true)?;

    // enable TFO
    setsockopt(&soc, libc::IPPROTO_TCP, libc::TCP_FASTOPEN_CONNECT, tfo_on)?;

    // build socket from raw fd
    let soc = unsafe { TcpSocket::from_raw_fd(soc.into_raw_fd()) };
    Ok(soc.connect(addr).await?)
}

fn setsockopt<Fd: AsRawFd>(
    socket: &Fd,
    level: libc::c_int,
    name: libc::c_int,
    value: libc::c_int,
) -> Result<(), std::io::Error> {
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            name,
            &value as *const _ as _,
            mem::size_of_val(&value) as _,
        )
    };

    if rc != -1 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn get_soerror<Fd: AsRawFd>(fd: &Fd) -> Result<(), std::io::Error> {
    let mut err: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;

    unsafe {
        // Correct way to call getsockopt for SO_ERROR
        let n = libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            &raw mut err as *mut _ as *mut _,
            &raw mut len,
        );
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{str::FromStr, time::Duration};

    use hickory_proto::{
        op::{Message, MessageType, OpCode, Query},
        rr::{Name, RecordType},
    };
    use tokio::{
        self,
        io::{self, DuplexStream},
    };

    // Helper to create a mock DNS message using hickory-proto
    fn new_msg(id: u16, name: &str) -> Message {
        let name = Name::from_str(name).unwrap();
        let query = Query::query(name, RecordType::A);
        let mut message = Message::new();
        message
            .set_id(id)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query)
            .add_query(query)
            .set_recursion_desired(true);
        message
    }

    fn dns_query(
        id: u16,
        name: &str,
        addr: SocketAddr,
    ) -> (DnsQuery<SerialMsg>, oneshot::Receiver<SerialMsg>) {
        let (reply, rx) = oneshot::channel();
        let query_msg = new_msg(id, name);
        let query = DnsQuery {
            to_send: SerialMsg::from_message(&query_msg, addr).unwrap(),
            reply,
        };
        (query, rx)
    }

    async fn test_conn(
        max_flight: Option<usize>,
    ) -> (Arc<TcpConnection>, tokio::io::DuplexStream, SocketAddr) {
        // use duplex for testing b/c it's all in memory
        const BUF_SIZE: usize = 4096;
        let (client, server) = io::duplex(BUF_SIZE);
        // split client for tcpconnection
        let (read, write) = io::split(client);
        let addr = "127.0.0.1:53".parse().unwrap();
        let config = TcpConnectionConfig {
            chan_size: BUF_SIZE,
            ka_idle: None,
            ka_interval: None,
            max_in_flight: max_flight,
        };
        let conn = TcpConnection::from_split(read, write, addr, config, None).unwrap();
        (conn, server, addr)
    }
    fn test_server(mut server: DuplexStream, addr: SocketAddr) -> tokio::task::JoinHandle<()> {
        // Test server task that mimics a DNS server
        tokio::spawn(async move {
            let mut id = 0;
            loop {
                let msg = SerialMsg::read(&mut server, addr).await.unwrap();
                // sender will be incrementing and sending to us
                assert_eq!(msg.msg_id(), id);
                // next send will have incremented id
                id += 1;

                // Create a response message, using the internal ID
                let mut resp = new_msg(msg.msg_id(), "example.com.");
                resp.set_message_type(MessageType::Response);
                let to_send = SerialMsg::from_message(&resp, addr).unwrap();

                // Send the response back
                to_send.write(&mut server).await.unwrap();
            }
        })
    }

    #[tokio::test]
    async fn test_send_and_receive_ok() {
        let (conn, server, addr) = test_conn(None).await;
        // start server
        let handle = test_server(server, addr);

        let original_id = 1234;
        let (query, rx) = dns_query(original_id, "example.com.", addr);

        conn.send(query).await.unwrap();

        let response = rx.await.unwrap();
        // Check that the original ID was restored by the connection manager
        assert_eq!(response.msg_id(), original_id);

        // send another
        let original_id = 1235;
        let (query, rx) = dns_query(original_id, "google.com.", addr);

        conn.send(query).await.unwrap();

        let response = rx.await.unwrap();
        // Check that the original ID was restored by the connection manager
        assert_eq!(response.msg_id(), original_id);

        handle.abort();
    }

    #[tokio::test]
    async fn test_max_in_flight() {
        let (conn, _server, addr) = test_conn(Some(1)).await;

        let (query, _rx) = dns_query(1, "google.com.", addr);

        // First send should be ok
        assert!(conn.can_reuse());
        conn.send(query).await.unwrap();

        // need to wait small amount of time for pending map to get added
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connection should now be unhealthy due to max_in_flight
        assert!(!conn.can_reuse());

        // Second send should fail
        let (query, _rx) = dns_query(2, "google.com.", addr);
        assert!(conn.send(query).await.is_err());
    }

    #[tokio::test]
    async fn test_connection_closes_on_read_error() {
        let (conn, server, _) = test_conn(None).await;
        assert!(conn.will_be_reusable());

        // drop the server end to cause a read error in the connection's read_half
        drop(server);

        // read_half needs a moment to detect error
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(!conn.will_be_reusable());
        assert!(conn.is_closing());
    }

    #[tokio::test]
    async fn test_connection_closes_send() {
        let (conn, server, addr) = test_conn(None).await;
        assert!(conn.will_be_reusable());

        // drop the server end to cause a read error in the connection's read_half
        drop(server);

        // read_half needs a moment to detect error
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (query, _rx) = dns_query(1, "google.com.", addr);
        let res = conn.send(query);

        // send errored
        assert!(res.await.is_err());
        assert!(!conn.will_be_reusable());
        assert!(conn.is_closing());
    }

    #[tokio::test]
    async fn test_connection_closes_on_send_error() {
        let (conn, server, addr) = test_conn(None).await;
        assert!(conn.will_be_reusable());

        // Drop the server end to cause a write error
        drop(server);

        let (query, rx) = dns_query(1, "query1.com.", addr);
        // this send might succeed in queueing, but the send_half task will fail to write.
        // then oneshots will be dropped
        conn.send(query).await.unwrap();

        // this will err b/c sender was dropped
        let result = rx.await;
        assert!(result.is_err());

        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(!conn.will_be_reusable());
        assert!(conn.is_closing());
    }
}
