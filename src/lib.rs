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
    time::Instant,
};

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
// use tokio_util::{bytes::Bytes, task::TaskTracker};
use tracing::{debug, warn};

use crate::msg::SerialMsg;

mod msg;
pub mod pool;

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

#[derive(Debug)]
pub struct PendingResponse<T> {
    pub original_id: u16,
    pub reply: oneshot::Sender<T>,
    pub sent_at: Instant,
}

#[derive(Debug)]
pub struct TcpConnection {
    read_fd: RawFd,
    addr: SocketAddr,
    pending: ResponseMap<PendingResponse<SerialMsg>>,
    queued_tx: mpsc::UnboundedSender<PendingSend<SerialMsg>>,
    next_id: Arc<AtomicU16>,
    is_closing: Arc<AtomicBool>,
    // last_activity: Arc<Mutex<Instant>>,
    max_in_flight: Option<usize>,
    tasks: JoinSet<Result<()>>,
}

type ResponseMap<T> = Arc<Mutex<HashMap<u16, T>>>;

impl TcpConnection {
    pub async fn new(addr: SocketAddr, max_in_flight: Option<usize>) -> Result<Arc<Self>> {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (read, send) = tcpstream_connect(addr).await?.into_split();
        let read_fd = read.as_ref().as_raw_fd();
        let (queued_tx, queued_rx) = mpsc::unbounded_channel();
        let (is_closing, mut conn) = new_conn(read_fd, addr, max_in_flight, &pending, queued_tx);
        conn.tasks.spawn(read_half(
            read,
            addr,
            is_closing,
            // last_activity,
            pending.clone(),
        ));
        conn.tasks.spawn(send_half(send, queued_rx, pending));

        Ok(Arc::new(conn))
    }

    // copied from new for testing
    // New generic function to handle both real streams and test streams
    #[cfg(test)]
    fn from_split<R, W>(
        read: R,
        send: W,
        addr: SocketAddr,
        max_in_flight: Option<usize>,
    ) -> Result<Arc<Self>>
    where
        R: AsyncReadExt + Unpin + Send + 'static + AsRawFd,
        W: AsyncWriteExt + Unpin + Send + 'static,
    {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (queued_tx, queued_rx) = mpsc::unbounded_channel();
        let read_fd = read.as_raw_fd();

        let (is_closing, mut conn) = new_conn(read_fd, addr, max_in_flight, &pending, queued_tx);
        conn.tasks.spawn(read_half(
            read,
            addr,
            is_closing,
            // last_activity,
            pending.clone(),
        ));
        conn.tasks.spawn(send_half(send, queued_rx, pending));

        Ok(Arc::new(conn))
    }

    // @TODO can be async if switch to bounded chan
    pub fn send(&self, query: DnsQuery<SerialMsg>) -> Result<()> {
        // should this be done in the pool and not on send?
        if !self.will_be_reusable() || self.queued_tx.is_closed() {
            self.set_closing();
            return Err(anyhow::Error::msg("connection unhealthy"));
        }
        if query.to_send.bytes().len() < 12 {
            // notify err? msg too small
            return Err(anyhow::Error::msg("message too small"));
        }
        let next_id = self.next_id.fetch_add(1, Ordering::Release);
        if next_id >= MAX_ID {
            self.set_closing();
        }
        let DnsQuery { to_send, reply } = query;
        // note: there is a small race here where we could have reached the max_in_flight
        // but still send because the send_half has not added to the pending map yet

        if let Err(err) = self.queued_tx.send(PendingSend {
            to_send,
            next_id,
            reply,
        }) {
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
    pub fn will_be_reusable(&self) -> bool {
        if self.is_closing() {
            return false;
        }
        if self.reached_max_id() {
            return false;
        }
        if let Some(max) = self.max_in_flight {
            let pending = { self.pending.lock().unwrap().len() };
            if pending >= max {
                return false;
            }
        }
        self.is_healthy()
    }
    pub fn can_reuse(&self) -> bool {
        if self.is_closing() {
            return false;
        }
        if self.reached_max_id() {
            return false;
        }
        self.is_healthy()
    }

    fn reached_max_id(&self) -> bool {
        self.next_id.load(Ordering::Relaxed) >= MAX_ID
    }
    pub fn is_healthy(&self) -> bool {
        match getso_err(&self.read_fd) {
            Ok(_) => true,
            Err(err) => {
                warn!(%err, "is_healthy returned false-- connection closed by remote");
                false
            }
        }
    }
    pub fn is_idle(&self) -> bool {
        self.pending.lock().unwrap().is_empty()
    }
}

fn new_conn(
    read_fd: RawFd,
    addr: SocketAddr,
    max_in_flight: Option<usize>,
    pending: &Arc<Mutex<HashMap<u16, PendingResponse<SerialMsg>>>>,
    queued_tx: mpsc::UnboundedSender<PendingSend<SerialMsg>>,
) -> (Arc<AtomicBool>, TcpConnection) {
    let is_closing = Arc::new(AtomicBool::new(false));
    let this = TcpConnection {
        read_fd,
        addr,
        pending: pending.clone(),
        tasks: JoinSet::new(),
        queued_tx,
        next_id: Arc::new(AtomicU16::new(0)),
        is_closing: is_closing.clone(),
        max_in_flight,
    };
    (is_closing, this)
}

async fn read_half<R: AsyncReadExt + Unpin>(
    mut recv: R,
    addr: SocketAddr,
    is_closing: Arc<AtomicBool>,
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
                // @TODO set last activity?
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
    mut queued_msgs: mpsc::UnboundedReceiver<PendingSend<SerialMsg>>,
    pending: ResponseMap<PendingResponse<SerialMsg>>,
) -> Result<()> {
    while let Some(msg) = queued_msgs.recv().await {
        let PendingSend {
            mut to_send,
            reply,
            next_id,
        } = msg;
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

async fn tcpstream_connect(addr: SocketAddr) -> Result<TcpStream> {
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
    // soc.set_reuse_port(true)?;
    soc.set_nonblocking(true)?;
    soc.set_tcp_nodelay(true)?;
    set_socket_option(&soc, libc::IPPROTO_TCP, libc::TCP_FASTOPEN_CONNECT, tfo_on)?;

    let soc = unsafe { TcpSocket::from_raw_fd(soc.into_raw_fd()) };
    Ok(soc.connect(addr).await?)
}

fn set_socket_option<Fd: AsRawFd>(
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

fn getso_err<Fd: AsRawFd>(fd: &Fd) -> Result<(), std::io::Error> {
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
    use hickory_proto::{
        op::{Message, MessageType, OpCode, Query},
        rr::{Name, RecordType},
    };
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, split};

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

    async fn test_conn(
        max_flight: Option<usize>,
    ) -> (Arc<TcpConnection>, tokio::io::DuplexStream, SocketAddr) {
        let (client, server) = tokio::io::duplex(BUF_SIZE);
        let (read, write) = split(client);
        let addr = "127.0.0.1:53".parse().unwrap();
        let conn = TcpConnection::from_split(read, write, addr, max_flight).unwrap();
        (conn, server, addr)
    }

    #[tokio::test]
    async fn test_send_and_receive_ok() {
        let (conn, mut server, addr) = test_conn(None).await;

        // Test server task that mimics a DNS server
        tokio::spawn(async move {
            // Read the length prefix
            let mut len_buf = [0u8; 2];
            server.read_exact(&mut len_buf).await.unwrap();
            let len = u16::from_be_bytes(len_buf) as usize;

            // Read the actual message
            let mut msg_buf = vec![0u8; len];
            server.read_exact(&mut msg_buf).await.unwrap();

            // The multiplexer should have replaced the original ID with its own internal ID.
            // Let's check that the received ID is 0 (the first one used).
            let received_id = u16::from_be_bytes(msg_buf[0..2].try_into().unwrap());
            assert_eq!(received_id, 0);

            // Create a response message, using the internal ID
            let mut resp = new_msg(received_id, "example.com.");
            resp.set_message_type(MessageType::Response);
            let msg = SerialMsg::from_message(&resp, addr).unwrap();

            // Send the response back
            msg.write(&mut server).await.unwrap();
        });

        let (tx, rx) = oneshot::channel();
        let original_id = 1234;
        let query_msg = new_msg(original_id, "example.com.");
        let query = DnsQuery {
            to_send: SerialMsg::from_message(&query_msg, addr).unwrap(),
            reply: tx,
        };

        conn.send(query).unwrap();

        let response = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("should get a response")
            .unwrap();

        // Check that the original ID was restored by the connection manager
        assert_eq!(response.msg_id(), original_id);
    }

    #[tokio::test]
    async fn test_max_in_flight() {
        let (conn, _server, addr) = test_conn(Some(1)).await;

        let (tx, _rx) = oneshot::channel();
        let query_msg = new_msg(1, "query1.com.");
        let query = DnsQuery {
            to_send: SerialMsg::from_message(&query_msg, addr).unwrap(),
            reply: tx,
        };

        // First send should be ok
        assert!(conn.will_be_reusable());
        conn.send(query).unwrap();

        // Connection should now be unhealthy due to max_in_flight
        assert!(!conn.will_be_reusable());

        // Second send should fail
        let (tx2, _rx2) = oneshot::channel();
        let query_msg2 = new_msg(2, "query2.com.");
        let query2 = DnsQuery {
            to_send: SerialMsg::from_message(&query_msg2, addr).unwrap(),
            reply: tx2,
        };
        assert!(conn.send(query2).is_err());
    }

    #[tokio::test]
    async fn test_connection_closes_on_read_error() {
        let (conn, server, _) = test_conn(None).await;
        assert!(conn.will_be_reusable());

        // Drop the server end to cause a read error in the connection's read_half
        drop(server);

        // Wait a moment for the read_half task to detect the error and set is_closing
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(!conn.will_be_reusable());
        assert!(conn.is_closing());
    }

    #[tokio::test]
    async fn test_connection_closes_on_send_error() {
        let (conn, server, addr) = test_conn(None).await;
        assert!(conn.will_be_reusable());

        // Drop the server end to cause a write error
        drop(server);

        let (tx, rx) = oneshot::channel();
        let query_msg = new_msg(1, "query1.com.");
        let query = DnsQuery {
            to_send: SerialMsg::from_message(&query_msg, addr).unwrap(),
            reply: tx,
        };

        // This send might succeed in queueing, but the send_half task will fail to write.
        // The error on send will drop the oneshot sender in PendingSend.
        let _ = conn.send(query);

        // The receiver should get an error because the sender was dropped.
        let result = rx.await;
        assert!(result.is_err());

        // Wait a moment for the tasks to process the error and update state
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(!conn.will_be_reusable());
        assert!(conn.is_closing());
    }
}
