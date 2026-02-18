use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::Index,
};

use anyhow::Result;
use hickory_proto::op::Query;
use tokio::{
    self,
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::*;

use crate::msg::SerialMsg;

#[derive(Debug)]
pub struct ChanMsg<T> {
    pub query: Query,
    pub to_send: T,
    pub reply: oneshot::Sender<T>,
}

impl<T> ChanMsg<T> {
    pub fn new(to_send: T, reply: oneshot::Sender<T>, query: Query) -> Self {
        Self {
            query,
            to_send,
            reply,
        }
    }
}

pub type ChanMap<T> = HashMap<u16, T>;

pub struct UdpConnection {
    addr: SocketAddr
}
// TODO: start one udp_multi task per namespace instance so we have
// not just one channel handling all the messages
pub async fn udp_multi(
    num: usize,
    mut chan_msg_rx: mpsc::Receiver<ChanMsg<SerialMsg>>,
) -> Result<()> {
    let ns = UdpSocket::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0_u16)).await?;

    let mut map: ChanMap<(oneshot::Sender<SerialMsg>, Query, u16)> = ChanMap::new();

    // a wrapping u16 query ID that increments each time we insert a pending response into the map
    let mut global_id = 0_u16;

    loop {
        tokio::select! {
            msg = SerialMsg::recv(&ns) => {
                if let Ok(mut msg) = msg {
                    let id = msg.msg_id();
                    if let Some((send, query, orig_id)) = map.remove(&id) {
                        let Ok(response_query) = msg.query() else {
                            warn!(?id, "Received response but query did not parse.");
                            continue;
                        };

                        // ensure the query section in the response matches what we expected
                        if !response_query.eq(&query) {
                            warn!(
                                ?id,
                                name = ?query.name(),
                                response_name = ?response_query.name(),
                                "Received response but query did not match."
                            );
                            // reinsert this value back into the map so we can receive a matching response
                            map.insert(id, (send, query, orig_id));
                            continue;
                        }

                        // restore original id
                        msg.replace_id(u16::to_be_bytes(orig_id));

                        // send reply back to handler
                        if let Err(err) = send.send(msg) {
                            error!(?err, ?num, "sending over oneshot channel failed (likely reason: got response back for an already dropped message)");
                        }
                    }
                }
            }
            // recv from handle
            sender = chan_msg_rx.recv() => {
                if let Some(ChanMsg { query, mut to_send, reply }) = sender {
                    let orig_id = to_send.msg_id();
                    // replace id
                    to_send.replace_id(u16::to_be_bytes(global_id));

                    // send to namespace instance
                    debug!(addr = ?to_send.addr(), bytes = to_send.bytes().len(), id = ?global_id, ?num, "sending to");
                    if ns.send_to(to_send.bytes(), to_send.addr()).await.is_ok() {
                        map.insert(global_id, (reply, query, orig_id));
                        global_id = global_id.wrapping_add(1);
                    }
                }
            }
        }
    }
}

pub fn task(num: usize, udp_rx: mpsc::Receiver<ChanMsg<SerialMsg>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = udp_multi(num, udp_rx).await {
            error!(?err, "Starting udp receiver failed");
        }
    })
}

#[derive(Debug, Copy, Clone)]
pub struct UdpTask;

impl UdpTask {
    pub fn make(num: usize, udp_rx: mpsc::Receiver<ChanMsg<SerialMsg>>) -> JoinHandle<()> {
        task(num, udp_rx)
    }
}

#[derive(Debug)]
pub struct UdpTasks {
    senders: Vec<mpsc::Sender<ChanMsg<SerialMsg>>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Index<usize> for UdpTasks {
    type Output = mpsc::Sender<ChanMsg<SerialMsg>>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.senders[index]
    }
}

impl UdpTasks {
    pub fn new(namespaces: usize, chan_size: usize) -> Self {
        let mut senders = Vec::with_capacity(namespaces);
        let mut tasks = Vec::new();
        for i in 0..namespaces {
            let (udp_tx, udp_rx) = mpsc::channel(chan_size);
            // create bg task for each sender
            tasks.push(UdpTask::make(i, udp_rx));
            senders.push(udp_tx);
        }
        Self { senders, tasks }
    }
    pub fn len(&self) -> usize {
        self.senders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for UdpTasks {
    fn drop(&mut self) {
        trace!("udp_multi bg task drop called");
        for handle in &self.tasks {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use crate::msg::SerialMsg;
    use hickory_proto::{
        op::{Message, MessageType, Query},
        rr::{Name, RData, Record, RecordType, rdata::A},
        serialize::binary::{BinDecodable, BinEncodable},
    };
    use tokio::{self, net::UdpSocket, sync::oneshot};

    use crate::transport::udp_multi::ChanMsg;

    use super::UdpTasks;

    // verifies we can perform a basic send and receive of DNS messages over UDP
    #[tokio::test]
    async fn test_basic_send_and_receive() {
        let test_address: SocketAddr = "127.0.0.1:48811".parse().unwrap();
        let test_name: Name = "bluecatnetworks.com.".parse().unwrap();
        let test_ip: Ipv4Addr = "1.2.3.4".parse().unwrap();

        let listening_socket = UdpSocket::bind(test_address).await.unwrap();

        let tasks = UdpTasks::new(1, 100);
        let sender = tasks[0].clone();

        let mut to_send = Message::new();
        let query = Query::query(test_name.clone(), RecordType::A);
        to_send.add_query(query.clone());

        // create a channel to send and receive the response
        let (tx, rx) = oneshot::channel();
        // send to udp multi task
        if let Err(e) = sender
            .send(ChanMsg::new(
                SerialMsg::new(to_send.to_bytes().unwrap(), test_address),
                tx,
                query,
            ))
            .await
        {
            panic!("Failed to send message over channel: {e}");
        }

        // read from the listening socket, verify the query matches, and send a response
        let mut buf = [0; 1024];
        let (len, src) = listening_socket.recv_from(&mut buf).await.unwrap();
        let mut received = match Message::from_bytes(&buf[0..len]) {
            Ok(r) => r,
            Err(e) => panic!("Error parsing message: {e}"),
        };
        assert_eq!(to_send.query().unwrap(), received.query().unwrap());
        received.set_header({
            let mut header = *received.header();
            header.set_answer_count(1);
            header.set_message_type(MessageType::Response);
            header
        });
        received.add_answer(Record::from_rdata(
            test_name.clone(),
            60,
            RData::A(A(test_ip)),
        ));
        if let Err(e) = listening_socket
            .send_to(&received.to_bytes().unwrap(), src)
            .await
        {
            panic!("Error sending response message: {e}");
        }

        // verify the response
        let raw_response = match rx.await {
            Ok(r) => r,
            Err(e) => panic!("Error receiving response: {e}"),
        };
        let response = match Message::from_bytes(raw_response.bytes()) {
            Ok(r) => r,
            Err(e) => panic!("Error parsing response: {e}"),
        };
        assert_eq!(&received, &response);
    }

    // verifies we drop messages with mismatched queries
    #[tokio::test]
    async fn test_provides_correct_response() {
        let test_address: SocketAddr = "127.0.0.1:48812".parse().unwrap();
        let test_name: Name = "bluecatnetworks.com.".parse().unwrap();
        let mismatch_name: Name = "mismatch.bluecatnetworks.com.".parse().unwrap();
        let test_ip: Ipv4Addr = "1.2.3.4".parse().unwrap();

        let listening_socket = UdpSocket::bind(test_address).await.unwrap();

        let tasks = UdpTasks::new(1, 100);
        let sender = tasks[0].clone();

        let mut to_send = Message::new();
        let query = Query::query(test_name.clone(), RecordType::A);
        to_send.add_query(query.clone());

        // create a channel to send and receive the response
        let (tx, rx) = oneshot::channel();
        // send to udp multi task
        if let Err(e) = sender
            .send(ChanMsg::new(
                SerialMsg::new(to_send.to_bytes().unwrap(), test_address),
                tx,
                query,
            ))
            .await
        {
            panic!("Failed to send message over channel: {e}");
        }

        // read from the listening socket, verify the query matches, and send a response
        let mut buf = [0; 1024];
        let (len, src) = listening_socket.recv_from(&mut buf).await.unwrap();
        let mut received = match Message::from_bytes(&buf[0..len]) {
            Ok(r) => r,
            Err(e) => panic!("Error parsing message: {e}"),
        };
        assert_eq!(to_send.query().unwrap(), received.query().unwrap());
        received.set_header({
            let mut header = *received.header();
            header.set_answer_count(1);
            header.set_message_type(MessageType::Response);
            header
        });
        let original_query = received.query().unwrap().clone();
        // replace the response query with a mismatch
        *received.queries_mut() = vec![Query::query(mismatch_name.clone(), RecordType::A)];
        received.add_answer(Record::from_rdata(
            mismatch_name.clone(),
            60,
            RData::A(A(test_ip)),
        ));
        // send the wrong query back, udp_multi should ignore this
        if let Err(e) = listening_socket
            .send_to(&received.to_bytes().unwrap(), src)
            .await
        {
            panic!("Error sending response message: {e}");
        }

        // now send the correct response back
        *received.queries_mut() = vec![original_query];
        *received.answers_mut() = vec![Record::from_rdata(
            test_name.clone(),
            60,
            RData::A(A(test_ip)),
        )];
        if let Err(e) = listening_socket
            .send_to(&received.to_bytes().unwrap(), src)
            .await
        {
            panic!("Error sending response message: {e}");
        }

        // verify the response
        let raw_response = match rx.await {
            Ok(r) => r,
            Err(e) => panic!("Error receiving response: {e}"),
        };
        let response = match Message::from_bytes(raw_response.bytes()) {
            Ok(r) => r,
            Err(e) => panic!("Error parsing response: {e}"),
        };
        // verify we only see the correct query
        assert_eq!(&test_name, response.query().unwrap().name());
    }
}
