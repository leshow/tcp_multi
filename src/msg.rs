use hickory_proto::{
    ProtoError,
    op::{Header, Message, Query},
    serialize::binary::{BinDecodable, BinDecoder},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
};

use std::{borrow::Borrow, io, net::SocketAddr};

const BUF_SIZE: usize = 4096;

// use crate::udp::UdpRecv;

/// A message pulled from TCP or UDP and serialized to bytes, stored with a
/// [`SocketAddr`]
///
/// [`SocketAddr`]: std::net::SocketAddr
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerialMsg {
    message: Vec<u8>,
    addr: SocketAddr,
}

impl SerialMsg {
    /// Construct a new `SerialMsg` and the source or destination address
    pub fn new(message: Vec<u8>, addr: SocketAddr) -> Self {
        SerialMsg { message, addr }
    }

    /// Constructs a new `SerialMsg` from another `SerialMsg` and a `SocketAddr`
    pub fn from_message(msg: &Message, addr: SocketAddr) -> io::Result<Self> {
        Ok(SerialMsg {
            message: msg.to_vec()?,
            addr,
        })
    }

    /// Get a reference to the bytes
    pub fn bytes(&self) -> &[u8] {
        &self.message
    }

    /// Get a mutable reference to the bytes
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.message
    }

    /// Get the source or destination address (context dependent)
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Set the source or destination address
    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.addr = addr;
    }

    /// Gets the bytes and address as a tuple
    pub fn contents(self) -> (Vec<u8>, SocketAddr) {
        (self.message, self.addr)
    }

    /// Deserializes the inner data into a Message
    pub fn to_message(&self) -> io::Result<Message> {
        Ok(Message::from_vec(&self.message)?)
    }

    /// Get dns message id from the buffer
    pub fn msg_id(&self) -> u16 {
        u16::from_be_bytes([self.message[0], self.message[1]])
    }

    /// Replace current dns message id (first 2 bytes) with a new id
    pub fn replace_id(&mut self, new_id: [u8; 2]) {
        self.message[0..2].copy_from_slice(&new_id)
    }

    /// Get the response code from the byte buffer (not using edns)
    pub fn r_code(&self) -> u8 {
        let qr_to_rcode = u8::from_be_bytes([self.message[3]]);
        0x0F & qr_to_rcode
    }

    /// Decode the first 12 bytes of `message` into a [`Header`].
    /// A decoded DNS query header.
    ///
    /// [`Header`]: hickory_proto::op::Header
    pub fn to_header(&self) -> io::Result<Header> {
        let mut decoder = BinDecoder::new(self.bytes());
        Ok(Header::read(&mut decoder)?)
    }

    /// Truncate the bytes to the specified size
    pub fn truncate_bytes(&mut self, size: usize) {
        self.message.truncate(size);
        self.set_truncated();
    }

    /// Set the TC bit, to indicate the message was truncated
    pub fn set_truncated(&mut self) {
        self.message[2] |= 0b0000_0010;
    }

    /// Build the Query from this msg
    pub fn query(&self) -> Result<Query, ProtoError> {
        Query::from_bytes(&self.bytes()[12..])
    }

    /// create a new `SerialMsg` from any `AsyncReadExt`
    ///
    /// Cancel-safety: not cancel-safe, uses an internal stack buffer & `read_exact`
    pub async fn read<S>(stream: &mut S, src: SocketAddr) -> io::Result<Self>
    where
        S: Unpin + AsyncReadExt,
    {
        let mut buf = [0u8; 2];
        let _bytes_len = stream.read_exact(&mut buf).await?;
        let len = u16::from_be_bytes(buf) as usize;

        let mut buf = vec![0; len];
        stream.read_exact(&mut buf).await?;

        Ok(SerialMsg::new(buf, src))
    }

    /// write a `SerialMsg` to any `AsyncWriteExt`
    ///
    /// Cancel-safety: not cancel-safe. `write_all` uses internal state
    pub async fn write<S>(&self, stream: &mut S) -> io::Result<usize>
    where
        S: Unpin + AsyncWriteExt,
    {
        let len = self.message.len();
        let byte_len = (len as u16).to_be_bytes();
        // send
        stream.write_all(&byte_len).await?;
        stream.write_all(&self.message).await?;

        Ok(len)
    }

    /// Receive a `SerialMsg` from any new `UdpRecv`
    ///
    /// Cancel-safety: not cancel-safe, uses an internal stack buffer
    pub async fn recv<S>(stream: &S) -> io::Result<Self>
    where
        S: Borrow<UdpSocket>,
    {
        let mut buf = [0u8; crate::msg::BUF_SIZE];
        let (len, src) = stream.borrow().recv_from(&mut buf).await?;
        Ok(SerialMsg::new(buf[..len].to_vec(), src))
    }
}
