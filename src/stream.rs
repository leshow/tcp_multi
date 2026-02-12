use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::timeout;
use uuid::Uuid;

use crate::msg::SerialMsg;

/// Configuration for a downstream backend
#[derive(Clone)]
pub struct BackendConfig {
    pub remote: SocketAddr,
    pub name: String,
    pub tcp_concurrent_connections_limit: usize,
    pub max_in_flight_queries_per_conn: usize,
    pub tcp_recv_timeout: Duration,
    pub tcp_send_timeout: Duration,
    pub tcp_connect_timeout: Duration,
    pub use_proxy_protocol: bool,
    pub tcp_fast_open: bool,
}

/// Metrics tracked for each backend
#[derive(Default, Clone)]
pub struct BackendMetrics {
    pub tcp_current_connections: usize,
    pub tcp_max_concurrent_connections: usize,
    pub tcp_too_many_concurrent_connections: u64,
    pub tcp_reused_connections: u64,
    pub tcp_new_connections: u64,
    pub tcp_died_sending_query: u64,
    pub tcp_died_reading_response: u64,
    pub tcp_gave_up: u64,
}

/// Represents a downstream backend server
pub struct DownstreamState {
    pub id: Uuid,
    pub config: BackendConfig,
    pub metrics: Arc<RwLock<BackendMetrics>>,
}

impl DownstreamState {
    pub fn new(config: BackendConfig) -> Self {
        Self {
            id: Uuid::new_v4(),
            config,
            metrics: Arc::new(RwLock::new(BackendMetrics::default())),
        }
    }

    pub async fn increment_current_connections(&self) {
        let mut metrics = self.metrics.write().await;
        metrics.tcp_current_connections += 1;
        if metrics.tcp_current_connections > metrics.tcp_max_concurrent_connections {
            metrics.tcp_max_concurrent_connections = metrics.tcp_current_connections;
        }
    }

    pub async fn decrement_current_connections(&self) {
        let mut metrics = self.metrics.write().await;
        if metrics.tcp_current_connections > 0 {
            metrics.tcp_current_connections -= 1;
        }
    }
}

/// State of a TCP connection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Idle,
    SendingQueryToBackend,
    WaitingForResponseFromBackend,
    ReadingResponseSizeFromBackend,
    ReadingResponseFromBackend,
}

/// Errors that can occur with connections
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Connection died")]
    ConnectionDied,
    #[error("Maximum stream ID reached")]
    MaxStreamIdReached,
    #[error("Maximum concurrent queries reached")]
    MaxConcurrentQueriesReached,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Timeout")]
    Timeout,
    #[error("Maximum connections reached: {0}")]
    TooManyConnections(String),
}

/// A connection to a backend server
pub struct TcpConn {
    downstream: Arc<DownstreamState>,
    stream: Option<TcpStream>,
    state: ConnectionState,
    connection_start_time: SystemTime,
    last_data_received_time: SystemTime,
    highest_stream_id: u16,
    pending_queries: VecDeque<Vec<u8>>,
    pending_responses: HashMap<u16, Vec<u8>>,
    queries_sent: u64,
    downstream_failures: u16,
    connection_died: bool,
    is_fresh: bool,
    enable_fast_open: bool,
}

impl TcpConn {
    /// Create a new connection to a backend
    pub async fn new(downstream: Arc<DownstreamState>) -> Result<Self, ConnectionError> {
        let stream = timeout(
            downstream.config.tcp_connect_timeout,
            TcpStream::connect(downstream.config.remote),
        )
        .await
        .map_err(|_| ConnectionError::Timeout)?
        .map_err(ConnectionError::IoError)?;

        downstream.increment_current_connections().await;

        let mut metrics = downstream.metrics.write().await;
        metrics.tcp_new_connections += 1;
        drop(metrics);

        let now = SystemTime::now();

        Ok(Self {
            downstream,
            stream: Some(stream),
            state: ConnectionState::Idle,
            connection_start_time: now,
            last_data_received_time: now,
            highest_stream_id: 0,
            pending_queries: VecDeque::new(),
            pending_responses: HashMap::new(),
            queries_sent: 0,
            downstream_failures: 0,
            connection_died: false,
            is_fresh: true,
            enable_fast_open: false,
        })
    }

    /// Check if the connection is usable
    pub fn is_usable(&mut self) -> bool {
        if self.stream.is_none() {
            self.connection_died = true;
            return false;
        }

        if self.connection_died {
            return false;
        }

        // Could add actual socket check here
        true
    }

    /// Check if the connection is idle
    pub fn is_idle(&self) -> bool {
        self.state == ConnectionState::Idle
            && self.pending_queries.is_empty()
            && self.pending_responses.is_empty()
    }

    /// Check if maximum stream ID has been reached
    pub fn reached_max_stream_id(&self) -> bool {
        // TCP/DoT has only 2^16 usable identifiers
        const MAX_STREAM_ID: u16 = u16::MAX - 1;
        self.highest_stream_id == MAX_STREAM_ID
    }

    /// Check if maximum concurrent queries has been reached
    pub fn reached_max_concurrent_queries(&self) -> bool {
        let concurrent = self.pending_queries.len()
            + self.pending_responses.len()
            + if self.state == ConnectionState::SendingQueryToBackend {
                1
            } else {
                0
            };

        concurrent > 0 && concurrent >= self.downstream.config.max_in_flight_queries_per_conn
    }

    /// Check if the connection can be reused
    pub fn can_be_reused(&self, same_client: bool) -> bool {
        if self.connection_died {
            return false;
        }

        // Can't reuse if proxy protocol is used and different client
        if self.downstream.config.use_proxy_protocol && !same_client {
            return false;
        }

        if self.reached_max_stream_id() {
            return false;
        }

        if self.reached_max_concurrent_queries() {
            return false;
        }

        true
    }

    /// Check if connection will be reusable in the future
    pub fn will_be_reusable(&self, same_client: bool) -> bool {
        if self.connection_died || self.reached_max_stream_id() {
            return false;
        }

        if self.downstream.config.use_proxy_protocol {
            return same_client;
        }

        true
    }

    /// Mark connection as reused
    pub fn set_reused(&mut self) {
        self.is_fresh = false;
    }

    /// Get the last time data was received
    pub fn last_data_received_time(&self) -> SystemTime {
        self.last_data_received_time
    }

    /// Send a query to the backend
    pub async fn send_query(&mut self, mut query: SerialMsg) -> Result<u16, ConnectionError> {
        let stream = self
            .stream
            .as_mut()
            .ok_or(ConnectionError::ConnectionDied)?;

        let query_id = self.highest_stream_id;
        self.highest_stream_id = self.highest_stream_id.wrapping_add(1);

        // TODO swap ID
        // Send with timeout
        timeout(
            self.downstream.config.tcp_send_timeout,
            query.writev(stream),
        )
        .await
        .map_err(|_| ConnectionError::Timeout)?
        .map_err(ConnectionError::IoError)?;

        self.queries_sent += 1;
        self.state = ConnectionState::WaitingForResponseFromBackend;

        Ok(query_id)
    }

    /// Read a response from the backend
    pub async fn read_response(&mut self) -> Result<Vec<u8>, ConnectionError> {
        let stream = self
            .stream
            .as_mut()
            .ok_or(ConnectionError::ConnectionDied)?;

        // Read length
        self.state = ConnectionState::ReadingResponseSizeFromBackend;
        let mut len_buf = [0u8; 2];
        timeout(
            self.downstream.config.tcp_recv_timeout,
            stream.read_exact(&mut len_buf),
        )
        .await
        .map_err(|_| ConnectionError::Timeout)?
        .map_err(ConnectionError::IoError)?;

        let response_len = u16::from_be_bytes(len_buf) as usize;

        // Read response
        self.state = ConnectionState::ReadingResponseFromBackend;
        let mut response = vec![0u8; response_len];
        timeout(
            self.downstream.config.tcp_recv_timeout,
            stream.read_exact(&mut response),
        )
        .await
        .map_err(|_| ConnectionError::Timeout)?
        .map_err(ConnectionError::IoError)?;

        self.last_data_received_time = SystemTime::now();
        self.state = ConnectionState::Idle;

        Ok(response)
    }

    /// Release the connection
    pub async fn release(&mut self) {
        if let Some(stream) = self.stream.take() {
            drop(stream);
        }
        self.downstream.decrement_current_connections().await;
    }
}

impl Drop for TcpConn {
    fn drop(&mut self) {
        // Note: We can't make drop async, so connection count cleanup
        // should be handled explicitly via release() method
    }
}

/// Connection pool configuration
pub struct PoolConfig {
    pub max_idle_connections_per_downstream: usize,
    pub cleanup_interval: Duration,
    pub max_idle_time: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_connections_per_downstream: 10,
            cleanup_interval: Duration::from_secs(60),
            max_idle_time: Duration::from_secs(300),
        }
    }
}

/// Lists of connections (active and idle)
#[derive(Default)]
struct ConnectionLists {
    actives: VecDeque<Arc<RwLock<TcpConn>>>,
    idles: VecDeque<Arc<RwLock<TcpConn>>>,
}

/// Manager for downstream TCP connections
pub struct DownstreamConnectionsManager {
    config: PoolConfig,
    connections: Arc<RwLock<HashMap<Uuid, ConnectionLists>>>,
    last_cleanup: Arc<RwLock<SystemTime>>,
}

impl DownstreamConnectionsManager {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            last_cleanup: Arc::new(RwLock::new(SystemTime::now())),
        }
    }

    /// Get a connection to a downstream backend
    pub async fn get_connection_to_downstream(
        &self,
        downstream: Arc<DownstreamState>,
    ) -> Result<Arc<RwLock<TcpConn>>, ConnectionError> {
        let now = SystemTime::now();
        let fresh_cutoff = now - Duration::from_secs(1);

        self.cleanup_closed_connections().await;

        let backend_id = downstream.id;

        // Try to find a usable connection
        let mut connections = self.connections.write().await;

        if !downstream.config.use_proxy_protocol {
            if let Some(lists) = connections.get_mut(&backend_id) {
                // First, try idle connections (most recent first)
                if let Some(conn) =
                    Self::find_usable_in_list(&mut lists.idles, fresh_cutoff, true).await
                {
                    let mut metrics = downstream.metrics.write().await;
                    metrics.tcp_reused_connections += 1;
                    drop(metrics);

                    lists.actives.push_front(conn.clone());
                    return Ok(conn);
                }

                // Then try active connections
                if let Some(conn) =
                    Self::find_usable_in_list(&mut lists.actives, fresh_cutoff, false).await
                {
                    let mut metrics = downstream.metrics.write().await;
                    metrics.tcp_reused_connections += 1;
                    drop(metrics);

                    return Ok(conn);
                }
            }
        }

        // Check if we've hit the connection limit
        let metrics = downstream.metrics.read().await;
        if downstream.config.tcp_concurrent_connections_limit > 0
            && metrics.tcp_current_connections >= downstream.config.tcp_concurrent_connections_limit
        {
            drop(metrics);
            let mut metrics = downstream.metrics.write().await;
            metrics.tcp_too_many_concurrent_connections += 1;
            drop(metrics);

            return Err(ConnectionError::TooManyConnections(format!(
                "Maximum number of TCP connections to {} reached, not creating a new one",
                downstream.config.name
            )));
        }
        drop(metrics);

        // Create a new connection
        let new_conn = TcpConn::new(downstream.clone()).await?;
        let conn_arc = Arc::new(RwLock::new(new_conn));

        // Add to active connections if not using proxy protocol
        if !downstream.config.use_proxy_protocol {
            let lists = connections
                .entry(backend_id)
                .or_insert_with(ConnectionLists::default);
            lists.actives.push_front(conn_arc.clone());
        }

        Ok(conn_arc)
    }

    /// Find a usable connection in a list
    async fn find_usable_in_list(
        list: &mut VecDeque<Arc<RwLock<TcpConn>>>,
        fresh_cutoff: SystemTime,
        remove_if_found: bool,
    ) -> Option<Arc<RwLock<TcpConn>>> {
        let mut index = 0;

        while index < list.len() {
            let conn = &list[index];
            let mut conn_lock = conn.write().await;

            if Self::is_connection_usable(&mut *conn_lock, fresh_cutoff) {
                conn_lock.set_reused();
                drop(conn_lock);

                return if remove_if_found {
                    list.remove(index)
                } else {
                    Some(conn.clone())
                };
            }

            if conn_lock.will_be_reusable(false) {
                drop(conn_lock);
                index += 1;
                continue;
            }

            // Connection won't be usable, remove it
            drop(conn_lock);
            list.remove(index);
        }

        None
    }

    /// Check if a connection is usable
    fn is_connection_usable(conn: &mut TcpConn, fresh_cutoff: SystemTime) -> bool {
        if !conn.can_be_reused(false) {
            return false;
        }

        // Recently used connections are assumed usable
        if conn.last_data_received_time() > fresh_cutoff {
            return true;
        }

        // Check if socket is still valid
        conn.is_usable()
    }

    /// Move a connection to the idle pool
    pub async fn move_to_idle(&self, conn: Arc<RwLock<TcpConn>>) -> bool {
        let conn_lock = conn.read().await;
        let backend_id = conn_lock.downstream.id;
        drop(conn_lock);

        let mut connections = self.connections.write().await;

        if let Some(lists) = connections.get_mut(&backend_id) {
            // Remove from actives
            lists.actives.retain(|c| !Arc::ptr_eq(c, &conn));

            // Check idle connection limit
            if lists.idles.len() >= self.config.max_idle_connections_per_downstream {
                // Remove oldest idle connection
                if let Some(old_conn) = lists.idles.pop_back() {
                    let mut old_lock = old_conn.write().await;
                    old_lock.release().await;
                }
            }

            // Add to front of idle list (most recent)
            lists.idles.push_front(conn);
            return true;
        }

        false
    }

    /// Clean up closed and stale connections
    pub async fn cleanup_closed_connections(&self) {
        let now = SystemTime::now();

        let mut last_cleanup = self.last_cleanup.write().await;
        if now.duration_since(*last_cleanup).unwrap_or(Duration::ZERO)
            < self.config.cleanup_interval
        {
            return;
        }
        *last_cleanup = now;
        drop(last_cleanup);

        let fresh_cutoff = now - Duration::from_secs(1);
        let idle_cutoff = now - self.config.max_idle_time;

        let mut connections = self.connections.write().await;

        let mut to_remove = Vec::new();

        for (backend_id, lists) in connections.iter_mut() {
            Self::cleanup_list(&mut lists.idles, fresh_cutoff, idle_cutoff).await;
            Self::cleanup_list(&mut lists.actives, fresh_cutoff, idle_cutoff).await;

            if lists.idles.is_empty() && lists.actives.is_empty() {
                to_remove.push(*backend_id);
            }
        }

        for backend_id in to_remove {
            connections.remove(&backend_id);
        }
    }

    /// Clean up a single list of connections
    async fn cleanup_list(
        list: &mut VecDeque<Arc<RwLock<TcpConn>>>,
        fresh_cutoff: SystemTime,
        idle_cutoff: SystemTime,
    ) {
        let mut index = 0;

        while index < list.len() {
            let conn = &list[index];
            let mut conn_lock = conn.write().await;

            // Don't check recently used connections
            if conn_lock.last_data_received_time() > fresh_cutoff {
                drop(conn_lock);
                index += 1;
                continue;
            }

            // Remove idle connections that are too old
            if conn_lock.is_idle() && conn_lock.last_data_received_time() < idle_cutoff {
                conn_lock.release().await;
                drop(conn_lock);
                list.remove(index);
                continue;
            }

            // Remove connections that are no longer usable
            if !conn_lock.is_usable() {
                if conn_lock.is_idle() {
                    conn_lock.release().await;
                }
                drop(conn_lock);
                list.remove(index);
                continue;
            }

            drop(conn_lock);
            index += 1;
        }
    }

    /// Get statistics about the connection pool
    pub async fn stats(&self) -> HashMap<Uuid, (usize, usize)> {
        let connections = self.connections.read().await;
        connections
            .iter()
            .map(|(id, lists)| (*id, (lists.actives.len(), lists.idles.len())))
            .collect()
    }
}

/// Background task to periodically cleanup connections
pub async fn cleanup_task(manager: Arc<DownstreamConnectionsManager>) {
    let mut interval = tokio::time::interval(manager.config.cleanup_interval);

    loop {
        interval.tick().await;
        manager.cleanup_closed_connections().await;
    }
}
