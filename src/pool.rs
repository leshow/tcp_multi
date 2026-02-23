use std::{
    collections::VecDeque,
    fmt::Debug,
    net::SocketAddr,
    ops::Deref,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, RwLock, Semaphore},
    task::AbortHandle,
};
use tracing::{debug, error, info, trace, warn};

use crate::{TcpConnection, TcpConnectionConfig, TcpConnectionError};

#[derive(Debug, Error)]
pub enum PoolError {
    #[error("all connections at capacity")]
    AllConnectionsBusy,

    #[error("failed to create connection: {0}")]
    ConnectionCreation(#[from] TcpConnectionError),

    #[error("failed to acquire permit")]
    AcquireError(#[from] tokio::sync::TryAcquireError),
}

#[derive(Clone)]
pub struct ConnectionPool(Arc<PoolInner>);

pub struct PoolInner {
    /// connections per backend
    connections: RwLock<VecDeque<PoolConnection>>,
    round_robin: AtomicUsize,
    addr: SocketAddr,
    config: PoolConfig,
    permit_available: Arc<Notify>,
    _cleanup: OnceLock<AbortHandle>,
}

#[derive(Clone)]
pub struct PoolConnection {
    inner: ConnectionInner,
}

impl Debug for PoolConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConnection")
            .field(
                "available_permits",
                &self.inner.max_handles.available_permits(),
            )
            .field("connection", &self.inner.conn)
            .finish()
    }
}

#[derive(Clone)]
struct ConnectionInner {
    conn: Arc<TcpConnection>,
    max_handles: Arc<Semaphore>,
}

pub struct ConnectionHandle {
    conn: Arc<TcpConnection>,
    pool: Weak<PoolInner>,
    _permit: OwnedSemaphorePermit,
}

impl Deref for ConnectionHandle {
    type Target = TcpConnection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

/// Configuration for the connection pool
#[derive(Clone, Copy, Debug)]
pub struct PoolConfig {
    /// How many handles can be given out for a single connection
    pub max_concurrent_per_conn: usize,
    /// Maximum idle connections per downstream backend
    pub max_connections: usize,
    /// Maximum time a connection can be idle before cleanup
    pub max_idle_time: Duration,
    /// Interval between cleanup runs
    pub cleanup_interval: Duration,
    /// Interval for logging stats (None = disabled)
    pub stats_interval: Option<Duration>,
    /// TCP connection config-- maximum concurrent connections per downstream (None = unlimited)
    pub max_in_flight_per: Option<usize>,
    pub keepalive: KeepaliveConfig,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct KeepaliveConfig {
    pub idle: Option<u64>,
    pub interval: Option<u64>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_concurrent_per_conn: 100,
            max_connections: 10,
            max_idle_time: Duration::from_secs(3),
            cleanup_interval: Duration::from_secs(30),
            stats_interval: Some(Duration::from_secs(2)),
            max_in_flight_per: None,
            keepalive: KeepaliveConfig::default(),
        }
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        // Try to upgrade weak reference
        if let Some(pool) = self.pool.upgrade() {
            trace!("notify connection on drop");
            pool.permit_available.notify_one();
        }
        // If pool is gone, nothing to notify
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        // Only abort the cleanup task if this is the last ConnectionPool reference
        if Arc::strong_count(&self.0) == 1 {
            trace!("last ConnectionPool dropped, aborting background task");
            if let Some(handle) = self.0._cleanup.get() {
                handle.abort();
            }
        } else {
            trace!(
                "ConnectionPool clone dropped, {} references remain",
                Arc::strong_count(&self.0) - 1
            );
        }
    }
}

impl ConnectionPool {
    pub fn new(addr: SocketAddr, config: PoolConfig) -> Self {
        let cleanup_interval = config.cleanup_interval;
        let stats_interval = config.stats_interval;

        let this = Self(Arc::new(PoolInner {
            connections: RwLock::new(VecDeque::new()),
            round_robin: AtomicUsize::new(0),
            addr,
            config,
            permit_available: Arc::new(Notify::new()),
            _cleanup: OnceLock::new(),
        }));

        let abort_handle = tokio::spawn({
            let weak = Arc::downgrade(&this.0);
            async move {
                let mut cleanup = tokio::time::interval(cleanup_interval);

                let mut stats = stats_interval.map(tokio::time::interval);

                cleanup.tick().await;
                if let Some(s) = &mut stats {
                    s.tick().await;
                }

                loop {
                    let Some(inner) = weak.upgrade() else {
                        error!("pool inner dropped, cleanup task exiting");
                        break;
                    };
                    match &mut stats {
                        Some(s) => {
                            tokio::select! {
                                _ = cleanup.tick() => {
                                    inner.cleanup().await;
                                }
                                _ = s.tick() => {
                                    inner.stats().await;
                                }
                            }
                        }
                        None => {
                            cleanup.tick().await;
                            inner.cleanup().await;
                        }
                    }
                }
                error!("background task exited loop");
            }
        })
        .abort_handle();
        // set handle
        let _ = this.0._cleanup.set(abort_handle);

        this
    }

    fn try_get_handle(&self, conn: &PoolConnection) -> Result<ConnectionHandle, PoolError> {
        // Try to acquire pool-wide permit first
        let permit = conn.inner.max_handles.clone().try_acquire_owned()?;
        trace!("got permit-- making new handle");
        Ok(ConnectionHandle {
            _permit: permit,
            conn: conn.inner.conn.clone(),
            // pool weak ref
            pool: Arc::downgrade(&self.0),
        })
    }

    pub async fn len(&self) -> usize {
        self.0.connections.read().await.len()
    }
    pub async fn is_empty(&self) -> bool {
        self.0.connections.read().await.is_empty()
    }

    pub async fn try_get_connection(&self) -> Result<ConnectionHandle, PoolError> {
        // Try to find connection with available permit (read lock)
        trace!("trying to get read lock");
        let (grow, cleanup) = {
            let conns = self.0.connections.read().await;
            // if we haven't created all connections yet, drop read lock and move to create
            if conns.len() < self.0.config.max_connections {
                trace!("connections less than max-- skip to create new");
                (true, false)
            } else {
                // At capacity - search for usable connection
                let needs_cleanup = match self._round_robin(&conns).await {
                    Ok(h) => return Ok(h),
                    Err(needs_cleanup) => needs_cleanup,
                };
                // All connections are at capacity
                (false, needs_cleanup)
            }
        };
        // Try to create new connection if under limit OR if all existing connections are unusable
        if grow || cleanup {
            debug!(%grow, %cleanup, "creating new connection");
            return self._create_connection(cleanup).await;
        }
        Err(PoolError::AllConnectionsBusy)
    }

    pub async fn get_connection(&self) -> Result<ConnectionHandle, PoolError> {
        let mut count = 0;

        loop {
            let notified = self.0.permit_available.notified();
            match self.try_get_connection().await {
                Ok(msg) => return Ok(msg),
                Err(err) => {
                    count += 1;
                    warn!(count, ?err, "waiting for wakeup");
                    notified.await;
                    // TODO: should this function ever error or loop indefinitely?
                    if count >= 3 {
                        return Err(PoolError::AllConnectionsBusy);
                    }
                }
            }
        }
    }

    #[inline]
    async fn _create_connection(&self, cleanup: bool) -> Result<ConnectionHandle, PoolError> {
        let mut conns = self.0.connections.write().await;

        if cleanup {
            info!("running cleanup");
            // Clean up any closing connections first
            conns.retain(|conn| !conn.inner.conn.is_closing());
        }

        // Re-check after acquiring write lock and cleanup to prevent race condition
        if conns.len() >= self.0.config.max_connections {
            debug!("already at max connections after acquiring write lock");
            // TODO should try to get another permit
            return Err(PoolError::AllConnectionsBusy);
        }

        match self.create_connection().await {
            Ok(new_conn) => {
                let handle = self.try_get_handle(&new_conn)?;
                conns.push_front(new_conn);
                Ok(handle)
            }
            Err(err) => {
                warn!(%err, "failed to create connection, falling back to existing");
                Err(err)
            }
        }
    }

    #[inline]
    async fn _round_robin(
        &self,
        conns: &VecDeque<PoolConnection>,
    ) -> Result<ConnectionHandle, bool> {
        trace!("round robin connections");
        let len = conns.len();
        let mut needs_cleanup = false;
        let now = Instant::now();

        // Round-robin selection among usable connections
        let start = self.0.round_robin.fetch_add(1, Ordering::Relaxed) % len;

        // Try from start to end
        for conn in conns.iter().skip(start) {
            if conn.inner.conn.is_usable(now) {
                if let Ok(handle) = self.try_get_handle(conn) {
                    return Ok(handle);
                }
            } else {
                needs_cleanup = true;
                conn.inner.conn.set_closing();
            }
        }

        // Try from beginning to start
        for conn in conns.iter().take(start) {
            if conn.inner.conn.is_usable(now) {
                if let Ok(handle) = self.try_get_handle(conn) {
                    return Ok(handle);
                }
            } else {
                needs_cleanup = true;
                conn.inner.conn.set_closing();
            }
        }
        Err(needs_cleanup)
    }
    async fn create_connection(&self) -> Result<PoolConnection, PoolError> {
        let conn = TcpConnection::new(
            self.0.addr,
            TcpConnectionConfig {
                ka_idle: self.0.config.keepalive.idle,
                ka_interval: self.0.config.keepalive.interval,
                max_in_flight: self.0.config.max_in_flight_per,
            },
        )
        .await?;

        Ok(PoolConnection {
            inner: ConnectionInner {
                conn: Arc::new(conn),
                max_handles: Arc::new(Semaphore::new(self.0.config.max_concurrent_per_conn)),
            },
        })
    }

    pub async fn cleanup(&self) {
        self.0.cleanup().await;
    }
}

impl PoolInner {
    async fn cleanup(&self) {
        let now = Instant::now();

        // Clean up idle connections
        {
            let mut conns = self.connections.write().await;
            let original_count = conns.len();

            conns.retain(|conn| {
                // Use sync method to get last activity
                let last_activity = conn.inner.conn.last_read();
                let age = now.duration_since(last_activity);

                if age > self.config.max_idle_time {
                    info!("removing idle connection (age: {:?})", age);
                    // to_shutdown.push(Arc::clone(conn));
                    conn.inner.conn.set_closing();
                    false
                } else if !conn.inner.conn.will_be_reusable() {
                    info!("removing non-reusable connection");
                    conn.inner.conn.set_closing();
                    false
                } else {
                    true
                }
            });

            let removed = original_count - conns.len();
            if removed > 0 {
                debug!("removed {} idle connection(s)", removed);
            }
        }
        trace!("cleanup: done");
    }
    async fn stats(&self) {
        let conns = self.connections.read().await;
        let now = Instant::now();

        // Collect stats before cleanup
        let mut handles_in_use = 0;
        let mut handles_available = 0;
        let mut connection_ages = Vec::new();

        for conn in conns.iter() {
            let available_permits = conn.inner.max_handles.available_permits();
            handles_in_use += self.config.max_concurrent_per_conn - available_permits;
            handles_available += available_permits;

            let last_activity = conn.inner.conn.last_read();
            let age = now.duration_since(last_activity);
            connection_ages.push(age);
        }
        // Log pool stats
        info!(
            "Pool stats: connections={:?}, handles_in_use={}, available={}, max_connections={}, max_concurrent_per_conn={}",
            conns,
            handles_in_use,
            handles_available,
            self.config.max_connections,
            self.config.max_concurrent_per_conn
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{net::TcpListener, sync::Notify};

    async fn tcp_echo() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let started = Arc::new(Notify::new());
        let addr = listener.local_addr().unwrap();

        tokio::spawn({
            let started = started.clone();
            async move {
                loop {
                    // we're started, tests can begin
                    started.notify_one();
                    if let Ok((socket, _)) = listener.accept().await {
                        tokio::spawn(async move {
                            // Keep the connection alive and echo data
                            let mut buf = vec![0u8; 1024];
                            while let Ok(n) = socket.try_read(&mut buf) {
                                if n == 0 {
                                    break;
                                }
                                let _ = socket.try_write(&buf[..n]);
                            }
                        });
                    }
                }
            }
        });

        started.notified().await;
        addr
    }

    #[tokio::test]
    async fn test_eager_connection_creation() {
        let addr = tcp_echo().await;
        let config = PoolConfig {
            max_connections: 5,
            ..Default::default()
        };

        let pool = ConnectionPool::new(addr, config);

        let mut handles = Vec::new();
        for _ in 0..5 {
            let handle = pool.get_connection().await.unwrap();
            handles.push(handle);
        }

        let len = pool.len().await;
        assert_eq!(len, 5, "Pool should have grown to max_connections");

        let handle6 = pool.get_connection().await.unwrap();
        let len = pool.len().await;
        assert_eq!(len, 5, "Pool should not create more than max_connections");
        drop(handle6);
        drop(handles);
    }

    #[tokio::test]
    async fn test_reuse_at_capacity() {
        let addr = tcp_echo().await;
        let config = PoolConfig {
            max_connections: 2,
            ..Default::default()
        };

        let pool = ConnectionPool::new(addr, config);

        let _handle1 = pool.get_connection().await.unwrap();
        let _handle2 = pool.get_connection().await.unwrap();

        let len = pool.len().await;
        assert_eq!(len, 2, "Should have 2 connections after 2 requests");

        let _handle3 = pool.get_connection().await.unwrap();
        let conns = pool.0.connections.read().await;
        assert_eq!(conns.len(), 2, "should reuse existing connection");
    }
}
