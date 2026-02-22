use std::{
    collections::VecDeque,
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
    sync::{OwnedSemaphorePermit, RwLock, Semaphore},
    task::AbortHandle,
};
use tracing::{debug, error, info, trace, warn};

use crate::{TcpConnection, TcpConnectionConfig, TcpConnectionError};

#[derive(Debug, Error)]
pub enum PoolError {
    #[error("all connections at capacity")]
    AllConnectionsBusy,

    #[error("pool semaphore closed")]
    AcquireError,

    #[error("failed to create connection: {0}")]
    ConnectionCreation(#[from] TcpConnectionError),
}

#[derive(Clone)]
pub struct ConnectionPool(Arc<PoolInner>);

pub struct PoolInner {
    /// connections per backend
    connections: RwLock<VecDeque<PoolConnection>>,
    addr: SocketAddr,
    config: PoolConfig,
    /// Pool-wide concurrency limit
    max_concurrent: Arc<Semaphore>,
    /// Round-robin counter for connection selection
    round_robin: AtomicUsize,
    _cleanup: OnceLock<AbortHandle>,
}

#[derive(Clone)]
pub struct PoolConnection {
    inner: ConnectionInner,
}

#[derive(Clone)]
struct ConnectionInner {
    conn: Arc<TcpConnection>,
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
    /// Pool-wide maximum concurrent requests
    pub max_concurrent: usize,
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
            max_concurrent: 100,
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
        // the weak reference is not currently used for anything
        // could be used to skip the OwnedPermit and some clones
        if let Some(_pool) = self.pool.upgrade() {
            trace!("notify connection on drop");
            // pool.max_concurrent.add_permits(1);
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
            addr,
            config,
            max_concurrent: Arc::new(Semaphore::new(config.max_concurrent)),
            round_robin: AtomicUsize::new(0),
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
    async fn get_permit(&self) -> Result<OwnedSemaphorePermit, PoolError> {
        match self.0.max_concurrent.clone().acquire_owned().await {
            Ok(permit) => Ok(permit),
            Err(_) => Err(PoolError::AcquireError),
        }
    }
    fn try_get_permit(&self) -> Result<OwnedSemaphorePermit, PoolError> {
        // Try to acquire pool-wide permit first
        match self.0.max_concurrent.clone().try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(_) => Err(PoolError::AllConnectionsBusy),
        }
    }

    pub async fn try_get_connection(&self) -> Result<ConnectionHandle, PoolError> {
        let permit = self.try_get_permit()?;
        self._get_connection(permit).await
    }
    pub async fn get_connection(&self) -> Result<ConnectionHandle, PoolError> {
        let permit = self.get_permit().await?;
        self._get_connection(permit).await
    }

    pub async fn len(&self) -> usize {
        self.0.connections.read().await.len()
    }
    pub async fn is_empty(&self) -> bool {
        self.0.connections.read().await.is_empty()
    }

    async fn _get_connection(
        &self,
        permit: OwnedSemaphorePermit,
    ) -> Result<ConnectionHandle, PoolError> {
        // Take read lock once to check length and potentially search
        let (create_conn, permit, needs_cleanup) = {
            let conns = self.0.connections.read().await;
            let len = conns.len();

            // If under max_connections, prefer creating new connection (eager growth)
            if len < self.0.config.max_connections {
                drop(conns);
                (true, permit, false)
            } else {
                // At capacity - search for usable connection
                let (permit, needs_cleanup) = match self._round_robin(&conns, permit).await {
                    Ok(h) => return Ok(h),
                    Err((permit, needs_cleanup)) => (permit, needs_cleanup),
                };
                drop(conns);
                // No usable connections found at capacity
                (false, permit, needs_cleanup)
            }
        }; // Read lock dropped here

        // Try to create new connection if under limit OR if cleanup is needed
        let permit = if create_conn || needs_cleanup {
            match self._create_connection(needs_cleanup, permit).await {
                Ok(h) => return Ok(h),
                Err(permit) => permit,
            }
        } else {
            permit
        };

        // Fallback: (connection creation failed or we're at capacity)
        let conns = self.0.connections.read().await;
        if conns.is_empty() {
            debug!("no connections available");
            return Err(PoolError::AllConnectionsBusy);
        }

        match self._round_robin(&conns, permit).await {
            Ok(h) => Ok(h),
            Err(_) => {
                debug!(
                    len = conns.len(),
                    max = self.0.config.max_connections,
                    "_get_connection failed: no usable connections",
                );

                Err(PoolError::AllConnectionsBusy)
            }
        }
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
            },
        })
    }

    pub async fn cleanup(&self) {
        self.0.cleanup().await;
    }

    #[inline]
    async fn _create_connection(
        &self,
        needs_cleanup: bool,
        permit: OwnedSemaphorePermit,
    ) -> Result<ConnectionHandle, OwnedSemaphorePermit> {
        let mut conns = self.0.connections.write().await;

        if needs_cleanup {
            info!("running cleanup");
            // Clean up any closing connections first
            conns.retain(|conn| !conn.inner.conn.is_closing());
        }

        // Re-check after acquiring write lock and cleanup to prevent race condition
        if conns.len() >= self.0.config.max_connections {
            debug!("already at max connections after acquiring write lock");
            // TODO should try to get another permit
            return Err(permit);
        }

        match self.create_connection().await {
            Ok(new_conn) => {
                let handle = ConnectionHandle {
                    _permit: permit,
                    conn: new_conn.inner.conn.clone(),
                    // pool weak ref
                    pool: Arc::downgrade(&self.0),
                };
                conns.push_front(new_conn);
                Ok(handle)
            }
            Err(err) => {
                warn!(%err, "failed to create connection, falling back to existing");
                // Fall through to try existing connections rather than failing immediately
                Err(permit)
            }
        }
    }

    #[inline]
    async fn _round_robin(
        &self,
        conns: &VecDeque<PoolConnection>,
        permit: OwnedSemaphorePermit,
    ) -> Result<ConnectionHandle, (OwnedSemaphorePermit, bool)> {
        let len = conns.len();
        let mut needs_cleanup = false;
        let now = Instant::now();

        // Round-robin selection among usable connections
        let start = self.0.round_robin.fetch_add(1, Ordering::Relaxed) % len;

        // Try from start to end
        for conn in conns.iter().skip(start) {
            if conn.inner.conn.is_usable(now) {
                return Ok(ConnectionHandle {
                    _permit: permit,
                    conn: conn.inner.conn.clone(),
                    pool: Arc::downgrade(&self.0),
                });
            } else {
                needs_cleanup = true;
                conn.inner.conn.set_closing();
            }
        }

        // Try from beginning to start
        for conn in conns.iter().take(start) {
            if conn.inner.conn.is_usable(now) {
                return Ok(ConnectionHandle {
                    _permit: permit,
                    conn: conn.inner.conn.clone(),
                    pool: Arc::downgrade(&self.0),
                });
            } else {
                needs_cleanup = true;
                conn.inner.conn.set_closing();
            }
        }
        Err((permit, needs_cleanup))
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
        let connection_count = conns.len();
        drop(conns);

        // Get pool-wide concurrency stats
        let available_permits = self.max_concurrent.available_permits();
        let handles_in_use = self.config.max_concurrent - available_permits;

        // Log pool stats
        info!(
            "Pool stats: connections={}, handles_in_use={}, available={}, max_connections={}, max_concurrent={}",
            connection_count,
            handles_in_use,
            available_permits,
            self.config.max_connections,
            self.config.max_concurrent
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
