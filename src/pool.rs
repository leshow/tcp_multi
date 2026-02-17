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
            max_concurrent: 1000,
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

    async fn _get_connection(
        &self,
        permit: OwnedSemaphorePermit,
    ) -> Result<ConnectionHandle, PoolError> {
        // Check if we should grow the pool first (read lock to check size)
        let should_grow = {
            let conns = self.0.connections.read().await;
            conns.len() < self.0.config.max_connections
        };

        // If under max_connections, try to create a new connection immediately
        if should_grow {
            let mut conns = self.0.connections.write().await;
            
            // Double-check after acquiring write lock
            if conns.len() < self.0.config.max_connections {
                match self.create_connection().await {
                    Ok(new_conn) => {
                        let handle = ConnectionHandle {
                            _permit: permit,
                            conn: new_conn.inner.conn.clone(),
                            pool: Arc::downgrade(&self.0),
                        };
                        conns.push_front(new_conn);
                        return Ok(handle);
                    }
                    Err(err) => {
                        warn!(%err, "failed to create connection, falling back to existing");
                        // Fall through to try existing connections
                    }
                }
            }
            // If creation failed or someone else created one, fall through to round-robin
        }

        // Get connection using round-robin (read lock only)
        let (len, cleanup) = {
            let conns = self.0.connections.read().await;
            let now = Instant::now();
            let mut needs_cleanup = false;

            if conns.is_empty() {
                // Pool is empty - need to create first connection
                (0, false)
            } else {
                // Round-robin selection among usable connections
                let start = self.0.round_robin.fetch_add(1, Ordering::Relaxed) % conns.len();

                // Try from start to end
                for i in start..conns.len() {
                    if conns[i].inner.conn.is_usable(now) {
                        return Ok(ConnectionHandle {
                            _permit: permit,
                            conn: conns[i].inner.conn.clone(),
                            pool: Arc::downgrade(&self.0),
                        });
                    } else {
                        needs_cleanup = true;
                        conns[i].inner.conn.set_closing();
                    }
                }

                // Try from beginning to start
                for i in 0..start {
                    if conns[i].inner.conn.is_usable(now) {
                        return Ok(ConnectionHandle {
                            _permit: permit,
                            conn: conns[i].inner.conn.clone(),
                            pool: Arc::downgrade(&self.0),
                        });
                    } else {
                        needs_cleanup = true;
                        conns[i].inner.conn.set_closing();
                    }
                }

                // No usable connections found
                (conns.len(), needs_cleanup)
            }
        };

        // Try to create new connection if under limit OR if all existing connections are unusable
        if len < self.0.config.max_connections || cleanup {
            let mut conns = self.0.connections.write().await;

            if cleanup {
                info!("running cleanup");
                // Clean up any closing connections first
                conns.retain(|conn| !conn.inner.conn.is_closing());
            }

            // Re-check after acquiring write lock and cleanup to prevent race condition
            if conns.len() >= self.0.config.max_connections {
                debug!("already at max connections after acquiring write lock");
                return Err(PoolError::AllConnectionsBusy);
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
                    return Ok(handle);
                }
                Err(err) => {
                    warn!(%err, "failed to create connection");
                    return Err(err);
                }
            }
        }
        debug!(
            "try_get_connection failed: len={}, max={}",
            len, self.0.config.max_connections
        );

        Err(PoolError::AllConnectionsBusy)
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
