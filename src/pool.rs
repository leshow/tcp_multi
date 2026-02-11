use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, OnceLock, RwLock, Weak},
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    task::AbortHandle,
};
use tracing::{debug, trace};

use crate::{TcpConnection, TcpConnectionConfig};

pub struct ConnectionPool(Arc<PoolInner>);

pub struct PoolInner {
    /// connections per backend
    connections: RwLock<VecDeque<PoolConnection>>,
    addr: SocketAddr,
    config: PoolConfig,
    permit_available: Arc<Notify>,
    _cleanup: OnceLock<AbortHandle>,
}

#[derive(Clone)]
pub struct PoolConnection {
    inner: ConnectionInner,
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
            max_in_flight_per: None,
            keepalive: KeepaliveConfig::default(),
        }
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        // Try to upgrade weak reference
        if let Some(pool) = self.pool.upgrade() {
            pool.permit_available.notify_one();
        }
        // If pool is gone, nothing to notify
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        if let Some(handle) = &self.0._cleanup.get() {
            handle.abort();
        }
    }
}

impl ConnectionPool {
    pub fn new(addr: SocketAddr, config: PoolConfig) -> Self {
        let cleanup_interval = config.cleanup_interval;
        let this = Self(Arc::new(PoolInner {
            connections: RwLock::new(VecDeque::new()),
            addr,
            config,
            permit_available: Arc::new(Notify::new()),
            _cleanup: OnceLock::new(),
        }));

        let abort_handle = tokio::spawn({
            let weak = Arc::downgrade(&this.0);
            async move {
                loop {
                    tokio::time::sleep(cleanup_interval).await;
                    match weak.upgrade() {
                        Some(pool) => pool.cleanup(),
                        None => break, // Pool was dropped, exit
                    }
                }
            }
        })
        .abort_handle();
        // set handle
        let _ = this.0._cleanup.set(abort_handle);

        this
    }

    pub async fn try_get_connection(&self) -> Result<ConnectionHandle> {
        // Try to find connection with available permit (read lock)
        let len = {
            let conns = self.0.connections.read().unwrap();
            let now = Instant::now();
            for pool_conn in conns.iter() {
                // check if usable first?
                if pool_conn.inner.conn.is_usable(now) {
                    // get a permit
                    if let Ok(permit) = pool_conn.inner.max_handles.clone().try_acquire_owned() {
                        return Ok(ConnectionHandle {
                            _permit: permit,
                            conn: pool_conn.inner.conn.clone(),
                            // pool weak ref
                            pool: Arc::downgrade(&self.0),
                        });
                    }
                }
                // @TODO else set_closing?
            }
            // All connections are at capacity
            conns.len()
        };

        // Try to create new connection if under limit (write lock)
        if len < self.0.config.max_connections {
            // @TODO should I switch to async rwlock so this connection is created after write lock obtained?
            let new_conn = self.create_connection().await?;

            let mut conns = self.0.connections.write().unwrap();

            // should never fail
            let permit = new_conn.inner.max_handles.clone().try_acquire_owned()?;
            let handle = ConnectionHandle {
                _permit: permit,
                conn: new_conn.inner.conn.clone(),
                // pool weak ref
                pool: Arc::downgrade(&self.0),
            };
            conns.push_back(new_conn);
            return Ok(handle);
            // At max connections
        }
        Err(anyhow::Error::msg("failed to get connection"))
    }

    pub async fn get_connection(&self) -> Result<ConnectionHandle> {
        loop {
            match self.try_get_connection().await {
                Ok(msg) => return Ok(msg),
                Err(_) => {
                    // All connections busy and at capacity - wait for notification
                    self.0.permit_available.notified().await;
                    // Loop back and try again
                }
            }
        }
    }

    async fn create_connection(&self) -> Result<PoolConnection> {
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

    pub fn cleanup(&self) {
        self.0.cleanup();
    }
}

impl PoolInner {
    fn cleanup(&self) {
        let now = Instant::now();

        // Clean up idle connections
        {
            let mut idle = self.connections.write().unwrap();

            let original_count = idle.len();

            idle.retain(|conn| {
                // Use sync method to get last activity
                let last_activity = conn.inner.conn.last_read();
                let age = now.duration_since(last_activity);

                if age > self.config.max_idle_time {
                    trace!("removing idle connection (age: {:?})", age);
                    // to_shutdown.push(Arc::clone(conn));
                    conn.inner.conn.set_closing();
                    false
                } else if !conn.inner.conn.will_be_reusable() {
                    trace!("removing non-reusable connection");
                    conn.inner.conn.set_closing();
                    false
                } else {
                    true
                }
            });

            let removed = original_count - idle.len();
            if removed > 0 {
                debug!("removed {} idle connection(s)", removed);
            }
        }
    }
}
