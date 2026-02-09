use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::task::{AbortHandle, JoinSet};
use tracing::{debug, trace};

use crate::{TcpConnection, TcpConnectionConfig};

pub struct ConnectionPool(Arc<PoolInner>);

struct PoolInner {
    /// Active connections per backend
    active: RwLock<VecDeque<Arc<TcpConnection>>>,
    /// Idle connections per backend
    idle: RwLock<VecDeque<Arc<TcpConnection>>>,
    _cleanup: Option<AbortHandle>,
    addr: SocketAddr,
    config: PoolConfig,
}

/// Configuration for the connection pool
#[derive(Clone, Copy, Debug)]
pub struct PoolConfig {
    /// Maximum idle connections per downstream backend
    pub max_idle_per: usize,
    /// Maximum time a connection can be idle before cleanup
    pub max_idle_time: Duration,
    /// Interval between cleanup runs
    pub cleanup_interval: Duration,
    /// Maximum concurrent connections per downstream (0 = unlimited)
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
            max_idle_per: 10,
            max_idle_time: Duration::from_secs(5),
            cleanup_interval: Duration::from_secs(30),
            max_in_flight_per: None,
            keepalive: KeepaliveConfig::default(),
        }
    }
}

impl ConnectionPool {
    pub fn new(addr: SocketAddr, config: PoolConfig) -> Self {
        let mut inner = PoolInner {
            active: RwLock::new(VecDeque::new()),
            idle: RwLock::new(VecDeque::new()),
            addr,
            config,
            _cleanup: None,
        };
        inner._cleanup = Some(
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(config.cleanup_interval).await;
                }
            })
            .abort_handle(),
        );

        Self(Arc::new(inner))
    }
    pub async fn get_connection(&self) -> Result<Arc<TcpConnection>> {
        let now = Instant::now();

        // Try to get from idle pool first
        {
            let mut idle = self.0.idle.write().unwrap();
            while let Some(conn) = idle.pop_back() {
                if conn.is_usable(now) {
                    trace!("reusing idle connection");
                    self.0.active.write().unwrap().push_back(Arc::clone(&conn));
                    return Ok(conn);
                } else if conn.will_be_reusable() {
                    trace!("connection busy but will be reusable, keeping in idle pool",);
                    idle.push_front(conn);
                    break;
                } else {
                    debug!("idle connection not reusable, discarding",);
                    // connection will be dropped here, triggering Drop::drop
                    // killing read/write tasks, etc
                    // should this use a cancellation token?
                    // alternative: set_closing
                    conn.set_closing();
                }
            }
        }

        // try to multiplex on an existing active connection
        {
            let active = self.0.active.read().unwrap();
            for conn in active.iter().rev() {
                if conn.is_usable(now) {
                    trace!("reusing active connection");
                    return Ok(Arc::clone(conn));
                }
            }
        }

        // No usable connection found, create a new one
        debug!("creating new connection");
        let conn = TcpConnection::new(
            self.0.addr,
            TcpConnectionConfig {
                ka_idle: self.0.config.keepalive.idle,
                ka_interval: self.0.config.keepalive.interval,
                max_in_flight: self.0.config.max_in_flight_per,
            },
        )
        .await?;

        self.0.active.write().unwrap().push_back(Arc::clone(&conn));

        Ok(conn)
    }

    pub fn move_to_idle(&self, conn: Arc<TcpConnection>) {
        let addr = conn.addr;

        if !conn.is_idle() || !conn.will_be_reusable() {
            trace!("connection not idle or not reusable, removing");
            self.remove_connection(&conn);
            return;
        }

        // Remove from active
        {
            let mut active = self.0.active.write().unwrap();
            active.retain(|c| !Arc::ptr_eq(c, &conn));
        }

        // Add to idle
        {
            let mut idle = self.0.idle.write().unwrap();

            if idle.len() >= self.0.config.max_idle_per
                && let Some(old) = idle.pop_back()
            {
                trace!("evicting oldest idle connection");
                // Explicitly shutdown before dropping
                // old.shutdown().await;
                old.set_closing();
            }

            idle.push_front(conn);
            trace!("moved connection to idle pool");
        }
    }

    pub fn remove_connection(&self, conn: &Arc<TcpConnection>) {
        let addr = conn.addr;

        {
            let mut active = self.0.active.write().unwrap();
            active.retain(|c| !Arc::ptr_eq(c, conn));
        }

        {
            let mut idle = self.0.idle.write().unwrap();
            idle.retain(|c| !Arc::ptr_eq(c, conn));
        }

        // how to drop connection? it's arc'd
        debug!("Removed connection to {}", addr);
    }

    pub fn cleanup(&self) {
        let now = Instant::now();

        // Clean up idle connections
        {
            let mut idle = self.0.idle.write().unwrap();

            let original_count = idle.len();

            // Collect connections to shutdown
            // let mut to_shutdown = Vec::new();

            idle.retain(|conn| {
                // Use sync method to get last activity
                let last_activity = conn.last_read();
                let age = now.duration_since(last_activity);

                if age > self.0.config.max_idle_time {
                    trace!("removing idle connection (age: {:?})", age);
                    // to_shutdown.push(Arc::clone(conn));
                    conn.set_closing();
                    false
                } else if !conn.will_be_reusable() {
                    trace!("removing non-reusable connection");
                    // to_shutdown.push(Arc::clone(conn));
                    conn.set_closing();
                    false
                } else {
                    true
                }
            });

            // // Shutdown outside of the retain closure
            // for conn in to_shutdown {
            //     // conn.shutdown().await;
            //     conn.set_closing();
            // }

            let removed = original_count - idle.len();
            if removed > 0 {
                trace!("removed {} idle connection(s)", removed);
            }
        }

        // Clean up dead active connections
        {
            let mut active = self.0.active.write().unwrap();
            // let mut to_shutdown = Vec::new();

            active.retain(|conn| {
                if conn.will_be_reusable() {
                    trace!(
                        addr = %conn.addr,
                        "Removing dead active connection",
                    );
                    // to_shutdown.push(Arc::clone(conn));
                    conn.set_closing();
                    false
                } else {
                    true
                }
            });

            // for conn in to_shutdown {
            //     // conn.shutdown().await;
            // }
        }
    }
    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let active_count: usize = self.0.active.read().unwrap().iter().count();
        let idle_count: usize = self.0.idle.read().unwrap().iter().count();

        PoolStats {
            active_connections: active_count,
            idle_connections: idle_count,
        }
    }
}

#[derive(Debug)]
pub struct PoolStats {
    pub active_connections: usize,
    pub idle_connections: usize,
}
