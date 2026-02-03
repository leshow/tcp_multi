use std::{
    collections::HashMap,
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::Result;
use tracing::{debug, trace};

use crate::TcpConnection;

pub struct ConnectionPool(Arc<PoolInner>);

struct PoolInner {
    /// Active connections per backend
    active: RwLock<HashMap<SocketAddr, VecDeque<Arc<TcpConnection>>>>,
    /// Idle connections per backend
    idle: RwLock<HashMap<SocketAddr, VecDeque<Arc<TcpConnection>>>>,
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
            max_idle_time: Duration::from_secs(300),
            cleanup_interval: Duration::from_secs(60),
            max_in_flight_per: None,
            keepalive: KeepaliveConfig::default(),
        }
    }
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        Self(Arc::new(PoolInner {
            active: RwLock::new(HashMap::new()),
            idle: RwLock::new(HashMap::new()),
            config,
        }))
    }
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<Arc<TcpConnection>> {
        let now = Instant::now();

        // Try to get from idle pool first
        {
            let mut idle_map = self.0.idle.write().unwrap();
            if let Some(connections) = idle_map.get_mut(&addr) {
                while let Some(conn) = connections.pop_back() {
                    if conn.is_usable(now) {
                        trace!(%addr, "reusing idle connection");

                        self.0
                            .active
                            .write()
                            .unwrap()
                            .entry(addr)
                            .or_default()
                            .push_back(Arc::clone(&conn));

                        return Ok(conn);
                    } else if conn.will_be_reusable() {
                        trace!(
                            %addr,
                            "connection busy but will be reusable, keeping in idle pool",
                        );
                        connections.push_front(conn);
                        break;
                    } else {
                        debug!(
                            %addr,
                            "idle connection not reusable, discarding",
                        );
                        // connection will be dropped here, triggering Drop::drop
                        // killing read/write tasks, etc
                        // should this use a cancellation token?
                    }
                }
            }
        }

        // try to multiplex on an existing active connection
        {
            let active_map = self.0.active.read().unwrap();
            if let Some(connections) = active_map.get(&addr) {
                for conn in connections.iter().rev() {
                    if conn.is_usable(now) {
                        trace!(
                            %addr,
                            "reusing active connection",
                        );
                        return Ok(Arc::clone(conn));
                    }
                }
            }
        }

        // No usable connection found, create a new one
        debug!(%addr, "creating new connection");
        let conn = TcpConnection::new(
            addr,
            self.0.config.keepalive.idle,
            self.0.config.keepalive.interval,
            self.0.config.max_in_flight_per,
        )
        .await?;

        self.0
            .active
            .write()
            .unwrap()
            .entry(addr)
            .or_default()
            .push_back(Arc::clone(&conn));

        Ok(conn)
    }

    pub fn move_to_idle(&self, conn: Arc<TcpConnection>) {
        let addr = conn.addr;

        if !conn.is_idle() || !conn.will_be_reusable() {
            trace!(%addr, "connection not idle or not reusable, removing");
            self.remove_connection(&conn);
            return;
        }

        // Remove from active
        {
            let mut active_map = self.0.active.write().unwrap();
            if let Some(connections) = active_map.get_mut(&addr) {
                connections.retain(|c| !Arc::ptr_eq(c, &conn));
            }
        }

        // Add to idle
        {
            let mut idle_map = self.0.idle.write().unwrap();
            let connections = idle_map.entry(addr).or_default();

            if connections.len() >= self.0.config.max_idle_per {
                if let Some(old) = connections.pop_back() {
                    trace!(%addr, "evicting oldest idle connection");
                    // Explicitly shutdown before dropping
                    // old.shutdown().await;
                }
            }

            connections.push_front(conn);
            trace!(%addr,"moved connection to idle pool");
        }
    }

    pub fn remove_connection(&self, conn: &Arc<TcpConnection>) {
        let addr = conn.addr;

        {
            let mut active_map = self.0.active.write().unwrap();
            if let Some(connections) = active_map.get_mut(&addr) {
                connections.retain(|c| !Arc::ptr_eq(c, conn));
            }
        }

        {
            let mut idle_map = self.0.idle.write().unwrap();
            if let Some(connections) = idle_map.get_mut(&addr) {
                connections.retain(|c| !Arc::ptr_eq(c, conn));
            }
        }

        // how to drop connection? it's arc'd
        debug!("Removed connection to {}", addr);
    }

    pub fn cleanup(&self) {
        let now = Instant::now();

        // Clean up idle connections
        {
            let mut idle_map = self.0.idle.write().unwrap();

            for (addr, connections) in idle_map.iter_mut() {
                let original_count = connections.len();

                // Collect connections to shutdown
                let mut to_shutdown = Vec::new();

                connections.retain(|conn| {
                    // Use sync method to get last activity
                    let last_activity = conn.last_read();
                    let age = now.duration_since(last_activity);

                    if age > self.0.config.max_idle_time {
                        trace!("removing idle connection to {} (age: {:?})", addr, age);
                        to_shutdown.push(Arc::clone(conn));
                        false
                    } else if !conn.will_be_reusable() {
                        trace!("removing non-reusable connection to {}", addr);
                        to_shutdown.push(Arc::clone(conn));
                        false
                    } else {
                        true
                    }
                });

                // Shutdown outside of the retain closure
                for conn in to_shutdown {
                    // conn.shutdown().await;
                }

                let removed = original_count - connections.len();
                if removed > 0 {
                    trace!("removed {} idle connection(s) to {}", removed, addr);
                }
            }

            idle_map.retain(|_, v| !v.is_empty());
        }

        // Clean up dead active connections
        {
            let mut active_map = self.0.active.write().unwrap();

            for connections in active_map.values_mut() {
                let mut to_shutdown = Vec::new();

                connections.retain(|conn| {
                    if conn.will_be_reusable() {
                        trace!(
                            addr = %conn.addr,
                            "Removing dead active connection",
                        );
                        to_shutdown.push(Arc::clone(conn));
                        false
                    } else {
                        true
                    }
                });

                for conn in to_shutdown {
                    // conn.shutdown().await;
                }
            }

            active_map.retain(|_, v| !v.is_empty());
        }
    }
    /// Get pool statistics
    async fn stats(&self) -> PoolStats {
        let active_count: usize = self
            .0
            .active
            .read()
            .unwrap()
            .values()
            .map(|v| v.len())
            .sum();
        let idle_count: usize = self.0.idle.read().unwrap().values().map(|v| v.len()).sum();

        PoolStats {
            active_connections: active_count,
            idle_connections: idle_count,
        }
    }
}

#[derive(Debug)]
struct PoolStats {
    active_connections: usize,
    idle_connections: usize,
}
