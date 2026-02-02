use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::Result;
use tracing::{debug, trace};

use crate::TcpConnection;

struct ConnectionPool {
    /// Active connections per backend
    active: RwLock<HashMap<SocketAddr, Vec<Arc<TcpConnection>>>>,
    /// Idle connections per backend
    idle: RwLock<HashMap<SocketAddr, Vec<Arc<TcpConnection>>>>,
    /// Maximum idle connections per backend
    max_idle_conns: usize,
    /// Maximum idle time before cleanup
    max_idle_time: Duration,
    max_in_flight_per: Option<usize>,
    ka_idle: Option<u64>,
    ka_interval: Option<u64>,
}

impl ConnectionPool {
    pub fn new(
        max_idle_conns: usize,
        max_idle_time: Duration,
        max_in_flight_per: Option<usize>,
        ka_idle: Option<u64>,
        ka_interval: Option<u64>,
    ) -> Self {
        Self {
            active: RwLock::new(HashMap::new()),
            idle: RwLock::new(HashMap::new()),
            max_idle_conns,
            max_idle_time,
            max_in_flight_per,
            ka_idle,
            ka_interval,
        }
    }
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<Arc<TcpConnection>> {
        let now = Instant::now();

        // Try to get from idle pool first
        {
            let mut idle_map = self.idle.write().unwrap();
            if let Some(connections) = idle_map.get_mut(&addr) {
                while let Some(conn) = connections.pop() {
                    if conn.is_usable(now) {
                        trace!(%addr, "reusing idle connection");

                        self.active
                            .write()
                            .unwrap()
                            .entry(addr)
                            .or_default()
                            .push(Arc::clone(&conn));

                        return Ok(conn);
                    } else if conn.will_be_reusable() {
                        trace!(
                            %addr,
                            "connection busy but will be reusable, keeping in idle pool",
                        );
                        connections.insert(0, conn);
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
            let active_map = self.active.read().unwrap();
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
        let conn = TcpConnection::new(addr, self.ka_idle, self.ka_interval, self.max_in_flight_per)
            .await?;

        self.active
            .write()
            .unwrap()
            .entry(addr)
            .or_default()
            .push(Arc::clone(&conn));

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
            let mut active_map = self.active.write().unwrap();
            if let Some(connections) = active_map.get_mut(&addr) {
                connections.retain(|c| !Arc::ptr_eq(c, &conn));
            }
        }

        // Add to idle
        {
            let mut idle_map = self.idle.write().unwrap();
            let connections = idle_map.entry(addr).or_default();

            if connections.len() >= self.max_idle_conns {
                if let Some(old) = connections.pop() {
                    trace!(%addr, "evicting oldest idle connection");
                    // Explicitly shutdown before dropping
                    // old.shutdown().await;
                }
            }

            connections.insert(0, conn);
            trace!(%addr,"moved connection to idle pool");
        }
    }

    pub fn remove_connection(&self, conn: &Arc<TcpConnection>) {
        let addr = conn.addr;

        {
            let mut active_map = self.active.write().unwrap();
            if let Some(connections) = active_map.get_mut(&addr) {
                connections.retain(|c| !Arc::ptr_eq(c, conn));
            }
        }

        {
            let mut idle_map = self.idle.write().unwrap();
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
            let mut idle_map = self.idle.write().unwrap();

            for (addr, connections) in idle_map.iter_mut() {
                let original_count = connections.len();

                // Collect connections to shutdown
                let mut to_shutdown = Vec::new();

                connections.retain(|conn| {
                    // Use sync method to get last activity
                    let last_activity = conn.last_activity();
                    let age = now.duration_since(last_activity);

                    if age > self.max_idle_time {
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
            let mut active_map = self.active.write().unwrap();

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
        let active_count: usize = self.active.read().unwrap().values().map(|v| v.len()).sum();
        let idle_count: usize = self.idle.read().unwrap().values().map(|v| v.len()).sum();

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
