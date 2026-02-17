use std::{net::SocketAddr, ops::Deref, sync::Arc, time::Instant};

use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult};
use tracing::info;

use crate::{TcpConnection, TcpConnectionConfig, TcpConnectionError};

pub type DeadpoolConnection = Object<TcpConnectionManager>;

/// Wrapper to make TcpConnection work with deadpool
#[derive(Clone)]
pub struct PooledConnection(Arc<TcpConnection>);

impl Deref for PooledConnection {
    type Target = TcpConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Manager for creating and recycling TcpConnection instances
pub struct TcpConnectionManager {
    addr: SocketAddr,
    config: TcpConnectionConfig,
}

impl TcpConnectionManager {
    pub fn new(addr: SocketAddr, config: TcpConnectionConfig) -> Self {
        Self { addr, config }
    }
}

impl Manager for TcpConnectionManager {
    type Type = PooledConnection;
    type Error = TcpConnectionError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        info!(?self.addr, "creating new deadpool connection");
        let conn = TcpConnection::new(self.addr, self.config).await?;
        Ok(PooledConnection(Arc::new(conn)))
    }

    async fn recycle(
        &self,
        conn: &mut Self::Type,
        _metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        // Check if connection is still usable
        if conn.0.is_usable(Instant::now()) {
            Ok(())
        } else {
            // Connection is not usable, needs to be recreated
            Err(deadpool::managed::RecycleError::message(
                "connection not usable",
            ))
        }
    }
}

pub fn create_pool(
    addr: SocketAddr,
    config: TcpConnectionConfig,
    max_size: usize,
) -> Pool<TcpConnectionManager> {
    let manager = TcpConnectionManager::new(addr, config);
    Pool::builder(manager).max_size(max_size).build().unwrap()
}
