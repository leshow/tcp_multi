use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{info, level_filters::LevelFilter, warn};
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        self,
        format::{Format, PrettyFields},
    },
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use tcp_multi::{BUF_SIZE, DnsQuery, TcpConnection, TcpConnectionConfig, msg::SerialMsg};

const DEFAULT_LOG_FORMAT: &str = "pretty";

#[derive(Clone, Copy)]
pub struct EnvVars {
    pub log_fmt: &'static str,
    pub log_lvl: &'static str,
}

impl Default for EnvVars {
    fn default() -> Self {
        Self {
            log_fmt: "LOG_FORMAT",
            log_lvl: "RUST_LOG",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(None)?;

    let addr_str = std::env::args()
        .nth(1)
        .context("usage: tcp_multi <tcp_addr:port>")?;
    let addr = addr_str
        .parse()
        .with_context(|| format!("invalid tcp addr: {addr_str}"))?;

    let config = TcpConnectionConfig {
        chan_size: BUF_SIZE,
        ka_idle: None,
        ka_interval: None,
        max_in_flight: None,
    };
    let conn = TcpConnection::new(addr, config).await?;

    let udp = Arc::new(tokio::net::UdpSocket::bind("[::]:9953").await?);
    info!(
        port = "9953",
        ?addr,
        "udp socket bound, tcp connection established"
    );

    loop {
        let msg = match SerialMsg::recv(&udp).await {
            Ok(msg) => msg,
            Err(err) => {
                warn!(%err, "udp recv failed");
                continue;
            }
        };

        tokio::spawn({
            let udp = udp.clone();
            let conn = conn.clone();
            async move {
                let (reply, rx) = tokio::sync::oneshot::channel();
                let query = DnsQuery {
                    to_send: msg,
                    reply,
                };
                if let Err(err) = conn.send(query).await {
                    warn!(%err, "tcp send failed");
                    return;
                }
                match rx.await {
                    Ok(reply) => {
                        if let Err(err) = udp.send_to(reply.bytes(), reply.addr()).await {
                            warn!(%err, "udp reply failed");
                        }
                    }
                    Err(err) => {
                        warn!(%err, "oneshot tcp send failed");
                    }
                };
            }
        });
    }
}

/// start tracing subscriber. env vars `RUST_LOG` and `LOG_FORMAT` used unless `vars` specifies alternate names
pub fn init_tracing(vars: Option<EnvVars>) -> Result<()> {
    let vars = vars.unwrap_or_default();
    let log_fmt: String =
        std::env::var(vars.log_fmt).unwrap_or_else(|_| DEFAULT_LOG_FORMAT.to_owned());
    // this might be unexpected? could build filter without setting RUST_LOG if it's not used
    if let Ok(lvl) = std::env::var(vars.log_lvl) {
        unsafe {
            std::env::set_var("RUST_LOG", lvl);
        }
    }
    // Log level comes from RUST_LOG now
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive("hyper=off".parse()?)
        .add_directive("hickory_resolver=error".parse()?)
        .add_directive("hickory_proto=error".parse()?);

    let log_filter = filter.to_string();
    match &log_fmt[..] {
        // default for production
        "json" => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().json())
                .try_init()?;
        }
        "compact" => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().compact().with_ansi(false).with_level(true))
                .try_init()?;
        }
        // default for debug & pipelines
        _ => {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .event_format(Format::default().with_source_location(false))
                        .fmt_fields(PrettyFields::new()),
                )
                .try_init()?;
        }
    }
    info!(log_filter, log_fmt, "initialized tracing");
    Ok(())
}
