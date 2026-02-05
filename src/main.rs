use std::time::Instant;
use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, bail};
use tokio::{net::TcpStream, sync::Semaphore, time::interval};
use tracing::{debug, info, level_filters::LevelFilter, warn};
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        self,
        format::{Format, PrettyFields},
    },
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use tcp_multi::{DnsQuery, SendError, TcpConnection, TcpConnectionConfig, msg::SerialMsg};

const DEFAULT_LOG_FORMAT: &str = "pretty";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportMode {
    TcpConnection,
    TcpStreamPerMessage,
}

#[derive(Debug)]
struct CliArgs {
    addr: SocketAddr,
    max_in_flight: usize,
    mode: TransportMode,
}

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

fn usage() -> &'static str {
    "usage: tcp_multi <tcp_addr:port> <max_in_flight> [--stream|--multi]"
}

fn parse_args() -> Result<CliArgs> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let args = args.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let (addr_str, max_in_flight_str, mode) = match args.as_slice() {
        ["-h"] | ["--help"] => bail!(usage()),
        [addr, max] | [addr, max, "--multi"] => (addr, max, TransportMode::TcpConnection),
        [addr, max, "--stream"] => (addr, max, TransportMode::TcpStreamPerMessage),
        _ => bail!(usage()),
    };

    let addr = addr_str
        .parse()
        .with_context(|| format!("invalid tcp addr: {addr_str}"))?;
    let max_in_flight = max_in_flight_str.parse().context("invalid max_in_flight")?;

    Ok(CliArgs {
        addr,
        max_in_flight,
        mode,
    })
}

async fn send_over_new_stream(
    addr: SocketAddr,
    msg: SerialMsg,
    reply_addr: SocketAddr,
    udp: Arc<tokio::net::UdpSocket>,
) -> Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);
    msg.write(&mut stream).await?;

    let mut reply = SerialMsg::read(&mut stream, addr).await?;
    reply.set_addr(reply_addr);
    udp.send_to(reply.bytes(), reply.addr()).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(None)?;

    let cli = parse_args()?;
    let addr = cli.addr;
    let total_in_flight = cli.max_in_flight;
    let mode = cli.mode;

    let config = TcpConnectionConfig {
        ka_idle: Some(1),
        ka_interval: Some(1),
        max_in_flight: None,
    };

    let udp = Arc::new(tokio::net::UdpSocket::bind("[::]:9953").await?);
    info!(?addr, "udp socket bound");

    let mut conn: Option<Arc<TcpConnection>> = None;
    let in_flight = Arc::new(Semaphore::new(total_in_flight));

    {
        let in_flight = in_flight.clone();
        tokio::spawn(async move {
            let mut ticker = interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                let available = in_flight.available_permits();
                let used = total_in_flight.saturating_sub(available);
                if used > 0 {
                    info!(used, total_in_flight, "in-flight tasks");
                }
            }
        });
    }

    loop {
        let msg = match SerialMsg::recv(&udp).await {
            Ok(msg) => msg,
            Err(err) => {
                warn!(%err, "udp recv failed");
                continue;
            }
        };
        let reply_addr = msg.addr();

        debug!(msg = ?msg.to_message());
        let permit = match in_flight.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                warn!("semaphore closed, dropping task");
                continue;
            }
        };

        match mode {
            TransportMode::TcpConnection => {
                if conn
                    .as_ref()
                    // !can_reuse()
                    .is_none_or(|existing| !existing.is_usable(Instant::now()))
                {
                    match TcpConnection::new(addr, config).await {
                        Ok(new_conn) => {
                            info!(?addr, "tcp connection established");
                            conn = Some(new_conn);
                        }
                        Err(err) => {
                            warn!(%err, "tcp connect failed");
                            continue;
                        }
                    }
                }

                let Some(conn) = conn.clone() else {
                    warn!("tcp connection missing after connect attempt");
                    continue;
                };

                tokio::spawn({
                    let udp = udp.clone();
                    async move {
                        let (reply, rx) = tokio::sync::oneshot::channel();
                        match conn
                            .send(DnsQuery {
                                to_send: msg,
                                reply,
                            })
                            .await
                        {
                            Ok(_) => {
                                // succeeded
                            }
                            Err(SendError::Closed { query }) => {
                                // channel closed, so send on another conn
                            }
                            Err(err) => {
                                // unrecoverable kind of error
                                return;
                            }
                        }
                        match rx.await {
                            Ok(mut reply) => {
                                // restore reply addr
                                reply.set_addr(reply_addr);
                                if let Err(err) = udp.send_to(reply.bytes(), reply.addr()).await {
                                    warn!(%err, "udp reply failed");
                                }
                            }
                            Err(err) => {
                                warn!(%err, "oneshot tcp send failed");
                            }
                        };
                        drop(permit);
                    }
                });
            }
            TransportMode::TcpStreamPerMessage => {
                tokio::spawn({
                    let udp = udp.clone();
                    async move {
                        if let Err(err) = send_over_new_stream(addr, msg, reply_addr, udp).await {
                            warn!(%err, "tcpstream per-message failed");
                        }
                        drop(permit);
                    }
                });
            }
        }
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
