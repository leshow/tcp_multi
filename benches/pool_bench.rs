// use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
// use std::net::SocketAddr;
// use std::sync::Arc;
// use std::time::Duration;
// use tcp_multi::pool::{ConnectionPool, KeepaliveConfig, PoolConfig};
// use tokio::runtime::Runtime;

// fn bench_get_connection_sequential(c: &mut Criterion) {
//     let mut group = c.benchmark_group("get_connection_sequential");

//     for pool_size in [0, 5, 10, 50].iter() {
//         group.throughput(Throughput::Elements(1));
//         group.bench_with_input(
//             BenchmarkId::new("existing_connections", pool_size),
//             pool_size,
//             |b, size| {
//                 let rt = Runtime::new().unwrap();
//                 b.to_async(&rt).iter(|| async {
//                     let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
//                     let pool = Arc::new(ConnectionPool::new(
//                         addr,
//                         PoolConfig {
//                             max_idle_time: Duration::from_secs(5),
//                             cleanup_interval: Duration::from_secs(300),
//                             max_in_flight_per: Some(10),
//                             keepalive: KeepaliveConfig::default(),
//                             max_concurrent_per_conn: 0,
//                             max_connections: 0,
//                         },
//                     ));

//                     // Pre-populate with connections (if we can)
//                     // For now, just benchmark the lookup path
//                     match pool.get_connection().await {
//                         Ok(_conn) => {
//                             // Connection created
//                         }
//                         Err(_) => {
//                             // Can't actually connect in benchmark, that's ok
//                             // We're measuring the pool overhead
//                         }
//                     }
//                 });
//             },
//         );
//     }
//     group.finish();
// }

// fn bench_get_connection_concurrent(c: &mut Criterion) {
//     let mut group = c.benchmark_group("get_connection_concurrent");
//     group.sample_size(50); // Reduce sample size for concurrent tests

//     for threads in [2, 4, 8, 16].iter() {
//         group.throughput(Throughput::Elements(*threads as u64));
//         group.bench_with_input(
//             BenchmarkId::new("concurrent_threads", threads),
//             threads,
//             |b, &num_tasks| {
//                 let rt = Runtime::new().unwrap();
//                 b.to_async(&rt).iter(|| async move {
//                     let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
//                     let pool = Arc::new(ConnectionPool::new(addr, PoolConfig::default()));

//                     // Spawn concurrent tasks trying to get connections
//                     let mut handles = Vec::new();
//                     for _ in 0..num_tasks {
//                         let pool_clone = pool.clone();
//                         let handle = tokio::spawn(async move {
//                             let _ = pool_clone.get_connection().await;
//                         });
//                         handles.push(handle);
//                     }

//                     // Wait for all to complete
//                     for handle in handles {
//                         let _ = handle.await;
//                     }
//                 });
//             },
//         );
//     }
//     group.finish();
// }

// fn bench_stats(c: &mut Criterion) {
//     let mut group = c.benchmark_group("stats");

//     let rt = Runtime::new().unwrap();
//     group.bench_function("pool_stats", |b| {
//         b.to_async(&rt).iter(|| async {
//             let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
//             let pool = ConnectionPool::new(addr, PoolConfig::default());

//             let stats = pool.stats().await;
//             black_box(stats);
//         });
//     });

//     group.finish();
// }

// fn bench_concurrent_stats(c: &mut Criterion) {
//     let mut group = c.benchmark_group("concurrent_stats");
//     group.sample_size(50);

//     for threads in [2, 4, 8, 16].iter() {
//         group.throughput(Throughput::Elements(*threads as u64));
//         group.bench_with_input(
//             BenchmarkId::new("threads", threads),
//             threads,
//             |b, &num_threads| {
//                 let rt = Runtime::new().unwrap();
//                 b.to_async(&rt).iter(|| async move {
//                     let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
//                     let pool = Arc::new(ConnectionPool::new(
//                         addr,
//                         PoolConfig {
//                             // max_active_per: 100,
//                             max_idle_per: 50,
//                             max_idle_time: Duration::from_secs(5),
//                             cleanup_interval: Duration::from_secs(300),
//                             max_in_flight_per: Some(10),
//                             keepalive: KeepaliveConfig::default(),
//                         },
//                     ));

//                     let mut handles = Vec::new();
//                     for _ in 0..num_threads {
//                         let pool_clone = pool.clone();
//                         let handle = tokio::spawn(async move {
//                             let stats = pool_clone.stats().await;
//                             black_box(stats);
//                         });
//                         handles.push(handle);
//                     }

//                     for handle in handles {
//                         let _ = handle.await;
//                     }
//                 });
//             },
//         );
//     }

//     group.finish();
// }

// criterion_group!(
//     benches,
//     bench_get_connection_sequential,
//     bench_get_connection_concurrent,
//     bench_stats,
//     bench_concurrent_stats,
// );
// criterion_main!(benches);
// //
