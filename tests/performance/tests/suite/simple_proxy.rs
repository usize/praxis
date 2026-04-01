//! Simple proxy throughput benchmarks.
//!
//! Measures baseline proxy performance with a minimal config:
//! one listener, one catch-all route, one backend. No extra
//! filters beyond the implicit router and load_balancer.

use praxis_core::config::Config;
use praxis_test_utils::{free_port, start_backend, start_proxy};

use crate::helpers::{BenchConfig, report_results, run_get_benchmark};

// -----------------------------------------------------------------------------
// Serial Throughput
// -----------------------------------------------------------------------------

#[test]
fn bench_simple_proxy_serial() {
    let backend_port = start_backend("ok");
    let proxy_port = free_port();
    let yaml = praxis_test_utils::simple_proxy_yaml(proxy_port, backend_port);
    let config = Config::from_yaml(&yaml).unwrap();
    let addr = start_proxy(&config);
    let cfg = BenchConfig::new("simple_proxy_serial").total(1000).concurrency(1);

    let result = run_get_benchmark(&cfg, &addr, "/");
    assert_eq!(result.errors, 0, "all requests should succeed");
    report_results(&result);
}

// -----------------------------------------------------------------------------
// Moderate Concurrency
// -----------------------------------------------------------------------------

#[test]
fn bench_simple_proxy_concurrent() {
    let backend_port = start_backend("ok");
    let proxy_port = free_port();
    let yaml = praxis_test_utils::simple_proxy_yaml(proxy_port, backend_port);
    let config = Config::from_yaml(&yaml).unwrap();
    let addr = start_proxy(&config);

    let cfg = BenchConfig::new("simple_proxy_concurrent").total(2000).concurrency(8);
    let result = run_get_benchmark(&cfg, &addr, "/");
    assert_eq!(result.errors, 0, "all requests should succeed");
    report_results(&result);
}

// -----------------------------------------------------------------------------
// High Concurrency
// -----------------------------------------------------------------------------

#[test]
fn bench_simple_proxy_high_concurrency() {
    let backend_port = start_backend("ok");
    let proxy_port = free_port();
    let yaml = praxis_test_utils::simple_proxy_yaml(proxy_port, backend_port);
    let config = Config::from_yaml(&yaml).unwrap();
    let addr = start_proxy(&config);

    let cfg = BenchConfig::new("simple_proxy_high_concurrency")
        .total(4000)
        .concurrency(16);
    let result = run_get_benchmark(&cfg, &addr, "/");
    assert_eq!(result.errors, 0, "all requests should succeed");
    report_results(&result);
}
