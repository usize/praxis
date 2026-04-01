#![deny(unsafe_code)]

//! AI filters and capabilities for the Praxis proxy.
//!
//! Provides two modules covering AI workloads:
//!
//! - [`agentic`]: filters for AI agent workloads (MCP, A2A,
//!   agent orchestration, tool-use proxying, etc.)
//! - [`inference`]: filters for AI inference workloads (model
//!   routing, token counting, prompt inspection,
//!   inference-aware load balancing, etc.)

pub mod agentic;
pub mod inference;
