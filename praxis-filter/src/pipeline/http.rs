//! HTTP pipeline execution: request, response, and body filter phases.

use bytes::Bytes;
use tracing::{debug, trace, warn};

use super::FilterPipeline;
use crate::{
    FilterError,
    actions::FilterAction,
    any_filter::AnyFilter,
    body::BodyAccess,
    condition::{should_execute, should_execute_response},
    filter::HttpFilterContext,
};

// -----------------------------------------------------------------------------
// FilterPipeline HTTP
// -----------------------------------------------------------------------------

impl FilterPipeline {
    /// Run all HTTP request filters in order.
    pub async fn execute_http_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        for (filter, conditions, _resp_conditions) in &self.filters {
            let http_filter = match filter {
                AnyFilter::Http(f) => f.as_ref(),
                AnyFilter::Tcp(_) => continue,
            };
            if !should_execute(conditions, ctx.request) {
                trace!(filter = http_filter.name(), "skipped by conditions");
                continue;
            }
            trace!(filter = http_filter.name(), "on_request");
            match http_filter.on_request(ctx).await {
                Ok(FilterAction::Continue | FilterAction::Release) => {},
                Ok(FilterAction::Reject(rejection)) => {
                    debug!(
                        filter = http_filter.name(),
                        status = rejection.status,
                        "filter rejected request"
                    );
                    return Ok(FilterAction::Reject(rejection));
                },
                Err(e) => {
                    warn!(filter = http_filter.name(), error = %e, "filter error during request");
                    return Err(e);
                },
            }
        }
        Ok(FilterAction::Continue)
    }

    /// Run all HTTP response filters in reverse order.
    pub async fn execute_http_response(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        let has_any_resp_conditions = self.filters.iter().any(|(_, _, rc)| !rc.is_empty());
        let resp_snapshot = if has_any_resp_conditions {
            ctx.response_header.as_ref().map(|r| crate::Response {
                status: r.status,
                headers: r.headers.clone(),
            })
        } else {
            None
        };

        for (filter, _req_conditions, resp_conditions) in self.filters.iter().rev() {
            let http_filter = match filter {
                AnyFilter::Http(f) => f.as_ref(),
                AnyFilter::Tcp(_) => continue,
            };
            if !resp_conditions.is_empty()
                && let Some(ref resp) = resp_snapshot
                && !should_execute_response(resp_conditions, resp)
            {
                trace!(filter = http_filter.name(), "skipped by response conditions");
                continue;
            }
            trace!(filter = http_filter.name(), "on_response");
            match http_filter.on_response(ctx).await {
                Ok(FilterAction::Continue | FilterAction::Release) => {},
                Ok(FilterAction::Reject(rejection)) => {
                    warn!(
                        filter = http_filter.name(),
                        status = rejection.status,
                        "filter rejected response"
                    );
                    return Ok(FilterAction::Reject(rejection));
                },
                Err(e) => {
                    warn!(filter = http_filter.name(), error = %e, "filter error during response");
                    return Err(e);
                },
            }
        }
        Ok(FilterAction::Continue)
    }

    /// Run all HTTP request body filters in order.
    ///
    /// Returns [`FilterAction::Release`] if any filter signals release
    /// (`StreamBuffer` mode). Release does not short-circuit: all filters
    /// still see the chunk.
    pub async fn execute_http_request_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        if let Some(b) = body.as_ref() {
            ctx.request_body_bytes += b.len() as u64;
        }

        let mut released = false;

        for (filter, conditions, _resp_conditions) in &self.filters {
            let http_filter = match filter {
                AnyFilter::Http(f) => f.as_ref(),
                AnyFilter::Tcp(_) => continue,
            };
            if http_filter.request_body_access() == BodyAccess::None {
                continue;
            }
            if !should_execute(conditions, ctx.request) {
                trace!(filter = http_filter.name(), "body hook skipped by conditions");
                continue;
            }
            trace!(filter = http_filter.name(), "on_request_body");
            match http_filter.on_request_body(ctx, body, end_of_stream).await {
                Ok(FilterAction::Continue) => {},
                Ok(FilterAction::Release) => {
                    debug!(filter = http_filter.name(), "filter released body");
                    released = true;
                },
                Ok(FilterAction::Reject(rejection)) => {
                    debug!(
                        filter = http_filter.name(),
                        status = rejection.status,
                        "filter rejected request body"
                    );
                    return Ok(FilterAction::Reject(rejection));
                },
                Err(e) => {
                    warn!(filter = http_filter.name(), error = %e, "filter error during request body");
                    return Err(e);
                },
            }
        }

        if released {
            Ok(FilterAction::Release)
        } else {
            Ok(FilterAction::Continue)
        }
    }

    /// Run all HTTP response body filters in reverse order.
    ///
    /// Returns [`FilterAction::Release`] if any filter signals release
    /// (`StreamBuffer` mode). Release does not short-circuit: all filters
    /// still see the chunk.
    ///
    /// # Sync constraint
    ///
    /// This method is synchronous because Pingora's `response_body_filter`
    /// callback is `fn`, not `async fn`. See [`on_response_body`] for the
    /// full explanation.
    ///
    /// [`on_response_body`]: crate::filter::HttpFilter::on_response_body
    pub fn execute_http_response_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        if let Some(b) = body.as_ref() {
            ctx.response_body_bytes += b.len() as u64;
        }

        let mut released = false;

        let has_any_resp_conditions = self.filters.iter().any(|(_, _, rc)| !rc.is_empty());
        let resp_snapshot = if has_any_resp_conditions {
            ctx.response_header.as_ref().map(|r| crate::Response {
                status: r.status,
                headers: r.headers.clone(),
            })
        } else {
            None
        };

        for (filter, _req_conditions, resp_conditions) in self.filters.iter().rev() {
            let http_filter = match filter {
                AnyFilter::Http(f) => f.as_ref(),
                AnyFilter::Tcp(_) => continue,
            };
            if http_filter.response_body_access() == BodyAccess::None {
                continue;
            }
            if !resp_conditions.is_empty()
                && let Some(ref resp) = resp_snapshot
                && !should_execute_response(resp_conditions, resp)
            {
                trace!(filter = http_filter.name(), "body hook skipped by response conditions");
                continue;
            }
            trace!(filter = http_filter.name(), "on_response_body");
            match http_filter.on_response_body(ctx, body, end_of_stream) {
                Ok(FilterAction::Continue) => {},
                Ok(FilterAction::Release) => {
                    debug!(filter = http_filter.name(), "filter released response body");
                    released = true;
                },
                Ok(FilterAction::Reject(rejection)) => {
                    debug!(
                        filter = http_filter.name(),
                        status = rejection.status,
                        "filter rejected response body"
                    );
                    return Ok(FilterAction::Reject(rejection));
                },
                Err(e) => {
                    warn!(filter = http_filter.name(), error = %e, "filter error during response body");
                    return Err(e);
                },
            }
        }

        if released {
            Ok(FilterAction::Release)
        } else {
            Ok(FilterAction::Continue)
        }
    }
}
