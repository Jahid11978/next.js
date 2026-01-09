use turbo_rcstr::RcStr;
use turbo_tasks::{FxIndexMap, ResolvedVc};

use crate::{
    project::{Instrumentation, Middleware},
    route::{Endpoint, Route},
};

#[turbo_tasks::value(shared)]
pub struct Entrypoints {
    #[bincode(with = "turbo_bincode::indexmap")]
    pub routes: FxIndexMap<RcStr, Route>,
    pub middleware: Option<Middleware>,
    pub instrumentation: Option<Instrumentation>,
    pub pages_document_endpoint: ResolvedVc<Box<dyn Endpoint>>,
    pub pages_app_endpoint: ResolvedVc<Box<dyn Endpoint>>,
    pub pages_error_endpoint: ResolvedVc<Box<dyn Endpoint>>,
    /// Paths that should be deferred until all other entries are compiled
    pub deferred_entries: Vec<RcStr>,
}

/// Checks if a pathname matches any of the deferred entry patterns.
pub fn is_deferred_entry(pathname: &str, deferred_entries: &[RcStr]) -> bool {
    if deferred_entries.is_empty() {
        return false;
    }

    // Normalize the pathname
    let normalized_pathname = if pathname.starts_with('/') {
        pathname.to_string()
    } else {
        format!("/{pathname}")
    };

    for pattern in deferred_entries {
        // Normalize the pattern
        let normalized_pattern = if pattern.starts_with('/') {
            pattern.as_str().to_string()
        } else {
            format!("/{pattern}")
        };

        // Check for exact match
        if normalized_pathname == normalized_pattern {
            return true;
        }

        // Check if the pathname is under the deferred directory
        if normalized_pathname.starts_with(&format!("{normalized_pattern}/")) {
            return true;
        }
    }

    false
}
