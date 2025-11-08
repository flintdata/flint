#![cfg(feature = "extensions")]
//! Extension auto-discovery and loading via inventory pattern
//!
//! Extensions self-register by implementing ExtensionLoader and using
//! inventory::submit! macro. No cfg attributes needed.

use crate::extensions::registry::{TypeRegistry, OperatorRegistry, FunctionRegistry};

/// Trait for self-registering extensions
pub trait ExtensionLoader: Send + Sync {
    /// Extension name (e.g., "point", "vector", "jsonb")
    fn name(&self) -> &str;

    /// Load types into registry
    fn load_types(&self, _registry: &mut TypeRegistry) {}

    /// Load operators into registry
    fn load_operators(&self, _registry: &mut OperatorRegistry) {}

    /// Load functions into registry
    fn load_functions(&self, _registry: &mut FunctionRegistry) {}
}

inventory::collect!(&'static dyn ExtensionLoader);

/// Load all registered extensions into registries
///
/// If `enabled_names` is provided, only load extensions matching those names.
/// If `enabled_names` is None, load all registered extensions.
pub fn load_all_extensions(
    type_registry: &mut TypeRegistry,
    operator_registry: &mut OperatorRegistry,
    function_registry: &mut FunctionRegistry,
    enabled_names: Option<&[String]>,
) {
    for loader in inventory::iter::<&'static dyn ExtensionLoader>() {
        // Check if extension is enabled (if a filter is provided)
        if let Some(enabled) = enabled_names {
            if !enabled.iter().any(|name| name == loader.name()) {
                tracing::debug!("Skipping disabled extension: {}", loader.name());
                continue;
            }
        }

        tracing::info!("Loading extension: {}", loader.name());
        loader.load_types(type_registry);
        loader.load_operators(operator_registry);
        loader.load_functions(function_registry);
    }
}