pub mod server;
pub mod config;
pub mod types;
#[cfg(feature = "extensions")]
pub mod extensions;
mod handler;
mod executor;
mod storage;
mod parser;
mod planner;

// Re-export extension types and registries for convenience
#[cfg(feature = "extensions")]
pub use extensions::registry::{TypeRegistry, OperatorRegistry, FunctionRegistry, IndexBuilderRegistry};
#[cfg(feature = "extensions")]
pub use extensions::{TypeExtension, OperatorExtension, FunctionExtension, IndexExtension, TypeCategory};