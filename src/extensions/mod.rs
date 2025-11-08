pub mod registry;
pub mod builtin;
pub mod loader;

use crate::types::{Value, DataType};
use crate::storage::TuplePointer;
use std::any::Any;

/// Type categories for operator coercion
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeCategory {
    Numeric,
    String,
    Boolean,
    Temporal,
    Array,
    Composite,
    Extension,
}

/// Extension trait for custom data types
pub trait TypeExtension: Send + Sync {
    /// PostgreSQL-compatible type OID
    fn type_oid(&self) -> u32;

    /// Type name (e.g., "vector", "jsonb")
    fn type_name(&self) -> &str;

    /// Category for type coercion
    fn type_category(&self) -> TypeCategory;

    /// Serialize extension value to bytes for storage
    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String>;

    /// Deserialize bytes back to extension value
    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String>;

    /// Convert to PostgreSQL type for protocol
    fn to_pgwire_type(&self) -> pgwire::api::Type;
}

/// Extension trait for custom operators
pub trait OperatorExtension: Send + Sync {
    /// Operator symbol (e.g., "<->", "<#>", "@>")
    fn operator_symbol(&self) -> &str;

    /// Check if this operator can handle these types
    fn can_handle(&self, left_type: &DataType, right_type: &DataType) -> bool;

    /// Execute the operator
    fn execute(&self, left: &Value, right: &Value) -> Result<Value, String>;

    /// Return type given input types
    fn return_type(&self, left_type: &DataType, right_type: &DataType) -> DataType;
}

/// Extension trait for scalar functions
pub trait FunctionExtension: Send + Sync {
    /// Function name
    fn name(&self) -> &str;

    /// Execute the function
    fn execute(&self, args: &[Value]) -> Result<Value, String>;

    /// Return type given argument types
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, String>;
}

/// Extension trait for custom index types
pub trait IndexExtension: Send + Sync {
    /// Index type name (e.g., "hnsw", "ivfflat", "gin")
    fn index_type(&self) -> &str;

    /// Insert a key-value pair
    fn insert(&mut self, key: &Value, pointer: TuplePointer) -> Result<(), String>;

    /// Standard lookup for point queries
    fn search(&self, key: &Value) -> Result<Vec<TuplePointer>, String>;

    /// k-NN search for vector similarity (returns k nearest)
    fn knn_search(&self, query: &Value, k: usize) -> Result<Vec<(TuplePointer, f64)>, String>;

    /// Serialize index to bytes for persistence
    fn serialize(&self) -> Result<Vec<u8>, String>;

    /// Deserialize index from bytes
    fn deserialize(bytes: &[u8]) -> Result<Box<dyn IndexExtension>, String>
    where
        Self: Sized;
}
