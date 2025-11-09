use super::{TypeExtension, TypeCategory};
use std::any::Any;
use pgwire::api::Type;
use crate::storage::index::{IndexBuilder, Index};
use crate::storage::PageId;
use crate::storage::index::btree::BTree;
use crate::storage::index::hash::HashIndex;

/// Built-in Int type extension
pub struct IntType;

impl TypeExtension for IntType {
    fn type_oid(&self) -> u32 {
        23 // PostgreSQL INT4 OID
    }

    fn type_name(&self) -> &str {
        "int"
    }

    fn type_category(&self) -> TypeCategory {
        TypeCategory::Numeric
    }

    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String> {
        let n = value
            .downcast_ref::<i64>()
            .ok_or("Invalid int value")?;
        Ok(n.to_le_bytes().to_vec())
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String> {
        if bytes.len() != 8 {
            return Err("Invalid int serialization".into());
        }
        let arr = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Ok(Box::new(i64::from_le_bytes(arr)))
    }

    fn to_pgwire_type(&self) -> Type {
        Type::INT4
    }
}

/// Built-in Float type extension
pub struct FloatType;

impl TypeExtension for FloatType {
    fn type_oid(&self) -> u32 {
        701 // PostgreSQL FLOAT8 OID
    }

    fn type_name(&self) -> &str {
        "float"
    }

    fn type_category(&self) -> TypeCategory {
        TypeCategory::Numeric
    }

    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String> {
        let f = value
            .downcast_ref::<f64>()
            .ok_or("Invalid float value")?;
        Ok(f.to_le_bytes().to_vec())
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String> {
        if bytes.len() != 8 {
            return Err("Invalid float serialization".into());
        }
        let arr = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Ok(Box::new(f64::from_le_bytes(arr)))
    }

    fn to_pgwire_type(&self) -> Type {
        Type::FLOAT8
    }
}

/// Built-in String type extension
pub struct StringType;

impl TypeExtension for StringType {
    fn type_oid(&self) -> u32 {
        1043 // PostgreSQL VARCHAR OID
    }

    fn type_name(&self) -> &str {
        "string"
    }

    fn type_category(&self) -> TypeCategory {
        TypeCategory::String
    }

    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String> {
        let s = value
            .downcast_ref::<String>()
            .ok_or("Invalid string value")?;
        Ok(s.as_bytes().to_vec())
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String> {
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|_| "Invalid UTF-8 in string".to_string())?;
        Ok(Box::new(s))
    }

    fn to_pgwire_type(&self) -> Type {
        Type::VARCHAR
    }
}

/// Built-in Bool type extension
pub struct BoolType;

impl TypeExtension for BoolType {
    fn type_oid(&self) -> u32 {
        16 // PostgreSQL BOOL OID
    }

    fn type_name(&self) -> &str {
        "bool"
    }

    fn type_category(&self) -> TypeCategory {
        TypeCategory::Boolean
    }

    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String> {
        let b = value
            .downcast_ref::<bool>()
            .ok_or("Invalid bool value")?;
        Ok(vec![if *b { 1 } else { 0 }])
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String> {
        if bytes.len() != 1 {
            return Err("Invalid bool serialization".into());
        }
        Ok(Box::new(bytes[0] != 0))
    }

    fn to_pgwire_type(&self) -> Type {
        Type::BOOL
    }
}

/// Built-in Null type extension
pub struct NullType;

impl TypeExtension for NullType {
    fn type_oid(&self) -> u32 {
        25 // PostgreSQL NULL OID (arbitrary)
    }

    fn type_name(&self) -> &str {
        "null"
    }

    fn type_category(&self) -> TypeCategory {
        TypeCategory::Extension
    }

    fn serialize(&self, _value: &dyn Any) -> Result<Vec<u8>, String> {
        Ok(vec![])
    }

    fn deserialize(&self, _bytes: &[u8]) -> Result<Box<dyn Any>, String> {
        Ok(Box::new(()))
    }

    fn to_pgwire_type(&self) -> Type {
        Type::UNKNOWN
    }
}

/// Built-in BTree index builder
pub struct BTreeBuilder;

impl IndexBuilder for BTreeBuilder {
    fn create(&self, root_page_id: Option<PageId>) -> Box<dyn Index> {
        Box::new(BTree::new(root_page_id))
    }

    fn type_name(&self) -> &str {
        "btree"
    }
}

/// Built-in Hash index builder
pub struct HashIndexBuilder;

impl IndexBuilder for HashIndexBuilder {
    fn create(&self, root_page_id: Option<PageId>) -> Box<dyn Index> {
        // Hash indexes use dynamic bucket allocation
        Box::new(HashIndex::new(root_page_id))
    }

    fn type_name(&self) -> &str {
        "hash"
    }
}

/// Register all built-in type extensions
pub fn register_builtin_types(registry: &mut super::registry::TypeRegistry) {
    registry.register(Box::new(IntType));
    registry.register(Box::new(FloatType));
    registry.register(Box::new(StringType));
    registry.register(Box::new(BoolType));
    registry.register(Box::new(NullType));
}

/// Register all built-in index builders
pub fn register_builtin_indexes(registry: &mut crate::storage::index::IndexBuilderRegistry) {
    registry.register("btree", Box::new(BTreeBuilder));
    registry.register("hash", Box::new(HashIndexBuilder));
}
