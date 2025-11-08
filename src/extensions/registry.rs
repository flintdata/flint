use super::{TypeExtension, OperatorExtension, FunctionExtension, IndexExtension};
use crate::types::DataType;
use std::collections::HashMap;

/// Registry for type extensions
pub struct TypeRegistry {
    types: HashMap<u32, Box<dyn TypeExtension>>,
    names: HashMap<String, u32>,
}

impl TypeRegistry {
    pub fn new() -> Self {
        TypeRegistry {
            types: HashMap::new(),
            names: HashMap::new(),
        }
    }

    pub fn register(&mut self, ext: Box<dyn TypeExtension>) {
        let oid = ext.type_oid();
        let name = ext.type_name().to_string();
        self.types.insert(oid, ext);
        self.names.insert(name, oid);
    }

    pub fn get_by_oid(&self, oid: u32) -> Option<&dyn TypeExtension> {
        self.types.get(&oid).map(|b| &**b)
    }

    pub fn get_by_name(&self, name: &str) -> Option<&dyn TypeExtension> {
        self.names
            .get(name)
            .and_then(|oid| self.types.get(oid))
            .map(|b| &**b)
    }
}

/// Registry for operator extensions
pub struct OperatorRegistry {
    operators: Vec<Box<dyn OperatorExtension>>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        OperatorRegistry {
            operators: Vec::new(),
        }
    }

    pub fn register(&mut self, ext: Box<dyn OperatorExtension>) {
        self.operators.push(ext);
    }

    pub fn find(
        &self,
        symbol: &str,
        left: &DataType,
        right: &DataType,
    ) -> Option<&dyn OperatorExtension> {
        self.operators
            .iter()
            .find(|op| op.operator_symbol() == symbol && op.can_handle(left, right))
            .map(|b| &**b)
    }
}

/// Registry for function extensions
pub struct FunctionRegistry {
    functions: HashMap<String, Box<dyn FunctionExtension>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        FunctionRegistry {
            functions: HashMap::new(),
        }
    }

    pub fn register(&mut self, ext: Box<dyn FunctionExtension>) {
        self.functions.insert(ext.name().to_string(), ext);
    }

    pub fn get(&self, name: &str) -> Option<&dyn FunctionExtension> {
        self.functions.get(name).map(|b| &**b)
    }
}

/// Registry for index builders
/// NOTE: Using a placeholder design for Phase 1. Full implementation with function pointers or enum dispatch
/// will be added in Phase 2 to maintain Send+Sync for Arc<Database>.
pub struct IndexBuilderRegistry {
    _placeholder: std::marker::PhantomData<()>,
}

impl IndexBuilderRegistry {
    pub fn new() -> Self {
        IndexBuilderRegistry {
            _placeholder: std::marker::PhantomData,
        }
    }

    /// Placeholder for future index builder registration
    #[allow(dead_code)]
    pub fn register(&mut self, _index_type: &str, _builder: impl Fn() -> Box<dyn IndexExtension> + 'static) {
        // TODO: Implement proper index builder registry with Send+Sync support
    }

    /// Placeholder for future index building
    #[allow(dead_code)]
    pub fn build(&self, _index_type: &str) -> Option<Box<dyn IndexExtension>> {
        // TODO: Implement proper index builder dispatch
        None
    }
}
