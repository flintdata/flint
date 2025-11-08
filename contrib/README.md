# Flint Extensions

Extensions provide custom data types, operators, functions, and indexes to Flint without modifying the core database code.

## How Extensions Work

Extensions use the **inventory pattern** for zero-configuration auto-discovery:

1. Create an extension crate implementing `ExtensionLoader`
2. Use `inventory::submit!()` to register (no cfg attributes needed)
3. Link the extension into your binary
4. Extensions auto-discover and register at startup

**Key insight**: Extensions are first-class Rust crates that depend on flintdb, but flintdb never depends on them. This avoids circular dependencies while allowing seamless auto-discovery.

## Creating a Custom Extension

### 1. Create the extension crate

```bash
cargo new --lib my-custom-ext
cd my-custom-ext
```

### 2. Add flintdb dependency

```toml
[package]
name = "my-custom-ext"
version = "0.1.0"
edition = "2024"

[dependencies]
flintdb = { path = "../.." }
pgwire = "0.35.0"
inventory = "0.3"
```

### 3. Implement extension traits

```rust
// src/lib.rs
use flintdb::extensions::{
    TypeExtension, OperatorExtension, FunctionExtension, TypeCategory,
    loader::ExtensionLoader,
    registry::{TypeRegistry, OperatorRegistry, FunctionRegistry},
};
use flintdb::types::{Value, DataType};
use std::any::Any;

// Your custom type
pub struct MyType;

impl TypeExtension for MyType {
    fn type_oid(&self) -> u32 { 9999 }
    fn type_name(&self) -> &str { "mytype" }
    fn type_category(&self) -> TypeCategory { TypeCategory::Composite }
    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String> { todo!() }
    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String> { todo!() }
    fn to_pgwire_type(&self) -> pgwire::api::Type { pgwire::api::Type::UNKNOWN }
}

// Your extension loader
#[derive(Default)]
pub struct MyExtLoader;

impl MyExtLoader {
    pub const INSTANCE: Self = MyExtLoader;
}

impl ExtensionLoader for MyExtLoader {
    fn name(&self) -> &str { "myext" }

    fn load_types(&self, registry: &mut TypeRegistry) {
        registry.register(Box::new(MyType));
    }

    fn load_operators(&self, _registry: &mut OperatorRegistry) {
        // Register custom operators here
    }

    fn load_functions(&self, _registry: &mut FunctionRegistry) {
        // Register custom functions here
    }
}

// Auto-register with Flint
inventory::submit! {
    &MyExtLoader::INSTANCE as &'static dyn ExtensionLoader
}
```

### 4. Link into your binary

**Option A: Fork flint and add to Cargo.toml (Recommended)**

Fork the flint repo, then add your extension as a path dependency:

```toml
[dependencies]
my-custom-ext = { path = "./contrib/my-custom-ext" }
```

Build:
```bash
cargo build --release
```

The extension is automatically discovered and loaded at startup.

**Option B: Embed flintdb as a library in your own project**

Create your own Rust binary project with flintdb as a dependency:

```toml
[dependencies]
flintdb = { path = "path/to/flintdb", features = ["extensions"] }
my-custom-ext = { path = "./my-custom-ext" }

[[bin]]
name = "my-flint"
path = "src/main.rs"
```

Just add the dependency—it auto-discovers when linked.

## Example: Point Extension

See `point-ext/` for a complete, working example that demonstrates:

- **TypeExtension**: 2D point type with 16-byte serialization (OID 600)
- **OperatorExtension**: Distance operator `<->` for point-to-point distances
- **FunctionExtension**: `magnitude(point)` and `distance(point, point)` scalar functions
- **Auto-registration**: Zero config, just link it in

Run the example:

```bash
cd ../..
cargo build --release
./target/release/flint &
psql -h 127.0.0.1 -U postgres -d postgres
```

## Extension Traits

### TypeExtension

Define custom data types:

- `type_oid()`: PostgreSQL-compatible OID (avoid 1-600, use 9000+)
- `type_name()`: Name for SQL (e.g., "vector", "jsonb")
- `type_category()`: Coercion category (Numeric, String, Composite, etc.)
- `serialize()`: Convert Rust value to bytes for storage
- `deserialize()`: Reconstruct from bytes
- `to_pgwire_type()`: pgwire protocol type (usually UNKNOWN for custom types)

### OperatorExtension

Define binary operators:

- `operator_symbol()`: SQL symbol (e.g., "<->", "<#>", "@>")
- `can_handle()`: Check if types match this operator
- `execute()`: Run the operation
- `return_type()`: Determine output type given inputs

### FunctionExtension

Define scalar functions:

- `name()`: Function name (e.g., "magnitude", "distance")
- `execute()`: Run the function with arguments
- `return_type()`: Determine output type given input types

### IndexExtension (Phase 2)

Define custom index types (vector indexes, etc.):

- `index_type()`: Name (e.g., "hnsw", "ivfflat")
- `insert()`: Add values to index
- `search()`: Point queries
- `knn_search()`: k-nearest neighbor search
- `serialize()/deserialize()`: Persistence

## Registration Details

### The inventory::submit! Pattern

```rust
inventory::submit! {
    &MyExtLoader::INSTANCE as &'static dyn ExtensionLoader
}
```

Why `&'static`? Allows const-time registration. The loader itself can be a zero-sized sentinel that delegates to implementations.

### Const INSTANCE

```rust
impl MyExtLoader {
    pub const INSTANCE: Self = MyExtLoader;  // Compiles as const
}
```

Enables compile-time construction without `Box::new()` or other runtime calls.

## Extension Discovery

At database startup (`Database::new()`):

```rust
crate::extensions::loader::load_all_extensions(
    &mut type_registry,
    &mut operator_registry,
    &mut function_registry,
);
```

This iterates all `inventory::iter::<&'static dyn ExtensionLoader>()` and calls:

1. `load_types()` → registers with TypeRegistry
2. `load_operators()` → registers with OperatorRegistry
3. `load_functions()` → registers with FunctionRegistry

Extensions registered this way become available for SQL immediately.

## Distribution

Extensions can be:

- **Part of your binary** (recommended): Include in dependencies, compile everything together
- **Separate crates**: Publish to crates.io, users add as dependencies
- **Workspace members**: Like `point-ext/`, shipped with Flint but optional

No special plugins or loaders needed. They're just Rust crates.

## Next Steps

1. **Vector extension**: HNSW or IVFFlat indexes for embeddings
2. **JSON extension**: JSON type with path operators
3. **PostGIS**: Geographic types and operators
4. **Time series**: Specialized aggregations and compression

All follow the same pattern - implement the traits, submit with inventory, ship as crates.