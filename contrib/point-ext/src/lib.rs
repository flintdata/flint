//! Point type extension for Flint
//!
//! Demonstrates all extension traits:
//! - TypeExtension: Point type serialization/deserialization
//! - OperatorExtension: Distance operator (<->)
//! - FunctionExtension: magnitude() and distance() scalar functions
//!
//! Auto-registers with Flint via inventory pattern (no cfg attributes needed)

use flintdb::extensions::{
    TypeExtension, OperatorExtension, FunctionExtension, TypeCategory, loader::ExtensionLoader,
    registry::{TypeRegistry, OperatorRegistry, FunctionRegistry},
};
use flintdb::types::{Value, DataType};
use std::any::Any;

/// 2D Cartesian point (x, y)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Point { x, y }
    }

    pub fn magnitude(&self) -> f64 {
        (self.x * self.x + self.y * self.y).sqrt()
    }

    pub fn distance_to(&self, other: &Point) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }
}

/// Point type extension (PostgreSQL OID 600)
pub struct PointType;

impl TypeExtension for PointType {
    fn type_oid(&self) -> u32 {
        600 // PostgreSQL point type OID
    }

    fn type_name(&self) -> &str {
        "point"
    }

    fn type_category(&self) -> TypeCategory {
        TypeCategory::Composite
    }

    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String> {
        if let Some(point) = value.downcast_ref::<Point>() {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&point.x.to_le_bytes());
            bytes.extend_from_slice(&point.y.to_le_bytes());
            Ok(bytes)
        } else {
            Err("Invalid point value".to_string())
        }
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String> {
        if bytes.len() != 16 {
            return Err(format!("Point must be 16 bytes, got {}", bytes.len()));
        }
        let x = f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let y = f64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        Ok(Box::new(Point { x, y }))
    }

    fn to_pgwire_type(&self) -> pgwire::api::Type {
        // No direct pgwire type for point - use UNKNOWN
        pgwire::api::Type::UNKNOWN
    }
}

/// Distance operator: point <-> point -> float
pub struct DistanceOperator;

impl OperatorExtension for DistanceOperator {
    fn operator_symbol(&self) -> &str {
        "<->"
    }

    fn can_handle(&self, left_type: &DataType, right_type: &DataType) -> bool {
        matches!(left_type, DataType::Extension { type_oid: 600, .. }) &&
        matches!(right_type, DataType::Extension { type_oid: 600, .. })
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value, String> {
        if let (Value::Extension { data: left_data, .. }, Value::Extension { data: right_data, .. }) = (left, right) {
            if let (Some(p1), Some(p2)) = (left_data.downcast_ref::<Point>(), right_data.downcast_ref::<Point>()) {
                return Ok(Value::Float(p1.distance_to(p2)));
            }
        }
        Err("Invalid point values for distance operator".to_string())
    }

    fn return_type(&self, left_type: &DataType, right_type: &DataType) -> DataType {
        if self.can_handle(left_type, right_type) {
            DataType::Float
        } else {
            DataType::Null
        }
    }
}

/// Magnitude function: magnitude(point) -> float
pub struct MagnitudeFunc;

impl FunctionExtension for MagnitudeFunc {
    fn name(&self) -> &str {
        "magnitude"
    }

    fn execute(&self, args: &[Value]) -> Result<Value, String> {
        if args.len() != 1 {
            return Err(format!("magnitude() expects 1 argument, got {}", args.len()));
        }

        if let Value::Extension { data, .. } = &args[0] {
            if let Some(point) = data.downcast_ref::<Point>() {
                return Ok(Value::Float(point.magnitude()));
            }
        }
        Err("magnitude() expects point argument".to_string())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, String> {
        if arg_types.len() != 1 {
            return Err(format!("magnitude() expects 1 argument, got {}", arg_types.len()));
        }

        if matches!(arg_types[0], DataType::Extension { type_oid: 600, .. }) {
            Ok(DataType::Float)
        } else {
            Err("magnitude() expects point argument".to_string())
        }
    }
}

/// Distance function: distance(point, point) -> float
pub struct DistanceFunc;

impl FunctionExtension for DistanceFunc {
    fn name(&self) -> &str {
        "distance"
    }

    fn execute(&self, args: &[Value]) -> Result<Value, String> {
        if args.len() != 2 {
            return Err(format!("distance() expects 2 arguments, got {}", args.len()));
        }

        if let (Value::Extension { data: left_data, .. }, Value::Extension { data: right_data, .. }) = (&args[0], &args[1]) {
            if let (Some(p1), Some(p2)) = (left_data.downcast_ref::<Point>(), right_data.downcast_ref::<Point>()) {
                return Ok(Value::Float(p1.distance_to(p2)));
            }
        }
        Err("distance() expects two point arguments".to_string())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, String> {
        if arg_types.len() != 2 {
            return Err(format!("distance() expects 2 arguments, got {}", arg_types.len()));
        }

        if matches!(arg_types[0], DataType::Extension { type_oid: 600, .. }) &&
           matches!(arg_types[1], DataType::Extension { type_oid: 600, .. }) {
            Ok(DataType::Float)
        } else {
            Err("distance() expects two point arguments".to_string())
        }
    }
}

// ============================================================================
// Auto-discovery registration via inventory pattern
// ============================================================================

/// Point extension loader - self-registers with Flint
#[derive(Default)]
pub struct PointExtLoader;

impl PointExtLoader {
    /// Const singleton instance for inventory registration
    pub const INSTANCE: Self = PointExtLoader;
}

impl ExtensionLoader for PointExtLoader {
    fn name(&self) -> &str {
        "point"
    }

    fn load_types(&self, registry: &mut TypeRegistry) {
        registry.register(Box::new(PointType));
    }

    fn load_operators(&self, registry: &mut OperatorRegistry) {
        registry.register(Box::new(DistanceOperator));
    }

    fn load_functions(&self, registry: &mut FunctionRegistry) {
        registry.register(Box::new(MagnitudeFunc));
        registry.register(Box::new(DistanceFunc));
    }
}

// Auto-register this extension via inventory
// Uses a static reference that can be constructed at compile time
inventory::submit! {
    &PointExtLoader::INSTANCE as &'static dyn ExtensionLoader
}