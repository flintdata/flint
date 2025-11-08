use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::any::Any;
use bincode::{Encode, Decode};

/// A single column value
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    // Extension type values (stored as Arc<dyn Any> for type-safe downcasting)
    Extension {
        type_oid: u32,
        data: Arc<dyn Any + Send + Sync>,
    },
}

// Manual serde impl to handle non-serializable Extension variant
impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_none(),
            Value::Int(n) => serializer.serialize_i64(*n),
            Value::Float(f) => serializer.serialize_f64(*f),
            Value::String(s) => serializer.serialize_str(s),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Extension { .. } => {
                Err(serde::ser::Error::custom(
                    "Extension values must be serialized through TypeExtension trait",
                ))
            }
        }
    }
}

// Manual deserialize impl
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Visitor;

        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a Value")
            }

            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Value, E> {
                Ok(Value::Bool(value))
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Value, E> {
                Ok(Value::Int(value))
            }

            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Value, E> {
                Ok(Value::Float(value))
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Value, E> {
                Ok(Value::String(value.to_string()))
            }

            fn visit_none<E>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

// Manual bincode Encode impl for Value (handles Extension variant)
impl Encode for Value {
    fn encode<E>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError>
    where
        E: bincode::enc::Encoder,
    {
        match self {
            Value::Null => {
                0u8.encode(encoder)?;
            }
            Value::Int(n) => {
                1u8.encode(encoder)?;
                n.encode(encoder)?;
            }
            Value::Float(f) => {
                2u8.encode(encoder)?;
                f.encode(encoder)?;
            }
            Value::String(s) => {
                3u8.encode(encoder)?;
                s.encode(encoder)?;
            }
            Value::Bool(b) => {
                4u8.encode(encoder)?;
                b.encode(encoder)?;
            }
            Value::Extension { type_oid, .. } => {
                // Extension values cannot be persisted in Phase 1
                // Store as Null with marker
                5u8.encode(encoder)?;
                type_oid.encode(encoder)?;
            }
        }
        Ok(())
    }
}

// Manual bincode Decode impl for Value (handles Extension variant)
impl Decode<()> for Value {
    fn decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::Decoder,
    {
        let tag = u8::decode(decoder)?;
        match tag {
            0 => Ok(Value::Null),
            1 => {
                let n = i64::decode(decoder)?;
                Ok(Value::Int(n))
            }
            2 => {
                let f = f64::decode(decoder)?;
                Ok(Value::Float(f))
            }
            3 => {
                let s = String::decode(decoder)?;
                Ok(Value::String(s))
            }
            4 => {
                let b = bool::decode(decoder)?;
                Ok(Value::Bool(b))
            }
            5 => {
                // Extension values are persisted as Null in Phase 1
                let _type_oid = u32::decode(decoder)?;
                Ok(Value::Null)
            }
            _ => Err(bincode::error::DecodeError::OtherString("Invalid Value tag".into())),
        }
    }
}

impl Value {
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int(n) => Some(*n as i32),
            _ => None,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Extension { type_oid, .. } => format!("<extension {}>", type_oid),
        }
    }
}

/// A single row (ordered list of values)
#[derive(Debug, Clone, Serialize, Deserialize, Encode)]
pub struct Row {
    pub values: Vec<Value>,
}

// Manual Decode for Row (to handle Value's custom Decode)
impl Decode<()> for Row {
    fn decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::Decoder<Context = ()>,
    {
        // Manually decode Vec<Value> to work with Value's custom Decode impl
        let len: u32 = bincode::Decode::decode(decoder)?;
        let mut values = Vec::with_capacity(len as usize);
        for _ in 0..len {
            values.push(Value::decode(decoder)?);
        }
        Ok(Row { values })
    }
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Row { values }
    }

    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Column metadata
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
}

/// SQL data types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub enum DataType {
    Int,
    Float,
    String,
    Bool,
    Null,
    // Extension types (custom types registered via extensions system)
    Extension {
        type_oid: u32,
        type_name: String,
    },
}

/// Table schema
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Schema { columns }
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}