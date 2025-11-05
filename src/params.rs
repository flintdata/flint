#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub trait Params {
    fn into_vec(self) -> Vec<Value>;
}

impl Params for () {
    fn into_vec(self) -> Vec<Value> {
        vec![]
    }
}

impl Params for Vec<Value> {
    fn into_vec(self) -> Vec<Value> {
        self
    }
}

impl<const N: usize> Params for [Value; N] {
    fn into_vec(self) -> Vec<Value> {
        self.into()
    }
}

impl<T: Into<Value>> Params for (T,) {
    fn into_vec(self) -> Vec<Value> {
        vec![self.0.into()]
    }
}

impl<T1: Into<Value>, T2: Into<Value>> Params for (T1, T2) {
    fn into_vec(self) -> Vec<Value> {
        vec![self.0.into(), self.1.into()]
    }
}

impl<T1: Into<Value>, T2: Into<Value>, T3: Into<Value>> Params for (T1, T2, T3) {
    fn into_vec(self) -> Vec<Value> {
        vec![self.0.into(), self.1.into(), self.2.into()]
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Real(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null,
        }
    }
}

#[macro_export]
macro_rules! params {
    () => { () };
    ($($param:expr),+ $(,)?) => {
        vec![$($crate::Value::from($param)),+]
    };
}