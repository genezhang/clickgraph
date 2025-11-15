use std::sync::Arc;
use serde::{Serialize, Deserialize};

pub fn serialize<S, T>(val: &Arc<T>, serializer: S) -> Result<S::Ok, S::Error> 
where
    S: serde::Serializer,
    T: Serialize,
{
    T::serialize(val.as_ref(), serializer)
}

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Arc<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Arc::new)
}
