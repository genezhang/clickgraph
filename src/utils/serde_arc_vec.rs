use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeSeq};
use std::sync::Arc;

pub fn serialize<'a, S, T>(v: &Vec<Arc<T>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize + 'static,
{
    let mut seq = s.serialize_seq(Some(v.len()))?;
    for e in v {
        seq.serialize_element(e.as_ref())?;
    }
    seq.end()
}

pub fn deserialize<'de, D, T>(d: D) -> Result<Vec<Arc<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + 'static,
{
    let v = Vec::<T>::deserialize(d)?;
    Ok(v.into_iter().map(Arc::new).collect())
}
