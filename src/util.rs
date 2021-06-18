use anyhow::Result;

pub(crate) fn to_base64<T>(val: &T) -> Result<String>
where
    T: serde::Serialize,
{
    let s = serde_json::to_string(val)?;
    let b = base64::encode(&s);
    Ok(b)
}

pub(crate) fn from_base64<T>(val: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let a = base64::decode(val)?;
    let s = std::str::from_utf8(a.as_slice())?;
    let r = serde_json::from_str::<T>(s)?;
    Ok(r)
}

// const CORRELATION_DATA_LENGTH: usize = 16;

// pub(crate) fn generate_correlation_data() -> String {
//     thread_rng()
//         .sample_iter(&Alphanumeric)
//         .take(CORRELATION_DATA_LENGTH)
//         .collect()
// }
