use anyhow::Result;

pub fn to_base64<T>(val: &T) -> Result<String>
where
    T: serde::Serialize,
{
    let s = serde_json::to_string(val)?;
    let b = base64::encode(&s);
    Ok(b)
}

pub fn from_base64<T>(val: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let a = base64::decode(val)?;
    let s = std::str::from_utf8(a.as_slice())?;
    let r = serde_json::from_str::<T>(s)?;
    Ok(r)
}

pub async fn spawn_blocking<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .expect("Spawn blocking closure panicked")
}
