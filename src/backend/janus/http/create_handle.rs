use serde::Deserialize;

#[derive(Debug)]
pub struct CreateHandleRequest {
    pub session_id: i64,
    pub opaque_id: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateHandleResponse {
    pub id: i64,
}
