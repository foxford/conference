use serde::Deserialize;

#[derive(Debug)]
pub struct CreateHandleRequest {
    pub session_id: i64,
}

#[derive(Debug, Deserialize)]
pub struct CreateHandleResponse {
    pub id: i64,
}
