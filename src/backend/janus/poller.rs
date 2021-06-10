use isahc::HttpClient;

pub struct Poller {
    http_client: HttpClient,
    session_id: i64,
}
