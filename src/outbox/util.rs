use chrono::{DateTime, Duration, Utc};

pub fn delivery_deadline_from_now(try_wake_interval: Duration) -> DateTime<Utc> {
    Utc::now() + try_wake_interval
}

pub fn next_delivery_deadline_at(
    retry_count: i32,
    delivery_deadline_at: DateTime<Utc>,
    try_wake_interval: Duration,
    max_delivery_interval: Duration,
) -> DateTime<Utc> {
    let seconds = std::cmp::min(
        try_wake_interval.num_seconds() * 2_i64.pow((retry_count + 1) as u32),
        max_delivery_interval.num_seconds(),
    );

    delivery_deadline_at + Duration::seconds(seconds)
}
