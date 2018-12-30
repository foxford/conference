#[macro_use]
extern crate diesel;

fn main() {
    env_logger::init();
    app::run();
}

pub fn establish_connection() -> diesel::pg::PgConnection {
    use diesel::connection::Connection;
    use diesel::pg::PgConnection;
    use std::env;

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be specified");
    PgConnection::establish(&database_url).expect(&format!("Error connecting to {}", database_url))
}

mod app;
mod backend;
mod schema;
mod transport;

pub mod sql {
    pub use crate::transport::sql::{Account_id, Agent_id};
}
