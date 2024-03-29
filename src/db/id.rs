use derive_more::{Display, FromStr};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Debug, Deserialize, Serialize, Display, Copy, Clone, Hash, PartialEq, Eq, FromStr, sqlx::Type,
)]
#[sqlx(transparent)]
pub struct Id(Uuid);

impl Id {
    pub fn random() -> Self {
        Id(Uuid::new_v4())
    }
}

impl sqlx::postgres::PgHasArrayType for Id {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("_uuid")
    }
}
