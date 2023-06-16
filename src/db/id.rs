use derive_more::{Display, FromStr};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, Display, Copy, Clone, Hash, PartialEq, Eq, FromStr)]
pub struct Id(Uuid);

impl Id {
    pub fn random() -> Self {
        Id(Uuid::new_v4())
    }
}

// TODO: remove everything after uuid update 0.8 -> 1.3

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for Id {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        let uuid = sqlx::types::Uuid::from(*self);
        uuid.encode_by_ref(buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for Id {
    fn decode(
        value: <sqlx::Postgres as sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let uuid = sqlx::types::Uuid::decode(value)?;
        Ok(Id::from(uuid))
    }
}

impl sqlx::Type<sqlx::Postgres> for Id {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        sqlx::types::Uuid::type_info()
    }
}

impl From<Id> for sqlx::types::Uuid {
    fn from(value: Id) -> Self {
        sqlx::types::Uuid::from_u128_le(value.0.to_u128_le())
    }
}

impl From<sqlx::types::Uuid> for Id {
    fn from(value: sqlx::types::Uuid) -> Self {
        Id(Uuid::from_u128_le(value.to_u128_le()))
    }
}
