use derive_more::{Display, FromStr};
use diesel_derive_newtype::DieselNewType;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Debug, Deserialize, Serialize, Display, Copy, Clone, DieselNewType, Hash, PartialEq, Eq, FromStr,
)]
pub struct Id(Uuid);

impl Id {
    pub fn random() -> Self {
        Id(Uuid::new_v4())
    }
}
