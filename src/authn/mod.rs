pub(crate) trait Authenticable: Sync + Send {
    fn account_label(&self) -> &str;
    fn audience(&self) -> &str;
}

pub(crate) mod jose;
