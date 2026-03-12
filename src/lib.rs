pgrx::pg_module_magic!();

pub mod core;
pub mod interface {
    pub use crate::core::interface::*;
}
mod pg;

#[cfg(feature = "pg_test")]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        Vec::new()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn extension_loads() {
        assert_eq!(1, 1);
    }
}
