pgrx::pg_module_magic!();

pub mod core;
pub mod interface {
    pub use crate::core::interface::*;
}
mod pg;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn extension_loads() {
        assert_eq!(1, 1);
    }
}
