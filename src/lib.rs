pgrx::pg_module_magic!();

mod am;
mod api;
mod catalog;
mod compression;
pub mod interface;
mod storage;
mod worker;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn extension_loads() {
        assert_eq!(1, 1);
    }
}
