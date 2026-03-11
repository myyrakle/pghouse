use pgrx::pg_sys;
use pgrx::prelude::pg_guard;

#[unsafe(no_mangle)]
#[doc(hidden)]
pub extern "C" fn pg_finfo_pghouse_am_handler_wrapper() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pghouse_am_handler_wrapper(
    _fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    unsafe { pg_sys::Datum::from(pg_sys::GetHeapamTableAmRoutine().cast_mut()) }
}

pgrx::extension_sql!(
    r#"
    CREATE FUNCTION pghouse_am_handler(internal)
    RETURNS table_am_handler
    AS 'MODULE_PATHNAME', 'pghouse_am_handler_wrapper'
    LANGUAGE C;

    CREATE ACCESS METHOD pghouse TYPE TABLE HANDLER pghouse_am_handler;
    "#,
    name = "bootstrap_pghouse_access_method"
);
