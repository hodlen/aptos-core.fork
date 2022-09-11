use crate::{
    counters::{GOT_CONNECTION, UNABLE_TO_GET_CONNECTION},
    database::{PgDbPool, PgPoolConnection},
};

/// Gets the connection.
/// If it was unable to do so (default timeout: 30s), it will keep retrying until it can.
pub fn get_pg_conn_from_pool(pool: &PgDbPool) -> PgPoolConnection {
    loop {
        match pool.get() {
            Ok(conn) => {
                GOT_CONNECTION.inc();
                return conn;
            }
            Err(err) => {
                UNABLE_TO_GET_CONNECTION.inc();
                aptos_logger::error!(
                    "Could not get DB connection from pool, will retry in {:?}. Err: {:?}",
                    pool.connection_timeout(),
                    err
                );
            }
        };
    }
}
