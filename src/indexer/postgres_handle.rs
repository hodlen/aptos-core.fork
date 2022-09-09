use crate::database::get_chunks;
use crate::util::bigdecimal_to_u64;
use crate::{
    counters::{GOT_CONNECTION, UNABLE_TO_GET_CONNECTION},
    database::{execute_with_better_error, PgDbPool, PgPoolConnection},
    models::processor_statuses::ProcessorStatusModel,
    schema,
};
use diesel::pg::upsert::excluded;
use diesel::{prelude::*, RunQueryDsl};
use field_count::FieldCount;
use schema::processor_statuses::{self, dsl};

use super::transaction_processor::ProcessorDataBaseHandle;
pub struct PostgresHandle {
    connection_pool: PgDbPool,
}

impl PostgresHandle {
    pub fn new(connection_pool: PgDbPool) -> Self {
        PostgresHandle { connection_pool }
    }

    /// Gets a reference to the connection pool
    /// This is used by the `get_conn()` helper below
    pub fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }

    /// Gets the connection.
    /// If it was unable to do so (default timeout: 30s), it will keep retrying until it can.
    pub fn get_conn(&self) -> PgPoolConnection {
        let pool = self.connection_pool();
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
}

#[async_trait::async_trait]
impl ProcessorDataBaseHandle for PostgresHandle {
    /// Gets all versions which were not successfully processed for this `TransactionProcessor` from the DB
    /// This is so the `Tailer` can know which versions to retry
    fn get_error_versions(&self, processor_name: &str) -> Vec<u64> {
        let conn = self.get_conn();

        dsl::processor_statuses
            .select(dsl::version)
            .filter(dsl::success.eq(false).and(dsl::name.eq(processor_name)))
            .load::<bigdecimal::BigDecimal>(&conn)
            .expect("Error loading the error versions only query")
            .iter()
            .map(bigdecimal_to_u64)
            .collect()
    }

    /// Gets the highest version for this `TransactionProcessor` from the DB
    /// This is so we know where to resume from on restarts
    fn get_max_version(&self, processor_name: &str) -> Option<u64> {
        let conn = self.get_conn();

        let res = dsl::processor_statuses
            .select(diesel::dsl::max(dsl::version))
            .filter(dsl::name.eq(processor_name))
            .first::<Option<bigdecimal::BigDecimal>>(&conn);

        res.expect("Error loading the max version query")
            .map(|v| bigdecimal_to_u64(&v))
    }

    /// Actually performs the write for a `ProcessorStatusModel` changeset
    fn apply_processor_status(&self, psms: &[ProcessorStatusModel]) {
        let conn = self.get_conn();
        let chunks = get_chunks(psms.len(), ProcessorStatusModel::field_count());
        for (start_ind, end_ind) in chunks {
            execute_with_better_error(
                &conn,
                diesel::insert_into(processor_statuses::table)
                    .values(&psms[start_ind..end_ind])
                    .on_conflict((dsl::name, dsl::version))
                    .do_update()
                    .set((
                        dsl::success.eq(excluded(dsl::success)),
                        dsl::details.eq(excluded(dsl::details)),
                        dsl::last_updated.eq(excluded(dsl::last_updated)),
                    )),
            )
            .expect("Error updating Processor Status!");
        }
    }
}
