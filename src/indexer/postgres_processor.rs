use std::sync::Arc;

use crate::database::get_chunks;
use crate::util::bigdecimal_to_u64;
use crate::{
    counters::{GOT_CONNECTION, UNABLE_TO_GET_CONNECTION},
    database::{execute_with_better_error, PgDbPool, PgPoolConnection},
    models::processor_statuses::ProcessorStatusModel,
    schema,
};
use aptos_rest_client::Transaction;
use diesel::pg::upsert::excluded;
use diesel::{prelude::*, RunQueryDsl};
use field_count::FieldCount;
use schema::processor_statuses::{self, dsl};

use super::errors::TransactionProcessingError;
use super::processing_result::ProcessingResult;
use super::transaction_processor::{ProcessorMetadataHandle, TransactionProcessor};

/// Handles Processor metadata and maintains them in a Postgres Relation.
pub struct PgProcessorMetadataHandle {
    get_conn: Arc<dyn Fn() -> PgPoolConnection + Send + Sync>,
}

impl ProcessorMetadataHandle for PgProcessorMetadataHandle {
    /// Gets all versions which were not successfully processed for this `TransactionProcessor` from the DB
    /// This is so the `Tailer` can know which versions to retry
    fn get_error_versions(&self, processor_name: &str) -> Vec<u64> {
        let conn = (self.get_conn)();

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
        let conn = (self.get_conn)();

        let res = dsl::processor_statuses
            .select(diesel::dsl::max(dsl::version))
            .filter(dsl::name.eq(processor_name))
            .first::<Option<bigdecimal::BigDecimal>>(&conn);

        res.expect("Error loading the max version query")
            .map(|v| bigdecimal_to_u64(&v))
    }

    /// Actually performs the write for a `ProcessorStatusModel` changeset
    fn apply_processor_status(&self, psms: &[ProcessorStatusModel]) {
        let conn = (self.get_conn)();
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

/// Implements processing logic for Transaction Processor backed by Postgres.
pub struct PgTransactionProcessor {
    name: &'static str,
    connection_pool: PgDbPool,
    metadata_handle: PgProcessorMetadataHandle,
    process_txns: Arc<
        dyn Fn(
                &Self,
                Vec<Transaction>,
                u64,
                u64,
            ) -> Result<ProcessingResult, TransactionProcessingError>
            + Send
            + Sync,
    >,
}

impl PgTransactionProcessor {
    pub fn new<F>(name: &'static str, connection_pool: PgDbPool, process_txns: F) -> Self
    where
        F: Fn(
                &Self,
                Vec<Transaction>,
                u64,
                u64,
            ) -> Result<ProcessingResult, TransactionProcessingError>
            + Send
            + Sync
            + 'static,
    {
        let meta_pool = connection_pool.clone();
        PgTransactionProcessor {
            name,
            connection_pool,
            process_txns: Arc::new(process_txns),
            metadata_handle: PgProcessorMetadataHandle {
                get_conn: Arc::new(move || get_pg_conn_from_pool(&meta_pool)),
            },
        }
    }

    pub fn get_connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }

    pub fn get_conn(&self) -> PgPoolConnection {
        get_pg_conn_from_pool(&self.connection_pool)
    }
}

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

#[async_trait::async_trait]
impl TransactionProcessor for PgTransactionProcessor {
    fn get_metadata_handle(&self) -> &dyn ProcessorMetadataHandle {
        &self.metadata_handle
    }

    fn name(&self) -> &'static str {
        self.name
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult, TransactionProcessingError> {
        (self.process_txns)(&self, transactions, start_version, end_version)
    }
}
