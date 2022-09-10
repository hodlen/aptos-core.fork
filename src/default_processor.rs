// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    database::{execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection},
    indexer::{
        errors::TransactionProcessingError, postgres_processor::PgTransactionProcessor,
        processing_result::ProcessingResult,
    },
    models::{
        events::EventModel,
        transactions::{BlockMetadataTransactionModel, TransactionModel, UserTransactionModel},
        write_set_changes::WriteSetChangeModel,
    },
    schema,
};
use aptos_rest_client::Transaction;

use field_count::FieldCount;
use std::fmt::Debug;

pub struct DefaultTransactionProcessor {
    processor: PgTransactionProcessor,
}

impl DefaultTransactionProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self {
            processor: PgTransactionProcessor::new(
                connection_pool,
                |processor: &PgTransactionProcessor,
                 transactions: Vec<Transaction>,
                 start_version: u64,
                 end_version: u64| {
                    process_transactions(
                        processor.get_conn(),
                        String::from("process_transactions"),
                        transactions,
                        start_version,
                        end_version,
                    )
                },
            ),
        }
    }
}

impl Debug for DefaultTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.processor.get_connection_pool().state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_events(conn: &PgPoolConnection, events: &Vec<EventModel>) {
    let chunks = get_chunks(events.len(), EventModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::events::table)
                .values(&events[start_ind..end_ind])
                .on_conflict_do_nothing(),
        )
        .expect("Error inserting row into database");
    }
}

fn insert_write_set_changes(conn: &PgPoolConnection, write_set_changes: &Vec<WriteSetChangeModel>) {
    let chunks = get_chunks(write_set_changes.len(), WriteSetChangeModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::write_set_changes::table)
                .values(&write_set_changes[start_ind..end_ind])
                .on_conflict_do_nothing(),
        )
        .expect("Error inserting row into database");
    }
}

fn insert_transactions(conn: &PgPoolConnection, txns: &[TransactionModel]) {
    let chunks = get_chunks(txns.len(), TransactionModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::transactions::table)
                .values(&txns[start_ind..end_ind])
                .on_conflict_do_nothing(),
        )
        .expect("Error inserting row into database");
    }
}

fn insert_user_transactions(conn: &PgPoolConnection, user_txns: &[UserTransactionModel]) {
    let chunks = get_chunks(user_txns.len(), UserTransactionModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::user_transactions::table)
                .values(&user_txns[start_ind..end_ind])
                .on_conflict_do_nothing(),
        )
        .expect("Error inserting row into database");
    }
}

fn insert_block_metadata_transactions(
    conn: &PgPoolConnection,
    bm_txns: &[BlockMetadataTransactionModel],
) {
    let chunks = get_chunks(bm_txns.len(), UserTransactionModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::block_metadata_transactions::table)
                .values(&bm_txns[start_ind..end_ind])
                .on_conflict_do_nothing(),
        )
        .expect("Error inserting row into database");
    }
}

fn insert_to_db(
    conn: &PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    txns: Vec<TransactionModel>,
    user_txns: Vec<UserTransactionModel>,
    bm_txns: Vec<BlockMetadataTransactionModel>,
    events: Vec<EventModel>,
    wscs: Vec<WriteSetChangeModel>,
) -> Result<(), diesel::result::Error> {
    aptos_logger::trace!(
        "[{}] inserting versions {} to {}",
        name,
        start_version,
        end_version
    );
    conn.build_transaction()
        .read_write()
        .run::<_, diesel::result::Error, _>(|| {
            insert_transactions(conn, &txns);
            insert_user_transactions(conn, &user_txns);
            insert_block_metadata_transactions(conn, &bm_txns);
            insert_events(conn, &events);
            insert_write_set_changes(conn, &wscs);
            Ok(())
        })
}

fn process_transactions(
    conn: &PgPoolConnection,
    name: String,
    transactions: Vec<Transaction>,
    start_version: u64,
    end_version: u64,
) -> Result<ProcessingResult, TransactionProcessingError> {
    let (txns, user_txns, bm_txns, events, write_set_changes) =
        TransactionModel::from_transactions(&transactions);

    let tx_result = insert_to_db(
        &conn,
        &name,
        start_version,
        end_version,
        txns,
        user_txns,
        bm_txns,
        events,
        write_set_changes,
    );
    match tx_result {
        Ok(_) => Ok(ProcessingResult::new(&name, start_version, end_version)),
        Err(err) => Err(TransactionProcessingError::TransactionCommitError((
            anyhow::Error::from(err),
            start_version,
            end_version,
            &name,
        ))),
    }
}
