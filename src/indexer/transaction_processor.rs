// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    counters::{PROCESSOR_ERRORS, PROCESSOR_INVOCATIONS, PROCESSOR_SUCCESSES},
    indexer::{errors::TransactionProcessingError, processing_result::ProcessingResult},
    models::processor_statuses::ProcessorStatusModel,
};
use aptos_rest_client::Transaction;
use async_trait::async_trait;

#[async_trait]
pub trait ProcessorMetadataHandle: Send + Sync {
    // Update `ProcessorStatusModel` changeset in the database
    async fn apply_processor_status(&self, psms: &[ProcessorStatusModel]);

    /// Gets all versions which were not successfully processed for this `TransactionProcessor` from the DB
    /// This is so the `Tailer` can know which versions to retry
    async fn get_error_versions(&self, processor_name: &str) -> Vec<u64>;

    /// Gets the highest version for this `TransactionProcessor` from the DB
    /// This is so we know where to resume from on restarts
    async fn get_max_version(&self, processor_name: &str) -> Option<u64>;
}

/// The `TransactionProcessor` is used by an instance of a `Tailer` to process transactions
#[async_trait]
pub trait TransactionProcessor: Send + Sync {
    fn get_metadata_handle(&self) -> &dyn ProcessorMetadataHandle;

    /// name of the processor, for status logging
    /// This will get stored in the database for each (`TransactionProcessor`, transaction_version) pair
    fn name(&self) -> &'static str;

    /// Process all transactions within a block and processes it. This method will be called from `process_transaction_with_status`
    /// In case a transaction cannot be processed, we will fail the entire block.
    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult, TransactionProcessingError>;

    //* Below are helper methods that don't need to be implemented *//
    /// This is a helper method, tying together the other helper methods to allow tracking status in the DB
    async fn process_transactions_with_status(
        &self,
        txns: Vec<Transaction>,
    ) -> Result<ProcessingResult, TransactionProcessingError> {
        assert!(
            !txns.is_empty(),
            "Must provide at least one transaction to this function"
        );
        PROCESSOR_INVOCATIONS
            .with_label_values(&[self.name()])
            .inc();

        let (start_version, end_version) = (
            txns.first().unwrap().version().unwrap(),
            txns.last().unwrap().version().unwrap(),
        );
        self.mark_versions_started(start_version, end_version).await;
        let res = self
            .process_transactions(txns, start_version, end_version)
            .await;
        // Handle block success/failure
        match res.as_ref() {
            Ok(processing_result) => self.update_status_success(processing_result),
            Err(tpe) => self.update_status_err(tpe),
        };
        res
    }

    /// Writes that a version has been started for this `TransactionProcessor` to the DB
    async fn mark_versions_started(&self, start_version: u64, end_version: u64) {
        aptos_logger::debug!(
            "[{}] Marking processing versions started from versions {} to {}",
            self.name(),
            start_version,
            end_version
        );
        let psms = ProcessorStatusModel::from_versions(
            self.name(),
            start_version,
            end_version,
            false,
            None,
        );
        self.get_metadata_handle()
            .apply_processor_status(&psms)
            .await;
    }

    /// Writes that a version has been completed successfully for this `TransactionProcessor` to the DB
    async fn update_status_success(&self, processing_result: &ProcessingResult) {
        aptos_logger::debug!(
            "[{}] Marking processing version OK from versions {} to {}",
            self.name(),
            processing_result.start_version,
            processing_result.end_version
        );
        PROCESSOR_SUCCESSES.with_label_values(&[self.name()]).inc();
        let psms = ProcessorStatusModel::from_versions(
            self.name(),
            processing_result.start_version,
            processing_result.end_version,
            true,
            None,
        );
        self.get_metadata_handle()
            .apply_processor_status(&psms)
            .await;
    }

    /// Writes that a version has errored for this `TransactionProcessor` to the DB
    async fn update_status_err(&self, tpe: &TransactionProcessingError) {
        aptos_logger::debug!(
            "[{}] Marking processing version Err: {:?}",
            self.name(),
            tpe
        );
        PROCESSOR_ERRORS.with_label_values(&[self.name()]).inc();
        let psm = ProcessorStatusModel::from_transaction_processing_err(tpe);
        self.get_metadata_handle()
            .apply_processor_status(&psm)
            .await;
    }
}
