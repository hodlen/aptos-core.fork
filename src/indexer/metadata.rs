use crate::models::ledger_info::LedgerInfo;

/// Indexer metadata handle.
/// Implement this trait in the caller side to support different DBs.
#[async_trait::async_trait]
pub trait IndexerMetaHandle: Send + Sync {
    async fn get_ledger_info(&self) -> Option<LedgerInfo>;
    async fn set_ledger_info(&self, ledger_info: LedgerInfo) -> std::io::Result<()>;
}
