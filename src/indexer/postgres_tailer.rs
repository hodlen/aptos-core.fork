use crate::models::ledger_info::LedgerInfo;

use crate::database::PgDbPool;

use super::tailer::TailerMetaHandle;

pub struct PgTailerMetaHandle {
    connection_pool: PgDbPool,
}

#[async_trait::async_trait]
impl TailerMetaHandle for PgTailerMetaHandle {
    async fn get_ledger_info(&self) -> Option<LedgerInfo> {
        None
    }
    async fn set_ledger_info(&self, ledger_info: LedgerInfo) -> std::io::Result<()> {
        Ok(())
    }
}

impl PgTailerMetaHandle {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}
