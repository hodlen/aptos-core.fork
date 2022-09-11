use super::postgres_utils::get_pg_conn_from_pool;
use super::tailer::TailerMetaHandle;
use crate::database::execute_with_better_error;
use crate::diesel::RunQueryDsl;
use crate::{
    database::PgDbPool,
    models::ledger_info::LedgerInfo,
    schema::ledger_infos::{self, dsl},
};

pub struct PgTailerMetaHandle {
    connection_pool: PgDbPool,
}

#[async_trait::async_trait]
impl TailerMetaHandle for PgTailerMetaHandle {
    async fn get_ledger_info(&self) -> Option<LedgerInfo> {
        let query_chain = dsl::ledger_infos
            .load::<LedgerInfo>(&get_pg_conn_from_pool(&self.connection_pool))
            .expect("Error loading chain id from db");
        query_chain.first().map(|l| *l)
    }
    async fn set_ledger_info(&self, ledger_info: LedgerInfo) -> std::io::Result<()> {
        match execute_with_better_error(
            &get_pg_conn_from_pool(&self.connection_pool),
            diesel::insert_into(ledger_infos::table).values(ledger_info),
        ) {
            Ok(_) => Ok(()),
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                e.to_string(),
            )),
        }
    }
}

impl PgTailerMetaHandle {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}
