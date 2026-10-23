//! blvm-wallet-index — script history module.
//!
//! Loads beside the node. The database is this process's module DB, not the node chain store.
//! Electrum binds only when `listen` is set in the module config.

use anyhow::Result;
use blvm_sdk::module::{ModuleBootstrap, ModuleDb};
use blvm_wallet_index::{
    HeaderHub, StatusHub, TREE_NAME, WalletIndexConfig, WalletIndexModule, serve, serve_esplora,
};
use std::sync::Arc;
use tracing::{info, warn};

const MODULE_NAME: &str = "blvm-wallet-index";

#[tokio::main]
async fn main() -> Result<()> {
    let bootstrap = ModuleBootstrap::init_module(MODULE_NAME);
    let db = ModuleDb::open_or_temp(&bootstrap.data_dir, MODULE_NAME)?;
    let outputs = Arc::from(db.as_db().open_tree(TREE_NAME)?);
    let hub = StatusHub::default();
    let headers = HeaderHub::default();
    let config = WalletIndexConfig::load(bootstrap.data_dir.join("config.toml"))?;
    if let Some(listen) = config.listen.clone() {
        match listen.parse() {
            Ok(addr) => {
                let tree = Arc::clone(&outputs);
                let hub = hub.clone();
                let headers = headers.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve(addr, tree, hub, headers).await {
                        warn!("electrum listener: {error}");
                    }
                });
                info!("electrum listen configured for {listen}");
            }
            Err(error) => warn!("electrum listen address {listen}: {error}"),
        }
    }
    if let Some(esplora) = config.esplora.clone() {
        match esplora.parse() {
            Ok(addr) => {
                let tree = Arc::clone(&outputs);
                tokio::spawn(async move {
                    if let Err(error) = serve_esplora(addr, tree).await {
                        warn!("esplora listener: {error}");
                    }
                });
                info!("esplora listen configured for {esplora}");
            }
            Err(error) => warn!("esplora listen address {esplora}: {error}"),
        }
    }

    let setup = move |_node_api: Arc<dyn blvm_node::module::traits::NodeAPI>,
                      _db: Arc<dyn blvm_node::storage::database::Database>,
                      _data_dir: &std::path::Path| {
        let outputs = Arc::clone(&outputs);
        let hub = hub.clone();
        let headers = headers.clone();
        async move {
            let module = WalletIndexModule::with_hubs(outputs, hub, headers);
            Ok((module.clone(), module))
        }
    };

    blvm_sdk::run_module! {
        bootstrap: &bootstrap,
        module_name: MODULE_NAME,
        module_type: WalletIndexModule,
        cli_type: WalletIndexModule,
        db: db.as_db(),
        setup: setup,
        event_types: WalletIndexModule::event_types(),
    }?;

    warn!("blvm-wallet-index shutting down");
    Ok(())
}
