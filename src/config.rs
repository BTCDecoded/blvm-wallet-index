//! Module configuration. Electrum and Esplora stay closed unless their addresses are set.

use blvm_sdk_macros::config;
use serde::{Deserialize, Serialize};

/// `listen` is a socket address such as `127.0.0.1:50001`. Unset means do not bind.
#[config(name = "blvm-wallet-index")]
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct WalletIndexConfig {
    #[serde(default)]
    #[config_env]
    pub listen: Option<String>,
    /// Esplora HTTP address, such as `127.0.0.1:3000`. Unset means do not bind.
    #[serde(default)]
    #[config_env]
    pub esplora: Option<String>,
}

blvm_sdk::impl_module_config!(WalletIndexConfig);

impl WalletIndexConfig {
    pub fn to_context_map(&self) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        if let Some(listen) = &self.listen {
            map.insert("blvm-wallet-index.listen".to_string(), listen.clone());
        }
        if let Some(esplora) = &self.esplora {
            map.insert("blvm-wallet-index.esplora".to_string(), esplora.clone());
        }
        map
    }
}
