//! Script history for one wallet index.
//!
//! The module process owns this database. It does not open the node chain store.
//! `NewBlock` writes one row per transaction output, and a spend when the prevout is
//! already in this database. `BlockDisconnected` deletes every row at that height
//! and above. Electrum `blockchain.scripthash.get_history` reads that store when
//! `listen` is set.

mod config;
mod electrum;
mod esplora;
mod index;
mod module;

pub use config::WalletIndexConfig;
pub use electrum::{HeaderHub, StatusHub, reply_line, serve, serve_listener};
pub use esplora::serve as serve_esplora;
pub use index::{
    HistoryTx, IndexedOutput, MempoolTx, ScriptActivity, TREE_NAME, clear_mempool, history,
    index_block, index_mempool, mempool_for, remove_mempool, rollback_from, script_activity,
    script_hash, script_hash_history,
};
pub use module::WalletIndexModule;
