//! Node events in, module database out.

use std::sync::Arc;

use blvm_node::module::ipc::protocol::{EventMessage, EventPayload};
use blvm_node::module::traits::ModuleError;
use blvm_node::storage::database::{Database, Tree};
use blvm_sdk::module::prelude::*;
use blvm_sdk_macros::module;
use serde_json::{Value, json};
use tracing::warn;

use crate::electrum::{HeaderHub, StatusHub};
use crate::index::{self, txid_rpc_hex};

#[derive(Clone)]
pub struct WalletIndexModule {
    outputs: Arc<dyn Tree>,
    hub: StatusHub,
    headers: HeaderHub,
}

impl WalletIndexModule {
    pub fn new(outputs: Arc<dyn Tree>) -> Self {
        Self::with_hub(outputs, StatusHub::default())
    }

    pub fn with_hub(outputs: Arc<dyn Tree>, hub: StatusHub) -> Self {
        Self::with_hubs(outputs, hub, HeaderHub::default())
    }

    pub fn with_hubs(outputs: Arc<dyn Tree>, hub: StatusHub, headers: HeaderHub) -> Self {
        Self {
            outputs,
            hub,
            headers,
        }
    }

    fn publish(&self, scripts: &[blvm_protocol::Hash]) {
        self.hub.publish(self.outputs.as_ref(), scripts);
    }
}

async fn mempool_fee(
    api: &dyn blvm_node::module::traits::NodeAPI,
    tx: &blvm_protocol::Transaction,
) -> Result<u64, ModuleError> {
    let mut inputs: i64 = 0;
    let mut known = true;
    for input in tx.inputs.iter() {
        if input.prevout.index == 0xffff_ffff {
            continue;
        }
        match api.get_utxo(&input.prevout).await? {
            Some(utxo) => inputs += utxo.value,
            None => known = false,
        }
    }
    if !known {
        return Ok(0);
    }
    let outputs: i64 = tx.outputs.iter().map(|output| output.value).sum();
    Ok(inputs.saturating_sub(outputs).max(0) as u64)
}

#[module(name = "blvm-wallet-index")]
impl WalletIndexModule {
    #[on_event(NewBlock)]
    async fn on_new_block(
        &self,
        event: &EventMessage,
        ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let EventPayload::NewBlock { block_hash, height } = &event.payload else {
            return Ok(());
        };
        let api = ctx
            .node_api()
            .ok_or_else(|| ModuleError::Other("wallet index requires the node API".into()))?;
        let block = match api.get_block(block_hash).await {
            Ok(Some(block)) => block,
            Ok(None) => {
                warn!(
                    "wallet index: block {} at height {} is absent",
                    txid_rpc_hex(block_hash),
                    height
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let (_rows, scripts) = index::index_block(self.outputs.as_ref(), &block, *height)
            .map_err(|error| ModuleError::Other(error.to_string()))?;
        self.publish(&scripts);
        self.headers.publish(self.outputs.as_ref());
        Ok(())
    }

    #[on_event(BlockDisconnected)]
    async fn on_block_disconnected(
        &self,
        event: &EventMessage,
        _ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let EventPayload::BlockDisconnected { height, .. } = &event.payload else {
            return Ok(());
        };
        let (_rows, scripts) = index::rollback_from(self.outputs.as_ref(), *height)
            .map_err(|error| ModuleError::Other(error.to_string()))?;
        self.publish(&scripts);
        self.headers.publish(self.outputs.as_ref());
        Ok(())
    }

    #[on_event(MempoolTransactionAdded)]
    async fn on_mempool_added(
        &self,
        event: &EventMessage,
        ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let EventPayload::MempoolTransactionAdded { tx_hash, .. } = &event.payload else {
            return Ok(());
        };
        let api = ctx
            .node_api()
            .ok_or_else(|| ModuleError::Other("wallet index requires the node API".into()))?;
        let tx = match api.get_mempool_transaction(tx_hash).await {
            Ok(Some(tx)) => tx,
            Ok(None) => {
                warn!(
                    "wallet index: mempool transaction {} is absent",
                    txid_rpc_hex(tx_hash)
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let fee = mempool_fee(api.as_ref(), &tx).await?;
        let scripts = index::index_mempool(self.outputs.as_ref(), &tx, fee)
            .map_err(|error| ModuleError::Other(error.to_string()))?;
        self.publish(&scripts);
        Ok(())
    }

    #[on_event(MempoolTransactionRemoved)]
    async fn on_mempool_removed(
        &self,
        event: &EventMessage,
        _ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let EventPayload::MempoolTransactionRemoved { tx_hash, .. } = &event.payload else {
            return Ok(());
        };
        let scripts = index::remove_mempool(self.outputs.as_ref(), tx_hash)
            .map_err(|error| ModuleError::Other(error.to_string()))?;
        self.publish(&scripts);
        Ok(())
    }

    #[on_event(MempoolCleared)]
    async fn on_mempool_cleared(
        &self,
        event: &EventMessage,
        _ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let EventPayload::MempoolCleared { .. } = &event.payload else {
            return Ok(());
        };
        let scripts = index::clear_mempool(self.outputs.as_ref())
            .map_err(|error| ModuleError::Other(error.to_string()))?;
        self.publish(&scripts);
        Ok(())
    }

    /// Subscribed so a reorg notice is not dropped. Row deletion is `BlockDisconnected`.
    #[on_event(ChainReorg)]
    async fn on_chain_reorg(
        &self,
        _event: &EventMessage,
        _ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        Ok(())
    }

    #[rpc_method(name = "getscripthistory")]
    fn getscripthistory(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        let script_hex = params.get(0).and_then(Value::as_str).ok_or_else(|| {
            ModuleError::Other("getscripthistory expects one script hex string".into())
        })?;
        let script = hex::decode(script_hex.trim())
            .map_err(|error| ModuleError::Other(format!("script hex: {error}")))?;
        let rows = index::history(self.outputs.as_ref(), &script)
            .map_err(|error| ModuleError::Other(error.to_string()))?;
        let history = rows
            .into_iter()
            .map(|row| {
                json!({
                    "txid": txid_rpc_hex(&row.txid),
                    "vout": row.vout,
                    "value": row.value,
                    "height": row.height,
                    "script_pubkey": hex::encode(&row.script_pubkey),
                })
            })
            .collect::<Vec<_>>();
        Ok(Value::Array(history))
    }

    #[command]
    fn help(&self, _ctx: &InvocationContext) -> Result<String, ModuleError> {
        Ok(
            "blvm-wallet-index stores script history in its own database.\n\
             getscripthistory <script hex> returns the outputs that created that script.\n\
             BlockDisconnected deletes rows at that height and above."
                .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::electrum::{HeaderHub, StatusHub, serve_listener};
    use crate::index::script_hash;
    use blvm_node::module::traits::EventType;
    use blvm_protocol::block::calculate_tx_id;
    use blvm_protocol::{
        Block, BlockHeader, OutPoint, Transaction, TransactionInput, TransactionOutput,
    };
    use blvm_sdk::module::ModuleDb;
    use blvm_sdk::module::runner::InvocationContext;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{Duration, timeout};

    fn open_module() -> (tempfile::TempDir, WalletIndexModule, Arc<dyn Database>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let outputs = db.tree(crate::TREE_NAME).expect("tree");
        let module = WalletIndexModule::new(outputs);
        (dir, module, db.as_db())
    }

    fn one_output_block(value: i64, script_pubkey: &[u8]) -> Block {
        let tx = Transaction {
            version: 2,
            inputs: vec![TransactionInput {
                prevout: OutPoint {
                    hash: [0u8; 32],
                    index: 0xffff_ffff,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }]
            .into(),
            outputs: vec![TransactionOutput {
                value,
                script_pubkey: script_pubkey.to_vec(),
            }]
            .into(),
            lock_time: 0,
        };
        Block::from_parts(BlockHeader::default(), Box::from(vec![tx]))
    }

    #[test]
    fn registers_the_phase0_events_and_getscripthistory() {
        let events = WalletIndexModule::event_types();
        assert!(events.contains(&EventType::NewBlock));
        assert!(events.contains(&EventType::BlockDisconnected));
        assert!(events.contains(&EventType::ChainReorg));
        assert!(events.contains(&EventType::MempoolTransactionAdded));
        assert!(events.contains(&EventType::MempoolTransactionRemoved));
        assert!(events.contains(&EventType::MempoolCleared));
        assert_eq!(
            WalletIndexModule::rpc_method_names(),
            vec!["getscripthistory"]
        );
    }

    #[tokio::test]
    async fn rpc_returns_the_output_and_disconnect_clears_it() {
        let (_dir, module, db) = open_module();
        let script = vec![0x51];
        let block = one_output_block(50, &script);
        let txid = calculate_tx_id(&block.transactions[0]);
        index::index_block(module.outputs.as_ref(), &block, 7).unwrap();

        let params = json!([hex::encode(&script)]);
        let rows = module
            .dispatch_rpc("getscripthistory", &params, &db)
            .unwrap();
        assert_eq!(rows.as_array().map(Vec::len), Some(1));
        assert_eq!(rows[0]["txid"], txid_rpc_hex(&txid));
        assert_eq!(rows[0]["vout"], 0);
        assert_eq!(rows[0]["value"], 50);
        assert_eq!(rows[0]["height"], 7);
        assert_eq!(rows[0]["script_pubkey"], hex::encode(&script));

        let ctx = InvocationContext::new(Arc::clone(&db));
        module
            .dispatch_event(
                EventMessage {
                    event_type: EventType::ChainReorg,
                    payload: EventPayload::ChainReorg {
                        old_tip: [1u8; 32],
                        new_tip: [2u8; 32],
                    },
                },
                &ctx,
            )
            .await
            .unwrap();
        let still = module
            .dispatch_rpc("getscripthistory", &params, &db)
            .unwrap();
        assert_eq!(still.as_array().map(Vec::len), Some(1));

        module
            .dispatch_event(
                EventMessage {
                    event_type: EventType::BlockDisconnected,
                    payload: EventPayload::BlockDisconnected {
                        hash: [3u8; 32],
                        height: 7,
                    },
                },
                &ctx,
            )
            .await
            .unwrap();
        let cleared = module
            .dispatch_rpc("getscripthistory", &params, &db)
            .unwrap();
        assert_eq!(cleared, json!([]));
    }

    #[tokio::test]
    async fn new_block_without_the_node_api_fails() {
        let (_dir, module, db) = open_module();
        let ctx = InvocationContext::new(db);
        let error = module
            .dispatch_event(
                EventMessage {
                    event_type: EventType::NewBlock,
                    payload: EventPayload::NewBlock {
                        block_hash: [4u8; 32],
                        height: 1,
                    },
                },
                &ctx,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("node API"));
    }

    #[tokio::test]
    async fn mempool_removal_drops_the_unconfirmed_row() {
        let (_dir, module, db) = open_module();
        let script = vec![0x51];
        let block = one_output_block(50, &script);
        index::index_block(module.outputs.as_ref(), &block, 1).unwrap();
        let created = &block.transactions[0];
        let spend = Transaction {
            version: 2,
            inputs: vec![TransactionInput {
                prevout: OutPoint {
                    hash: calculate_tx_id(created),
                    index: 0,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }]
            .into(),
            outputs: vec![TransactionOutput {
                value: 40,
                script_pubkey: vec![0x52],
            }]
            .into(),
            lock_time: 0,
        };
        let txid = calculate_tx_id(&spend);
        index::index_mempool(module.outputs.as_ref(), &spend, 10).unwrap();
        assert_eq!(
            index::mempool_for(module.outputs.as_ref(), &script_hash(&script))
                .unwrap()
                .len(),
            1
        );
        let ctx = InvocationContext::new(db);
        module
            .dispatch_event(
                EventMessage {
                    event_type: EventType::MempoolTransactionRemoved,
                    payload: EventPayload::MempoolTransactionRemoved {
                        tx_hash: txid,
                        reason: "replaced".to_string(),
                        mempool_size: 0,
                    },
                },
                &ctx,
            )
            .await
            .unwrap();
        assert!(
            index::mempool_for(module.outputs.as_ref(), &script_hash(&script))
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn disconnect_pushes_a_null_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let outputs = db.tree(crate::TREE_NAME).expect("tree");
        let hub = StatusHub::default();
        let module = WalletIndexModule::with_hub(Arc::clone(&outputs), hub.clone());
        let script = vec![0x51];
        index::index_block(outputs.as_ref(), &one_output_block(50, &script), 7).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tree = Arc::clone(&outputs);
        tokio::spawn(async move {
            let _ = serve_listener(listener, tree, hub, HeaderHub::default()).await;
        });
        let mut socket = TcpStream::connect(addr).await.unwrap();
        let hash = hex::encode(script_hash(&script));
        let request = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"blockchain.scripthash.subscribe\",\"params\":[\"{hash}\"]}}\n"
        );
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut lines = BufReader::new(socket).lines();
        let first: serde_json::Value = serde_json::from_str(
            &timeout(Duration::from_secs(2), lines.next_line())
                .await
                .expect("subscribe reply")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(first["result"].is_string());

        let ctx = InvocationContext::new(db.as_db());
        module
            .dispatch_event(
                EventMessage {
                    event_type: EventType::BlockDisconnected,
                    payload: EventPayload::BlockDisconnected {
                        hash: [9u8; 32],
                        height: 7,
                    },
                },
                &ctx,
            )
            .await
            .unwrap();
        let note: serde_json::Value = serde_json::from_str(
            &timeout(Duration::from_secs(2), lines.next_line())
                .await
                .expect("status push")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(note["method"], "blockchain.scripthash.subscribe");
        assert_eq!(note["params"][0], hash);
        assert!(note["params"][1].is_null());
    }

    #[tokio::test]
    async fn signet_invalidate_disconnects_the_index() {
        use blvm_consensus::reorganization::BlockUndoLog;
        use blvm_node::module::api::events::EventManager;
        use blvm_node::module::ipc::protocol::ModuleMessage;
        use blvm_node::node::event_publisher::EventPublisher;
        use blvm_node::rpc::blockchain::BlockchainRpc;
        use blvm_node::storage::Storage;
        use blvm_node::storage::chainstate::ChainParams;
        use blvm_node::storage::hashing::hash_to_rpc_hex;
        use blvm_protocol::{BitcoinProtocolEngine, ProtocolVersion};
        use tokio::sync::mpsc;

        let chain_dir = tempfile::tempdir().expect("chain dir");
        let storage = Arc::new(Storage::new(chain_dir.path()).expect("storage"));
        let protocol = Arc::new(BitcoinProtocolEngine::new(ProtocolVersion::Signet).unwrap());
        let genesis = protocol.get_network_params().genesis_block.header.clone();
        let params = ChainParams {
            network: "signet".to_string(),
            ..ChainParams::default()
        };
        storage
            .chain()
            .initialize_with_params(&genesis, params)
            .unwrap();
        let genesis_hash = storage.chain().get_tip_hash().unwrap().unwrap();
        storage
            .blocks()
            .store_header(&genesis_hash, &genesis)
            .unwrap();

        let script = vec![0x51];
        let coinbase = Transaction {
            version: 2,
            inputs: vec![TransactionInput {
                prevout: OutPoint {
                    hash: [0u8; 32],
                    index: 0xffff_ffff,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }]
            .into(),
            outputs: vec![TransactionOutput {
                value: 50_000,
                script_pubkey: script.clone(),
            }]
            .into(),
            lock_time: 0,
        };
        let block = Block::from_parts(
            BlockHeader {
                version: genesis.version,
                prev_block_hash: genesis_hash,
                merkle_root: [0u8; 32],
                timestamp: genesis.timestamp.saturating_add(1),
                bits: genesis.bits,
                nonce: genesis.nonce.wrapping_add(1),
            },
            Box::from(vec![coinbase]),
        );
        let hash = storage.blocks().get_block_hash(&block);
        storage.blocks().store_block(&block).unwrap();
        storage.blocks().store_header(&hash, &block.header).unwrap();
        storage.blocks().store_height(1, &hash).unwrap();
        storage
            .blocks()
            .store_undo_log(&hash, &BlockUndoLog::new())
            .unwrap();
        storage.chain().update_tip(&hash, &block.header, 1).unwrap();

        let info = BlockchainRpc::with_dependencies_and_protocol(
            Arc::clone(&storage),
            Arc::clone(&protocol),
        )
        .get_blockchain_info()
        .await
        .unwrap();
        assert_eq!(info["chain"], "signet");

        let (_dir, module, db) = open_module();
        index::index_block(module.outputs.as_ref(), &block, 1).unwrap();
        assert_eq!(
            index::history(module.outputs.as_ref(), &script)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            index::header_tip(module.outputs.as_ref())
                .unwrap()
                .unwrap()
                .0,
            1
        );
        let (status, _, body) =
            crate::esplora::response(module.outputs.as_ref(), "GET /blocks/tip/height HTTP/1.1");
        assert_eq!(status, 200);
        assert_eq!(body, "1");

        let events = Arc::new(EventManager::new());
        let (sender, mut receiver) = mpsc::channel(8);
        events
            .subscribe_module(
                "blvm-wallet-index_signet".to_string(),
                vec![EventType::BlockDisconnected, EventType::ChainReorg],
                sender,
            )
            .await
            .unwrap();
        BlockchainRpc::with_dependencies(Arc::clone(&storage))
            .with_event_publisher(Some(Arc::new(EventPublisher::new(events))))
            .invalidate_block(&json!([hash_to_rpc_hex(&hash)]))
            .await
            .unwrap();
        assert_eq!(storage.chain().get_height().unwrap().unwrap(), 0);

        let ctx = InvocationContext::new(Arc::clone(&db));
        for _ in 0..2 {
            let message = timeout(Duration::from_secs(2), receiver.recv())
                .await
                .expect("disconnect event")
                .unwrap();
            let ModuleMessage::Event(event) = message else {
                panic!("expected an event");
            };
            if event.event_type == EventType::BlockDisconnected {
                let EventPayload::BlockDisconnected { height, .. } = &event.payload else {
                    panic!("disconnect payload");
                };
                assert_eq!(*height, 1);
            }
            module.dispatch_event(event, &ctx).await.unwrap();
        }
        assert!(
            index::history(module.outputs.as_ref(), &script)
                .unwrap()
                .is_empty()
        );
        assert!(
            index::header_tip(module.outputs.as_ref())
                .unwrap()
                .is_none()
        );
        let (status, _, body) =
            crate::esplora::response(module.outputs.as_ref(), "GET /blocks/tip/height HTTP/1.1");
        assert_eq!(status, 404);
        assert_eq!(body, "no header");
    }

    #[test]
    fn getscripthistory_rejects_a_missing_or_bad_script() {
        let (_dir, module, db) = open_module();
        assert!(
            module
                .dispatch_rpc("getscripthistory", &json!([]), &db)
                .is_err()
        );
        assert!(
            module
                .dispatch_rpc("getscripthistory", &json!(["zz"]), &db)
                .is_err()
        );
    }
}
