//! Electrum protocol 1.4 lines over the module database.
//!
//! Answers `server.version`, `server.ping`, `server.banner`,
//! `blockchain.scripthash.get_history`, `blockchain.scripthash.subscribe`,
//! `blockchain.headers.subscribe`, and `blockchain.scripthash.get_mempool`.
//! The scripthash argument is the hex of [`crate::script_hash`] bytes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use blvm_node::storage::database::Tree;
use blvm_protocol::Hash;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::info;

use crate::index::{self, txid_rpc_hex};

/// Listeners for one scripthash. A failed send drops that connection.
#[derive(Clone, Default)]
pub struct StatusHub {
    waiters: Arc<Mutex<HashMap<Hash, Vec<mpsc::UnboundedSender<String>>>>>,
}

impl StatusHub {
    pub fn watch(&self, script_hash: Hash, sender: mpsc::UnboundedSender<String>) {
        self.waiters
            .lock()
            .expect("status hub")
            .entry(script_hash)
            .or_default()
            .push(sender);
    }

    /// Push `blockchain.scripthash.subscribe` notifications for scripts whose rows changed.
    pub fn publish(&self, tree: &dyn Tree, script_hashes: &[Hash]) {
        let mut waiters = self.waiters.lock().expect("status hub");
        for script_hash in script_hashes {
            let Some(senders) = waiters.get_mut(script_hash) else {
                continue;
            };
            let line = notification_line(tree, script_hash);
            senders.retain(|sender| sender.send(line.clone()).is_ok());
        }
    }
}

/// Listeners for `blockchain.headers.subscribe`. A failed send drops that connection.
#[derive(Clone, Default)]
pub struct HeaderHub {
    waiters: Arc<Mutex<Vec<mpsc::UnboundedSender<String>>>>,
}

impl HeaderHub {
    pub fn watch(&self, sender: mpsc::UnboundedSender<String>) {
        self.waiters.lock().expect("header hub").push(sender);
    }

    /// Push the current tip, or JSON null when no header remains.
    pub fn publish(&self, tree: &dyn Tree) {
        let mut waiters = self.waiters.lock().expect("header hub");
        let line = header_notification(tree);
        waiters.retain(|sender| sender.send(line.clone()).is_ok());
    }
}

/// Bind `addr` and answer one JSON-RPC object, or a batch array, per line.
pub async fn serve(
    addr: std::net::SocketAddr,
    tree: Arc<dyn Tree>,
    hub: StatusHub,
    headers: HeaderHub,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("electrum listening on {addr}");
    serve_listener(listener, tree, hub, headers).await
}

/// Accept connections on an already-bound listener.
pub async fn serve_listener(
    listener: TcpListener,
    tree: Arc<dyn Tree>,
    hub: StatusHub,
    headers: HeaderHub,
) -> Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let tree = Arc::clone(&tree);
        let hub = hub.clone();
        let headers = headers.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(socket, tree, hub, headers).await {
                tracing::debug!("electrum connection: {error}");
            }
        });
    }
}

pub async fn handle_connection(
    socket: TcpStream,
    tree: Arc<dyn Tree>,
    hub: StatusHub,
    headers: HeaderHub,
) -> Result<()> {
    let (read, mut write) = socket.into_split();
    let mut lines = BufReader::new(read).lines();
    let (sender, mut notes) = mpsc::unbounded_channel();
    loop {
        tokio::select! {
            incoming = lines.next_line() => {
                match incoming {
                    Ok(Some(line)) if !line.is_empty() => {
                        for script_hash in subscribe_hashes(&line) {
                            hub.watch(script_hash, sender.clone());
                        }
                        if subscribes_headers(&line) {
                            headers.watch(sender.clone());
                        }
                        let response = reply_line(tree.as_ref(), &line);
                        write.write_all(response.as_bytes()).await?;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(error) => return Err(error.into()),
                }
            }
            note = notes.recv() => {
                let Some(note) = note else { break };
                write.write_all(note.as_bytes()).await?;
            }
        }
    }
    Ok(())
}

fn header_notification(tree: &dyn Tree) -> String {
    let tip = index::header_tip(tree)
        .ok()
        .flatten()
        .map(|(height, bytes)| json!({"hex": hex::encode(bytes), "height": height}));
    format!(
        "{}\n",
        json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [tip],
        })
    )
}

fn subscribes_headers(line: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
        return false;
    };
    let requests = match value {
        Value::Array(items) => items,
        other => vec![other],
    };
    requests.into_iter().any(|request| {
        request.get("method").and_then(Value::as_str) == Some("blockchain.headers.subscribe")
    })
}

fn notification_line(tree: &dyn Tree, script_hash: &Hash) -> String {
    let status = index::script_hash_history(tree, script_hash)
        .ok()
        .as_deref()
        .and_then(history_status);
    format!(
        "{}\n",
        json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [hex::encode(script_hash), status],
        })
    )
}

fn subscribe_hashes(line: &str) -> Vec<Hash> {
    let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
        return Vec::new();
    };
    let requests = match value {
        Value::Array(items) => items,
        other => vec![other],
    };
    requests
        .into_iter()
        .filter_map(|request| {
            if request.get("method").and_then(Value::as_str)
                != Some("blockchain.scripthash.subscribe")
            {
                return None;
            }
            let hex_hash = request
                .get("params")
                .and_then(|params| params.get(0))
                .and_then(Value::as_str)?;
            let bytes = hex::decode(hex_hash.trim()).ok()?;
            if bytes.len() != 32 {
                return None;
            }
            let mut script_hash = [0u8; 32];
            script_hash.copy_from_slice(&bytes);
            Some(script_hash)
        })
        .collect()
}

/// One request line in, one response line out, including the trailing newline.
pub fn reply_line(tree: &dyn Tree, line: &str) -> String {
    let parsed = match serde_json::from_str::<Value>(line.trim()) {
        Ok(value) => value,
        Err(_) => {
            return format!("{}\n", error_reply(Value::Null, -32700, "parse error"));
        }
    };
    let reply = if let Value::Array(batch) = parsed {
        Value::Array(
            batch
                .iter()
                .map(|request| reply_one(tree, request))
                .collect(),
        )
    } else {
        reply_one(tree, &parsed)
    };
    format!("{reply}\n")
}

fn reply_one(tree: &dyn Tree, request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let empty = Value::Array(Vec::new());
    let params = request.get("params").unwrap_or(&empty);
    match method {
        "server.version" => ok_reply(id, json!(["blvm-wallet-index 0.1.0", "1.4"])),
        "server.ping" => ok_reply(id, Value::Null),
        "server.banner" => ok_reply(id, json!("blvm-wallet-index")),
        "blockchain.scripthash.get_history" => match history_result(tree, params) {
            Ok(rows) => ok_reply(id, rows),
            Err(message) => error_reply(id, -32602, &message),
        },
        "blockchain.scripthash.get_mempool" => match mempool_result(tree, params) {
            Ok(rows) => ok_reply(id, rows),
            Err(message) => error_reply(id, -32602, &message),
        },
        "blockchain.scripthash.subscribe" => match status_result(tree, params) {
            Ok(status) => ok_reply(id, status.map(Value::String).unwrap_or(Value::Null)),
            Err(message) => error_reply(id, -32602, &message),
        },
        "blockchain.headers.subscribe" => match index::header_tip(tree) {
            Ok(Some((height, bytes))) => {
                ok_reply(id, json!({"hex": hex::encode(bytes), "height": height}))
            }
            Ok(None) => ok_reply(id, Value::Null),
            Err(error) => error_reply(id, -32603, &error.to_string()),
        },
        _ => error_reply(id, -32601, "method not found"),
    }
}

fn history_result(tree: &dyn Tree, params: &Value) -> Result<Value, String> {
    let rows = history_rows(tree, params)?;
    Ok(Value::Array(
        rows.into_iter()
            .map(|row| {
                json!({
                    "tx_hash": txid_rpc_hex(&row.txid),
                    "height": row.height,
                })
            })
            .collect(),
    ))
}

fn status_result(tree: &dyn Tree, params: &Value) -> Result<Option<String>, String> {
    let mut rows = history_rows(tree, params)?;
    rows.extend(mempool_status_rows(tree, params)?);
    Ok(history_status(&rows))
}

/// Electrum status: SHA256 of `tx_hash:height:` for each history row, hex-encoded.
/// No rows is `None`, which the protocol returns as JSON null.
fn history_status(rows: &[index::HistoryTx]) -> Option<String> {
    if rows.is_empty() {
        return None;
    }
    let mut payload = String::new();
    for row in rows {
        payload.push_str(&txid_rpc_hex(&row.txid));
        payload.push(':');
        payload.push_str(&row.height.to_string());
        payload.push(':');
    }
    Some(hex::encode(Sha256::digest(payload.as_bytes())))
}

fn mempool_result(tree: &dyn Tree, params: &Value) -> Result<Value, String> {
    let script_hash = script_hash_param(params)?;
    let rows = index::mempool_for(tree, &script_hash).map_err(|error| error.to_string())?;
    Ok(Value::Array(
        rows.into_iter()
            .map(|row| {
                json!({
                    "tx_hash": txid_rpc_hex(&row.txid),
                    "height": 0,
                    "fee": row.fee_sats,
                })
            })
            .collect(),
    ))
}

fn mempool_status_rows(tree: &dyn Tree, params: &Value) -> Result<Vec<index::HistoryTx>, String> {
    let script_hash = script_hash_param(params)?;
    let rows = index::mempool_for(tree, &script_hash).map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| index::HistoryTx {
            txid: row.txid,
            height: 0,
        })
        .collect())
}

fn history_rows(tree: &dyn Tree, params: &Value) -> Result<Vec<index::HistoryTx>, String> {
    let script_hash = script_hash_param(params)?;
    index::script_hash_history(tree, &script_hash).map_err(|error| error.to_string())
}

fn script_hash_param(params: &Value) -> Result<Hash, String> {
    let script_hash_hex = params
        .get(0)
        .and_then(Value::as_str)
        .ok_or_else(|| "scripthash hex required".to_string())?;
    let bytes = hex::decode(script_hash_hex.trim()).map_err(|error| error.to_string())?;
    if bytes.len() != 32 {
        return Err("scripthash must be 32 bytes".to_string());
    }
    let mut script_hash = [0u8; 32];
    script_hash.copy_from_slice(&bytes);
    Ok(script_hash)
}

fn ok_reply(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_reply(id: Value, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{TREE_NAME, index_block, index_mempool, rollback_from, script_hash};
    use blvm_protocol::block::calculate_tx_id;
    use blvm_protocol::{
        Block, BlockHeader, OutPoint, Transaction, TransactionInput, TransactionOutput,
    };
    use blvm_sdk::module::ModuleDb;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn tx(inputs: Vec<TransactionInput>, outputs: Vec<TransactionOutput>) -> Transaction {
        Transaction {
            version: 2,
            inputs: inputs.into(),
            outputs: outputs.into(),
            lock_time: 0,
        }
    }

    fn coinbase(value: i64, script: &[u8]) -> Transaction {
        tx(
            vec![TransactionInput {
                prevout: OutPoint {
                    hash: [0u8; 32],
                    index: 0xffff_ffff,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }],
            vec![TransactionOutput {
                value,
                script_pubkey: script.to_vec(),
            }],
        )
    }

    fn block(tx: Transaction) -> Block {
        Block::from_parts(BlockHeader::default(), Box::from(vec![tx]))
    }

    #[tokio::test]
    async fn get_history_lists_the_spend_and_disconnect_drops_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let tree = db.tree(TREE_NAME).expect("tree");
        let script_a = vec![0x51];
        let script_b = vec![0x52];
        let created = coinbase(50, &script_a);
        let created_txid = calculate_tx_id(&created);
        index_block(tree.as_ref(), &block(created), 1).unwrap();
        let spent = tx(
            vec![TransactionInput {
                prevout: OutPoint {
                    hash: created_txid,
                    index: 0,
                },
                script_sig: vec![0x00],
                sequence: 0xffff_ffff,
            }],
            vec![TransactionOutput {
                value: 40,
                script_pubkey: script_b.clone(),
            }],
        );
        let spent_txid = calculate_tx_id(&spent);
        index_block(tree.as_ref(), &block(spent), 2).unwrap();

        let hash_a = hex::encode(script_hash(&script_a));
        let request = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "blockchain.scripthash.get_history",
            "params": [hash_a]
        });
        let response: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &request.to_string()).trim()).unwrap();
        let rows = response["result"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["tx_hash"], txid_rpc_hex(&created_txid));
        assert_eq!(rows[0]["height"], 1);
        assert_eq!(rows[1]["tx_hash"], txid_rpc_hex(&spent_txid));
        assert_eq!(rows[1]["height"], 2);

        assert_eq!(rollback_from(tree.as_ref(), 2).unwrap().0, 2);
        let after: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &request.to_string()).trim()).unwrap();
        let rows = after["result"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["height"], 1);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Arc::clone(&tree);
        tokio::spawn(async move {
            let _ =
                serve_listener(listener, shared, StatusHub::default(), HeaderHub::default()).await;
        });
        let mut socket = TcpStream::connect(addr).await.unwrap();
        let line = format!("{request}\n");
        socket.write_all(line.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        let tcp_reply: Value =
            serde_json::from_str(std::str::from_utf8(&buf[..n]).unwrap().trim()).unwrap();
        assert_eq!(tcp_reply["result"].as_array().unwrap().len(), 1);
        assert_eq!(tcp_reply["id"], 7);

        let version = reply_line(
            tree.as_ref(),
            r#"{"jsonrpc":"2.0","id":1,"method":"server.version","params":["client","1.4"]}"#,
        );
        assert!(version.contains("1.4"));
        let missing = reply_line(
            tree.as_ref(),
            r#"{"jsonrpc":"2.0","id":1,"method":"blockchain.scripthash.get_history","params":[]}"#,
        );
        assert!(missing.contains("-32602"));
    }

    #[test]
    fn subscribe_status_is_null_until_history_and_changes_on_disconnect() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let tree = db.tree(TREE_NAME).expect("tree");
        let script = vec![0x51];
        let hash = hex::encode(script_hash(&script));
        let subscribe = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "blockchain.scripthash.subscribe",
            "params": [hash]
        });
        let empty: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &subscribe.to_string()).trim()).unwrap();
        assert!(empty["result"].is_null());

        let created = coinbase(50, &script);
        let created_txid = calculate_tx_id(&created);
        index_block(tree.as_ref(), &block(created), 4).unwrap();
        let spent = tx(
            vec![TransactionInput {
                prevout: OutPoint {
                    hash: created_txid,
                    index: 0,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }],
            vec![TransactionOutput {
                value: 40,
                script_pubkey: vec![0x52],
            }],
        );
        let spent_txid = calculate_tx_id(&spent);
        index_block(tree.as_ref(), &block(spent), 5).unwrap();

        let both: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &subscribe.to_string()).trim()).unwrap();
        let expected_both = status_payload(&[
            (txid_rpc_hex(&created_txid), 4),
            (txid_rpc_hex(&spent_txid), 5),
        ]);
        assert_eq!(both["result"], expected_both);

        rollback_from(tree.as_ref(), 5).unwrap();
        let one: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &subscribe.to_string()).trim()).unwrap();
        assert_eq!(
            one["result"],
            status_payload(&[(txid_rpc_hex(&created_txid), 4)])
        );
    }

    #[tokio::test]
    async fn headers_subscribe_follows_the_indexed_tip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let tree = db.tree(TREE_NAME).expect("tree");
        let request = json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "blockchain.headers.subscribe",
            "params": []
        });
        let empty: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &request.to_string()).trim()).unwrap();
        assert!(empty["result"].is_null());

        index_block(tree.as_ref(), &block_at(coinbase(50, &[0x51]), 4), 4).unwrap();
        index_block(tree.as_ref(), &block_at(coinbase(50, &[0x52]), 5), 5).unwrap();
        let tip: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &request.to_string()).trim()).unwrap();
        assert_eq!(tip["result"]["height"], 5);
        assert!(tip["result"]["hex"].as_str().unwrap().ends_with("05000000"));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let headers = HeaderHub::default();
        let pushed = headers.clone();
        let shared = Arc::clone(&tree);
        tokio::spawn(async move {
            let _ = serve_listener(listener, shared, StatusHub::default(), headers).await;
        });
        let mut socket = TcpStream::connect(addr).await.unwrap();
        socket
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut lines = BufReader::new(socket).lines();
        let first: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(first["result"]["height"], 5);

        rollback_from(tree.as_ref(), 5).unwrap();
        pushed.publish(tree.as_ref());
        let note: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(note["method"], "blockchain.headers.subscribe");
        assert_eq!(note["params"][0]["height"], 4);
        assert!(
            note["params"][0]["hex"]
                .as_str()
                .unwrap()
                .ends_with("04000000")
        );
    }

    fn block_at(tx: Transaction, nonce: u64) -> Block {
        let header = BlockHeader {
            nonce,
            ..BlockHeader::default()
        };
        Block::from_parts(header, Box::from(vec![tx]))
    }

    #[test]
    fn get_mempool_lists_the_unconfirmed_spend_until_it_confirms() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let tree = db.tree(TREE_NAME).expect("tree");
        let script_a = vec![0x51];
        let script_b = vec![0x52];
        let created = coinbase(50, &script_a);
        let created_txid = calculate_tx_id(&created);
        index_block(tree.as_ref(), &block(created), 1).unwrap();
        let spent = tx(
            vec![TransactionInput {
                prevout: OutPoint {
                    hash: created_txid,
                    index: 0,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }],
            vec![TransactionOutput {
                value: 40,
                script_pubkey: script_b,
            }],
        );
        let spent_txid = calculate_tx_id(&spent);
        index_mempool(tree.as_ref(), &spent, 10).unwrap();

        let hash_a = hex::encode(script_hash(&script_a));
        let request = json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "blockchain.scripthash.get_mempool",
            "params": [hash_a]
        });
        let response: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &request.to_string()).trim()).unwrap();
        let rows = response["result"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["tx_hash"], txid_rpc_hex(&spent_txid));
        assert_eq!(rows[0]["height"], 0);
        assert_eq!(rows[0]["fee"], 10);

        let history = json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "blockchain.scripthash.get_history",
            "params": [hash_a]
        });
        let confirmed: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &history.to_string()).trim()).unwrap();
        assert_eq!(confirmed["result"].as_array().unwrap().len(), 1);

        let subscribe = json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "blockchain.scripthash.subscribe",
            "params": [hash_a]
        });
        let status: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &subscribe.to_string()).trim()).unwrap();
        assert_eq!(
            status["result"],
            status_payload(&[
                (txid_rpc_hex(&created_txid), 1),
                (txid_rpc_hex(&spent_txid), 0),
            ])
        );

        index_block(tree.as_ref(), &block(spent), 2).unwrap();
        let after: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &request.to_string()).trim()).unwrap();
        assert_eq!(after["result"], json!([]));
        let history_after: Value =
            serde_json::from_str(reply_line(tree.as_ref(), &history.to_string()).trim()).unwrap();
        assert_eq!(history_after["result"].as_array().unwrap().len(), 2);
    }

    fn status_payload(rows: &[(String, u64)]) -> String {
        let mut payload = String::new();
        for (tx_hash, height) in rows {
            payload.push_str(tx_hash);
            payload.push(':');
            payload.push_str(&height.to_string());
            payload.push(':');
        }
        hex::encode(Sha256::digest(payload.as_bytes()))
    }
}
