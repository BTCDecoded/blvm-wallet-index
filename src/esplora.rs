//! Esplora HTTP over the same script index as Electrum.
//!
//! `GET /blocks/tip/height` is the highest stored header.
//! `GET /address/<address>` is confirmed output totals for that script.
//! This is not a full Esplora server: transaction hex and `/address/.../txs` are absent.

use std::sync::Arc;

use anyhow::Result;
use blvm_node::rpc::rawtx::address_string_to_script_pubkey;
use blvm_node::storage::database::Tree;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::index::{self, header_tip};

/// Bind `addr` and answer one HTTP request per connection.
pub async fn serve(addr: std::net::SocketAddr, tree: Arc<dyn Tree>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("esplora listening on {addr}");
    loop {
        let (socket, _) = listener.accept().await?;
        let tree = Arc::clone(&tree);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(socket, tree).await {
                tracing::debug!("esplora connection: {error}");
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream, tree: Arc<dyn Tree>) -> Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = socket.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let (status, content_type, body) = response(tree.as_ref(), request);
    let head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reason(status),
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    Ok(())
}

/// Status, content type, and body for one request head.
pub fn response(tree: &dyn Tree, request: &str) -> (u16, &'static str, String) {
    let Some(start) = request.lines().next() else {
        return (400, "text/plain", "bad request".to_string());
    };
    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("").split('?').next().unwrap_or("");
    if method != "GET" {
        return (405, "text/plain", "method not allowed".to_string());
    }
    if path == "/blocks/tip/height" {
        return match header_tip(tree) {
            Ok(Some((height, _))) => (200, "text/plain", height.to_string()),
            Ok(None) => (404, "text/plain", "no header".to_string()),
            Err(error) => (500, "text/plain", error.to_string()),
        };
    }
    if let Some(address) = path.strip_prefix("/address/") {
        if address.is_empty() || address.contains('/') {
            return (404, "text/plain", "not found".to_string());
        }
        let script = match address_string_to_script_pubkey(address) {
            Ok(script) => script,
            Err(error) => return (400, "text/plain", error.to_string()),
        };
        return match index::script_activity(tree, &script) {
            Ok(stats) => (200, "application/json", address_body(address, &stats)),
            Err(error) => (500, "text/plain", error.to_string()),
        };
    }
    (404, "text/plain", "not found".to_string())
}

fn address_body(address: &str, stats: &index::ScriptActivity) -> String {
    let spent_sum = match stats.spent_txo_sum {
        Some(sum) => Value::from(sum),
        None => Value::Null,
    };
    json!({
        "address": address,
        "chain_stats": {
            "funded_txo_count": stats.funded_txo_count,
            "funded_txo_sum": stats.funded_txo_sum,
            "spent_txo_count": stats.spent_txo_count,
            "spent_txo_sum": spent_sum,
            "tx_count": stats.tx_count,
        },
        "mempool_stats": {
            "tx_count": stats.mempool_tx_count,
        },
    })
    .to_string()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{TREE_NAME, index_block};
    use blvm_protocol::block::calculate_tx_id;
    use blvm_protocol::{
        Block, BlockHeader, OutPoint, Transaction, TransactionInput, TransactionOutput,
    };
    use blvm_sdk::module::ModuleDb;

    const ADDRESS: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";

    fn script() -> Vec<u8> {
        address_string_to_script_pubkey(ADDRESS).unwrap()
    }

    fn coinbase(value: i64, script: &[u8]) -> Transaction {
        Transaction {
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
                script_pubkey: script.to_vec(),
            }]
            .into(),
            lock_time: 0,
        }
    }

    fn block_at(tx: Transaction, height: u64) -> Block {
        let mut header = BlockHeader::default();
        header.nonce = height;
        Block::from_parts(header, Box::from(vec![tx]))
    }

    #[test]
    fn tip_height_and_address_stats_follow_the_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let tree = db.tree(TREE_NAME).expect("tree");
        let (status, _, body) = response(tree.as_ref(), "GET /blocks/tip/height HTTP/1.1");
        assert_eq!(status, 404);
        assert_eq!(body, "no header");

        let created = coinbase(50_000, &script());
        let created_txid = calculate_tx_id(&created);
        index_block(tree.as_ref(), &block_at(created, 4), 4).unwrap();
        let (status, kind, body) = response(tree.as_ref(), "GET /blocks/tip/height HTTP/1.1");
        assert_eq!(status, 200);
        assert_eq!(kind, "text/plain");
        assert_eq!(body, "4");

        let path = format!("GET /address/{ADDRESS} HTTP/1.1");
        let (status, kind, body) = response(tree.as_ref(), &path);
        assert_eq!(status, 200);
        assert_eq!(kind, "application/json");
        let stats: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(stats["chain_stats"]["funded_txo_count"], 1);
        assert_eq!(stats["chain_stats"]["funded_txo_sum"], 50_000);
        assert_eq!(stats["chain_stats"]["spent_txo_count"], 0);
        assert_eq!(stats["chain_stats"]["spent_txo_sum"], 0);
        assert_eq!(stats["chain_stats"]["tx_count"], 1);
        assert_eq!(stats["mempool_stats"]["tx_count"], 0);

        let spend = Transaction {
            version: 2,
            inputs: vec![TransactionInput {
                prevout: OutPoint {
                    hash: created_txid,
                    index: 0,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }]
            .into(),
            outputs: vec![TransactionOutput {
                value: 40_000,
                script_pubkey: vec![0x51],
            }]
            .into(),
            lock_time: 0,
        };
        index_block(tree.as_ref(), &block_at(spend, 5), 5).unwrap();
        let (_, _, body) = response(tree.as_ref(), &path);
        let stats: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(stats["chain_stats"]["spent_txo_count"], 1);
        assert_eq!(stats["chain_stats"]["spent_txo_sum"], 50_000);
        assert_eq!(stats["chain_stats"]["tx_count"], 2);
        assert_eq!(
            response(tree.as_ref(), "GET /blocks/tip/height HTTP/1.1").2,
            "5"
        );

        let (status, _, _) = response(tree.as_ref(), "GET /address/not-an-address HTTP/1.1");
        assert_eq!(status, 400);
        let (status, _, _) = response(tree.as_ref(), "GET /tx/abcd HTTP/1.1");
        assert_eq!(status, 404);
    }
}
