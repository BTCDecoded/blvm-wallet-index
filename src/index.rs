//! Output rows keyed by height and by script hash.
//!
//! Script hash bytes are SHA256(scriptPubKey), reversed. That is the byte string
//! an Electrum client sends after it hex-decodes `scripthash`.

use anyhow::{Result, bail};
use blvm_node::storage::database::Tree;
use blvm_protocol::block::calculate_tx_id;
use blvm_protocol::{Block, BlockHeader, Hash};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Named tree inside the module database. Not a node LMDB tree.
pub const TREE_NAME: &str = "script_history";

/// One created output. `txid` is internal digest order, the same order `calculate_tx_id` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexedOutput {
    pub txid: Hash,
    pub vout: u32,
    pub value: i64,
    pub script_pubkey: Vec<u8>,
    pub height: u64,
}

/// One confirmed transaction that created or spent a script. `txid` is internal digest order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryTx {
    pub txid: Hash,
    pub height: u64,
}

/// SHA256(scriptPubKey), byte-reversed.
pub fn script_hash(script_pubkey: &[u8]) -> Hash {
    let digest = Sha256::digest(script_pubkey);
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&digest);
    hash.reverse();
    hash
}

/// Display order used by node RPC hash fields: reverse the internal digest, then hex.
pub fn txid_rpc_hex(txid: &Hash) -> String {
    let mut display = *txid;
    display.reverse();
    hex::encode(display)
}

/// Write every output in `block` at `height`. Writing the same block again replaces those rows.
///
/// Outputs are stored first. An input whose prevout is already in this database records a spend
/// of that script at `height`, including a later transaction in the same block.
pub fn index_block(tree: &dyn Tree, block: &Block, height: u64) -> Result<(usize, Vec<Hash>)> {
    let mut written = 0;
    let mut scripts = Vec::new();
    for tx in block.transactions.iter() {
        let txid = calculate_tx_id(tx);
        for (vout, output) in tx.outputs.iter().enumerate() {
            let vout = u32::try_from(vout)?;
            let script_hash = script_hash(&output.script_pubkey);
            let mut value = Vec::with_capacity(8 + output.script_pubkey.len());
            value.extend_from_slice(&output.value.to_be_bytes());
            value.extend_from_slice(&output.script_pubkey);
            tree.insert(&out_key(height, &txid, vout), &value)?;
            tree.insert(&script_key(&script_hash, height, &txid, vout), &[0])?;
            tree.insert(&prevout_key(&txid, vout), &height.to_be_bytes())?;
            scripts.push(script_hash);
            written += 1;
        }
    }
    for tx in block.transactions.iter() {
        let txid = calculate_tx_id(tx);
        for (vin, input) in tx.inputs.iter().enumerate() {
            if input.prevout.index == 0xffff_ffff {
                continue;
            }
            if let Some(script_hash) =
                record_spend(tree, &txid, u32::try_from(vin)?, input.prevout, height)?
            {
                scripts.push(script_hash);
            }
        }
    }
    for tx in block.transactions.iter() {
        scripts.extend(remove_mempool(tree, &calculate_tx_id(tx))?);
    }
    tree.insert(&header_key(height), &header_wire(&block.header))?;
    Ok((written, dedup_hashes(scripts)))
}

/// One unconfirmed transaction that creates or spends a script. `txid` is internal digest order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolTx {
    pub txid: Hash,
    pub fee_sats: u64,
}

/// Record an unconfirmed transaction. A later `index_block` of the same txid drops these rows.
pub fn index_mempool(
    tree: &dyn Tree,
    tx: &blvm_protocol::Transaction,
    fee_sats: u64,
) -> Result<Vec<Hash>> {
    let txid = calculate_tx_id(tx);
    let _ = remove_mempool(tree, &txid)?;
    let mut scripts = Vec::new();
    for output in tx.outputs.iter() {
        scripts.push(script_hash(&output.script_pubkey));
    }
    for input in tx.inputs.iter() {
        if input.prevout.index == 0xffff_ffff {
            continue;
        }
        if let Some(script_hash) = spent_script(tree, input.prevout)? {
            scripts.push(script_hash);
        }
    }
    let scripts = dedup_hashes(scripts);
    let fee = fee_sats.to_be_bytes();
    for script_hash in &scripts {
        tree.insert(&mempool_script_key(script_hash, &txid), &fee)?;
        tree.insert(&mempool_tx_key(&txid, script_hash), &[0])?;
    }
    Ok(scripts)
}

/// Drop one unconfirmed transaction. Returns the script hashes that changed.
pub fn remove_mempool(tree: &dyn Tree, txid: &Hash) -> Result<Vec<Hash>> {
    let mut prefix = [0u8; 33];
    prefix[0] = b't';
    prefix[1..].copy_from_slice(txid);
    let rows: Vec<(Vec<u8>, Vec<u8>)> = tree.iter().collect::<Result<Vec<_>, _>>()?;
    let mut scripts = Vec::new();
    for (key, _) in rows {
        if key.len() != MEMPOOL_KEY_LEN || !key.starts_with(&prefix) {
            continue;
        }
        let mut script_hash = [0u8; 32];
        script_hash.copy_from_slice(&key[33..65]);
        tree.remove(&mempool_script_key(&script_hash, txid))?;
        tree.remove(&key)?;
        scripts.push(script_hash);
    }
    Ok(dedup_hashes(scripts))
}

/// Drop every unconfirmed row.
pub fn clear_mempool(tree: &dyn Tree) -> Result<Vec<Hash>> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = tree.iter().collect::<Result<Vec<_>, _>>()?;
    let mut scripts = Vec::new();
    for (key, _) in rows {
        if key.first() != Some(&b'u') && key.first() != Some(&b't') {
            continue;
        }
        if key.first() == Some(&b'u') && key.len() == MEMPOOL_KEY_LEN {
            let mut script_hash = [0u8; 32];
            script_hash.copy_from_slice(&key[1..33]);
            scripts.push(script_hash);
        }
        tree.remove(&key)?;
    }
    Ok(dedup_hashes(scripts))
}

/// Unconfirmed transactions for `script_hash`, txid order.
pub fn mempool_for(tree: &dyn Tree, script_hash: &Hash) -> Result<Vec<MempoolTx>> {
    let mut prefix = [0u8; 33];
    prefix[0] = b'u';
    prefix[1..].copy_from_slice(script_hash);
    let mut rows = Vec::new();
    for item in tree.iter() {
        let (key, value) = item?;
        if key.len() != MEMPOOL_KEY_LEN || !key.starts_with(&prefix) || value.len() != 8 {
            continue;
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&key[33..65]);
        rows.push(MempoolTx {
            txid,
            fee_sats: u64::from_be_bytes(value.try_into().expect("8 fee bytes")),
        });
    }
    rows.sort_by(|a, b| a.txid.cmp(&b.txid));
    Ok(rows)
}

/// Highest stored header, as Electrum `hex` (80-byte header) and height.
pub fn header_tip(tree: &dyn Tree) -> Result<Option<(u64, Vec<u8>)>> {
    let mut best: Option<(u64, Vec<u8>)> = None;
    for item in tree.iter() {
        let (key, value) = item?;
        if key.first() != Some(&b'h') || key.len() != HEADER_KEY_LEN || value.len() != 80 {
            continue;
        }
        let row_height = u64::from_be_bytes(key[1..9].try_into().expect("8 height bytes"));
        if best.as_ref().is_none_or(|(height, _)| row_height > *height) {
            best = Some((row_height, value));
        }
    }
    Ok(best)
}

/// 80-byte header. Version, timestamp, bits, and nonce are the 4-byte wire widths.
pub fn header_wire(header: &BlockHeader) -> [u8; 80] {
    let mut bytes = [0u8; 80];
    bytes[0..4].copy_from_slice(&(header.version as i32).to_le_bytes());
    bytes[4..36].copy_from_slice(&header.prev_block_hash);
    bytes[36..68].copy_from_slice(&header.merkle_root);
    bytes[68..72].copy_from_slice(&(header.timestamp as u32).to_le_bytes());
    bytes[72..76].copy_from_slice(&(header.bits as u32).to_le_bytes());
    bytes[76..80].copy_from_slice(&(header.nonce as u32).to_le_bytes());
    bytes
}

/// Delete output rows whose height is `height` or greater.
pub fn rollback_from(tree: &dyn Tree, height: u64) -> Result<(usize, Vec<Hash>)> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = tree.iter().collect::<Result<Vec<_>, _>>()?;
    let mut removed = 0;
    let mut scripts = Vec::new();
    for (key, value) in &rows {
        if key.first() != Some(&b'o') || key.len() != OUT_KEY_LEN || value.len() < 8 {
            continue;
        }
        let row_height = u64::from_be_bytes(key[1..9].try_into().expect("8 height bytes"));
        if row_height < height {
            continue;
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&key[9..41]);
        let vout = u32::from_be_bytes(key[41..45].try_into().expect("4 vout bytes"));
        let script_hash = script_hash(&value[8..]);
        tree.remove(&script_key(&script_hash, row_height, &txid, vout))?;
        tree.remove(&prevout_key(&txid, vout))?;
        tree.remove(&key)?;
        scripts.push(script_hash);
        removed += 1;
    }
    for (key, _) in rows {
        if key.first() == Some(&b'h') && key.len() == HEADER_KEY_LEN {
            let row_height = u64::from_be_bytes(key[1..9].try_into().expect("8 height bytes"));
            if row_height >= height {
                tree.remove(&key)?;
            }
            continue;
        }
        if key.first() != Some(&b'd') || key.len() != SPEND_KEY_LEN {
            continue;
        }
        let row_height = u64::from_be_bytes(key[33..41].try_into().expect("8 height bytes"));
        if row_height < height {
            continue;
        }
        let mut script_hash = [0u8; 32];
        script_hash.copy_from_slice(&key[1..33]);
        tree.remove(&key)?;
        scripts.push(script_hash);
        removed += 1;
    }
    Ok((removed, dedup_hashes(scripts)))
}

/// Outputs that created `script_pubkey`, oldest height first.
pub fn history(tree: &dyn Tree, script_pubkey: &[u8]) -> Result<Vec<IndexedOutput>> {
    let script_hash = script_hash(script_pubkey);
    let mut prefix = [0u8; 33];
    prefix[0] = b's';
    prefix[1..].copy_from_slice(&script_hash);
    let mut rows = Vec::new();
    for item in tree.iter() {
        let (key, _) = item?;
        if key.len() != SCRIPT_KEY_LEN || !key.starts_with(&prefix) {
            continue;
        }
        let height = u64::from_be_bytes(key[33..41].try_into().expect("8 height bytes"));
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&key[41..73]);
        let vout = u32::from_be_bytes(key[73..77].try_into().expect("4 vout bytes"));
        let Some(stored) = tree.get(&out_key(height, &txid, vout))? else {
            bail!("script index row has no output record");
        };
        if stored.len() < 8 {
            bail!("output record is shorter than its value");
        }
        let value = i64::from_be_bytes(stored[..8].try_into().expect("8 value bytes"));
        rows.push(IndexedOutput {
            txid,
            vout,
            value,
            script_pubkey: stored[8..].to_vec(),
            height,
        });
    }
    rows.sort_by(|a, b| {
        a.height
            .cmp(&b.height)
            .then(a.txid.cmp(&b.txid))
            .then(a.vout.cmp(&b.vout))
    });
    Ok(rows)
}

/// Confirmed transactions that created or spent `script_hash`, oldest height first.
///
/// One transaction is one entry even when it has several outputs for that script.
pub fn script_hash_history(tree: &dyn Tree, script_hash: &Hash) -> Result<Vec<HistoryTx>> {
    let mut created = [0u8; 33];
    created[0] = b's';
    created[1..].copy_from_slice(script_hash);
    let mut spent = created;
    spent[0] = b'd';
    let mut rows = Vec::new();
    for item in tree.iter() {
        let (key, _) = item?;
        if key.len() != SCRIPT_KEY_LEN || !(key.starts_with(&created) || key.starts_with(&spent)) {
            continue;
        }
        let height = u64::from_be_bytes(key[33..41].try_into().expect("8 height bytes"));
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&key[41..73]);
        rows.push(HistoryTx { txid, height });
    }
    rows.sort_by(|a, b| a.height.cmp(&b.height).then(a.txid.cmp(&b.txid)));
    rows.dedup_by(|a, b| a.height == b.height && a.txid == b.txid);
    Ok(rows)
}

/// Confirmed output totals for one script, plus how many of those outputs were spent.
///
/// `spent_txo_sum` is `Some` when no output is spent, or when every output is spent.
/// A partial spend does not invent a satoshi total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptActivity {
    pub funded_txo_count: u64,
    pub funded_txo_sum: i64,
    pub spent_txo_count: u64,
    pub spent_txo_sum: Option<i64>,
    pub tx_count: u64,
    pub mempool_tx_count: u64,
}

pub fn script_activity(tree: &dyn Tree, script_pubkey: &[u8]) -> Result<ScriptActivity> {
    let outputs = history(tree, script_pubkey)?;
    let funded_txo_sum = outputs.iter().map(|row| row.value).sum();
    let script_hash = script_hash(script_pubkey);
    let mut spent_prefix = [0u8; 33];
    spent_prefix[0] = b'd';
    spent_prefix[1..].copy_from_slice(&script_hash);
    let mut spent_txo_count = 0u64;
    for item in tree.iter() {
        let (key, _) = item?;
        if key.len() == SPEND_KEY_LEN && key.starts_with(&spent_prefix) {
            spent_txo_count += 1;
        }
    }
    let funded_txo_count = outputs.len() as u64;
    let spent_txo_sum = if spent_txo_count == 0 {
        Some(0)
    } else if spent_txo_count == funded_txo_count {
        Some(funded_txo_sum)
    } else {
        None
    };
    Ok(ScriptActivity {
        funded_txo_count,
        funded_txo_sum,
        spent_txo_count,
        spent_txo_sum,
        tx_count: script_hash_history(tree, &script_hash)?.len() as u64,
        mempool_tx_count: mempool_for(tree, &script_hash)?.len() as u64,
    })
}

fn spent_script(tree: &dyn Tree, prevout: blvm_protocol::OutPoint) -> Result<Option<Hash>> {
    let Some(created_at) = tree.get(&prevout_key(&prevout.hash, prevout.index))? else {
        return Ok(None);
    };
    if created_at.len() != 8 {
        bail!("prevout locator is shorter than a height");
    }
    let created_at = u64::from_be_bytes(created_at.try_into().expect("8 height bytes"));
    let Some(stored) = tree.get(&out_key(created_at, &prevout.hash, prevout.index))? else {
        return Ok(None);
    };
    if stored.len() < 8 {
        bail!("spent output record is shorter than its value");
    }
    Ok(Some(script_hash(&stored[8..])))
}

fn record_spend(
    tree: &dyn Tree,
    spending_txid: &Hash,
    vin: u32,
    prevout: blvm_protocol::OutPoint,
    height: u64,
) -> Result<Option<Hash>> {
    let Some(script_hash) = spent_script(tree, prevout)? else {
        return Ok(None);
    };
    tree.insert(&spend_key(&script_hash, height, spending_txid, vin), &[0])?;
    Ok(Some(script_hash))
}

fn dedup_hashes(mut hashes: Vec<Hash>) -> Vec<Hash> {
    hashes.sort_unstable();
    hashes.dedup();
    hashes
}

const HEADER_KEY_LEN: usize = 9;
const MEMPOOL_KEY_LEN: usize = 65;
const OUT_KEY_LEN: usize = 45;
const SCRIPT_KEY_LEN: usize = 77;
const SPEND_KEY_LEN: usize = 77;

fn mempool_script_key(script_hash: &Hash, txid: &Hash) -> [u8; MEMPOOL_KEY_LEN] {
    let mut key = [0u8; MEMPOOL_KEY_LEN];
    key[0] = b'u';
    key[1..33].copy_from_slice(script_hash);
    key[33..65].copy_from_slice(txid);
    key
}

fn mempool_tx_key(txid: &Hash, script_hash: &Hash) -> [u8; MEMPOOL_KEY_LEN] {
    let mut key = [0u8; MEMPOOL_KEY_LEN];
    key[0] = b't';
    key[1..33].copy_from_slice(txid);
    key[33..65].copy_from_slice(script_hash);
    key
}

fn header_key(height: u64) -> [u8; HEADER_KEY_LEN] {
    let mut key = [0u8; HEADER_KEY_LEN];
    key[0] = b'h';
    key[1..9].copy_from_slice(&height.to_be_bytes());
    key
}

fn out_key(height: u64, txid: &Hash, vout: u32) -> [u8; OUT_KEY_LEN] {
    let mut key = [0u8; OUT_KEY_LEN];
    key[0] = b'o';
    key[1..9].copy_from_slice(&height.to_be_bytes());
    key[9..41].copy_from_slice(txid);
    key[41..45].copy_from_slice(&vout.to_be_bytes());
    key
}

fn prevout_key(txid: &Hash, vout: u32) -> [u8; 37] {
    let mut key = [0u8; 37];
    key[0] = b'p';
    key[1..33].copy_from_slice(txid);
    key[33..37].copy_from_slice(&vout.to_be_bytes());
    key
}

fn spend_key(script_hash: &Hash, height: u64, txid: &Hash, vin: u32) -> [u8; SPEND_KEY_LEN] {
    let mut key = script_key(script_hash, height, txid, vin);
    key[0] = b'd';
    key
}

fn script_key(script_hash: &Hash, height: u64, txid: &Hash, vout: u32) -> [u8; SCRIPT_KEY_LEN] {
    let mut key = [0u8; SCRIPT_KEY_LEN];
    key[0] = b's';
    key[1..33].copy_from_slice(script_hash);
    key[33..41].copy_from_slice(&height.to_be_bytes());
    key[41..73].copy_from_slice(txid);
    key[73..77].copy_from_slice(&vout.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use blvm_protocol::{BlockHeader, OutPoint, Transaction, TransactionInput, TransactionOutput};
    use blvm_sdk::module::ModuleDb;

    fn output(value: i64, script_pubkey: &[u8]) -> TransactionOutput {
        TransactionOutput {
            value,
            script_pubkey: script_pubkey.to_vec(),
        }
    }

    fn block(outputs: Vec<TransactionOutput>) -> Block {
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
            outputs: outputs.into(),
            lock_time: 0,
        };
        Block::from_parts(BlockHeader::default(), Box::from(vec![tx]))
    }

    fn open_tree() -> (tempfile::TempDir, std::sync::Arc<dyn Tree>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = ModuleDb::open(dir.path()).expect("module db");
        let tree = db.tree(TREE_NAME).expect("tree");
        (dir, tree)
    }

    #[test]
    fn empty_script_hash_is_sha256_reversed() {
        let hash = script_hash(b"");
        assert_eq!(
            hex::encode(hash),
            "55b852781b9995a44c939b64e441ae2724b96f99c8f4fb9a141cfc9842c4b0e3"
        );
    }

    #[test]
    fn disconnect_deletes_that_height_and_keeps_the_parent() {
        let (_dir, tree) = open_tree();
        let parent_script = vec![0x51];
        let child_script = vec![0x52, 0x53];
        let parent = block(vec![output(50, &parent_script), output(25, &child_script)]);
        let child = block(vec![output(10, &child_script)]);

        assert_eq!(index_block(tree.as_ref(), &parent, 10).unwrap().0, 2);
        assert_eq!(index_block(tree.as_ref(), &child, 11).unwrap().0, 1);

        let before = history(tree.as_ref(), &child_script).unwrap();
        assert_eq!(before.len(), 2);
        assert_eq!(before[0].height, 10);
        assert_eq!(before[0].value, 25);
        assert_eq!(before[0].vout, 1);
        assert_eq!(before[1].height, 11);
        assert_eq!(before[1].value, 10);

        assert_eq!(rollback_from(tree.as_ref(), 11).unwrap().0, 1);

        let after_child = history(tree.as_ref(), &child_script).unwrap();
        assert_eq!(after_child.len(), 1);
        assert_eq!(after_child[0].height, 10);
        assert_eq!(history(tree.as_ref(), &parent_script).unwrap().len(), 1);

        assert_eq!(rollback_from(tree.as_ref(), 10).unwrap().0, 2);
        assert!(history(tree.as_ref(), &parent_script).unwrap().is_empty());
        assert!(history(tree.as_ref(), &child_script).unwrap().is_empty());
    }
}
