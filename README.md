# blvm-wallet-index

Script history for `blvm`. This process keeps its own database. It does not open the node chain store.

Electrum binds when module config `listen` is set, for example `127.0.0.1:50001`. Esplora binds when `esplora` is set, for example `127.0.0.1:3000`. With neither set, the process still indexes blocks and does not open those ports.

The node config pin is `blvm-wallet-index = "0.1.*"`.

## Build

```bash
cd blvm-wallet-index && cargo build
```

`[patch.crates-io]` points at sibling crates. CI strips that section.
