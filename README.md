# swarm-mutable-batch-test

A minimal, self-contained experiment that answers one question empirically:

> **Does a Swarm postage batch's `immutableFlag` actually do anything?**

A Bee developer claimed that *mutable* postage batches are "ineffectual and de facto
ignored." This repo tests that claim against a real, local Bee cluster.

## The probe

The one documented difference between a **mutable** batch (`immutableFlag = false`) and
an **immutable** one (`immutableFlag = true`) is, per the bee-js docs:

> "Controls whether data can be overwritten that was uploaded with this postage batch."

A **Single Owner Chunk (SOC)** is the sharpest way to exercise this. A SOC's address is
`keccak256(identifier ‖ owner)` — it depends only on the identifier and owner, **never on
the payload**. So you can upload two *different* payloads to the *same* address and ask
the network which one it keeps.

The script does this on **both** a mutable and an immutable batch and compares:

1. Upload a SOC with `VALUE-1`.
2. Overwrite the same SOC address with `VALUE-2`.
3. Read it back and see which value survives.

### Filtering out caching

Reading the value back is surprisingly subtle. The **uploader** node tends to keep
serving the *original* value from its own local store, masking the overwrite, while
other nodes return the new one. To get a trustworthy answer, the script determines
**which node is the responsible storer** — in Swarm a chunk is stored by the node whose
overlay address is *closest* (highest proximity order) to the chunk address — and reads
the value from there. It queries every node's overlay (`GET /addresses`), ranks them by
proximity to the SOC address, and judges the overwrite by the **storer's** value.

## Prerequisites

- **Docker** (the cluster runs in containers)
- **Node.js ≥ 18**

## Quick start

```bash
npm install

# Terminal 1 — start a local Bee cluster (queen + 3 full workers).
# First run builds/pulls Docker images and can take a while.
npm run cluster:start

# Terminal 2 — run the experiment.
npm test

# When done:
npm run cluster:stop
```

The cluster exposes node APIs on `http://localhost:1633` (queen) and
`http://localhost:16331`, `:16332`, `:16333` (workers) — the script's defaults.

## Example output

```
discovering node overlays…
  http://localhost:1633  overlay 0292b7ca…
  http://localhost:16331 overlay 676790fc…
  http://localhost:16332 overlay 1ec1e220…
  http://localhost:16333 overlay 48013b5c…

[IMMUTABLE]   http://localhost:16333 overlay 48013b5c PO   5  -> "VALUE-2"  [STORER]
[IMMUTABLE]   http://localhost:16331 overlay 676790fc PO   2  -> "VALUE-2"
[IMMUTABLE]   http://localhost:1633  overlay 0292b7ca PO   1  -> "VALUE-2"  [UPLOADER]
[IMMUTABLE]   http://localhost:16332 overlay 1ec1e220 PO   1  -> "VALUE-2"

================ VERDICT ================
⚠️  The immutableFlag had NO observable effect: the MUTABLE and IMMUTABLE
    batches behaved identically (overwrite took on both, at the storer).
    Both batches allow an SOC slot to be overwritten and serve the new value.
    => The mutable/immutable distinction is de facto ignored on this node;
       everything behaves as MUTABLE. The SOC overwrite itself DOES work.
=========================================
```

### What this shows

On the local dev cluster the `immutableFlag` has **no observable effect**: the SOC slot
can be overwritten and the storer serves the new value on *both* batch types. The
overwrite itself genuinely works — and the uploader node's own local store, which keeps
serving the original value, is exactly the cache artifact that makes single-node reads
misleading. Point the script at the node/network where you observed different behavior
to compare (see env vars below).

## Configuration

All optional, via environment variables:

| Env | Default | Purpose |
| --- | --- | --- |
| `BEE_API_URL` | `http://localhost:1633` | node to upload to |
| `CLUSTER_NODE_URLS` | the 4 local nodes | comma-separated candidate storer nodes to query for overlays + values (`BEE_API_URL` is always included) |
| `MUTABLE_BATCH_ID` | _(buys one)_ | reuse an existing `immutableFlag=false` batch |
| `IMMUTABLE_BATCH_ID` | _(buys one)_ | reuse an existing `immutableFlag=true` batch |
| `BATCH_DEPTH` | `17` | depth for auto-bought batches (must be ≥ 17) |
| `BATCH_AMOUNT` | `1200000000` | amount (PLUR) for auto-bought batches |
| `DEFERRED` | `true` | upload mode; `false` forces synchronous pushsync |

Reuse batch IDs across runs to skip the purchase + usability wait:

```bash
MUTABLE_BATCH_ID=<id> IMMUTABLE_BATCH_ID=<id> npm test
```

## Caveats

- This is a **closed local dev cluster**. On a tiny cluster the SOC address can be far
  from (or tied across) every node's overlay, so the "storer" can be weakly determined —
  the script flags this. Against a real network you can only rank the nodes you list in
  `CLUSTER_NODE_URLS`; the true storer may be a node you don't control.
- If you set `DEFERRED=false` and uploads hang for ~30s, that's the Bee public-
  reachability issue on the bridge network, not this script.

## License

MIT — see [LICENSE](./LICENSE).
