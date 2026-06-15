// Demonstrate CAC stamp-index-collision DIVERGENCE across neighborhood nodes — the
// content-addressed counterpart to the SOC fork (same-stamp-divergence-test.ts), and a
// reproduction of `stamp-index-collision-divergence.md`.
//
// Mint N DIFFERENT content-addressed chunks that share a bucket (top 16 = bucketDepth address
// bits) and stamp them all with the SAME (batchID, index, timestamp), each validly signed by
// the batch owner. Deliver chunk_i to node_i so each node stores its own first. Because the
// reserve's equal-timestamp tie-break is `prev >= curr → reject` with NO global ordering
// (reserve.go:143), each node keeps the chunk IT saw first; pull-sync then delivers the sibling
// but the reserve REJECTS it (equal timestamp, "overwrite newer chunk") — so different nodes
// PERMANENTLY hold different chunks at the same (batchID, index) slot. That makes their
// redistribution samples disagree → freezing.
//
// (An earlier version of this script ground every chunk into one node's bucket and only checked
// that single node's reserve — which "converges" and MISSES the cross-node divergence. With
// storageRadius=0 on the dev cluster every node stores every chunk, so the divergence is across
// nodes; this version checks each node's reserve authoritatively via its node log.)
//
// Unlike the SOC fork, the addresses differ here, so pull-sync DELIVERS the sibling (the
// `stampHash` differs) — but the reserve rejects it, reaching the same persistent divergence.
//
// Run:  npm run test:cac-collision   (needs the local cluster: npm run cluster:start)
//
// Env (all optional):
//   BEE_API_URL        node that mints/owns the batch (default http://localhost:1633)
//   CLUSTER_NODE_URLS  comma-separated cluster nodes (default the 4 local)
//   BATCH_ID           batch owned by OWNER_KEY (else one is bought)
//   OWNER_KEY          batch owner private key (default the dev-cluster queen key)
//   NUM_CHUNKS         how many colliding chunks / nodes (default = node count, min 2)
//   BATCH_DEPTH/AMOUNT batch params for an auto-bought batch (default 17 / 1200000000)
//   DEFERRED           "true"|"false" upload mode (default true)
//   SETTLE_MS          ms to wait for pushsync before reading the reserves (default 5000)

import { execSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { BatchId, Bee, MerkleTree, PrivateKey } from '@ethersphere/bee-js'

const DEFAULT_BEE_API_URL = 'http://localhost:1633'
const DEFAULT_CLUSTER_NODE_URLS = [
  'http://localhost:1633',
  'http://localhost:16331',
  'http://localhost:16332',
  'http://localhost:16333',
]
const DEFAULT_OWNER_KEY = '566058308ad5fa3888173c741a1fb902c9f1f19559b11fc2738dfc53637ce4e9'
const DEFAULT_BATCH_DEPTH = 17
const DEFAULT_BATCH_AMOUNT = '1200000000'
const SPAN_BYTES = 8
const ADDRESS_BYTES = 32
const ADDRESS_LOG_PREFIX = 12
const GRIND_LOG_EVERY = 20000
const SLOT_HEIGHT = 0
const OVERLAY_SHORT_CHARS = 8
const UPLOAD_RETRIES = 4
const UPLOAD_RETRY_DELAY_MS = 400
const DEFAULT_SETTLE_MS = 5000
const PRE_SEED_SETTLE_MS = 2000
const LOG_SINCE_S = 150
const LOG_MAX_BUFFER = 16 * 1024 * 1024
// dev-cluster URL → docker container, for the authoritative per-node reserve check.
const NODE_CONTAINERS: Record<string, string> = {
  'http://localhost:1633': 'bee-compose-queen',
  'http://localhost:16331': 'bee-compose-worker-1',
  'http://localhost:16332': 'bee-compose-worker-2',
  'http://localhost:16333': 'bee-compose-worker-3',
}

const uploadUrl = process.env.BEE_API_URL ?? DEFAULT_BEE_API_URL
const clusterUrls = dedupe([
  uploadUrl,
  ...(process.env.CLUSTER_NODE_URLS?.split(',').map((u) => u.trim()) ?? DEFAULT_CLUSTER_NODE_URLS),
])
const ownerKey = new PrivateKey(process.env.OWNER_KEY ?? DEFAULT_OWNER_KEY)
const batchDepth = Number(process.env.BATCH_DEPTH ?? DEFAULT_BATCH_DEPTH)
const batchAmount = process.env.BATCH_AMOUNT ?? DEFAULT_BATCH_AMOUNT
const numChunks = process.env.NUM_CHUNKS ? Number(process.env.NUM_CHUNKS) : undefined
const deferred = (process.env.DEFERRED ?? 'true') !== 'false'
const settleMs = Number(process.env.SETTLE_MS ?? DEFAULT_SETTLE_MS)

const uploadBee = new Bee(uploadUrl)
const encoder = new TextEncoder()

interface Node {
  url: string
  bee: Bee
  overlayHex: string
}

interface Cac {
  label: string
  address: Uint8Array
  addressHex: string
  data: Uint8Array // span ‖ payload
}

function dedupe(values: string[]): string[] {
  return [...new Set(values)]
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function isTransient(error: unknown): boolean {
  return /socket hang up|ECONNRESET|ECONNREFUSED|fetch failed|EPIPE|ETIMEDOUT|network/i.test(errorMessage(error))
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

function concatBytes(...arrays: Uint8Array[]): Uint8Array {
  const total = arrays.reduce((n, a) => n + a.length, 0)
  const out = new Uint8Array(total)
  let offset = 0
  for (const a of arrays) {
    out.set(a, offset)
    offset += a.length
  }
  return out
}

function u32BE(n: number): Uint8Array {
  const out = new Uint8Array(4)
  new DataView(out.buffer).setUint32(0, n, false)
  return out
}

function u64BE(n: bigint): Uint8Array {
  const out = new Uint8Array(SPAN_BYTES)
  new DataView(out.buffer).setBigUint64(0, n, false)
  return out
}

function u64LE(n: bigint): Uint8Array {
  const out = new Uint8Array(SPAN_BYTES)
  new DataView(out.buffer).setBigUint64(0, n, true)
  return out
}

// bucket = first 16 bits (bucketDepth) of the chunk address, big-endian.
function bucketOf(address: Uint8Array): number {
  return (address[0] << 8) | address[1]
}

async function makeCac(label: string): Promise<Cac> {
  const payload = encoder.encode(label)
  const chunk = await MerkleTree.root(payload)
  const address = chunk.hash()
  const data = concatBytes(u64LE(BigInt(payload.length)), payload)
  return { label, address, addressHex: toHex(address), data }
}

// Grind `count` chunks that all share a bucket (the bucket of the first, free, chunk).
async function grindCollidingChunks(count: number): Promise<Cac[]> {
  const first = await makeCac(`cac-collision|0|${toHex(randomBytes(4))}`)
  const targetBucket = bucketOf(first.address)
  const chunks: Cac[] = [first]
  console.log(`grinding ${count} chunks into bucket 0x${targetBucket.toString(16).padStart(4, '0')}…`)
  console.log(`  chunk 0: ${first.addressHex}`)
  for (let i = 1; i < count; i++) {
    let attempts = 0
    for (;;) {
      attempts++
      const cac = await makeCac(`cac-collision|${i}|${toHex(randomBytes(6))}`)
      if (bucketOf(cac.address) === targetBucket) {
        chunks.push(cac)
        console.log(`  chunk ${i}: ${cac.addressHex} (after ${attempts} tries)`)
        break
      }
      if (attempts % GRIND_LOG_EVERY === 0) {
        console.log(`  chunk ${i}: ${attempts} tries…`)
      }
    }
  }
  return chunks
}

async function grindOneInBucket(label: string, targetBucket: number): Promise<Cac> {
  for (;;) {
    const cac = await makeCac(`${label}|${toHex(randomBytes(6))}`)
    if (bucketOf(cac.address) === targetBucket) return cac
  }
}

// Client-side stamp (envelope) for `address` at a FIXED slot, signed by the batch owner —
// replicates bee-js Stamper.stamp but with a caller-controlled index + timestamp.
function buildStamp(batchId: BatchId, address: Uint8Array, bucket: number, height: number, timestamp: bigint) {
  const index = concatBytes(u32BE(bucket), u32BE(height))
  const ts = u64BE(timestamp)
  const signature = ownerKey.sign(concatBytes(address, batchId.toUint8Array(), index, ts)).toUint8Array()
  return { batchId, index, timestamp: ts, signature, issuer: ownerKey.publicKey().address().toUint8Array() }
}

async function uploadWithRetry(bee: Bee, stamp: unknown, data: Uint8Array): Promise<void> {
  let lastError: unknown
  for (let attempt = 0; attempt < UPLOAD_RETRIES; attempt++) {
    try {
      await bee.uploadChunk(stamp as Parameters<Bee['uploadChunk']>[0], data, { deferred })
      return
    } catch (error) {
      lastError = error
      if (!isTransient(error)) {
        throw error
      }
      await sleep(UPLOAD_RETRY_DELAY_MS)
    }
  }
  throw lastError
}

async function discoverNodes(): Promise<Node[]> {
  const nodes: Node[] = []
  for (const url of clusterUrls) {
    const bee = new Bee(url)
    try {
      const addresses = await bee.getNodeAddresses()
      nodes.push({ url, bee, overlayHex: addresses.overlay.toHex() })
    } catch (error) {
      console.log(`  ! could not reach ${url}: ${errorMessage(error)} (skipping)`)
    }
  }
  return nodes
}

async function resolveBatchId(): Promise<string> {
  if (process.env.BATCH_ID) {
    console.log(`using batch from env: ${process.env.BATCH_ID}`)
    return process.env.BATCH_ID
  }
  console.log(`buying a batch (amount=${batchAmount}, depth=${batchDepth})…`)
  const id = await uploadBee.createPostageBatch(batchAmount, batchDepth, {
    immutableFlag: false,
    waitForUsable: true,
    label: 'cac-collision',
  })
  console.log(`bought batch: ${id.toHex()}  (reuse via BATCH_ID=…)`)
  return id.toHex()
}

async function ownerMatchesBatch(batchId: BatchId): Promise<boolean> {
  const probe = `0x${toHex(randomBytes(ADDRESS_BYTES))}`
  const envelope = await uploadBee.createEnvelope(batchId, probe)
  return toHex(envelope.issuer) === ownerKey.publicKey().address().toHex()
}

// Authoritative, NON-MUTATING per-node reserve read. We pre-seed the slot with a lower-timestamp
// throwaway, so the real chunk each node stores REPLACES it and is logged as
// `replacing chunk stamp index … new_chunk="<X>"`. The X for a node = the chunk that node holds.
function reserveHeld(node: Node, chunks: Cac[]): string | undefined {
  const container = NODE_CONTAINERS[node.url]
  if (!container) return undefined
  let log: string
  try {
    log = execSync(
      `docker logs ${container} --since ${LOG_SINCE_S}s 2>&1 | grep -i "replacing chunk stamp index" || true`,
      { encoding: 'utf8', maxBuffer: LOG_MAX_BUFFER },
    )
  } catch {
    return undefined
  }
  const held = chunks.filter((c) => log.includes(`new_chunk"="${c.addressHex.slice(0, ADDRESS_LOG_PREFIX)}`))
  return held.length === 1 ? held[0].addressHex : undefined
}

async function main(): Promise<void> {
  console.log(`mint/owner node: ${uploadUrl}`)
  console.log(`cluster:         ${clusterUrls.join(', ')}`)
  console.log(`owner address:   ${ownerKey.publicKey().address().toHex()}`)
  console.log(`deferred:        ${deferred}`)

  const nodes = await discoverNodes()
  if (nodes.length < 2) {
    throw Error('need at least 2 reachable nodes to show cross-node divergence')
  }
  // Two nodes (chunk X → A, chunk Y → B) is the cleanest, most reliable divergence; more nodes
  // race harder and tend to converge on one chunk.
  const count = Math.max(2, Math.min(numChunks ?? 2, nodes.length))

  const batchId = new BatchId(await resolveBatchId())
  if (!(await ownerMatchesBatch(batchId))) {
    throw Error(
      `OWNER_KEY (${ownerKey.publicKey().address().toHex()}) is not the owner of batch ${batchId.toHex()}. ` +
        `Set OWNER_KEY to the batch owner's private key.`,
    )
  }
  console.log('owner key matches the batch owner ✓')

  const chunks = await grindCollidingChunks(count)
  const bucket = bucketOf(chunks[0].address)
  const timestamp = BigInt(Date.now())
  console.log(
    `\nslot = (batch ${batchId.toHex().slice(0, OVERLAY_SHORT_CHARS)}…, ` +
      `index bucket=0x${bucket.toString(16).padStart(4, '0')} height=${SLOT_HEIGHT}, timestamp=${timestamp})`,
  )

  const targets = nodes.slice(0, count)

  // Pre-seed the slot on every target with a LOWER-timestamp throwaway, so the real chunk each
  // node ends up storing replaces it and is cleanly logged as `replacing chunk stamp index`.
  const seed = await grindOneInBucket('cac-collision-seed', bucket)
  const seedStamp = buildStamp(batchId, seed.address, bucket, SLOT_HEIGHT, timestamp - 1n)
  console.log('\npre-seeding the slot (lower timestamp) on every target…')
  await Promise.all(targets.map((n) => uploadWithRetry(n.bee, seedStamp, seed.data).catch(() => undefined)))
  await sleep(PRE_SEED_SETTLE_MS)

  // Deliver chunk_i → node_i simultaneously, so each node stores ITS OWN first.
  console.log('\ndelivering a different chunk to each node (same slot), simultaneously…')
  await Promise.all(
    chunks.map(async (cac, i) => {
      const node = targets[i]
      const stamp = buildStamp(batchId, cac.address, bucket, SLOT_HEIGHT, timestamp)
      try {
        await uploadWithRetry(node.bee, stamp, cac.data)
        console.log(`  ${cac.label} (${cac.addressHex.slice(0, OVERLAY_SHORT_CHARS)}) → ${node.url}`)
      } catch (e) {
        console.log(`  ${cac.label} (${cac.addressHex.slice(0, OVERLAY_SHORT_CHARS)}) → ${node.url}  upload error: ${errorMessage(e)}`)
      }
    }),
  )

  console.log(`\nsettling ${settleMs}ms for pushsync…`)
  await sleep(settleMs)

  // Read each node's reserve (authoritative, non-mutating) and compare.
  console.log('\nper-node reserve contents at the slot (from node logs):')
  const held = new Map<string, string | undefined>()
  for (const node of targets) {
    const h = reserveHeld(node, chunks)
    held.set(node.url, h)
    const label = h ? chunks.find((c) => c.addressHex === h)?.label : undefined
    console.log(`  ${node.url.padEnd(22)} holds ${h ? `${label} (${h.slice(0, OVERLAY_SHORT_CHARS)})` : '<could not determine from log>'}`)
  }

  const determined = [...held.values()].filter((h): h is string => h !== undefined)
  const distinct = dedupe(determined)

  console.log('\n================ VERDICT ================')
  if (distinct.length > 1) {
    console.log('🚨 DIVERGENCE CONFIRMED — different nodes permanently hold DIFFERENT chunks at the')
    console.log('   SAME (batchID, index, timestamp) slot. The reserve\'s equal-timestamp tie-break')
    console.log('   (reserve.go:143, prev>=curr→reject) has no global ordering, so each node keeps')
    console.log('   the chunk it saw first; pull-sync delivers the sibling but the reserve rejects it')
    console.log('   ("overwrite newer chunk"). This is the CAC index-collision divergence primitive —')
    console.log('   a second lever alongside the SOC fork: when such a chunk enters the redistribution')
    console.log('   sample, the diverging nodes reveal different hashes and the minority is frozen.')
  } else if (distinct.length === 1 && determined.length === targets.length) {
    console.log('⚠️  All nodes converged on one chunk this run (a sibling reached a node before its own).')
    console.log('   It is a race; re-run (each run uses fresh chunks). The divergence is durable once')
    console.log('   it occurs — pull-sync cannot heal equal-timestamp collisions.')
  } else {
    console.log('❓ Could not read every node\'s reserve from logs (non-dev-cluster, or log window).')
    console.log('   Inspect manually:  docker logs <node container> --since 120s | grep "overwrite newer chunk"')
    console.log(`   addresses this run: ${chunks.map((c) => c.addressHex.slice(0, ADDRESS_LOG_PREFIX)).join(' ')}`)
  }
  console.log('=========================================\n')
}

main().catch((error) => {
  console.error(errorMessage(error))
  process.exit(1)
})
