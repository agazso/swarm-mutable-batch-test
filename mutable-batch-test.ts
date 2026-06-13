// Decide empirically whether *mutable* postage batches are effective on a Bee node,
// reading the result from the node that actually STORES the chunk (filtering caches).
//
// A Bee developer claimed mutable batches are "ineffectual and de facto ignored".
// The one documented difference between a mutable batch (immutableFlag=false) and an
// immutable one (immutableFlag=true) is, per the bee-js docs:
//
//   "Controls whether data can be overwritten that was uploaded with this postage batch."
//
// A Single Owner Chunk is the sharpest probe for this: its address is
// keccak256(identifier ‖ owner) and does NOT depend on the payload. So we can upload
// two *different* payloads to the *same* address and ask the node which one it keeps.
//
// Caching confounds this: the uploader node tends to keep serving the original value
// from its local store, and any *other* node may serve a cached copy too. In Swarm a
// chunk is stored by the node whose overlay address is CLOSEST (highest proximity
// order) to the chunk address. So we discover every cluster node's overlay, compute
// which one is the responsible storer for the SOC address, and judge the overwrite by
// the STORER's value — the authoritative reserve copy.
//
// This runs the overwrite probe on BOTH a mutable and an immutable batch and compares.
//
// Run:  npm test          (or)  npx tsx mutable-batch-test.ts
// Needs a local Bee cluster first:  npm run cluster:start  (queen + 3 full workers).
//
// Env (all optional):
//   BEE_API_URL          node to upload to            (default http://localhost:1633)
//   CLUSTER_NODE_URLS    comma-separated list of all candidate storer nodes to query
//                        (default the 4 dev-cluster nodes; BEE_API_URL is unioned in)
//   MUTABLE_BATCH_ID     reuse an immutableFlag=false batch (else one is bought)
//   IMMUTABLE_BATCH_ID   reuse an immutableFlag=true  batch (else one is bought)
//   BATCH_DEPTH          depth for auto-bought batches (default 17, must be >= 17)
//   BATCH_AMOUNT         amount (PLUR) for auto-bought batches (default 1200000000)
//   DEFERRED             "true"|"false" upload mode    (default true)

import { randomBytes } from 'node:crypto'
import { Bee, EthAddress, Identifier, PrivateKey } from '@ethersphere/bee-js'

const DEFAULT_BEE_API_URL = 'http://localhost:1633'
const DEFAULT_CLUSTER_NODE_URLS = [
  'http://localhost:1633',
  'http://localhost:16331',
  'http://localhost:16332',
  'http://localhost:16333',
]
const DEFAULT_BATCH_DEPTH = 17
const DEFAULT_BATCH_AMOUNT = '1200000000'
const IDENTIFIER_BYTES = 32
const PRIVATE_KEY_BYTES = 32
const BITS_PER_BYTE = 8
const READ_RETRIES = 4
const READ_RETRY_DELAY_MS = 500
const OVERLAY_SHORT_CHARS = 8
const MIN_TRUSTWORTHY_PROXIMITY = 1
const VALUE_1 = 'VALUE-1'
const VALUE_2 = 'VALUE-2'

const uploadUrl = process.env.BEE_API_URL ?? DEFAULT_BEE_API_URL
const clusterUrls = dedupe([
  uploadUrl,
  ...(process.env.CLUSTER_NODE_URLS?.split(',').map((url) => url.trim()) ??
    DEFAULT_CLUSTER_NODE_URLS),
])
const batchDepth = Number(process.env.BATCH_DEPTH ?? DEFAULT_BATCH_DEPTH)
const batchAmount = process.env.BATCH_AMOUNT ?? DEFAULT_BATCH_AMOUNT
const deferred = (process.env.DEFERRED ?? 'true') !== 'false'

const uploadBee = new Bee(uploadUrl)

const decoder = new TextDecoder()
const encoder = new TextEncoder()

interface NodeInfo {
  url: string
  bee: Bee
  overlay: Uint8Array
  overlayHex: string
}

interface NodeRead {
  node: NodeInfo
  proximityOrder: number
  value: string
  isStorer: boolean
  isUploader: boolean
}

interface ArmResult {
  label: string
  batchId: string
  socAddress: string
  upload2Error: string | undefined
  nodeReads: NodeRead[]
  // Value at the responsible storer (closest overlay) — the authoritative answer.
  storerValue: string | undefined
  // Value the uploader node serves from its own local store (for the cache contrast).
  uploaderValue: string | undefined
  // Did the overwrite take, as seen by the storer?
  overwritten: boolean
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

// Proximity order = number of matching leading bits between two addresses.
// Higher = closer. Mirrors cafe-utility's `proximity()` (the responsible storer is the
// node whose overlay shares the most leading bits with the chunk address).
function proximityOrder(a: Uint8Array, b: Uint8Array): number {
  const length = Math.min(a.length, b.length)
  for (let byteIndex = 0; byteIndex < length; byteIndex++) {
    const diff = a[byteIndex] ^ b[byteIndex]
    if (diff !== 0) {
      for (let bit = 0; bit < BITS_PER_BYTE; bit++) {
        if ((diff >> (BITS_PER_BYTE - 1 - bit)) & 1) {
          return byteIndex * BITS_PER_BYTE + bit
        }
      }
    }
  }
  return length * BITS_PER_BYTE
}

// Buy a batch with the given mutability, or reuse one supplied via env.
async function resolveBatch(
  label: string,
  envId: string | undefined,
  immutableFlag: boolean,
): Promise<string> {
  if (envId) {
    console.log(`[${label}] reusing batch from env: ${envId}`)
    return envId
  }
  console.log(
    `[${label}] buying batch (amount=${batchAmount}, depth=${batchDepth}, immutableFlag=${immutableFlag})…`,
  )
  const batchId = await uploadBee.createPostageBatch(batchAmount, batchDepth, {
    immutableFlag,
    waitForUsable: true,
    label: `mutable-batch-test-${label.toLowerCase()}`,
  })
  console.log(`[${label}] bought batch: ${batchId.toHex()}  (reuse next time via env)`)
  return batchId.toHex()
}

// Query each candidate node for its overlay address. Nodes that don't answer (e.g. a
// public gateway that hides /addresses) are skipped so the script still runs.
async function discoverNodes(): Promise<NodeInfo[]> {
  const nodes: NodeInfo[] = []
  for (const url of clusterUrls) {
    const bee = new Bee(url)
    try {
      const addresses = await bee.getNodeAddresses()
      nodes.push({
        url,
        bee,
        overlay: addresses.overlay.toUint8Array(),
        overlayHex: addresses.overlay.toHex(),
      })
    } catch (error) {
      console.log(`  ! could not read overlay from ${url}: ${errorMessage(error)} (skipping)`)
    }
  }
  return nodes
}

async function readSoc(bee: Bee, owner: EthAddress, identifier: Identifier): Promise<string> {
  let lastError: unknown
  for (let attempt = 0; attempt < READ_RETRIES; attempt++) {
    try {
      const chunk = await bee.makeSOCReader(owner).download(identifier)
      return decoder.decode(chunk.payload.toUint8Array())
    } catch (error) {
      lastError = error
      await sleep(READ_RETRY_DELAY_MS)
    }
  }
  return `<download failed: ${errorMessage(lastError)}>`
}

// Rank nodes by proximity to the SOC address (closest = storer) and read the value each
// one serves. The storer is read first so its reserve copy is captured before reads
// from far nodes pull-and-cache the chunk elsewhere.
async function readFromCluster(
  nodes: NodeInfo[],
  socAddress: Uint8Array,
  owner: EthAddress,
  identifier: Identifier,
): Promise<NodeRead[]> {
  const ranked = nodes
    .map((node) => ({ node, proximityOrder: proximityOrder(socAddress, node.overlay) }))
    .sort((a, b) => b.proximityOrder - a.proximityOrder)

  const reads: NodeRead[] = []
  for (let i = 0; i < ranked.length; i++) {
    const { node, proximityOrder: po } = ranked[i]
    reads.push({
      node,
      proximityOrder: po,
      value: await readSoc(node.bee, owner, identifier),
      isStorer: i === 0,
      isUploader: node.url === uploadUrl,
    })
  }
  return reads
}

async function runArm(label: string, batchId: string, nodes: NodeInfo[]): Promise<ArmResult> {
  // Fresh owner + identifier per run so prior runs never pollute the result.
  const signer = new PrivateKey(new Uint8Array(randomBytes(PRIVATE_KEY_BYTES)))
  const owner = signer.publicKey().address()
  const identifier = new Identifier(new Uint8Array(randomBytes(IDENTIFIER_BYTES)))
  const writer = uploadBee.makeSOCWriter(signer)

  console.log(`\n[${label}] owner=${owner.toHex()} identifier=${identifier.toHex()}`)

  // 1. Write the original value.
  const up1 = await writer.upload(batchId, identifier, encoder.encode(VALUE_1), { deferred })
  const socAddress = up1.reference.toUint8Array()
  console.log(`[${label}] uploaded "${VALUE_1}" -> SOC ${up1.reference.toHex()}`)

  // 2. Overwrite the SAME address with a DIFFERENT value. On a genuinely immutable
  //    batch this should be refused or have no effect; on a mutable batch it should
  //    propagate so the responsible storer serves the new value.
  let upload2Error: string | undefined
  try {
    await writer.upload(batchId, identifier, encoder.encode(VALUE_2), { deferred })
    console.log(`[${label}] overwrite with "${VALUE_2}" accepted by upload node`)
  } catch (error) {
    upload2Error = errorMessage(error)
    console.log(`[${label}] overwrite with "${VALUE_2}" REJECTED: ${upload2Error}`)
  }

  // 3. Read from every node, ranked by proximity; judge by the storer's value.
  const fallbackValue = nodes.length === 0 ? await readSoc(uploadBee, owner, identifier) : undefined
  const nodeReads =
    nodes.length === 0 ? [] : await readFromCluster(nodes, socAddress, owner, identifier)

  const storerRead = nodeReads.find((read) => read.isStorer)
  const uploaderRead = nodeReads.find((read) => read.isUploader)
  const storerValue = storerRead?.value ?? fallbackValue
  const uploaderValue = uploaderRead?.value ?? fallbackValue

  for (const read of nodeReads) {
    const tags = [read.isStorer ? 'STORER' : '', read.isUploader ? 'UPLOADER' : '']
      .filter(Boolean)
      .join(',')
    console.log(
      `[${label}]   ${read.node.url.padEnd(22)} overlay ${read.node.overlayHex.slice(0, OVERLAY_SHORT_CHARS)} ` +
        `PO ${String(read.proximityOrder).padStart(3)}  -> "${read.value}"${tags ? `  [${tags}]` : ''}`,
    )
  }

  // The closest-of-known-nodes is only a trustworthy storer when it is meaningfully
  // closer than the rest. On a tiny cluster the SOC address is often far from every
  // node (PO 0) or tied at the top, so flag when the determination is weak.
  if (storerRead) {
    const tiedCount = nodeReads.filter(
      (read) => read.proximityOrder === storerRead.proximityOrder,
    ).length
    if (tiedCount > 1) {
      console.log(
        `[${label}] ⚠ storer ambiguous: ${tiedCount} nodes tie at PO ${storerRead.proximityOrder} ` +
          `— too few/too-distant nodes to single out the neighborhood.`,
      )
    } else if (storerRead.proximityOrder < MIN_TRUSTWORTHY_PROXIMITY) {
      console.log(
        `[${label}] ⚠ storer PO ${storerRead.proximityOrder} is very low — the chunk is far from all known ` +
          `nodes; the real storer may be a node not in CLUSTER_NODE_URLS.`,
      )
    }
  }
  if (nodes.length === 0) {
    console.log(
      `[${label}] no overlays discovered; storer unknown, using upload node read: "${fallbackValue}"`,
    )
  }

  return {
    label,
    batchId,
    socAddress: up1.reference.toHex(),
    upload2Error,
    nodeReads,
    storerValue,
    uploaderValue,
    overwritten: storerValue === VALUE_2,
  }
}

function printVerdict(mutable: ArmResult, immutable: ArmResult): void {
  console.log('\n================ RESULTS ================')
  for (const arm of [mutable, immutable]) {
    const storer = arm.nodeReads.find((read) => read.isStorer)
    const where = storer
      ? `${storer.node.url} (PO ${storer.proximityOrder})`
      : 'upload node (storer unknown)'
    console.log(
      `${arm.label.padEnd(9)} | overwrite ${arm.overwritten ? 'TOOK' : 'did NOT take'} @ storer | ` +
        `storer="${arm.storerValue}" uploader="${arm.uploaderValue}" | via ${where}` +
        (arm.upload2Error ? ` | upload2 rejected: ${arm.upload2Error}` : ''),
    )
  }

  // Surface the cache artifact: the uploader can keep serving the original value while
  // the responsible storer holds the overwrite.
  const maskedSomewhere = [mutable, immutable].some(
    (arm) => arm.uploaderValue !== undefined && arm.storerValue !== arm.uploaderValue,
  )
  if (maskedSomewhere) {
    console.log(
      '\nNOTE: storer and uploader values DISAGREE — the uploader kept the original value\n' +
        '      in its local store while the overwrite reached the responsible storer.\n' +
        '      Reading from the uploader (or a random node) would give a misleading answer.',
    )
  }

  console.log('\n================ VERDICT ================')
  if (mutable.overwritten === immutable.overwritten) {
    console.log('⚠️  The immutableFlag had NO observable effect: the MUTABLE and IMMUTABLE')
    console.log(
      `    batches behaved identically (overwrite ${mutable.overwritten ? 'took' : 'did not take'} on both, at the storer).`,
    )
    if (mutable.overwritten) {
      console.log('    Both batches allow an SOC slot to be overwritten and serve the new value.')
      console.log('    => The mutable/immutable distinction is de facto ignored on this node;')
      console.log('       everything behaves as MUTABLE. The SOC overwrite itself DOES work.')
    } else {
      console.log('    Neither batch surfaced the overwrite at the storer — immutability appears')
      console.log('    enforced for both, or the chunk had not propagated yet. Re-run to confirm.')
    }
  } else if (mutable.overwritten && !immutable.overwritten) {
    console.log('✅ The immutableFlag is EFFECTIVE.')
    console.log('   Overwrite reached the storer on the MUTABLE batch but NOT the IMMUTABLE one.')
    console.log('   => Mutable batches behave differently from immutable ones.')
  } else {
    console.log('❓ Unexpected: overwrite took on the IMMUTABLE batch but not the MUTABLE one.')
    console.log('   Inspect the per-arm output above.')
  }
  console.log('=========================================\n')
}

async function main(): Promise<void> {
  console.log(`upload node: ${uploadUrl}`)
  console.log(`cluster:     ${clusterUrls.join(', ')}`)
  console.log(`deferred:    ${deferred}`)

  console.log('\ndiscovering node overlays…')
  const nodes = await discoverNodes()
  for (const node of nodes) {
    console.log(`  ${node.url.padEnd(22)} overlay ${node.overlayHex}`)
  }
  if (nodes.length === 0) {
    console.log(
      '  (no overlays available — falling back to upload-node reads; storer cannot be determined)',
    )
  }

  const mutableBatchId = await resolveBatch('MUTABLE', process.env.MUTABLE_BATCH_ID, false)
  const immutableBatchId = await resolveBatch('IMMUTABLE', process.env.IMMUTABLE_BATCH_ID, true)

  const mutable = await runArm('MUTABLE', mutableBatchId, nodes)
  const immutable = await runArm('IMMUTABLE', immutableBatchId, nodes)

  printVerdict(mutable, immutable)
}

main().catch((error) => {
  console.error(errorMessage(error))
  process.exit(1)
})
