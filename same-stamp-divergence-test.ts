// Demonstrate that ONE postage stamp can authorize MANY different values at the SAME
// SOC address — and that different nodes then serve different content for that address.
//
// A postage stamp (here an "envelope") signs only the chunk ADDRESS, not the payload.
// A Single Owner Chunk's address is keccak256(identifier ‖ owner) — independent of the
// payload. So a single envelope minted for that address is valid for ANY payload at it.
//
// This script:
//   1. mints ONE envelope (same batchID + index + timestamp + signature) for a SOC address,
//   2. uploads a DIFFERENT payload to each cluster node, all carrying that same envelope,
//   3. reads the SOC back from every node.
// Expectation: each node keeps the first value it saw, so the same address resolves to
// different values on different nodes — there is no single source of truth.
//
// Run:  npm run test:divergence    (or)  npx tsx same-stamp-divergence-test.ts
// Needs a local Bee cluster first:  npm run cluster:start  (queen + 3 full workers).
//
// Env (all optional):
//   BEE_API_URL        node that mints the envelope; MUST own BATCH_ID (default :1633)
//   CLUSTER_NODE_URLS  comma-separated nodes to write to / read from (default the 4 local)
//   BATCH_ID           batch to use; must be owned by BEE_API_URL's node (default: buys one)
//   BATCH_DEPTH        depth for an auto-bought batch (default 17, must be >= 17)
//   BATCH_AMOUNT       amount (PLUR) for an auto-bought batch (default 1200000000)
//   DEFERRED           "true"|"false" upload mode (default true; keeps each node's copy)

import { randomBytes } from 'node:crypto'
import { Bee, Identifier, PrivateKey } from '@ethersphere/bee-js'

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

const uploadUrl = process.env.BEE_API_URL ?? DEFAULT_BEE_API_URL
const clusterUrls = dedupe([
  uploadUrl,
  ...(process.env.CLUSTER_NODE_URLS?.split(',').map((url) => url.trim()) ?? DEFAULT_CLUSTER_NODE_URLS),
])
const batchDepth = Number(process.env.BATCH_DEPTH ?? DEFAULT_BATCH_DEPTH)
const batchAmount = process.env.BATCH_AMOUNT ?? DEFAULT_BATCH_AMOUNT
const deferred = (process.env.DEFERRED ?? 'true') !== 'false'

const uploadBee = new Bee(uploadUrl)
const decoder = new TextDecoder()
const encoder = new TextEncoder()

// bee-js types makeSOCWriter().upload's `stamp` as BatchId | Uint8Array | string, but
// prepareRequestHeaders detects an envelope object at runtime and marshals it into the
// swarm-postage-stamp header. We pass the envelope object through this cast.
type SocStamp = Parameters<ReturnType<Bee['makeSOCWriter']>['upload']>[0]

interface NodeInfo {
  url: string
  bee: Bee
  overlay: Uint8Array
  overlayHex: string
}

interface NodeResult {
  node: NodeInfo
  proximityOrder: number
  isStorer: boolean
  wrote: string
  uploadError: string | undefined
  serves: string
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

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

// Proximity order = number of matching leading bits (higher = closer = responsible storer).
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

async function discoverNodes(): Promise<NodeInfo[]> {
  const nodes: NodeInfo[] = []
  for (const url of clusterUrls) {
    const bee = new Bee(url)
    try {
      const addresses = await bee.getNodeAddresses()
      nodes.push({ url, bee, overlay: addresses.overlay.toUint8Array(), overlayHex: addresses.overlay.toHex() })
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
  console.log(`buying a mutable batch (amount=${batchAmount}, depth=${batchDepth})…`)
  const batchId = await uploadBee.createPostageBatch(batchAmount, batchDepth, {
    immutableFlag: false,
    waitForUsable: true,
    label: 'same-stamp-divergence',
  })
  console.log(`bought batch: ${batchId.toHex()}  (reuse next time via BATCH_ID=…)`)
  return batchId.toHex()
}

async function readSoc(bee: Bee, owner: PrivateKey, identifier: Identifier): Promise<string> {
  const ownerAddress = owner.publicKey().address()
  let lastError: unknown
  for (let attempt = 0; attempt < READ_RETRIES; attempt++) {
    try {
      const chunk = await bee.makeSOCReader(ownerAddress).download(identifier)
      return decoder.decode(chunk.payload.toUint8Array())
    } catch (error) {
      lastError = error
      await sleep(READ_RETRY_DELAY_MS)
    }
  }
  return `<download failed: ${errorMessage(lastError)}>`
}

async function main(): Promise<void> {
  console.log(`mint node:  ${uploadUrl}`)
  console.log(`cluster:    ${clusterUrls.join(', ')}`)
  console.log(`deferred:   ${deferred}`)

  console.log('\ndiscovering node overlays…')
  const nodes = await discoverNodes()
  for (const node of nodes) {
    console.log(`  ${node.url.padEnd(22)} overlay ${node.overlayHex}`)
  }
  if (nodes.length < 2) {
    throw Error('need at least 2 reachable nodes to demonstrate divergence')
  }

  // Fresh SOC owner + identifier so prior runs never pollute the result.
  const signer = new PrivateKey(new Uint8Array(randomBytes(PRIVATE_KEY_BYTES)))
  const owner = signer.publicKey().address()
  const identifier = new Identifier(new Uint8Array(randomBytes(IDENTIFIER_BYTES)))
  const socAddress = uploadBee.calculateSingleOwnerChunkAddress(identifier, owner)
  console.log(`\nSOC owner=${owner.toHex()} identifier=${identifier.toHex()}`)
  console.log(`SOC address=${socAddress.toHex()}`)

  // ONE envelope (= one postage stamp) for that address, reused for every upload.
  const batchId = await resolveBatchId()
  const envelope = await uploadBee.createEnvelope(batchId, socAddress)
  console.log('\nminted ONE envelope (shared by every upload):')
  console.log(`  batchId   = ${envelope.batchId.toHex()}`)
  console.log(`  index     = ${bytesToHex(envelope.index)}`)
  console.log(`  timestamp = ${bytesToHex(envelope.timestamp)}`)

  // Rank nodes by proximity to the SOC address; the closest is the responsible storer.
  const ranked = [...nodes].sort(
    (a, b) => proximityOrder(socAddress.toUint8Array(), b.overlay) - proximityOrder(socAddress.toUint8Array(), a.overlay),
  )
  const storerUrl = ranked[0].url

  // Write a DIFFERENT payload to each node, all carrying the SAME envelope. Upload in
  // PARALLEL so each node stores its own write before background pushsync can propagate
  // another node's value to it (otherwise the first writer's value wins everywhere).
  console.log('\nwriting different content to each node with the same stamp (in parallel)…')
  const results: NodeResult[] = await Promise.all(
    nodes.map(async (node): Promise<NodeResult> => {
      const wrote = `written-by ${node.url}`
      let uploadError: string | undefined
      try {
        await node.bee
          .makeSOCWriter(signer)
          .upload(envelope as unknown as SocStamp, identifier, encoder.encode(wrote), { deferred })
        console.log(`  ${node.url.padEnd(22)} wrote "${wrote}"`)
      } catch (error) {
        uploadError = errorMessage(error)
        console.log(`  ${node.url.padEnd(22)} upload REJECTED: ${uploadError}`)
      }
      return {
        node,
        proximityOrder: proximityOrder(socAddress.toUint8Array(), node.overlay),
        isStorer: node.url === storerUrl,
        wrote,
        uploadError,
        serves: '',
      }
    }),
  )

  // Read the SOC back from every node, in parallel and immediately, to snapshot what
  // each node holds before reads/pushsync converge them.
  console.log('\nreading the SOC back from every node…')
  await Promise.all(
    results.map(async (result) => {
      result.serves = await readSoc(result.node.bee, signer, identifier)
    }),
  )

  console.log('\n================ RESULTS ================')
  for (const result of results) {
    const tag = result.isStorer ? '  [STORER]' : ''
    console.log(
      `${result.node.url.padEnd(22)} overlay ${result.node.overlayHex.slice(0, OVERLAY_SHORT_CHARS)} ` +
        `PO ${String(result.proximityOrder).padStart(3)} | wrote "${result.wrote}" | now serves "${result.serves}"${tag}`,
    )
  }

  const servedValues = results.map((result) => result.serves).filter((value) => !value.startsWith('<download failed'))
  const distinctValues = dedupe(servedValues)
  const storerResult = results.find((result) => result.isStorer)

  console.log('\n================ VERDICT ================')
  if (distinctValues.length > 1) {
    console.log('✅ Problem demonstrated: ONE stamp, MANY values.')
    console.log(`   The same SOC address resolves to ${distinctValues.length} different values across nodes,`)
    console.log('   all authorized by an identical postage stamp (same batchID, index, timestamp).')
    console.log('   Each node kept the first value it saw — there is no single source of truth.')
    console.log(`   The responsible storer (${storerUrl}) serves "${storerResult?.serves}" — the likely`)
    console.log('   eventual winner once pushsync converges; until then the nodes disagree.')
  } else if (distinctValues.length === 1) {
    console.log('⚠️  All reachable nodes currently serve the same value:')
    console.log(`   "${distinctValues[0]}"`)
    console.log('   The reads likely converged on the storer\'s first-seen value via pushsync/retrieval.')
    console.log('   Re-run (each run uses a fresh address) or set DEFERRED=true to catch the divergence.')
  } else {
    console.log('❓ No node served the SOC — inspect the upload errors above.')
  }
  console.log('=========================================\n')
}

main().catch((error) => {
  console.error(errorMessage(error))
  process.exit(1)
})
