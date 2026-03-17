import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest"
import WorkflowEngine, { defineWorkflow } from "../src/index.js"

let postgresDriver = null
let Pool = null

try {
  ;({ postgresDriver } = await import("../src/postgresDriver.js"))
  const pg = await import("pg")
  Pool = pg.default?.Pool || pg.Pool
} catch {}

const connectionString = process.env.WORKFLOW_TEST_POSTGRES_URL ?? "postgres://workflow:workflow_password@127.0.0.1:5432/workflow_test"
const describeIfPostgres = postgresDriver && Pool ? describe : describe.skip

describeIfPostgres("postgres driver", () => {
  let adminPool = null
  let postgresReady = false

  async function sleep(ms) {
    await new Promise((resolve) => setTimeout(resolve, ms))
  }

  beforeAll(async () => {
    adminPool = new Pool({ connectionString })
    try {
      await adminPool.query("SELECT 1")
      postgresReady = true
    } catch {
      postgresReady = false
    }
  })

  afterEach(async () => {
    if (!postgresReady) return
    await adminPool.query("DROP TABLE IF EXISTS workflow_executions")
  })

  afterAll(async () => {
    if (adminPool) await adminPool.end()
  })

  function requirePostgres() {
    if (!postgresReady) {
      throw new Error("Postgres test database is not available. Start docker compose or set WORKFLOW_TEST_POSTGRES_URL.")
    }
  }

  function makeWorkflow(handler) {
    return defineWorkflow({
      name: "pg-workflow",
      version: "1",
      start: "work",
      steps: {
        work: {
          type: "activity",
          next: "done",
          retry: { maxAttempts: 1, backoff: 0 },
          run: handler,
        },
        done: {
          type: "succeed",
          result: ({ data, steps }) => ({ data, output: steps.work.output }),
        },
      },
    })
  }

  it("claims an execution atomically across competing workers", async () => {
    requirePostgres()

    const seed = new WorkflowEngine({ storage: postgresDriver({ connectionString }) })
    seed.register(makeWorkflow(async () => ({ ok: true })))
    const execution = await seed.start("pg-workflow", {})

    const driverA = postgresDriver({ connectionString })
    const driverB = postgresDriver({ connectionString })
    await Promise.all([driverA.init(), driverB.init()])

    const [claimedA, claimedB] = await Promise.all([
      driverA.claimAvailable({ now: Date.now(), owner: "worker-a", leaseMs: 1000, limit: 1 }),
      driverB.claimAvailable({ now: Date.now(), owner: "worker-b", leaseMs: 1000, limit: 1 }),
    ])

    expect(claimedA.length + claimedB.length).toBe(1)
    expect([claimedA[0]?.id, claimedB[0]?.id].filter(Boolean)).toEqual([execution.id])

    await Promise.all([seed.close(), driverA.close(), driverB.close()])
  })

  it("recovers work after a stale lease expires", async () => {
    requirePostgres()

    const storage = postgresDriver({ connectionString })
    const engine = new WorkflowEngine({ storage })
    engine.register(makeWorkflow(async ({ data }) => {
      data.recovered = true
      return { ok: true }
    }))

    const execution = await engine.start("pg-workflow", {})
    const deadWorker = postgresDriver({ connectionString })
    await deadWorker.init()

    const claimed = await deadWorker.claimAvailable({
      now: Date.now(),
      owner: "dead-worker",
      leaseMs: 40,
      limit: 1,
    })

    expect(claimed).toHaveLength(1)
    await sleep(60)

    await engine.runUntilIdle()

    const persisted = await engine.getExecution(execution.id)
    expect(persisted.status).toBe("succeeded")
    expect(persisted.data.recovered).toBe(true)

    await Promise.all([engine.close(), deadWorker.close()])
  })

  it("renews leases for long-running steps so another worker cannot steal them", async () => {
    requirePostgres()

    let sideEffects = 0
    const workflow = makeWorkflow(async ({ data, step }) => {
      data.lastKey = step.idempotencyKey
      await sleep(120)
      sideEffects += 1
      return { ok: true }
    })

    const engineA = new WorkflowEngine({
      storage: postgresDriver({ connectionString }),
      owner: "engine-a",
      leaseMs: 40,
      leaseRenewInterval: 10,
    })
    const engineB = new WorkflowEngine({
      storage: postgresDriver({ connectionString }),
      owner: "engine-b",
      leaseMs: 40,
      leaseRenewInterval: 10,
    })

    engineA.register(workflow)
    engineB.register(workflow)

    const execution = await engineA.start("pg-workflow", {})
    const runA = engineA.runDue()
    await sleep(70)
    const processedByB = await engineB.runDue()
    await runA
    await engineA.runUntilIdle()

    const persisted = await engineA.getExecution(execution.id)
    expect(processedByB).toBe(0)
    expect(sideEffects).toBe(1)
    expect(persisted.status).toBe("succeeded")
    expect(persisted.steps.work.attempts).toBe(1)

    await Promise.all([engineA.close(), engineB.close()])
  })
})
