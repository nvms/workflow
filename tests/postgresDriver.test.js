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
    engine.register(makeWorkflow(async () => {
      return { recovered: true }
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

  it("signals a wait step from one worker after another suspended it", async () => {
    requirePostgres()

    const reviewWorkflow = defineWorkflow({
      name: "pg-review", version: "1", start: "gate",
      steps: {
        gate: {
          type: "wait",
          transitions: { approved: "ok", rejected: "rej" },
          resolve: ({ signal }) => signal.decision,
        },
        ok: { type: "succeed", result: ({ steps }) => steps.gate.output },
        rej: { type: "fail", result: () => ({ name: "R", message: "no" }) },
      },
    })

    const engineA = new WorkflowEngine({ storage: postgresDriver({ connectionString }), owner: "a" })
    const engineB = new WorkflowEngine({ storage: postgresDriver({ connectionString }), owner: "b" })
    engineA.register(reviewWorkflow)
    engineB.register(reviewWorkflow)

    const execution = await engineA.start("pg-review", {})
    await engineA.runUntilIdle()
    expect((await engineA.getExecution(execution.id)).status).toBe("suspended")

    await engineB.signal(execution.id, { decision: "approved", who: "alice" })
    await engineB.runUntilIdle()

    const final = await engineA.getExecution(execution.id)
    expect(final.status).toBe("succeeded")
    expect(final.output).toEqual({ decision: "approved", who: "alice" })

    await Promise.all([engineA.close(), engineB.close()])
  })

  it("AlreadySignaledError is thrown when two workers signal the same suspended execution", async () => {
    requirePostgres()

    const reviewWorkflow = defineWorkflow({
      name: "pg-race", version: "1", start: "gate",
      steps: {
        gate: { type: "wait", transitions: { approved: "ok" }, resolve: ({ signal }) => signal.decision },
        ok: { type: "succeed" },
      },
    })

    const engineA = new WorkflowEngine({ storage: postgresDriver({ connectionString }), owner: "a" })
    const engineB = new WorkflowEngine({ storage: postgresDriver({ connectionString }), owner: "b" })
    engineA.register(reviewWorkflow)
    engineB.register(reviewWorkflow)

    const exec = await engineA.start("pg-race", {})
    await engineA.runUntilIdle()

    const results = await Promise.allSettled([
      engineA.signal(exec.id, { decision: "approved" }),
      engineB.signal(exec.id, { decision: "approved" }),
    ])

    const fulfilled = results.filter((r) => r.status === "fulfilled")
    const rejected = results.filter((r) => r.status === "rejected")
    expect(fulfilled).toHaveLength(1)
    expect(rejected).toHaveLength(1)
    expect(rejected[0].reason.name).toBe("AlreadySignaledError")

    await Promise.all([engineA.close(), engineB.close()])
  })

  it("findChildren locates child executions by parent id", async () => {
    requirePostgres()

    const child = defineWorkflow({
      name: "pg-child", version: "1", start: "done",
      steps: { done: { type: "succeed" } },
    })
    const parent = defineWorkflow({
      name: "pg-parent", version: "1", start: "sub",
      steps: {
        sub: {
          type: "subworkflow", workflow: "pg-child",
          transitions: { succeeded: "ok", failed: "rej", canceled: "rej" },
        },
        ok: { type: "succeed" },
        rej: { type: "fail" },
      },
    })

    const engine = new WorkflowEngine({ storage: postgresDriver({ connectionString }) })
    engine.register(parent)
    engine.register(child)

    const exec = await engine.start("pg-parent", {})
    await engine.runUntilIdle()

    const finalParent = await engine.getExecution(exec.id)
    expect(finalParent.status).toBe("succeeded")
    expect(finalParent.steps.sub.childExecutionId).toBeTruthy()

    const childExec = await engine.getExecution(finalParent.steps.sub.childExecutionId)
    expect(childExec.parent).toEqual({ executionId: exec.id, step: "sub" })

    await engine.close()
  })
})
