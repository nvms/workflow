import { afterEach, describe, expect, it } from 'vitest'
import WorkflowEngine, { defineWorkflow } from '../src/index.js'

let sqliteDriver = null

try {
  ;({ sqliteDriver } = await import('../src/sqliteDriver.js'))
} catch {}

const describeIfSqlite = sqliteDriver ? describe : describe.skip

describeIfSqlite('sqlite driver', () => {
  const files = new Set()

  afterEach(async () => {
    const { rm } = await import('node:fs/promises')
    await Promise.all(Array.from(files).map((file) => rm(file, { force: true })))
    files.clear()
  })

  it('persists executions across engine instances', async () => {
    const file = `/tmp/prsm-workflow-${Date.now()}.sqlite`
    files.add(file)

    const workflow = defineWorkflow({
      name: 'persisted',
      version: '1',
      start: 'step-a',
      steps: {
        'step-a': {
          type: 'activity',
          next: 'done',
          run: async ({ data }) => {
            data.count = 1
            return { ok: true }
          },
        },
        done: {
          type: 'succeed',
          result: ({ data }) => ({ count: data.count }),
        },
      },
    })

    const engineA = new WorkflowEngine({ storage: sqliteDriver({ filename: file }) })
    engineA.register(workflow)
    const started = await engineA.start('persisted', {})
    await engineA.runUntilIdle()

    const engineB = new WorkflowEngine({ storage: sqliteDriver({ filename: file }) })
    engineB.register(workflow)
    const execution = await engineB.getExecution(started.id)

    expect(execution.status).toBe('succeeded')
    expect(execution.output).toEqual({ count: 1 })
  })
})
