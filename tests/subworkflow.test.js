import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow } from '../src/index.js'

function makeEngine() {
  return new WorkflowEngine({ leaseRenewInterval: 60_000 })
}

describe('subworkflow step validation', () => {
  it('rejects without a workflow name', () => {
    expect(() =>
      defineWorkflow({
        name: 'p', version: '1', start: 'sub',
        steps: {
          sub: { type: 'subworkflow', transitions: { succeeded: 'ok', failed: 'ok', canceled: 'ok' } },
          ok: { type: 'succeed' },
        },
      }),
    ).toThrow(/workflow name/)
  })

  it('rejects when missing required terminal transitions', () => {
    expect(() =>
      defineWorkflow({
        name: 'p', version: '1', start: 'sub',
        steps: {
          sub: { type: 'subworkflow', workflow: 'child', transitions: { succeeded: 'ok' } },
          ok: { type: 'succeed' },
        },
      }),
    ).toThrow(/missing required transition/)
  })

  it('rejects non-function input', () => {
    expect(() =>
      defineWorkflow({
        name: 'p', version: '1', start: 'sub',
        steps: {
          sub: {
            type: 'subworkflow',
            workflow: 'child',
            input: 'nope',
            transitions: { succeeded: 'ok', failed: 'ok', canceled: 'ok' },
          },
          ok: { type: 'succeed' },
        },
      }),
    ).toThrow(/input must be a function/)
  })

  it('exposes terminal transitions as graph edges', () => {
    const wf = defineWorkflow({
      name: 'p', version: '1', start: 'sub',
      steps: {
        sub: {
          type: 'subworkflow',
          workflow: 'child',
          transitions: { succeeded: 'ok', failed: 'rej', canceled: 'rej' },
        },
        ok: { type: 'succeed' },
        rej: { type: 'fail' },
      },
    })
    const labels = wf.graph.edges.filter((e) => e.from === 'sub').map((e) => e.label).sort()
    expect(labels).toEqual(['canceled', 'failed', 'succeeded'])
  })
})

const childSucceed = defineWorkflow({
  name: 'child', version: '1', start: 'work',
  steps: {
    work: {
      type: 'activity', next: 'done',
      run: ({ input }) => ({ doubled: input.value * 2 }),
    },
    done: { type: 'succeed', result: ({ data }) => data },
  },
})

const childFail = defineWorkflow({
  name: 'childFail', version: '1', start: 'boom',
  steps: {
    boom: { type: 'fail', result: () => ({ name: 'Boom', message: 'no' }) },
  },
})

const parent = defineWorkflow({
  name: 'parent', version: '1', start: 'sub',
  steps: {
    sub: {
      type: 'subworkflow',
      workflow: 'child',
      input: ({ input }) => ({ value: input.n }),
      transitions: { succeeded: 'ok', failed: 'rej', canceled: 'rej' },
    },
    ok: { type: 'succeed', result: ({ steps }) => ({ child: steps.sub.output.output }) },
    rej: { type: 'fail', result: ({ steps }) => ({ name: 'ChildFailed', message: JSON.stringify(steps.sub.output) }) },
  },
})

const parentFail = defineWorkflow({
  name: 'parentFail', version: '1', start: 'sub',
  steps: {
    sub: {
      type: 'subworkflow',
      workflow: 'childFail',
      transitions: { succeeded: 'ok', failed: 'rej', canceled: 'rej' },
    },
    ok: { type: 'succeed' },
    rej: { type: 'fail', result: ({ steps }) => steps.sub.output.error },
  },
})

describe('subworkflow execution', () => {
  it('spawns child, suspends parent, resumes on child success', async () => {
    const engine = makeEngine()
    engine.register(parent)
    engine.register(childSucceed)

    const exec = await engine.start('parent', { n: 5 })

    await engine.runDue()
    const midParent = await engine.getExecution(exec.id)
    expect(midParent.status).toBe('suspended')
    expect(midParent.currentStep).toBe('sub')
    expect(midParent.lockOwner).toBeNull()
    expect(midParent.steps.sub.childExecutionId).toBeTruthy()

    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ child: { doubled: 10 } })
    expect(final.steps.sub.route).toBe('succeeded')

    const journalTypes = final.journal.map((e) => e.type)
    expect(journalTypes).toContain('step.subworkflow-started')
    expect(journalTypes).toContain('step.subworkflow-resolved')

    const child = await engine.getExecution(final.steps.sub.childExecutionId)
    expect(child.status).toBe('succeeded')
    expect(child.parent).toEqual({ executionId: exec.id, step: 'sub' })
  })

  it('pins child to the version present at spawn time', async () => {
    const engine = makeEngine()

    const childV1 = defineWorkflow({
      name: 'pinned', version: '1', start: 'work',
      steps: {
        work: { type: 'activity', next: 'done', run: () => ({ from: 'v1' }) },
        done: { type: 'succeed', result: ({ data }) => data },
      },
    })
    const childV2 = defineWorkflow({
      name: 'pinned', version: '2', start: 'work',
      steps: {
        work: { type: 'activity', next: 'done', run: () => ({ from: 'v2' }) },
        done: { type: 'succeed', result: ({ data }) => data },
      },
    })
    const parentPinned = defineWorkflow({
      name: 'parentPinned', version: '1', start: 'sub',
      steps: {
        sub: {
          type: 'subworkflow',
          workflow: 'pinned',
          version: '1',
          transitions: { succeeded: 'ok', failed: 'rej', canceled: 'rej' },
        },
        ok: { type: 'succeed', result: ({ steps }) => steps.sub.output.output },
        rej: { type: 'fail' },
      },
    })

    engine.register(parentPinned)
    engine.register(childV1)
    engine.register(childV2)

    const exec = await engine.start('parentPinned', {})
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.output).toEqual({ from: 'v1' })

    const child = await engine.getExecution(final.steps.sub.childExecutionId)
    expect(child.workflowVersion).toBe('1')
  })

  it('routes to failed transition when child fails', async () => {
    const engine = makeEngine()
    engine.register(parentFail)
    engine.register(childFail)

    const exec = await engine.start('parentFail', {})
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('failed')
    expect(final.steps.sub.route).toBe('failed')
    expect(final.error).toEqual({ name: 'Boom', message: 'no' })
  })

  it('routes to canceled transition when child is canceled', async () => {
    const engine = makeEngine()
    engine.register(parent)
    engine.register(childSucceed)

    let started
    engine.on('execution:queued', (e) => {
      if (e.execution.workflow === 'child') started = e.execution.id
    })

    const exec = await engine.start('parent', { n: 1 })

    await engine.runDue()
    expect(started).toBeTruthy()

    await engine.cancel(started, 'manual')
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('failed')
    expect(final.steps.sub.route).toBe('canceled')
  })

  it('canceling parent cascades to suspended children', async () => {
    const engine = makeEngine()

    const slowChild = defineWorkflow({
      name: 'slowChild', version: '1', start: 'gate',
      steps: {
        gate: { type: 'wait', transitions: { ok: 'done' } },
        done: { type: 'succeed' },
      },
    })

    const parentCascade = defineWorkflow({
      name: 'parentCascade', version: '1', start: 'sub',
      steps: {
        sub: {
          type: 'subworkflow',
          workflow: 'slowChild',
          transitions: { succeeded: 'ok', failed: 'rej', canceled: 'rej' },
        },
        ok: { type: 'succeed' },
        rej: { type: 'fail', result: () => ({ name: 'CanceledChild', message: 'ok' }) },
      },
    })

    engine.register(parentCascade)
    engine.register(slowChild)

    let childId
    engine.on('execution:queued', (e) => {
      if (e.execution.workflow === 'slowChild') childId = e.execution.id
    })

    const exec = await engine.start('parentCascade', {})
    await engine.runUntilIdle()
    expect((await engine.getExecution(exec.id)).status).toBe('suspended')
    expect((await engine.getExecution(childId)).status).toBe('suspended')

    await engine.cancel(exec.id, 'parent went away')

    const child = await engine.getExecution(childId)
    expect(child.status).toBe('canceled')
    expect(child.error.message).toMatch(/parent .* canceled/)
  })

  it('child cancellation propagates upward to a suspended parent', async () => {
    const engine = makeEngine()
    engine.register(parent)
    engine.register(childSucceed)

    let childId
    engine.on('execution:queued', (e) => {
      if (e.execution.workflow === 'child') childId = e.execution.id
    })

    const exec = await engine.start('parent', { n: 1 })
    await engine.runDue()
    expect(childId).toBeTruthy()

    await engine.cancel(childId, 'oops')
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('failed')
    expect(final.steps.sub.route).toBe('canceled')
  })

  it('signal cannot be used on subworkflow steps', async () => {
    const engine = makeEngine()
    engine.register(parent)
    engine.register(childSucceed)

    const exec = await engine.start('parent', { n: 1 })
    await engine.runDue()

    await expect(engine.signal(exec.id, { route: 'succeeded' })).rejects.toThrow(/not at a wait step/)
  })

  it('subworkflow output is structured { status, output, error }', async () => {
    const engine = makeEngine()
    engine.register(parent)
    engine.register(childSucceed)

    const exec = await engine.start('parent', { n: 7 })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.steps.sub.output).toMatchObject({
      status: 'succeeded',
      output: { doubled: 14 },
      error: null,
    })
    expect(typeof final.steps.sub.output.executionId).toBe('string')
  })
})
