import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow, AlreadySignaledError } from '../src/index.js'

function makeEngine() {
  const engine = new WorkflowEngine({ leaseRenewInterval: 60_000 })
  return engine
}

describe('wait step validation', () => {
  it('rejects wait without transitions', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: { a: { type: 'wait' } },
      }),
    ).toThrow(/must define transitions/)
  })

  it('rejects wait with empty transitions', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: { a: { type: 'wait', transitions: {} } },
      }),
    ).toThrow(/at least one transition/)
  })

  it('rejects wait routing to unknown step', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: { a: { type: 'wait', transitions: { ok: 'nope' } } },
      }),
    ).toThrow(/unknown step/)
  })

  it('rejects wait with timeout but no timeout transition', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: {
          a: { type: 'wait', timeout: '1s', transitions: { ok: 'done' } },
          done: { type: 'succeed' },
        },
      }),
    ).toThrow(/no "timeout" transition/)
  })

  it('rejects non-function resolve', () => {
    expect(() =>
      defineWorkflow({
        name: 'w', version: '1', start: 'a',
        steps: {
          a: { type: 'wait', resolve: 'nope', transitions: { ok: 'done' } },
          done: { type: 'succeed' },
        },
      }),
    ).toThrow(/resolve must be a function/)
  })

  it('emits wait edges in graph introspection', () => {
    const wf = defineWorkflow({
      name: 'w', version: '1', start: 'a',
      steps: {
        a: { type: 'wait', transitions: { yes: 'ok', no: 'rej' } },
        ok: { type: 'succeed' },
        rej: { type: 'fail' },
      },
    })
    const labels = wf.graph.edges.filter((e) => e.from === 'a').map((e) => e.label).sort()
    expect(labels).toEqual(['no', 'yes'])
  })
})

describe('wait step suspension and signaling', () => {
  const buildWorkflow = () =>
    defineWorkflow({
      name: 'review', version: '1', start: 'gate',
      steps: {
        gate: {
          type: 'wait',
          transitions: { approved: 'pub', rejected: 'rej' },
          resolve: ({ signal }) => signal.decision,
        },
        pub: { type: 'succeed', result: ({ steps }) => ({ gate: steps.gate.output }) },
        rej: { type: 'fail', result: () => ({ name: 'Rejected', message: 'no' }) },
      },
    })

  it('suspends at a wait step and emits step:suspended', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    const events = []
    engine.on('step:suspended', (e) => events.push(e))

    const exec = await engine.start('review', {})
    await engine.runUntilIdle()

    const after = await engine.getExecution(exec.id)
    expect(after.status).toBe('suspended')
    expect(after.currentStep).toBe('gate')
    expect(after.steps.gate.status).toBe('awaiting')
    expect(after.lockOwner).toBeNull()
    expect(after.availableAt).toBeNull()
    expect(events).toHaveLength(1)
    expect(events[0].step).toBe('gate')

    const journalTypes = after.journal.map((e) => e.type)
    expect(journalTypes).toContain('step.suspended')
  })

  it('signal advances to the chosen route and produces the right output', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    const exec = await engine.start('review', {})
    await engine.runUntilIdle()

    await engine.signal(exec.id, { decision: 'approved', by: 'jane' })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.steps.gate.route).toBe('approved')
    expect(final.steps.gate.output).toEqual({ decision: 'approved', by: 'jane' })
    expect(final.output).toEqual({ gate: { decision: 'approved', by: 'jane' } })

    const journalTypes = final.journal.map((e) => e.type)
    expect(journalTypes).toContain('step.signaled')
    expect(journalTypes).toContain('step.routed')
  })

  it('signal can route to a fail terminal', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    const exec = await engine.start('review', {})
    await engine.runUntilIdle()

    await engine.signal(exec.id, { decision: 'rejected' })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('failed')
    expect(final.error).toEqual({ name: 'Rejected', message: 'no' })
  })

  it('signal with no resolve falls back to payload.route', async () => {
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 'r', version: '1', start: 'gate',
        steps: {
          gate: { type: 'wait', transitions: { approved: 'done' } },
          done: { type: 'succeed' },
        },
      }),
    )
    const exec = await engine.start('r', {})
    await engine.runUntilIdle()

    await engine.signal(exec.id, { route: 'approved' })
    await engine.runUntilIdle()

    expect((await engine.getExecution(exec.id)).status).toBe('succeeded')
  })

  it('rejects signal with unknown route', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    const exec = await engine.start('review', {})
    await engine.runUntilIdle()

    await expect(engine.signal(exec.id, { decision: 'maybe' })).rejects.toThrow(/unknown route/)

    const after = await engine.getExecution(exec.id)
    expect(after.status).toBe('suspended')
  })

  it('rejects signal on a non-suspended execution', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    const exec = await engine.start('review', {})

    await expect(engine.signal(exec.id, { decision: 'approved' })).rejects.toThrow(AlreadySignaledError)
  })

  it('rejects signal on unknown execution', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    await expect(engine.signal('does-not-exist', {})).rejects.toThrow(/not found/)
  })

  it('throws AlreadySignaledError on duplicate signal', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    const exec = await engine.start('review', {})
    await engine.runUntilIdle()

    await engine.signal(exec.id, { decision: 'approved' })
    await expect(engine.signal(exec.id, { decision: 'approved' })).rejects.toThrow(AlreadySignaledError)
  })

  it('cancel from suspended is clean', async () => {
    const engine = makeEngine()
    engine.register(buildWorkflow())
    const exec = await engine.start('review', {})
    await engine.runUntilIdle()

    await engine.cancel(exec.id, 'changed our minds')

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('canceled')
    expect(final.error).toEqual({ name: 'Canceled', message: 'changed our minds' })
  })
})

describe('wait step timeout', () => {
  it('routes via the timeout transition when timer expires', async () => {
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 't', version: '1', start: 'gate',
        steps: {
          gate: {
            type: 'wait',
            timeout: 10,
            transitions: { approved: 'ok', timeout: 'remind' },
          },
          ok: { type: 'succeed' },
          remind: { type: 'succeed', result: () => ({ reminded: true }) },
        },
      }),
    )

    const exec = await engine.start('t', {})
    await engine.runUntilIdle()
    expect((await engine.getExecution(exec.id)).status).toBe('suspended')

    await new Promise((r) => setTimeout(r, 25))
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.status).toBe('succeeded')
    expect(final.output).toEqual({ reminded: true })
    expect(final.steps.gate.route).toBe('timeout')

    const journalTypes = final.journal.map((e) => e.type)
    expect(journalTypes).toContain('step.timed-out')
  })

  it('signal arriving before timeout still wins', async () => {
    const engine = makeEngine()
    engine.register(
      defineWorkflow({
        name: 't2', version: '1', start: 'gate',
        steps: {
          gate: {
            type: 'wait',
            timeout: '5s',
            transitions: { approved: 'ok', timeout: 'remind' },
            resolve: ({ signal }) => signal.decision,
          },
          ok: { type: 'succeed', result: () => ({ via: 'signal' }) },
          remind: { type: 'succeed', result: () => ({ via: 'timeout' }) },
        },
      }),
    )

    const exec = await engine.start('t2', {})
    await engine.runUntilIdle()
    await engine.signal(exec.id, { decision: 'approved' })
    await engine.runUntilIdle()

    const final = await engine.getExecution(exec.id)
    expect(final.output).toEqual({ via: 'signal' })
  })
})
