import { describe, it, expect } from 'vitest'
import WorkflowEngine, { defineWorkflow, memoryDriver } from '../src/index.js'

describe('defineWorkflow', () => {
  const minimal = (overrides) => ({
    name: 'test',
    version: '1',
    start: 'a',
    steps: {
      a: { type: 'activity', next: 'done', run: () => {} },
      done: { type: 'succeed' },
    },
    ...overrides,
  })

  it('rejects cycles', () => {
    expect(() =>
      defineWorkflow({
        name: 'cycle',
        version: '1',
        start: 'a',
        steps: {
          a: { type: 'activity', next: 'b', run: () => {} },
          b: { type: 'activity', next: 'a', run: () => {} },
        },
      }),
    ).toThrow(/cycle detected/)
  })

  it('rejects unreachable steps', () => {
    expect(() =>
      defineWorkflow({
        name: 'island',
        version: '1',
        start: 'a',
        steps: {
          a: { type: 'activity', next: 'done', run: () => {} },
          done: { type: 'succeed' },
          orphan: { type: 'activity', next: 'done', run: () => {} },
        },
      }),
    ).toThrow(/unreachable/)
  })

  it('rejects unknown step types', () => {
    expect(() =>
      defineWorkflow(
        minimal({
          steps: {
            a: { type: 'magic', next: 'done' },
            done: { type: 'succeed' },
          },
        }),
      ),
    ).toThrow(/unsupported type/)
  })

  it('rejects missing required fields', () => {
    expect(() => defineWorkflow({})).toThrow()
    expect(() => defineWorkflow(minimal({ name: '' }))).toThrow()
    expect(() => defineWorkflow(minimal({ start: 'nonexistent' }))).toThrow()
  })

  it('rejects activity steps without run function', () => {
    expect(() =>
      defineWorkflow(
        minimal({
          steps: {
            a: { type: 'activity', next: 'done' },
            done: { type: 'succeed' },
          },
        }),
      ),
    ).toThrow(/must define run/)
  })

  it('rejects decision steps with empty transitions', () => {
    expect(() =>
      defineWorkflow({
        name: 'empty-decision',
        version: '1',
        start: 'd',
        steps: {
          d: { type: 'decision', decide: () => 'x', transitions: {} },
        },
      }),
    ).toThrow(/at least one transition/)
  })

  it('freezes the returned definition', () => {
    const workflow = defineWorkflow(minimal())
    expect(() => {
      workflow.name = 'hacked'
    }).toThrow()
    expect(() => {
      workflow.steps.newStep = {}
    }).toThrow()
  })
})

describe('@prsm/workflow', () => {
  it('describes a workflow graph for future visualization', () => {
    const workflow = defineWorkflow({
      name: 'mission-check',
      version: '1',
      start: 'validate',
      steps: {
        validate: {
          type: 'activity',
          next: 'route',
          run: async () => ({ ok: true }),
        },
        route: {
          type: 'decision',
          transitions: {
            proceed: 'complete',
            reject: 'reject',
          },
          decide: () => 'proceed',
        },
        complete: {
          type: 'succeed',
          result: () => ({ status: 'green' }),
        },
        reject: {
          type: 'fail',
          result: () => ({ name: 'Rejected', message: 'Rejected' }),
        },
      },
    })

    expect(workflow.graph.nodes.map((node) => node.name)).toEqual(['validate', 'route', 'complete', 'reject'])
    expect(workflow.graph.edges).toEqual([
      { from: 'validate', to: 'route', label: 'next' },
      { from: 'route', to: 'complete', label: 'proceed' },
      { from: 'route', to: 'reject', label: 'reject' },
    ])
  })

  it('runs an execution to success with deterministic routing', async () => {
    const workflow = defineWorkflow({
      name: 'review',
      version: '1',
      start: 'fetch',
      steps: {
        fetch: {
          type: 'activity',
          next: 'route',
          run: async ({ input, data }) => {
            data.message = input.message.trim()
            return { normalized: data.message }
          },
        },
        route: {
          type: 'decision',
          transitions: {
            spam: 'reject',
            normal: 'complete',
          },
          decide: ({ data }) => (data.message.includes('buy now') ? 'spam' : 'normal'),
        },
        complete: {
          type: 'succeed',
          result: ({ data }) => ({ outcome: 'sent', message: data.message }),
        },
        reject: {
          type: 'fail',
          result: ({ data }) => ({ name: 'Spam', message: data.message }),
        },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)

    const started = await engine.start('review', { message: '  hello control room  ' })
    expect(started.status).toBe('queued')

    await engine.runUntilIdle()

    const execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('succeeded')
    expect(execution.output).toEqual({ outcome: 'sent', message: 'hello control room' })
    expect(execution.steps.fetch.status).toBe('succeeded')
    expect(execution.steps.route.route).toBe('normal')
  })

  it('retries activity steps before failing the execution', async () => {
    let attempts = 0

    const workflow = defineWorkflow({
      name: 'retryable',
      version: '1',
      start: 'unstable',
      steps: {
        unstable: {
          type: 'activity',
          next: 'done',
          retry: { maxAttempts: 3, backoff: 0 },
          run: async () => {
            attempts += 1
            if (attempts < 3) throw new Error('temporary fault')
            return { ok: true }
          },
        },
        done: {
          type: 'succeed',
          result: ({ steps }) => steps.unstable.output,
        },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)

    const started = await engine.start('retryable', { mission: 'apollo' })
    await engine.runUntilIdle()

    const execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('succeeded')
    expect(execution.steps.unstable.attempts).toBe(3)
    expect(execution.journal.filter((entry) => entry.type === 'step.retry-scheduled')).toHaveLength(2)
  })

  it('supports resuming a failed execution from the failing step', async () => {
    let shouldPass = false

    const workflow = defineWorkflow({
      name: 'resumeable',
      version: '1',
      start: 'run',
      steps: {
        run: {
          type: 'activity',
          next: 'done',
          retry: { maxAttempts: 1, backoff: 0 },
          run: async () => {
            if (!shouldPass) throw new Error('hard fault')
            return { ok: true }
          },
        },
        done: {
          type: 'succeed',
          result: { ok: true },
        },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)

    const started = await engine.start('resumeable', {})
    await engine.runUntilIdle()

    let execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('failed')
    expect(execution.currentStep).toBe('run')

    shouldPass = true
    await engine.resume(started.id)
    await engine.runUntilIdle()

    execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('succeeded')
  })

  it('fails the execution when retries are exhausted', async () => {
    const workflow = defineWorkflow({
      name: 'doomed',
      version: '1',
      start: 'flaky',
      steps: {
        flaky: {
          type: 'activity',
          next: 'done',
          retry: { maxAttempts: 2, backoff: 0 },
          run: async () => {
            throw new Error('permanent fault')
          },
        },
        done: { type: 'succeed' },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('doomed', {})
    await engine.runUntilIdle()

    const execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('failed')
    expect(execution.error.message).toBe('permanent fault')
    expect(execution.steps.flaky.attempts).toBe(2)
  })

  it('routes to a fail step and produces the correct error', async () => {
    const workflow = defineWorkflow({
      name: 'rejection',
      version: '1',
      start: 'check',
      steps: {
        check: {
          type: 'decision',
          transitions: { bad: 'reject' },
          decide: () => 'bad',
        },
        reject: {
          type: 'fail',
          result: () => ({ name: 'PolicyViolation', message: 'denied by policy' }),
        },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('rejection', {})
    await engine.runUntilIdle()

    const execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('failed')
    expect(execution.error).toEqual({ name: 'PolicyViolation', message: 'denied by policy' })
    expect(execution.steps.reject.status).toBe('failed')
  })

  it('fails when a decision step returns an unknown route', async () => {
    const workflow = defineWorkflow({
      name: 'bad-route',
      version: '1',
      start: 'decide',
      steps: {
        decide: {
          type: 'decision',
          transitions: { a: 'done' },
          decide: () => 'nonexistent',
        },
        done: { type: 'succeed' },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('bad-route', {})
    await engine.runUntilIdle()

    const execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('failed')
    expect(execution.error.message).toMatch(/unknown route/)
  })

  it('times out an activity step that exceeds its deadline', async () => {
    const workflow = defineWorkflow({
      name: 'slow',
      version: '1',
      start: 'hang',
      steps: {
        hang: {
          type: 'activity',
          next: 'done',
          timeout: 10,
          run: () => new Promise((resolve) => setTimeout(resolve, 5000)),
        },
        done: { type: 'succeed' },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('slow', {})
    await engine.runUntilIdle()

    const execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('failed')
    expect(execution.error.message).toMatch(/timed out/)
  })

  it('cancels a queued execution', async () => {
    const workflow = defineWorkflow({
      name: 'cancelable',
      version: '1',
      start: 'work',
      steps: {
        work: { type: 'activity', next: 'done', run: async () => ({}) },
        done: { type: 'succeed' },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('cancelable', {})
    const canceled = await engine.cancel(started.id, 'abort mission')
    expect(canceled.status).toBe('canceled')
    expect(canceled.error.message).toBe('abort mission')

    await engine.runUntilIdle()
    const execution = await engine.getExecution(started.id)
    expect(execution.status).toBe('canceled')
  })

  it('cancel on a terminal execution is a no-op', async () => {
    const workflow = defineWorkflow({
      name: 'already-done',
      version: '1',
      start: 'done',
      steps: {
        done: { type: 'succeed', result: { v: 1 } },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('already-done', {})
    await engine.runUntilIdle()

    const result = await engine.cancel(started.id)
    expect(result.status).toBe('succeeded')
  })

  it('resolves the correct version when multiple are registered', async () => {
    const v1 = defineWorkflow({
      name: 'versioned',
      version: '1',
      start: 'done',
      steps: { done: { type: 'succeed', result: { v: 1 } } },
    })
    const v2 = defineWorkflow({
      name: 'versioned',
      version: '2',
      start: 'done',
      steps: { done: { type: 'succeed', result: { v: 2 } } },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(v1)
    engine.register(v2)

    const s1 = await engine.start('versioned', {}, { version: '1' })
    const s2 = await engine.start('versioned', {}, { version: '2' })
    await engine.runUntilIdle()

    const e1 = await engine.getExecution(s1.id)
    const e2 = await engine.getExecution(s2.id)
    expect(e1.output).toEqual({ v: 1 })
    expect(e2.output).toEqual({ v: 2 })
  })

  it('resume only works on failed executions', async () => {
    const workflow = defineWorkflow({
      name: 'guard',
      version: '1',
      start: 'done',
      steps: { done: { type: 'succeed' } },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('guard', {})
    await engine.runUntilIdle()

    await expect(engine.resume(started.id)).rejects.toThrow(/only failed/)
  })

  it('provides step context with idempotency key and attempt number', async () => {
    let captured = null

    const workflow = defineWorkflow({
      name: 'context-check',
      version: '1',
      start: 'work',
      steps: {
        work: {
          type: 'activity',
          next: 'done',
          run: async (ctx) => {
            captured = { key: ctx.step.idempotencyKey, attempt: ctx.step.attempt }
            return {}
          },
        },
        done: { type: 'succeed' },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver() })
    engine.register(workflow)
    const started = await engine.start('context-check', {})
    await engine.runUntilIdle()

    expect(captured.key).toBe(`${started.id}:work`)
    expect(captured.attempt).toBe(1)
  })

  it('processes a batch of executions concurrently', async () => {
    let peak = 0
    let inflight = 0

    const workflow = defineWorkflow({
      name: 'concurrent',
      version: '1',
      start: 'work',
      steps: {
        work: {
          type: 'activity',
          next: 'done',
          run: async () => {
            inflight++
            peak = Math.max(peak, inflight)
            await new Promise((r) => setTimeout(r, 50))
            inflight--
            return {}
          },
        },
        done: { type: 'succeed' },
      },
    })

    const engine = new WorkflowEngine({ storage: memoryDriver(), batchSize: 5 })
    engine.register(workflow)

    for (let i = 0; i < 5; i++) await engine.start('concurrent', {})
    await engine.runUntilIdle()

    const executions = await engine.listExecutions({ workflow: 'concurrent' })
    expect(executions.every((e) => e.status === 'succeeded')).toBe(true)
    expect(peak).toBe(5)
  })
})
