import { EventEmitter } from 'events'
import { randomUUID } from 'crypto'
import ms from '@prsm/ms'
import { memoryDriver } from './memoryDriver.js'

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value))
}

function serializeError(error) {
  if (!error) return { name: 'Error', message: 'Unknown error' }

  return {
    name: error.name ?? 'Error',
    message: error.message ?? String(error),
    code: error.code,
    stack: error.stack,
  }
}

function withTimeout(promise, timeoutMs, message) {
  if (!timeoutMs || timeoutMs <= 0) return promise

  let timer = null
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), timeoutMs)
      timer.unref?.()
    }),
  ]).finally(() => {
    if (timer) clearTimeout(timer)
  })
}

function normalizeMs(value, fallback = 0) {
  if (value == null) return ms(fallback)
  return ms(value)
}

function ensureStepState(execution, stepName) {
  execution.steps[stepName] = execution.steps[stepName] ?? {
    status: 'pending',
    attempts: 0,
    output: null,
    error: null,
    startedAt: null,
    endedAt: null,
    route: null,
    idempotencyKey: `${execution.id}:${stepName}`,
  }
  return execution.steps[stepName]
}

function stepContext(execution, workflow, stepName) {
  const currentStep = workflow.steps[stepName]

  return {
    execution: clone(execution),
    workflow: {
      name: workflow.name,
      version: workflow.version,
    },
    input: clone(execution.input),
    data: clone(execution.data),
    metadata: execution.metadata,
    steps: clone(execution.steps),
    step: {
      name: stepName,
      type: currentStep.type,
      attempt: execution.steps[stepName]?.attempts ?? 0,
      idempotencyKey: execution.steps[stepName]?.idempotencyKey ?? `${execution.id}:${stepName}`,
    },
    getStep(name) {
      return clone(execution.steps[name] ?? null)
    },
  }
}

function terminalState(type) {
  return type === 'succeed' ? 'succeeded' : 'failed'
}

class LeaseLostError extends Error {
  constructor(executionId, stepName) {
    super(`workflow execution lease lost: ${executionId}:${stepName}`)
    this.name = 'LeaseLostError'
    this.executionId = executionId
    this.stepName = stepName
  }
}

export class WorkflowEngine extends EventEmitter {
  constructor(options = {}) {
    super()

    this._storage = options.storage ?? memoryDriver()
    this._workflows = new Map()
    this._owner = options.owner ?? `workflow-engine:${randomUUID()}`
    this._leaseMs = normalizeMs(options.leaseMs ?? '5m')
    this._defaultActivityTimeout = normalizeMs(options.defaultActivityTimeout ?? '30s')
    this._leaseRenewInterval = normalizeMs(options.leaseRenewInterval ?? Math.max(10, Math.floor(this._leaseMs / 3)))
    this._pollTimer = null
    this._started = false
    this._activePoll = null
    this._batchSize = options.batchSize ?? 10
  }

  async ready() {
    if (this._started) return
    this._started = true
    if (this._storage.init) await this._storage.init()
  }

  register(workflow) {
    const key = `${workflow.name}@${workflow.version}`
    if (this._workflows.has(key)) throw new Error(`workflow already registered: ${key}`)
    this._workflows.set(key, workflow)
    return this
  }

  listWorkflows() {
    return Array.from(this._workflows.values()).map((workflow) => ({
      name: workflow.name,
      version: workflow.version,
      description: workflow.description,
    }))
  }

  describe(name, version) {
    const workflow = this._resolveWorkflow(name, version)
    return clone({
      name: workflow.name,
      version: workflow.version,
      description: workflow.description,
      graph: workflow.graph,
    })
  }

  async start(name, input, options = {}) {
    await this.ready()
    const workflow = this._resolveWorkflow(name, options.version)
    const now = Date.now()

    const execution = {
      id: options.id ?? randomUUID(),
      workflow: workflow.name,
      workflowVersion: workflow.version,
      status: 'queued',
      currentStep: workflow.start,
      input: clone(input),
      data: clone(options.data ?? {}),
      metadata: clone(options.metadata ?? {}),
      tags: clone(options.tags ?? []),
      createdAt: now,
      updatedAt: now,
      availableAt: now,
      completedAt: null,
      output: null,
      error: null,
      lockOwner: null,
      lockExpiresAt: null,
      journal: [
        {
          type: 'execution.created',
          at: now,
          step: workflow.start,
        },
      ],
      steps: {},
    }

    ensureStepState(execution, workflow.start)
    await this._storage.createExecution(execution)
    this.emit('execution:queued', { execution: clone(execution) })
    return clone(execution)
  }

  async getExecution(id) {
    await this.ready()
    return await this._storage.getExecution(id)
  }

  async listExecutions(filter = {}) {
    await this.ready()
    return await this._storage.listExecutions(filter)
  }

  async cancel(id, reason = 'Canceled') {
    await this.ready()
    const execution = await this._storage.getExecution(id)
    if (!execution) throw new Error(`execution not found: ${id}`)
    if (['succeeded', 'failed', 'canceled'].includes(execution.status)) return clone(execution)

    execution.status = 'canceled'
    execution.error = { name: 'Canceled', message: reason }
    execution.completedAt = Date.now()
    execution.updatedAt = execution.completedAt
    execution.currentStep = null
    execution.lockOwner = null
    execution.lockExpiresAt = null
    execution.journal.push({
      type: 'execution.canceled',
      at: execution.completedAt,
      reason,
    })
    await this._storage.saveExecution(execution)
    this.emit('execution:canceled', { execution: clone(execution) })
    return clone(execution)
  }

  async resume(id) {
    await this.ready()
    const execution = await this._storage.getExecution(id)
    if (!execution) throw new Error(`execution not found: ${id}`)
    if (execution.status !== 'failed') throw new Error('only failed executions can be resumed')
    if (!execution.currentStep) throw new Error('failed execution has no current step to resume')

    execution.status = 'queued'
    execution.availableAt = Date.now()
    execution.updatedAt = execution.availableAt
    execution.error = null
    execution.lockOwner = null
    execution.lockExpiresAt = null
    execution.journal.push({
      type: 'execution.resumed',
      at: execution.availableAt,
      step: execution.currentStep,
    })
    await this._storage.saveExecution(execution)
    this.emit('execution:queued', { execution: clone(execution) })
    return clone(execution)
  }

  async runDue(options = {}) {
    await this.ready()
    const now = Date.now()
    const claimed = await this._storage.claimAvailable({
      now,
      owner: this._owner,
      limit: options.limit ?? this._batchSize,
      leaseMs: this._leaseMs,
    })

    const results = await Promise.allSettled(
      claimed.map((item) => this._processExecution(item.id)),
    )

    return results.length
  }

  async runUntilIdle(options = {}) {
    const maxPasses = options.maxPasses ?? 100

    for (let pass = 0; pass < maxPasses; pass++) {
      const processed = await this.runDue({ limit: options.limit })
      if (processed === 0) return pass
    }

    throw new Error(`runUntilIdle exceeded maxPasses=${maxPasses}`)
  }

  async startWorker(options = {}) {
    await this.ready()
    if (this._pollTimer) throw new Error('worker already started')

    const interval = normalizeMs(options.interval ?? '1s')
    const batchSize = options.batchSize ?? this._batchSize

    const tick = async () => {
      if (this._activePoll) return this._activePoll
      this._activePoll = this.runDue({ limit: batchSize }).finally(() => {
        this._activePoll = null
      })
      return this._activePoll
    }

    this._pollTimer = setInterval(() => {
      tick().catch((error) => this.emit('worker:error', { error }))
    }, interval)
    this._pollTimer.unref?.()

    await tick()
  }

  async close() {
    if (this._pollTimer) clearInterval(this._pollTimer)
    this._pollTimer = null
    await this._activePoll
    if (this._storage.close) await this._storage.close()
  }

  _resolveWorkflow(name, version) {
    if (version) {
      const direct = this._workflows.get(`${name}@${version}`)
      if (!direct) throw new Error(`workflow not registered: ${name}@${version}`)
      return direct
    }

    const matches = Array.from(this._workflows.values()).filter((workflow) => workflow.name === name)
    if (matches.length === 0) throw new Error(`workflow not registered: ${name}`)
    if (matches.length > 1) throw new Error(`workflow version required for "${name}"`)
    return matches[0]
  }

  async _processExecution(id) {
    const execution = await this._storage.getExecution(id)
    if (!execution) return
    if (!['queued', 'waiting'].includes(execution.status)) return
    if (execution.lockOwner !== this._owner) return

    const workflow = this._resolveWorkflow(execution.workflow, execution.workflowVersion)
    const stepName = execution.currentStep
    const definition = workflow.steps[stepName]
    if (!definition) {
      throw new Error(`execution ${id} points to unknown step "${stepName}"`)
    }

    const stepState = ensureStepState(execution, stepName)

    if (stepState.status === 'succeeded') {
      const now = Date.now()
      execution.updatedAt = now
      execution.journal.push({ type: 'step.skipped', at: now, step: stepName, reason: 'already succeeded' })

      if (definition.type === 'activity') {
        execution.currentStep = definition.next
      } else if (definition.type === 'decision') {
        execution.currentStep = definition.transitions[stepState.route]
      } else {
        return
      }

      execution.status = 'queued'
      execution.availableAt = now
      execution.lockOwner = null
      execution.lockExpiresAt = null
      ensureStepState(execution, execution.currentStep)
      await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
      return
    }

    const now = Date.now()
    stepState.status = 'running'
    stepState.attempts += 1
    stepState.startedAt = now
    stepState.endedAt = null
    stepState.error = null
    execution.status = 'running'
    execution.updatedAt = now
    execution.journal.push({
      type: 'step.started',
      at: now,
      step: stepName,
      attempt: stepState.attempts,
    })
    const started = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!started) {
      this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
      return
    }
    this.emit('step:started', { execution: clone(execution), step: stepName, attempt: stepState.attempts })

    const heartbeat = this._startLeaseHeartbeat(execution.id, stepName)

    try {
      const context = stepContext(execution, workflow, stepName)
      const timeoutMs =
        definition.type === 'activity'
          ? normalizeMs(definition.timeout ?? this._defaultActivityTimeout)
          : normalizeMs(definition.timeout ?? 0)

      if (definition.type === 'activity') {
        const output = await withTimeout(
          Promise.resolve(definition.run(context)),
          timeoutMs,
          `Step "${stepName}" timed out after ${timeoutMs}ms`,
        )

        if (output != null && typeof output === 'object' && !Array.isArray(output)) {
          Object.assign(execution.data, output)
        }

        stepState.status = 'succeeded'
        stepState.output = clone(output)
        stepState.endedAt = Date.now()
        execution.updatedAt = stepState.endedAt
        execution.status = 'queued'
        execution.currentStep = definition.next
        execution.availableAt = execution.updatedAt
        execution.lockOwner = null
        execution.lockExpiresAt = null
        execution.journal.push({
          type: 'step.succeeded',
          at: execution.updatedAt,
          step: stepName,
        })
        ensureStepState(execution, definition.next)
        heartbeat.assertHeld()
        const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
        if (!saved) throw new LeaseLostError(execution.id, stepName)
        this.emit('step:succeeded', { execution: clone(execution), step: stepName, output: clone(output) })
        return
      }

      if (definition.type === 'decision') {
        const route = await withTimeout(
          Promise.resolve(definition.decide(context)),
          timeoutMs,
          `Step "${stepName}" timed out after ${timeoutMs}ms`,
        )

        if (!Object.hasOwn(definition.transitions, route)) {
          throw new Error(`decision step "${stepName}" returned unknown route "${route}"`)
        }

        stepState.status = 'succeeded'
        stepState.route = route
        stepState.output = route
        stepState.endedAt = Date.now()
        execution.updatedAt = stepState.endedAt
        execution.status = 'queued'
        execution.currentStep = definition.transitions[route]
        execution.availableAt = execution.updatedAt
        execution.lockOwner = null
        execution.lockExpiresAt = null
        execution.journal.push({
          type: 'step.routed',
          at: execution.updatedAt,
          step: stepName,
          route,
          to: execution.currentStep,
        })
        ensureStepState(execution, execution.currentStep)
        heartbeat.assertHeld()
        const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
        if (!saved) throw new LeaseLostError(execution.id, stepName)
        this.emit('step:routed', {
          execution: clone(execution),
          step: stepName,
          route,
          to: execution.currentStep,
        })
        return
      }

      if (definition.type === 'succeed' || definition.type === 'fail') {
        const output =
          typeof definition.result === 'function'
            ? await Promise.resolve(definition.result(context))
            : clone(definition.result ?? null)

        stepState.status = definition.type === 'succeed' ? 'succeeded' : 'failed'
        stepState.output = definition.type === 'succeed' ? output : null
        stepState.error =
          definition.type === 'fail' ? clone(output ?? { name: 'WorkflowFailed', message: 'Workflow failed' }) : null
        stepState.endedAt = Date.now()
        execution.updatedAt = stepState.endedAt
        execution.status = terminalState(definition.type)
        execution.completedAt = stepState.endedAt
        execution.output = definition.type === 'succeed' ? clone(output) : null
        execution.error = definition.type === 'fail' ? clone(stepState.error) : null
        execution.currentStep = null
        execution.availableAt = null
        execution.lockOwner = null
        execution.lockExpiresAt = null
        execution.journal.push({
          type: `execution.${execution.status}`,
          at: execution.completedAt,
          step: stepName,
        })
        heartbeat.assertHeld()
        const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
        if (!saved) throw new LeaseLostError(execution.id, stepName)
        this.emit(`execution:${execution.status}`, { execution: clone(execution) })
      }
    } catch (error) {
      if (error instanceof LeaseLostError || heartbeat.lost) {
        this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
        return
      }

      const serialized = serializeError(error)
      const retry = definition.retry ?? { maxAttempts: 1, backoff: 0 }
      const endedAt = Date.now()

      stepState.status = 'failed'
      stepState.error = serialized
      stepState.endedAt = endedAt
      execution.updatedAt = endedAt
      execution.lockOwner = null
      execution.lockExpiresAt = null

      if (stepState.attempts < retry.maxAttempts) {
        execution.status = 'waiting'
        execution.availableAt = endedAt + normalizeMs(retry.backoff ?? 0)
        execution.journal.push({
          type: 'step.retry-scheduled',
          at: endedAt,
          step: stepName,
          attempt: stepState.attempts,
          availableAt: execution.availableAt,
          error: serialized,
        })
        const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
        if (!saved) {
          this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
          return
        }
        this.emit('step:retry', {
          execution: clone(execution),
          step: stepName,
          attempt: stepState.attempts,
          error: serialized,
          availableAt: execution.availableAt,
        })
        return
      }

      execution.status = 'failed'
      execution.error = serialized
      execution.completedAt = endedAt
      execution.availableAt = null
      execution.journal.push({
        type: 'execution.failed',
        at: endedAt,
        step: stepName,
        error: serialized,
      })
      const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
      if (!saved) {
        this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
        return
      }
      this.emit('step:failed', {
        execution: clone(execution),
        step: stepName,
        attempt: stepState.attempts,
        error: serialized,
      })
      this.emit('execution:failed', { execution: clone(execution) })
    } finally {
      heartbeat.stop()
    }
  }

  _startLeaseHeartbeat(executionId, stepName) {
    if (!this._storage.renewLease) {
      return {
        lost: false,
        stop() {},
        assertHeld() {},
      }
    }

    let lost = false
    const interval = setInterval(async () => {
      try {
        const renewed = await this._storage.renewLease(executionId, {
          owner: this._owner,
          now: Date.now(),
          leaseMs: this._leaseMs,
        })
        if (!renewed) lost = true
      } catch {
        lost = true
      }
    }, this._leaseRenewInterval)
    interval.unref?.()

    return {
      get lost() {
        return lost
      },
      stop() {
        clearInterval(interval)
      },
      assertHeld() {
        if (lost) throw new LeaseLostError(executionId, stepName)
      },
    }
  }
}
