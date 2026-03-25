import { EventEmitter } from 'events'
import { randomUUID } from 'crypto'
import ms from '@prsm/ms'
import { memoryDriver } from './memoryDriver.js'
import { clone } from './util.js'

const DEFAULT_RETRY = { maxAttempts: 1, backoff: 0 }
const TERMINAL_EXECUTION_STATUSES = new Set(['succeeded', 'failed', 'canceled'])
const PROCESSABLE_EXECUTION_STATUSES = new Set(['queued', 'waiting'])

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
  const stepState = execution.steps[stepName]

  return {
    execution: clone(execution),
    workflow: {
      name: workflow.name,
      version: workflow.version,
    },
    input: clone(execution.input),
    data: clone(execution.data),
    metadata: clone(execution.metadata),
    steps: clone(execution.steps),
    step: {
      name: stepName,
      type: currentStep.type,
      attempt: stepState?.attempts ?? 0,
      idempotencyKey: stepState?.idempotencyKey ?? `${execution.id}:${stepName}`,
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
    this._maxJournalEntries = options.maxJournalEntries ?? 0
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
    const execution = this._createExecution(workflow, input, options)

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
    if (TERMINAL_EXECUTION_STATUSES.has(execution.status)) return clone(execution)

    const completedAt = Date.now()
    execution.status = 'canceled'
    execution.error = { name: 'Canceled', message: reason }
    execution.completedAt = completedAt
    execution.updatedAt = completedAt
    execution.currentStep = null
    this._releaseLock(execution)
    execution.journal.push({
      type: 'execution.canceled',
      at: completedAt,
      reason,
    })

    this._trimJournal(execution)
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

    const queuedAt = Date.now()
    execution.status = 'queued'
    execution.availableAt = queuedAt
    execution.updatedAt = queuedAt
    execution.error = null
    this._releaseLock(execution)
    execution.journal.push({
      type: 'execution.resumed',
      at: queuedAt,
      step: execution.currentStep,
    })

    this._trimJournal(execution)
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

    const results = await Promise.allSettled(claimed.map(({ id }) => this._processExecution(id)))
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

  _createExecution(workflow, input, options) {
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
    return execution
  }

  _resolveWorkflow(name, version) {
    if (version) {
      const workflow = this._workflows.get(`${name}@${version}`)
      if (!workflow) throw new Error(`workflow not registered: ${name}@${version}`)
      return workflow
    }

    const matches = Array.from(this._workflows.values()).filter((workflow) => workflow.name === name)
    if (matches.length === 0) throw new Error(`workflow not registered: ${name}`)
    if (matches.length > 1) throw new Error(`workflow version required for "${name}"`)
    return matches[0]
  }

  _trimJournal(execution) {
    if (this._maxJournalEntries > 0 && execution.journal.length > this._maxJournalEntries) {
      execution.journal = execution.journal.slice(-this._maxJournalEntries)
    }
  }

  _releaseLock(execution) {
    execution.lockOwner = null
    execution.lockExpiresAt = null
  }

  _setNextQueuedStep(execution, nextStep, at) {
    execution.status = 'queued'
    execution.currentStep = nextStep
    execution.availableAt = at
    execution.updatedAt = at
    this._releaseLock(execution)
    ensureStepState(execution, nextStep)
  }

  _beginStepRun(execution, stepName, stepState) {
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
  }

  async _skipSucceededStep(execution, definition, stepName, stepState) {
    let nextStep
    if (definition.type === 'activity') nextStep = definition.next
    else if (definition.type === 'decision') nextStep = definition.transitions[stepState.route]

    if (!nextStep) {
      throw new Error(`cannot skip step "${stepName}" (type=${definition.type}): no next step resolvable`)
    }

    const now = Date.now()
    execution.updatedAt = now
    execution.journal.push({
      type: 'step.skipped',
      at: now,
      step: stepName,
      reason: 'already succeeded',
    })
    this._setNextQueuedStep(execution, nextStep, now)

    this._trimJournal(execution)
    await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
  }

  async _completeActivityStep(execution, definition, stepName, stepState, output, heartbeat) {
    if (output != null && typeof output === 'object' && !Array.isArray(output)) {
      Object.assign(execution.data, output)
    }

    const endedAt = Date.now()
    stepState.status = 'succeeded'
    const clonedOutput = clone(output) ?? null
    stepState.output = clonedOutput
    stepState.endedAt = endedAt

    this._setNextQueuedStep(execution, definition.next, endedAt)
    execution.journal.push({
      type: 'step.succeeded',
      at: endedAt,
      step: stepName,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit('step:succeeded', { execution: clone(execution), step: stepName, output: clonedOutput })
  }

  async _completeDecisionStep(execution, definition, stepName, stepState, route, heartbeat) {
    if (!Object.hasOwn(definition.transitions, route)) {
      throw new Error(`decision step "${stepName}" returned unknown route "${route}"`)
    }

    const endedAt = Date.now()
    stepState.status = 'succeeded'
    stepState.route = route
    stepState.output = route
    stepState.endedAt = endedAt

    const nextStep = definition.transitions[route]
    this._setNextQueuedStep(execution, nextStep, endedAt)
    execution.journal.push({
      type: 'step.routed',
      at: endedAt,
      step: stepName,
      route,
      to: nextStep,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit('step:routed', {
      execution: clone(execution),
      step: stepName,
      route,
      to: execution.currentStep,
    })
  }

  async _completeTerminalStep(execution, definition, stepName, stepState, output, heartbeat) {
    const endedAt = Date.now()
    const status = terminalState(definition.type)
    const error =
      definition.type === 'fail' ? clone(output ?? { name: 'WorkflowFailed', message: 'Workflow failed' }) : null

    stepState.status = definition.type === 'succeed' ? 'succeeded' : 'failed'
    stepState.output = definition.type === 'succeed' ? output : null
    stepState.error = error
    stepState.endedAt = endedAt

    execution.updatedAt = endedAt
    execution.status = status
    execution.completedAt = endedAt
    execution.output = definition.type === 'succeed' ? clone(output) : null
    execution.error = error
    execution.currentStep = null
    execution.availableAt = null
    this._releaseLock(execution)
    execution.journal.push({
      type: `execution.${status}`,
      at: endedAt,
      step: stepName,
    })

    heartbeat.assertHeld()
    this._trimJournal(execution)
    const saved = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!saved) throw new LeaseLostError(execution.id, stepName)

    this.emit(`execution:${status}`, { execution: clone(execution) })
  }

  async _handleStepFailure(execution, definition, stepName, stepState, error, heartbeat) {
    if (error instanceof LeaseLostError || heartbeat.lost) {
      this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
      return
    }

    const serialized = serializeError(error)
    const retry = definition.retry ?? DEFAULT_RETRY
    const endedAt = Date.now()

    stepState.status = 'failed'
    stepState.error = serialized
    stepState.endedAt = endedAt

    execution.updatedAt = endedAt
    this._releaseLock(execution)

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

      this._trimJournal(execution)
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

    this._trimJournal(execution)
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
  }

  async _processExecution(id) {
    const execution = await this._storage.getExecution(id)
    if (!execution) return
    if (!PROCESSABLE_EXECUTION_STATUSES.has(execution.status)) return
    if (execution.lockOwner !== this._owner) return

    const workflow = this._resolveWorkflow(execution.workflow, execution.workflowVersion)
    const stepName = execution.currentStep
    const definition = workflow.steps[stepName]
    if (!definition) throw new Error(`execution ${id} points to unknown step "${stepName}"`)

    const stepState = ensureStepState(execution, stepName)
    if (stepState.status === 'succeeded') {
      await this._skipSucceededStep(execution, definition, stepName, stepState)
      return
    }

    this._beginStepRun(execution, stepName, stepState)
    this._trimJournal(execution)
    const started = await this._storage.saveExecution(execution, { expectedLockOwner: this._owner })
    if (!started) {
      this.emit('execution:lease-lost', { executionId: execution.id, step: stepName })
      return
    }

    this.emit('step:started', { execution: clone(execution), step: stepName, attempt: stepState.attempts })

    const heartbeat = this._startLeaseHeartbeat(execution.id, stepName)

    try {
      const context = stepContext(execution, workflow, stepName)
      const timeoutMs = normalizeMs(definition.timeout ?? (definition.type === 'activity' ? this._defaultActivityTimeout : 0))

      if (definition.type === 'activity') {
        const output = await withTimeout(
          Promise.resolve(definition.run(context)),
          timeoutMs,
          `Step "${stepName}" timed out after ${timeoutMs}ms`,
        )

        await this._completeActivityStep(execution, definition, stepName, stepState, output, heartbeat)
        return
      }

      if (definition.type === 'decision') {
        const route = await withTimeout(
          Promise.resolve(definition.decide(context)),
          timeoutMs,
          `Step "${stepName}" timed out after ${timeoutMs}ms`,
        )

        await this._completeDecisionStep(execution, definition, stepName, stepState, route, heartbeat)
        return
      }

      if (definition.type === 'succeed' || definition.type === 'fail') {
        const output = typeof definition.result === 'function'
          ? await withTimeout(
              Promise.resolve(definition.result(context)),
              timeoutMs,
              `Step "${stepName}" timed out after ${timeoutMs}ms`,
            )
          : clone(definition.result ?? null)
        await this._completeTerminalStep(execution, definition, stepName, stepState, output, heartbeat)
      }
    } catch (error) {
      await this._handleStepFailure(execution, definition, stepName, stepState, error, heartbeat)
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
