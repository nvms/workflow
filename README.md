<p align="center">
  <img src=".github/logo.svg" width="80" height="80" alt="workflow logo">
</p>

<h1 align="center">@prsm/workflow</h1>

Durable workflow engine backed by Postgres or SQLite. Define a workflow as a graph of steps (activities, decisions, terminal states) and the engine persists every step's result, picks up where it left off if a worker dies, and keeps a full journal of what happened. Multiple workers can process executions concurrently with lease-based coordination.

## Installation

```bash
npm install @prsm/workflow
```

Optional storage adapters:

```bash
npm install sqlite3
npm install pg
```

## Quick Start

```js
import WorkflowEngine, { defineWorkflow } from '@prsm/workflow'

const workflow = defineWorkflow({
  name: 'review',
  version: '1',
  start: 'fetch',
  steps: {
    fetch: {
      type: 'activity',
      next: 'route',
      retry: { maxAttempts: 3, backoff: '5s' },
      run: async ({ input }) => ({ message: input.message.trim() }),
    },
    route: {
      type: 'decision',
      transitions: { spam: 'reject', normal: 'complete' },
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

const engine = new WorkflowEngine()
engine.register(workflow)

const execution = await engine.start('review', { message: '  hello  ' })
await engine.runUntilIdle()

const result = await engine.getExecution(execution.id)
console.log(result.status) // "succeeded"
console.log(result.output) // { outcome: "sent", message: "hello" }
```

## Step Types

Four step types:

| Type       | Purpose                                                     |
| ---------- | ----------------------------------------------------------- |
| `activity` | Run application code, then move to one explicit `next` step |
| `decision` | Choose one route from a fixed `transitions` map             |
| `succeed`  | End the workflow successfully                               |
| `fail`     | End the workflow with a failure                             |

### Activity

```js
{
  type: "activity",
  next: "publish",
  timeout: "30s",
  retry: { maxAttempts: 3, backoff: "5s" },
  run: async ({ input, data, steps, step }) => {
    console.log(step.idempotencyKey)
    return await doWork(input)
  },
}
```

### Decision

```js
{
  type: "decision",
  timeout: "10s",
  transitions: {
    approved: "publish",
    rejected: "reject",
  },
  decide: ({ data, steps }) => "approved",
}
```

The chosen route is persisted in the step state and execution journal.

If `decide()` returns a route name that is not in `transitions`, the step fails and normal retry/failure rules apply.

Decision steps have no default timeout. If your `decide()` function calls external services, set `timeout` explicitly or a hanging call will block the worker indefinitely. Activity steps default to `defaultActivityTimeout` (30s); decisions and terminal steps default to no timeout unless you set one.

## Engine API

```js
const engine = new WorkflowEngine({
  storage, // defaults to in-memory
  leaseMs: '5m', // how long a worker owns an execution before another worker may reclaim it
  leaseRenewInterval: '100s', // how often the lease is renewed while a step is running (defaults to leaseMs / 3)
  defaultActivityTimeout: '30s',
  owner: 'node-a', // worker identity written into claims and checked on save
  batchSize: 10, // max executions to claim and process concurrently per polling cycle
  maxJournalEntries: 100, // cap journal size, 0 = unlimited (default)
})
```

Core methods:

```js
engine.register(workflow)
await engine.start(name, input, options?)
await engine.runDue(options?)
await engine.runUntilIdle(options?)
await engine.startWorker(options?)
await engine.getExecution(id)
await engine.listExecutions(filter?)
await engine.cancel(id, reason?)
await engine.resume(id)
engine.describe(name, version?)
```

## Triggering Workflows

| Method               | What it does                                                  |
| -------------------- | ------------------------------------------------------------- |
| `start(name, input)` | Create a persisted execution                                  |
| `runDue()`           | Process ready executions once, then return                    |
| `runUntilIdle()`     | Keep calling `runDue()` until nothing is immediately runnable |
| `startWorker()`      | Poll forever on a timer, calling `runDue()` each interval     |

`runUntilIdle()` is useful in tests and scripts. `startWorker()` is what you use in production.

### In a Real App

Create the engine once, register workflows, start the worker loop. Trigger workflows from routes, jobs, or anywhere else.

```js
// workflows/engine.js
import WorkflowEngine from '@prsm/workflow'
import { postgresDriver } from '@prsm/workflow/postgres'
import { triageWorkflow } from './triage.js'

export const engine = new WorkflowEngine({
  storage: postgresDriver({ connectionString: process.env.DATABASE_URL }),
  owner: `worker-${process.pid}`,
  leaseMs: '30s',
})

engine.register(triageWorkflow)

engine.on('execution:succeeded', ({ execution }) => {
  console.log('done:', execution.id, execution.output)
})

engine.on('execution:failed', ({ execution }) => {
  console.error('failed:', execution.id, execution.error)
})

await engine.startWorker({ interval: '100ms' })
```

```js
// routes/tickets.js
import { engine } from '../workflows/engine.js'

router.post('/', async (req, res) => {
  const execution = await engine.start('triage', {
    subject: req.body.subject,
    message: req.body.message,
  })
  res.json({ executionId: execution.id })
})
```

## Storage

### Memory

For tests and local prototypes:

```js
import { memoryDriver } from '@prsm/workflow'
```

### SQLite

Single-node durable storage:

```js
import WorkflowEngine from '@prsm/workflow'
import { sqliteDriver } from '@prsm/workflow/sqlite'

const engine = new WorkflowEngine({
  storage: sqliteDriver({ filename: './workflow.db' }),
})
```

### Postgres

Shared storage for distributed workers:

```js
import WorkflowEngine from '@prsm/workflow'
import { postgresDriver } from '@prsm/workflow/postgres'

const engine = new WorkflowEngine({
  storage: postgresDriver({
    connectionString: process.env.DATABASE_URL,
  }),
})
```

The Postgres adapter uses atomic claiming and owner-guarded saves so only one worker can acquire a ready execution, and a stale worker cannot overwrite a newer claimant's state.

## Execution State

Each execution stores:

- `status`: `queued | waiting | running | succeeded | failed | canceled`
- `currentStep`
- `steps[stepName]` - persisted state for each step
- `journal` - timeline of what happened during the execution
- final `output` or `error`

Each step stores:

- `status`
- `attempts`
- `output`
- `error`
- `startedAt`
- `endedAt`
- `route`
- `idempotencyKey`

Step idempotency keys are stable per execution:

```txt
<executionId>:<stepName>
```

Use them for external side effects.

## Graph Introspection

Workflow definitions expose a serializable graph of nodes and edges:

```js
const { graph } = engine.describe('review')
// graph.nodes - step name, type, label, retry config, timeout
// graph.edges - from, to, label (route name for decisions)
```

[@prsm/devtools](https://github.com/nvms/devtools) uses this to render live workflow visualization alongside execution state, journals, and step output:

<p align="center">
  <img src="https://raw.githubusercontent.com/nvms/devtools/main/.github/executions.png" width="800" alt="workflow execution view in @prsm/devtools">
</p>

## Events

Execution lifecycle - use these to sync external state (update your database, push to websockets, trigger alerts):

```js
engine.on('execution:queued', ({ execution }) => {})
engine.on('execution:succeeded', ({ execution }) => {})
engine.on('execution:failed', ({ execution }) => {})
engine.on('execution:canceled', ({ execution }) => {})
```

Step lifecycle - use these for logging, metrics, or debugging:

```js
engine.on('step:started', ({ execution, step, attempt }) => {})
engine.on('step:succeeded', ({ execution, step, output }) => {})
engine.on('step:routed', ({ execution, step, route, to }) => {})
engine.on('step:retry', ({ execution, step, attempt, error, availableAt }) => {})
engine.on('step:failed', ({ execution, step, attempt, error }) => {})
```

Worker health:

```js
engine.on('execution:lease-lost', ({ executionId, step }) => {})
engine.on('worker:error', ({ error }) => {})
```

## Data Merging

When an activity step returns a plain object, it is shallow-merged into `execution.data` via `Object.assign`. Subsequent steps see the merged result in `context.data`. Arrays, primitives, `null`, and `undefined` are stored in `step.output` but not merged.

All context fields passed to step handlers are cloned - mutations inside a handler do not affect stored execution state.

## Resuming Failed Executions

`engine.resume(id)` re-queues a failed execution from the step that failed. The step's attempt counter is not reset - resume gives the step exactly one more execution attempt. If that attempt fails and the retry budget is already exhausted, the execution fails immediately.

## Journal

Every step start, completion, retry, routing decision, and terminal event is appended to `execution.journal`. By default the journal is unbounded. Set `maxJournalEntries` to cap it:

```js
const engine = new WorkflowEngine({
  maxJournalEntries: 100, // keeps the most recent 100 entries
})
```

When the limit is reached, older entries are dropped.

## How It Works

Workflow graphs are versioned and static. `activity` steps run code and move to one `next` step. `decision` steps choose one named route from a fixed map. Terminal steps end the execution.

Workers claim ready executions using a lease and process them concurrently (up to `batchSize` per polling cycle). While a step is running, the worker renews that lease. If the lease expires, another worker can reclaim the execution. Saves are guarded by lock owner so a stale worker cannot commit after losing ownership.

### Completed steps are never re-executed

If a worker completes a step but crashes before advancing to the next one, another worker will reclaim the execution. The engine checks whether the current step already succeeded and skips it, advancing directly to the next step without re-running the handler. This prevents duplicate side effects like double-charging a credit card or sending an email twice.

For side effects that happen *within* a step (e.g. the step sends an email, then crashes before the step result is saved), each step exposes a stable `idempotencyKey` (`<executionId>:<stepName>`) that you can pass to external services for deduplication.

## Distributed Tests

The package includes integration tests for:

- atomic claiming across competing Postgres workers
- recovery after stale lease expiry
- lease renewal for long-running steps

Run them with:

```bash
make up
npm test
make down
```

## Development

```bash
make install
make test
make types
```

## License

MIT
