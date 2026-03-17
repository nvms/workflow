<p align="center">
  <img src=".github/logo.svg" width="80" height="80" alt="workflow logo">
</p>

<h1 align="center">@prsm/workflow</h1>

Durable workflow engine with explicit steps, persisted execution state, retries, and inspectable history.

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
import WorkflowEngine, { defineWorkflow, memoryDriver } from '@prsm/workflow'

const workflow = defineWorkflow({
  name: 'review',
  version: '1',
  start: 'fetch',
  steps: {
    fetch: {
      type: 'activity',
      next: 'route',
      retry: { maxAttempts: 3, backoff: '5s' },
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

const engine = new WorkflowEngine({
  storage: memoryDriver(),
})

engine.register(workflow)

const execution = await engine.start('review', { message: '  hello  ' })
await engine.runUntilIdle()

console.log((await engine.getExecution(execution.id)).status)
// succeeded
```

In that example:

- `engine.start('review', input)` creates a workflow execution
- `engine.runUntilIdle()` processes ready executions until nothing is immediately runnable

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
  transitions: {
    approved: "publish",
    rejected: "reject",
  },
  decide: ({ data, steps }) => "approved",
}
```

The chosen route is persisted in the step state and execution journal.

If `decide()` returns a route name that is not in `transitions`, the step fails and normal retry/failure rules apply.

## Engine API

```js
const engine = new WorkflowEngine({
  storage, // defaults to in-memory
  leaseMs: '5m', // how long a worker owns an execution before another worker may reclaim it
  leaseRenewInterval: '100s', // how often the lease is renewed while a step is running (defaults to leaseMs / 3)
  defaultActivityTimeout: '30s',
  owner: 'node-a', // worker identity written into claims and checked on save
  batchSize: 10, // max executions to claim and process concurrently per polling cycle
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

Typical project layout:

```
workflows/review.js   - defineWorkflow()
workflow/engine.js    - engine setup + startWorker()
routes/reviews.js     - HTTP routes that call engine.start()
server.js             - express app entry point
```

Engine setup - create the engine once, register workflows, start the worker:

```js
// workflow/engine.js
import WorkflowEngine from '@prsm/workflow'
import { postgresDriver } from '@prsm/workflow/postgres'
import { reviewWorkflow } from '../workflows/review.js'

export const engine = new WorkflowEngine({
  storage: postgresDriver({ connectionString: process.env.DATABASE_URL }),
  owner: `api-${process.pid}`,
})

engine.register(reviewWorkflow)

await engine.startWorker()
```

Then trigger workflows from routes, jobs, or anywhere else:

```js
// routes/reviews.js
const execution = await engine.start('review', { message: req.body.message })
res.json({ executionId: execution.id, status: execution.status })
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

Workflow definitions expose a serializable graph:

```js
const graph = engine.describe('review')
```

That graph includes nodes and edges, and is intended for future visualization/devtools layers.
Use it if you want to render the workflow shape in your own UI, inspect it programmatically, or build devtools on top of it.

## Events

```js
engine.on('execution:queued', ({ execution }) => {})
engine.on('execution:succeeded', ({ execution }) => {})
engine.on('execution:failed', ({ execution }) => {})
engine.on('execution:canceled', ({ execution }) => {})

engine.on('step:started', ({ execution, step, attempt }) => {})
engine.on('step:succeeded', ({ execution, step, output }) => {})
engine.on('step:routed', ({ execution, step, route, to }) => {})
engine.on('step:retry', ({ execution, step, attempt, error, availableAt }) => {})
engine.on('step:failed', ({ execution, step, attempt, error }) => {})
engine.on('execution:lease-lost', ({ executionId, step }) => {})
```

## How It Works

Workflow graphs are versioned and static. `activity` steps run code and move to one `next` step. `decision` steps choose one named route from a fixed map. Terminal steps end the execution.

Workers claim ready executions using a lease and process them concurrently (up to `batchSize` per polling cycle). While a step is running, the worker renews that lease. If the lease expires, another worker can reclaim the execution. Saves are guarded by lock owner so a stale worker cannot commit after losing ownership.

The execution model is at-least-once. Activity handlers must be idempotent.

That means a step may run again after timeout, crash, or lease loss, so external side effects must be safe to repeat with the same idempotency key.

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
