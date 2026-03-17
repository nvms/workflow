import sqlite3 from 'sqlite3'

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value))
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) return reject(err)
      resolve(this)
    })
  })
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) return reject(err)
      resolve(row ?? null)
    })
  })
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err)
      resolve(rows ?? [])
    })
  })
}

export function sqliteDriver({ filename }) {
  if (!filename) throw new Error('sqlite filename is required')

  const db = new sqlite3.Database(filename)
  const init = (async () => {
    await run(
      db,
      `CREATE TABLE IF NOT EXISTS workflow_executions (
        id TEXT PRIMARY KEY,
        workflow TEXT NOT NULL,
        status TEXT NOT NULL,
        available_at INTEGER NOT NULL,
        lock_owner TEXT,
        lock_expires_at INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        body TEXT NOT NULL
      )`,
    )
    await run(
      db,
      `CREATE INDEX IF NOT EXISTS workflow_executions_claim_idx ON workflow_executions(status, available_at, lock_expires_at)`,
    )
    await run(
      db,
      `CREATE INDEX IF NOT EXISTS workflow_executions_workflow_idx ON workflow_executions(workflow, created_at)`,
    )
  })()

  return {
    async init() {
      await init
    },

    async createExecution(execution) {
      await init
      await run(
        db,
        `INSERT INTO workflow_executions (
          id, workflow, status, available_at, lock_owner, lock_expires_at, created_at, updated_at, body
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          execution.id,
          execution.workflow,
          execution.status,
          execution.availableAt ?? 0,
          execution.lockOwner ?? null,
          execution.lockExpiresAt ?? null,
          execution.createdAt,
          execution.updatedAt,
          JSON.stringify(execution),
        ],
      )
    },

    async getExecution(id) {
      await init
      const row = await get(db, `SELECT body FROM workflow_executions WHERE id = ?`, [id])
      return row ? JSON.parse(row.body) : null
    },

    async saveExecution(execution, options = {}) {
      await init
      const sql =
        options.expectedLockOwner != null
          ? `UPDATE workflow_executions
           SET workflow = ?, status = ?, available_at = ?, lock_owner = ?, lock_expires_at = ?, updated_at = ?, body = ?
           WHERE id = ? AND lock_owner = ?`
          : `UPDATE workflow_executions
           SET workflow = ?, status = ?, available_at = ?, lock_owner = ?, lock_expires_at = ?, updated_at = ?, body = ?
           WHERE id = ?`
      const params = [
        execution.workflow,
        execution.status,
        execution.availableAt ?? 0,
        execution.lockOwner ?? null,
        execution.lockExpiresAt ?? null,
        execution.updatedAt,
        JSON.stringify(execution),
        execution.id,
      ]
      if (options.expectedLockOwner != null) params.push(options.expectedLockOwner)

      const result = await run(db, sql, params)
      if (options.expectedLockOwner != null && result.changes === 0) return null
      return clone(execution)
    },

    async claimAvailable({ now, owner, limit = 10, leaseMs }) {
      await init
      await run(db, 'BEGIN IMMEDIATE TRANSACTION')
      try {
        const rows = await all(
          db,
          `SELECT id
           FROM workflow_executions
           WHERE status IN ('queued', 'waiting')
             AND available_at <= ?
             AND (lock_owner IS NULL OR lock_expires_at IS NULL OR lock_expires_at <= ?)
           ORDER BY available_at ASC, created_at ASC
           LIMIT ?`,
          [now, now, limit],
        )

        const claimed = []
        for (const row of rows) {
          const claimedRow = await get(db, `SELECT body FROM workflow_executions WHERE id = ?`, [row.id])
          if (!claimedRow) continue
          const execution = JSON.parse(claimedRow.body)
          execution.lockOwner = owner
          execution.lockExpiresAt = now + leaseMs
          execution.updatedAt = now

          await run(
            db,
            `UPDATE workflow_executions
             SET lock_owner = ?, lock_expires_at = ?, updated_at = ?, body = ?
             WHERE id = ?`,
            [owner, now + leaseMs, now, JSON.stringify(execution), row.id],
          )
          claimed.push(execution)
        }

        await run(db, 'COMMIT')
        return claimed
      } catch (error) {
        await run(db, 'ROLLBACK').catch(() => {})
        throw error
      }
    },

    async renewLease(id, { now, owner, leaseMs }) {
      await init
      const row = await get(
        db,
        `SELECT body FROM workflow_executions
         WHERE id = ? AND lock_owner = ? AND lock_expires_at > ?`,
        [id, owner, now],
      )
      if (!row) return false

      const execution = JSON.parse(row.body)
      execution.lockOwner = owner
      execution.lockExpiresAt = now + leaseMs
      execution.updatedAt = now

      const result = await run(
        db,
        `UPDATE workflow_executions
         SET lock_expires_at = ?, updated_at = ?, body = ?
         WHERE id = ? AND lock_owner = ? AND lock_expires_at > ?`,
        [execution.lockExpiresAt, now, JSON.stringify(execution), id, owner, now],
      )

      return result.changes > 0
    },

    async listExecutions(filter = {}) {
      await init

      const clauses = []
      const params = []

      if (filter.workflow) {
        clauses.push('workflow = ?')
        params.push(filter.workflow)
      }

      if (filter.status) {
        clauses.push('status = ?')
        params.push(filter.status)
      }

      const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''
      const limit = filter.limit ? `LIMIT ${Number(filter.limit)}` : ''
      const rows = await all(
        db,
        `SELECT body FROM workflow_executions ${where} ORDER BY created_at DESC ${limit}`,
        params,
      )

      return rows.map((row) => JSON.parse(row.body))
    },

    async close() {
      await init
      return new Promise((resolve, reject) => {
        db.close((err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    },
  }
}
