function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value))
}

export function postgresDriver(options = {}) {
  const opts = {
    host: 'localhost',
    port: 5432,
    database: 'workflow_test',
    user: 'workflow',
    password: 'workflow_password',
    max: 10,
    ...options,
  }

  let pool = null
  let initialized = false

  async function query(text, params = []) {
    if (!pool) throw new Error('Postgres driver not initialized')
    return await pool.query(text, params)
  }

  async function createTables() {
    await query(`
      CREATE TABLE IF NOT EXISTS workflow_executions (
        id TEXT PRIMARY KEY,
        workflow TEXT NOT NULL,
        status TEXT NOT NULL,
        available_at BIGINT,
        lock_owner TEXT,
        lock_expires_at BIGINT,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        body JSONB NOT NULL
      )
    `)
    await query(
      `CREATE INDEX IF NOT EXISTS workflow_executions_claim_idx ON workflow_executions(status, available_at, lock_expires_at)`,
    )
    await query(
      `CREATE INDEX IF NOT EXISTS workflow_executions_workflow_idx ON workflow_executions(workflow, created_at)`,
    )
  }

  return {
    async init() {
      if (initialized) return
      const pg = await import('pg')
      const Pool = pg.default?.Pool || pg.Pool
      pool = new Pool(
        opts.connectionString
          ? { connectionString: opts.connectionString, max: opts.max, ssl: opts.ssl }
          : {
              host: opts.host,
              port: opts.port,
              database: opts.database,
              user: opts.user,
              password: opts.password,
              ssl: opts.ssl,
              max: opts.max,
            },
      )
      await createTables()
      initialized = true
    },

    async createExecution(execution) {
      await this.init()
      await query(
        `INSERT INTO workflow_executions (
          id, workflow, status, available_at, lock_owner, lock_expires_at, created_at, updated_at, body
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
        [
          execution.id,
          execution.workflow,
          execution.status,
          execution.availableAt ?? null,
          execution.lockOwner ?? null,
          execution.lockExpiresAt ?? null,
          execution.createdAt,
          execution.updatedAt,
          JSON.stringify(execution),
        ],
      )
    },

    async getExecution(id) {
      await this.init()
      const result = await query(`SELECT body FROM workflow_executions WHERE id = $1`, [id])
      return result.rows[0] ? result.rows[0].body : null
    },

    async saveExecution(execution, options = {}) {
      await this.init()

      const sql =
        options.expectedLockOwner != null
          ? `UPDATE workflow_executions
           SET workflow = $1, status = $2, available_at = $3, lock_owner = $4, lock_expires_at = $5, updated_at = $6, body = $7::jsonb
           WHERE id = $8 AND lock_owner = $9`
          : `UPDATE workflow_executions
           SET workflow = $1, status = $2, available_at = $3, lock_owner = $4, lock_expires_at = $5, updated_at = $6, body = $7::jsonb
           WHERE id = $8`

      const params = [
        execution.workflow,
        execution.status,
        execution.availableAt ?? null,
        execution.lockOwner ?? null,
        execution.lockExpiresAt ?? null,
        execution.updatedAt,
        JSON.stringify(execution),
        execution.id,
      ]

      if (options.expectedLockOwner != null) params.push(options.expectedLockOwner)

      const result = await query(sql, params)
      if (options.expectedLockOwner != null && result.rowCount === 0) return null
      return clone(execution)
    },

    async claimAvailable({ now, owner, limit = 10, leaseMs }) {
      await this.init()
      const client = await pool.connect()
      try {
        await client.query('BEGIN')

        const selected = await client.query(
          `SELECT id, body
           FROM workflow_executions
           WHERE status IN ('queued', 'waiting')
             AND available_at <= $1
             AND (lock_owner IS NULL OR lock_expires_at IS NULL OR lock_expires_at <= $1)
           ORDER BY available_at ASC, created_at ASC
           FOR UPDATE SKIP LOCKED
           LIMIT $2`,
          [now, limit],
        )

        const claimed = []
        for (const row of selected.rows) {
          const execution = row.body
          execution.lockOwner = owner
          execution.lockExpiresAt = now + leaseMs
          execution.updatedAt = now

          await client.query(
            `UPDATE workflow_executions
             SET lock_owner = $1, lock_expires_at = $2, updated_at = $3, body = $4::jsonb
             WHERE id = $5`,
            [owner, now + leaseMs, now, JSON.stringify(execution), row.id],
          )

          claimed.push(execution)
        }

        await client.query('COMMIT')
        return claimed
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {})
        throw error
      } finally {
        client.release()
      }
    },

    async renewLease(id, { now, owner, leaseMs }) {
      await this.init()
      const result = await query(
        `UPDATE workflow_executions
         SET lock_expires_at = $1,
             updated_at = $2,
             body = jsonb_set(
               jsonb_set(body, '{lockExpiresAt}', to_jsonb($1::bigint), true),
               '{updatedAt}', to_jsonb($2::bigint), true
             )
         WHERE id = $3
           AND lock_owner = $4
           AND lock_expires_at > $2`,
        [now + leaseMs, now, id, owner],
      )
      return result.rowCount > 0
    },

    async listExecutions(filter = {}) {
      await this.init()

      const clauses = []
      const params = []
      let index = 1

      if (filter.workflow) {
        clauses.push(`workflow = $${index++}`)
        params.push(filter.workflow)
      }

      if (filter.status) {
        clauses.push(`status = $${index++}`)
        params.push(filter.status)
      }

      const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''
      const limit = filter.limit ? `LIMIT ${Number(filter.limit)}` : ''
      const result = await query(
        `SELECT body FROM workflow_executions ${where} ORDER BY created_at DESC ${limit}`,
        params,
      )
      return result.rows.map((row) => row.body)
    },

    async close() {
      if (!pool) return
      await pool.end()
      pool = null
      initialized = false
    },
  }
}
