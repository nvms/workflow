function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value))
}

export function memoryDriver() {
  const executions = new Map()

  return {
    async init() {},

    async createExecution(execution) {
      executions.set(execution.id, clone(execution))
    },

    async getExecution(id) {
      const execution = executions.get(id)
      return execution ? clone(execution) : null
    },

    async saveExecution(execution, options = {}) {
      const current = executions.get(execution.id)
      if (!current) return null
      if (options.expectedLockOwner != null && current.lockOwner !== options.expectedLockOwner) {
        return null
      }
      executions.set(execution.id, clone(execution))
      return clone(execution)
    },

    async claimAvailable({ now, owner, limit = 10, leaseMs }) {
      const claimed = []

      for (const execution of executions.values()) {
        if (claimed.length >= limit) break
        if (!['queued', 'waiting'].includes(execution.status)) continue
        if ((execution.availableAt ?? 0) > now) continue
        if (execution.lockOwner && (execution.lockExpiresAt ?? 0) > now) continue

        execution.lockOwner = owner
        execution.lockExpiresAt = now + leaseMs
        execution.updatedAt = now
        claimed.push(clone(execution))
      }

      return claimed
    },

    async renewLease(id, { now, owner, leaseMs }) {
      const execution = executions.get(id)
      if (!execution) return false
      if (execution.lockOwner !== owner) return false
      if ((execution.lockExpiresAt ?? 0) <= now) return false

      execution.lockExpiresAt = now + leaseMs
      execution.updatedAt = now
      return true
    },

    async listExecutions(filter = {}) {
      let values = Array.from(executions.values())

      if (filter.workflow) values = values.filter((execution) => execution.workflow === filter.workflow)
      if (filter.status) values = values.filter((execution) => execution.status === filter.status)

      values.sort((a, b) => b.createdAt - a.createdAt)

      if (filter.limit) values = values.slice(0, filter.limit)
      return values.map(clone)
    },

    async close() {},
  }
}
