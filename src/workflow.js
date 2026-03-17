function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value))
}

function normalizeRetry(retry) {
  if (retry == null) return { maxAttempts: 1, backoff: 0 }
  if (typeof retry === 'number') return { maxAttempts: retry, backoff: 0 }

  return {
    maxAttempts: retry.maxAttempts ?? 1,
    backoff: retry.backoff ?? 0,
  }
}

function inferEdgeLabels(step) {
  if (step.type === 'decision') {
    return Object.entries(step.transitions).map(([label, to]) => ({
      from: step.name,
      to,
      label,
    }))
  }

  if (step.type === 'activity' && step.next) {
    return [{ from: step.name, to: step.next, label: 'next' }]
  }

  return []
}

function collectReachable(start, steps) {
  const seen = new Set()
  const visiting = new Set()

  function visit(stepName) {
    if (visiting.has(stepName)) {
      throw new Error(`workflow graph must be acyclic; cycle detected at "${stepName}"`)
    }
    if (seen.has(stepName)) return
    const step = steps[stepName]
    if (!step) throw new Error(`unknown step referenced: "${stepName}"`)

    visiting.add(stepName)
    for (const edge of inferEdgeLabels(step)) {
      visit(edge.to)
    }
    visiting.delete(stepName)
    seen.add(stepName)
  }

  visit(start)
  return seen
}

function validateStep(name, step, stepNames) {
  if (!step || typeof step !== 'object') throw new Error(`step "${name}" must be an object`)
  if (!step.type) throw new Error(`step "${name}" must declare a type`)

  if (step.type === 'activity') {
    if (typeof step.run !== 'function') throw new Error(`activity step "${name}" must define run(ctx)`)
    if (typeof step.next !== 'string') throw new Error(`activity step "${name}" must define next`)
    if (!stepNames.has(step.next)) throw new Error(`activity step "${name}" points to unknown step "${step.next}"`)
  } else if (step.type === 'decision') {
    if (typeof step.decide !== 'function') throw new Error(`decision step "${name}" must define decide(ctx)`)
    if (!step.transitions || typeof step.transitions !== 'object') {
      throw new Error(`decision step "${name}" must define transitions`)
    }
    if (Object.keys(step.transitions).length === 0) {
      throw new Error(`decision step "${name}" must define at least one transition`)
    }
    for (const [route, to] of Object.entries(step.transitions)) {
      if (!route) throw new Error(`decision step "${name}" has an empty route label`)
      if (!stepNames.has(to)) throw new Error(`decision step "${name}" route "${route}" points to unknown step "${to}"`)
    }
  } else if (step.type === 'succeed' || step.type === 'fail') {
    if (step.next) throw new Error(`terminal step "${name}" cannot define next`)
  } else {
    throw new Error(`step "${name}" has unsupported type "${step.type}"`)
  }

  const retry = normalizeRetry(step.retry)
  if (!Number.isInteger(retry.maxAttempts) || retry.maxAttempts < 1) {
    throw new Error(`step "${name}" retry.maxAttempts must be a positive integer`)
  }
}

export function defineWorkflow(definition) {
  if (!definition || typeof definition !== 'object') throw new Error('workflow definition required')
  if (!definition.name || typeof definition.name !== 'string') throw new Error('workflow name is required')
  if (!definition.version || typeof definition.version !== 'string') throw new Error('workflow version is required')
  if (!definition.start || typeof definition.start !== 'string') throw new Error('workflow start step is required')
  if (!definition.steps || typeof definition.steps !== 'object') throw new Error('workflow steps are required')

  const stepNames = new Set(Object.keys(definition.steps))
  if (!stepNames.has(definition.start)) throw new Error(`workflow start step "${definition.start}" does not exist`)

  const steps = {}
  for (const [name, rawStep] of Object.entries(definition.steps)) {
    const step = {
      name,
      label: rawStep.label ?? name,
      description: rawStep.description ?? '',
      ...rawStep,
      retry: normalizeRetry(rawStep.retry),
    }
    validateStep(name, step, stepNames)
    steps[name] = step
  }

  const reachable = collectReachable(definition.start, steps)
  for (const name of stepNames) {
    if (!reachable.has(name)) {
      throw new Error(`step "${name}" is unreachable from start step "${definition.start}"`)
    }
  }

  const nodes = Object.values(steps).map((step) => ({
    name: step.name,
    label: step.label,
    type: step.type,
    description: step.description,
    retry: clone(step.retry),
    timeout: step.timeout ?? null,
  }))

  const edges = Object.values(steps).flatMap((step) => inferEdgeLabels(step))

  return Object.freeze({
    name: definition.name,
    version: definition.version,
    start: definition.start,
    description: definition.description ?? '',
    steps: Object.freeze(steps),
    graph: Object.freeze({
      start: definition.start,
      nodes: Object.freeze(nodes),
      edges: Object.freeze(edges),
    }),
  })
}
