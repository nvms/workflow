export function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value))
}
