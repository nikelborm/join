const sym = Symbol('discarded-values')

/**
 * Join two `Map` objects.
 */
export function join<K, L, R, U>(
  left: Map<K, L>,
  right: Map<K, R>,
  selectOrType: SelectFnOrJoinType<K, L, R>,
  resolve: ResolveFn<K, L, R, U>
): Generator<U, void, unknown> {
  if (!(left instanceof Map) || !(right instanceof Map)) {
    throw new TypeError('Left and right members must be Map instances')
  }

  const select = typeof selectOrType === 'function'
    ? selectOrType
    : getSelectFnFromType(selectOrType)

  if (typeof resolve !== 'function') {
    throw new TypeError('A resolve function is required')
  }

  const result = iterate(left, right, select, resolve)

  Object.defineProperty(result, sym, {
    get () {
      return iterate(left, right, not(select), pickOne)
    }
  })

  return result
}

function pickOne<L, R> (leftValue: L, rightValue: R) {
  return leftValue === undefined ? rightValue : leftValue
}

/**
 * Retrieves the discarded values from a join result.
 */
export function getDiscardedValues<T = any>(
  joined: Iterable<any>
): Generator<T, void, unknown> {
  if (!(sym in Object(joined))) {
    throw new Error('Expected joined iterable')
  }
  return joined[sym]
}


function * iterate<K, L, R, U>(
  leftMap: Map<K, L>,
  rightMap: Map<K, R>,
  select: SelectFn<K, L, R>,
  resolve: ResolveFn<K, L, R, U>
) {
  for (const [key, leftValue] of leftMap) {
    const rightValue = rightMap.get(key)
    if (select(leftValue, rightValue, key)) {
      yield resolve(leftValue, rightValue, key)
    }
  }

  for (const [key, rightValue] of rightMap) {
    if (!leftMap.has(key) && select(undefined, rightValue, key)) {
      yield resolve(undefined, rightValue, key)
    }
  }
}

/**
 * Returns the select function from join type string.
 *
 */
function getSelectFnFromType (joinType: JoinType) {
  const joinTypeToSelectFn = {
    'left': leftSelect,
    'right': rightSelect,
    'inner': innerSelect,
    'outer': outerSelect,
    'full': fullSelect,
    'leftOuter': leftOuterSelect,
    'rightOuter': rightOuterSelect,
  };

  if (!(joinType in joinTypeToSelectFn)) {
    throw new Error(`Unexpected join type: ${joinType}`)
  }

  return joinTypeToSelectFn[joinType]
}


type Chooser = <L, R>(l: L, r: R) => boolean;

const leftSelect      : Chooser = (l, r) => l !== undefined
const rightSelect     : Chooser = (l, r) => r !== undefined
const innerSelect     : Chooser = (l, r) => l !== undefined && r !== undefined
const outerSelect     : Chooser = (l, r) => !(l !== undefined && r !== undefined)
const fullSelect      : Chooser = (l, r) => l !== undefined || r !== undefined
const leftOuterSelect : Chooser = (l, r) => l !== undefined && r === undefined
const rightOuterSelect: Chooser = (l, r) => r !== undefined && l === undefined

/**
 * Negate a join type or select function.
 */
export function not<K, L, R>(
  selectOrType: SelectFnOrJoinType<K, L, R>
): SelectFnOrJoinType<K, L, R> {
  if (typeof selectOrType === 'function') {
    return (l, r, k) => !selectOrType(l, r, k)
  }
  switch (selectOrType) {
    case 'left':
      return 'rightOuter'
    case 'right':
      return 'leftOuter'
    case 'inner':
      return 'outer'
    case 'outer':
      return 'inner'
    case 'full':
      return () => false
    case 'leftOuter':
      return 'right'
    case 'rightOuter':
      return 'left'
    default:
      throw new Error(`Unexpected join type: ${selectOrType}`)
  }
}
/**
 * Cast an iterable object to a `Map` instance.
 * @param iterable The iterable object to cast.
 * @param fn A function that returns the key of the currently iterated element.
 * @param mode If set to `"ignore"`, all key collisions will be ignore. If set to `"override"`, all key collisions will be updated with the last version of the element.
 * @returns
 */
export function fromIterable<K, T>(
  iterable: Iterable<T>,
  getKey: (value: T, index: number) => K,
  collisionResolutionStrategy?: "ignore" | "override"
): Map<K, T> {
  const map = new Map()

  let index = 0
  for (const value of iterable) {
    const key = getKey(value, index++)
    const isKeyAlreadyPresent = !map.has(key)

    if (collisionResolutionStrategy === 'override' || isKeyAlreadyPresent) {
      map.set(key, value)
    } else if (collisionResolutionStrategy !== 'ignore') {
      throw new Error(`Key ${key} already exist`)
    }
  }

  return map
}

export function leftJoin<K, L, R, U>(
  left: Map<K, L>,
  right: Map<K, R>,
  resolve: (leftValue: L, rightValue: R | undefined, key: K) => U
): Generator<U, void, unknown> {
  return join(left, right, leftSelect, resolve)
}

export function rightJoin<K, L, R, U>(
  left: Map<K, L>,
  right: Map<K, R>,
  resolve: (leftValue: L | undefined, rightValue: R, key: K) => U
): Generator<U, void, unknown> {
  return join(left, right, rightSelect, resolve)
}

export function innerJoin<K, L, R, U>(
  left: Map<K, L>,
  right: Map<K, R>,
  resolve: (leftValue: L, rightValue: R, key: K) => U
): Generator<U, void, unknown> {
  return join(left, right, innerSelect, resolve)
}

export function outerJoin<K, L, R, U>(
  left: Map<K, L>,
  right: Map<K, R>,
  resolve: (leftValue: L | undefined, rightValue: R | undefined, key: K) => U
): Generator<U, void, unknown> {
  return join(left, right, outerSelect, resolve)
}

export function fullJoin<K, L, R, U>(
  left: Map<K, L>,
  right: Map<K, R>,
  resolve: (leftValue: L | undefined, rightValue: R | undefined, key: K) => U
): Generator<U, void, unknown> {
  return join(left, right, fullSelect, resolve)
}

export function leftOuterJoin<K, L, U>(
  left: Map<K, L>,
  right: Map<K, any>,
  resolve: (leftValue: L, rightValue: undefined, key: K) => U
): Generator<U, void, unknown> {
  return join(left, right, leftOuterSelect, resolve)
}

export function rightOuterJoin<K, R, U>(
  left: Map<K, any>,
  right: Map<K, R>,
  resolve: (leftValue: undefined, rightValue: R, key: K) => U
): Generator<U, void, unknown> {
  return join(left, right, rightOuterSelect, resolve)
}


/**
 * Join type.
 * - `"left"`: Keep all elements from the left set.
 * - `"right"`: Keep all elements from the right set.
 * - `"inner"`: Keep all elements present in both sets.
 * - `"outer"`: Keep all elements present just in one set (unique).
 * - `"leftOuter"`: Keel all elements present just in the left set (unique).
 * - `"rightOuter"`: Keel all elements present just in the right set (unique).
 * - `"full"`: Keep all elements from both sets.
 */
export type JoinType =
  | "left"
  | "right"
  | "inner"
  | "outer"
  | "leftOuter"
  | "rightOuter"
  | "full";

export type SelectFnOrJoinType<K, L, R> = SelectFn<K, L, R> | JoinType;

/**
 * Function that select the elements that will be resolved.
 */
export type SelectFn<K, L, R> = ResolveFn<K, L, R, boolean>;

/**
 * Function that resolves the join collisions and maps the result.
 */
export type ResolveFn<K, L, R, U> = (
  leftValue: L | undefined,
  rightValue: R | undefined,
  key: K
) => U;
