import { transactionSet } from '@wonderlandlabs/transact/dist/types'
import { collectObj, generalObj } from '@wonderlandlabs/collect/lib/types'
import Query from './Query'

export type tableConfig = {
  name: string
  onCreate?: creatorFn
  onUpdate?: mutatorFn
  testRecord?: recordTestFn
  testTable?: tableTestFn
  identity?: string | identityConfigFn
  records?: unknown[] | StringKeyRecord
}

export type recordGenerator = (table: TableObj) => Generator<any, any, any>
export type recordSetGenerator = (record: unknown, id: unknown) => Generator<any, any, any>

export type recordTestValue = false | null | void | undefined | string
export type tableTestFn = (table: TableObj) => void;
export type recordTestFn = (record: unknown, identity: unknown, table: TableObj) => recordTestValue;
export type tableRecordTestFn = (record: unknown, identity: unknown) => recordTestValue;
export type creatorFn = (data: unknown, table: TableObj) => unknown
export type mutatorFn = (data: unknown, table: TableObj, current: unknown) => unknown
export type identityFn = (data: unknown) => unknown
export type identityConfigFn = (data: unknown, table: TableObj) => unknown
export type dataMap = Map<any, any>
export type contextConfig = {
  joins?: any
  tables?: tableConfig[]
}

export type StringKeyRecord = Record<string, unknown>
export type enumerator = (record: unknown, key: unknown) => void
export type joinDirection = 'from' | 'to';

/**
 * note - 'data' is a raw value; but when added with add/update,
 * it is processed by recordFor into the proper type, i.e., a value that
 * can survive testRecord
 */
export interface TableObj {
  name: string,
  base: BaseObj
  coll: collectObj // a clone of the record collection
  identityFor: identityFn
  size: number
  readonly joins: Map<string, JoinItem>
  // these are internal operations, not to be called externally
  $records: dataMap
  readonly $coll: collectObj
  $testRecord: tableRecordTestFn
  has(identity: unknown): boolean
  recordFor(data: unknown, identity?: unknown): unknown
  add(data: unknown, identity?: unknown): void
  update(identity: unknown, data: unknown): void
  updateMany(records: unknown[] | dataMap): void
  delete(identity: unknown): void
  get(identity: unknown): unknown
  getMany(identities: unknown[]): dataMap;
  getRecords(identities: unknown[]): unknown[];
  generate(gen: recordGenerator, exclusive?: boolean): unknown
  forEach(en: enumerator): void
  $set(identity: unknown, record: unknown): void
  $testTable(): unknown
  getItem(identity: unknown): TableItem
  addJoin(join: JoinObj, direction?: joinDirection): void
}

export type JoinItem = {
  join: JoinObj,
  direction: joinDirection
}

export interface TypedTableObj<IdentityType, RecordType> extends TableObj {
  identityFor: ((data: unknown) => IdentityType)
  has(identity: IdentityType): boolean
  recordFor(data: unknown, identity?: unknown): RecordType
  get(identity: IdentityType): RecordType | undefined
  $set(identity: IdentityType, record: RecordType): void
  add(data: unknown, identity?: IdentityType): void
  updateMany(records: RecordType[] | Map<IdentityType, RecordType>, restrict?: boolean): void
  getItem(identity: IdentityType): TypedTableItem<RecordType, IdentityType>
}

export interface BaseObj {
  readonly trans: transactionSet
  readonly joins: Map<string, JoinObj>
  table(name: string): TableObj | undefined
  has(name: string, identity?: unknown): boolean
  addJoin(config: JoinConfig): void
  query(queryDef: QueryDefObj): QueryObj
}

type BaseJoinDef = {
  table: string,
  compare?(a: unknown, b: unknown): boolean
}
export type JoinFieldDef = { field: any }
export type JoinKeygenDef = {
  keyGen: recordSetGenerator
}
type JoinIdentityDef = { identity: true }

export function isJoinIdentityDef(arg: unknown): arg is JoinIdentityDef {
  const target = arg as generalObj
  return target.identity === true;
}

export function isKeygenDef(arg: unknown): arg is JoinKeygenDef {
  const target = arg as generalObj
  return 'keyDef' in target && (typeof target.keyDef === 'function')
}

export function isFieldDef(arg: unknown): arg is JoinFieldDef {
  const target = arg as generalObj
  return 'field' in target;
}

export type JoinDef = BaseJoinDef & (JoinFieldDef | JoinKeygenDef | JoinIdentityDef)

export type JoinConfig = {
  from: string | JoinDef
  to: string | JoinDef
  name?: string
  intrinsic?: boolean
}

export type joinsMap = Map<string, TableItem[]>
export type identityMap = Map<unknown, unknown[]>

export interface JoinObj {
  name: string
  fromTable: TableObj,
  toTable: TableObj,
  toRecordsFor(fromIdentity: unknown): dataMap,
  toRecordsForArray(fromIdentity: unknown): unknown[],
  fromRecordsFor(toIdentity: unknown): dataMap,
  fromRecordsForArray(fromIdentity: unknown): unknown[],
  purgeIndexes(): void,
  fromIdentities(identity: unknown) : unknown[],
  toIdentities(identity: unknown) : unknown[],
  from(identities: unknown[]) : identityMap,
  to(identities: unknown[]): identityMap,
}

export function isJoin(arg: unknown): arg is JoinObj {
  return true
}

export interface JoinPair {
  from: unknown, // from records identity
  to: unknown,  // to records identity
}

export type TableItem = {
  readonly value: unknown,
  readonly data: unknown,
  readonly identity: unknown,
  readonly exists: boolean,
  readonly table: TableObj,
  joins?: Map<string, TableItem[]>,
}

export type TypedTableItem<RecordType, IdentityType> = TableItem & {
  readonly value: RecordType,
  readonly identity: IdentityType,
}

export type tableItemFilter = (item: TableItem) => boolean

type SelectorChooserObj = {
  filter?: tableItemFilter,
  from?: number,
  until?: number,
  count?: number
}

export type tableItemComparator = (t1: TableItem, t2: TableItem) => number

type SelectorSorterObj = {
  sort: tableItemComparator
}

export type selectorMap = (item: TableItem) => unknown

export type SelectorObj = SelectorChooserObj | SelectorSorterObj | selectorMap

export type QueryJoinDefObj = {
  name: string,
  sel?: SelectorObj | SelectorObj[]
}

// @TODO: sort
export type QueryDefObj = {
  table: string,
  sel?: SelectorObj | SelectorObj[]
  joins?: QueryJoinDefObj[]
}

export type QueryObj = {
  value: TableItem[],
  base: BaseObj
}
