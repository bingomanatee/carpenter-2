import { transactionSet } from '@wonderlandlabs/transact/dist/types'
import { collectObj, generalObj } from '@wonderlandlabs/collect/lib/types'

export type BaseTableConfig = {
  onCreate?: creatorFn
  onUpdate?: mutatorFn
  testRecord?: recordTestFn
  testTable?: tableTestFn
  identityFromRecord?: string | identityConfigFn
  records?: unknown[] | StringKeyRecord
}

export function isTableConfig(arg: unknown) : arg is TableConfig {
  return !!(arg && typeof arg === 'object' && 'name' in arg)
}
export type TableConfig = {
  name: string
} & BaseTableConfig

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
export type BaseConfig = {
  joins?: JoinConfig[] | Record<string, JoinConfig>
  tables?: TableConfig[] | Record<string, BaseTableConfig>
}

export type StringKeyRecord = Record<string, unknown>
export type enumerator = (record: unknown, key: unknown) => void
export type joinDirection = 'from' | 'to';

/**
 * note - 'data' is a raw value; but when added with add/update,
 * it is processed by processData into the proper type, i.e., a value that
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
  processData(data: unknown, identity?: unknown): unknown
  add(data: unknown, identity?: unknown): void
  update(identity: unknown, data: unknown): void
  updateMany(records: unknown[] | dataMap): dataMap
  setField(identity: unknown, field: unknown, value: unknown): void
  delete(identity: unknown): void
  get(identity: unknown): unknown
  getMany(identities: unknown[]): dataMap;
  getRecords(identities: unknown[]): unknown[];
  generate(gen: recordGenerator, exclusive?: boolean): unknown
  getItem(identity: unknown): TableItem
  addJoin(join: JoinObj, direction?: joinDirection): void
  join(identity: unknown, term: JoinTerm): void;
  forEach(en: enumerator): void
  query(queryDef: TableQueryDefObj): QueryObj;

  $set(identity: unknown, record: unknown): void
  $testTable(): unknown
  $joinFromTerm(term: JoinTerm): JoinItem | null
}

export type JoinItem = {
  join: JoinObj,
  direction: joinDirection
}

export interface TypedTableObj<IdentityType, RecordType> extends TableObj {
  identityFor: ((data: unknown) => IdentityType)
  has(identity: IdentityType): boolean
  processData(data: unknown, identity?: unknown): RecordType
  get(identity: IdentityType): RecordType | undefined
  $set(identity: IdentityType, record: RecordType): void
  add(data: unknown, identity?: IdentityType): void
  updateMany(records: RecordType[] | Map<IdentityType, RecordType>, restrict?: boolean): Map<IdentityType, RecordType>
  getItem(identity: IdentityType): TypedTableItem<RecordType, IdentityType>
}

export interface BaseObj {
  readonly trans: transactionSet,
  readonly joins: Map<string, JoinObj>,
  table(name: string): TableObj | undefined,
  has(name: string, identity?: unknown): boolean,
  addJoin(config: JoinConfig): void,
  query(queryDef: QueryDefObj): QueryObj,
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
  return !!(
    arg
    && typeof arg === 'object'
    && 'identity' in arg
  );
}

export function isKeygenDef(arg: unknown): arg is JoinKeygenDef {
  return !!(
    arg
    && typeof arg === 'object'
    && 'keyDef' in arg
    && typeof arg.keyDef === 'function'
  );
}

export function isFieldDef(arg: unknown): arg is JoinFieldDef {
  return !!(
    arg
    && typeof arg === 'object'
    && 'field' in arg
  );
}

export type JoinDef = BaseJoinDef & (JoinFieldDef | JoinKeygenDef | JoinIdentityDef)

export type JoinConfig = {
  from: string | JoinDef
  to: string | JoinDef
  name?: string
  via?: boolean | string
}

export type joinStrategy = 'field-field' | 'field-identity' | 'identity-field' | 'identity-identity'

export function isJoinTermJoinNameBase(arg: unknown): arg is JoinTermJoinNameBase {
  return !!(arg && typeof arg === 'object' && 'joinName' in arg);
}

export function isJoinTermTableNameBase(arg: unknown): arg is JoinTermTableNameBase {
  return !!(arg && typeof arg === 'object' && 'tableName' in arg);
}

type JoinTermJoinNameBase = { joinName: string }
type JoinTermTableNameBase = { tableName: string }
type JoinTermBase = JoinTermJoinNameBase | JoinTermTableNameBase;

type JoinTermIdentityDataBase = { identity: unknown, data: unknown }
export function isJoinTermIdentityDataBase(arg: unknown): arg is JoinTermIdentityDataBase {
  return !!(arg
    && typeof arg === 'object'
    && 'identity' in arg
    && 'data' in arg
  );
}

type JoinTermDataBase = { data: unknown }
export function isJoinTermDataBase(arg: unknown): arg is JoinTermIdentityDataBase {
  return !!(arg
    && typeof arg === 'object'
    && 'data' in arg
  );
}

type JoinTermIdentityBase = { identity: unknown }

export function isJoinTermIdentityBase(arg: unknown): arg is JoinTermIdentityBase {
  return !!(arg
    && typeof arg === 'object'
    && 'identity' in arg
  );
}

type JoinTermDatasBase = { datas: unknown[] }
export function isJoinTermDatasBase(arg: unknown): arg is JoinTermDatasBase {
  return !!(arg
    && typeof arg === 'object'
    && 'datas' in arg
    && Array.isArray(arg.datas)
  );
}
type JoinTermDataPairsBase = { dataPairs: unknown[] }
export function isJoinTermDataPairsBase(arg: unknown): arg is JoinTermDataPairsBase {
  return !!(arg
    && typeof arg === 'object'
    && 'dataPairs' in arg
    && (Array.isArray(arg.dataPairs) || arg.dataPairs instanceof Map)
  );
}

type JoinTermTargetBase = JoinTermIdentityDataBase
  | JoinTermDataBase
  | JoinTermIdentityBase
  | JoinTermDatasBase
  | JoinTermDataPairsBase

export type JoinTerm = JoinTermBase & JoinTermTargetBase;

export type joinsMap = Map<string, TableItem[]>
export type identityMap = Map<unknown, unknown[]>

export type JoinObjType = 'JoinObj';

export interface JoinObj {
  name: string
  fromTable: TableObj,
  toTable: TableObj,
  $type: JoinObjType,
  toRecordsFor(fromIdentity: unknown): dataMap,
  toRecordsForArray(fromIdentity: unknown): unknown[],
  fromRecordsFor(toIdentity: unknown): dataMap,
  fromRecordsForArray(fromIdentity: unknown): unknown[],
  purgeIndexes(): void,
  fromIdentities(identity: unknown): unknown[],
  toIdentities(identity: unknown): unknown[],
  from(identities: unknown[]): identityMap,
  to(identities: unknown[]): identityMap,
  link(id1: unknown, id2: unknown): void,
  linkMany(fromIdentity: unknown, dataItems: unknown[] | dataMap,
           direction: joinDirection,
           isPairs?: boolean) : void
}

export function isJoin(arg: unknown): arg is JoinObj {
  return !!(arg
    && typeof arg === 'object'
    && '$type' in arg
    && arg.$type === 'JoinObj');
}

export interface JoinPair {
  from: unknown, // from records identityFromRecord
  to: unknown,  // to records identityFromRecord
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
  joinName: string,
  sel?: SelectorObj | SelectorObj[]
  joins?: QueryJoinDefObj[]
}

export type TableQueryDefObj = {
  sel?: SelectorObj | SelectorObj[]
  joins?: QueryJoinDefObj[]
}
export type QueryDefObj = {
  table: string,
} & TableQueryDefObj

export type QueryObj = {
  value: TableItem[],
  base: BaseObj,
  toJSON: Record<string, unknown>[]
}

