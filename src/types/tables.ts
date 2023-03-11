import {
  creatorFn,
  dataMap,
  enumerator,
  identityConfigFn,
  identityFn,
  mutatorFn,
  recordGenerator,
  StringKeyRecord,
} from './basic'
import { collectObj } from '@wonderlandlabs/collect/lib/types'
import { JoinPart, JoinObj, JoinTerm } from './joins'
import { BaseObj, QueryObj, TableQueryDefObj } from '../types'

export type recordTestValue = false | null | void | undefined | string
export type tableTestFn = (table: TableObj) => void;
export type recordTestFn = (record: unknown, identity: unknown, table: TableObj) => recordTestValue;
export type tableRecordTestFn = (record: unknown, identity: unknown) => recordTestValue;

export type BaseTableConfig = {
  onCreate?: creatorFn
  onUpdate?: mutatorFn
  testRecord?: recordTestFn
  testTable?: tableTestFn
  identityFromRecord?: string | identityConfigFn
  records?: unknown[] | StringKeyRecord
}

export function isTableConfig(arg: unknown): arg is TableConfig {
  return !!(arg && typeof arg === 'object' && 'name' in arg)
}

export type TableConfig = {
  name: string
} & BaseTableConfig;

/**
 * note - 'data' is a raw val; but when added with add/update,
 * it is processed by dataToRecord into the proper type, i.e., a val that
 * can survive testRecord
 */
export interface TableObj {
  name: string,
  base: BaseObj
  coll: collectObj // a clone of the record collection
  identityFor: identityFn
  size: number
  readonly joins: Map<string, JoinPart>
  // these are internal operations, not to be called externally
  $records: dataMap
  readonly $coll: collectObj
  $testRecord: tableRecordTestFn
  has(identity: unknown): boolean
  dataToRecord(data: unknown, identity?: unknown): unknown
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
  addJoin(join: JoinObj): void
  join(identity: unknown, term: JoinTerm): void;
  forEach(en: enumerator): void
  query(queryDef: TableQueryDefObj): QueryObj;
  queryFor(identity: unknown, queryDef: TableQueryDefObj): QueryObj;
  purgeIndexes(): void;

  $set(identity: unknown, record: unknown): void
  $testTable(): unknown
  $joinFromTerm(term: JoinTerm, allowMultiple?: boolean): JoinPart[]
  $clearJoins(): void;
  $joinFromTableName(name: string, allowMultiple?: boolean): JoinPart[]
}

// -------- TableItems

export type TableItemBase = {
  readonly val: unknown,
}

export type tableItemJSONJoinRecord = Record<string, TableItemJSON[]>

export function isTableItemJSON(arg: unknown): arg is TableItemJSON {
  return !!(
    arg &&
    typeof arg === 'object' &&
    't' in arg &&
    'id' in arg
  );
}

export type TableItemJSON = {
  $?: tableItemJSONJoinRecord
  t: string
  id: unknown
} & TableItemBase

export type tableItemJoinMap = Map<string, TableItem[]>;

export function isTableItem(arg: unknown): arg is TableItem {
  return !!(
    arg &&
    typeof arg === 'object' &&
    'table' in arg &&
    'identity' in arg
  );
}

export type TableItem = {
  readonly data: unknown,
  readonly exists: boolean,
  readonly table: TableObj,
  readonly identity: unknown,
  joins?: tableItemJoinMap | null
} & TableItemBase

export type TypedTableItem<RecordType, IdentityType> = TableItem & {
  readonly val: RecordType,
  readonly identity: IdentityType,
}
