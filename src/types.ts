import { transactionSet, transObj } from '@wonderlandlabs/transact/dist/types'
import { MonoTypeOperatorFunction, Observable, Observer } from 'rxjs'

export * from './types/joins';
export * from './types/tables';
export * from './types/basic';

import {
  JoinConfig,
  JoinObj,
} from './types/joins';
import {
  scalar,
} from './types/basic'

import {
  BaseTableConfig,
  TableConfig,
  TableItem,
  TableItemJSON,
  TableObj,
  TypedTableItem
} from './types/tables'

export function isScalar(arg: unknown): arg is scalar {
  if (typeof arg === 'string') {
    return true;
  }
  if (typeof arg !== 'number') {
    return false;
  }
  return Number.isFinite(arg) && !(Number.isNaN(arg))
}

// -------- BASE

export type BaseConfig = {
  joins?: JoinConfig[] | Record<string, JoinConfig>
  tables?: TableConfig[] | Record<string, BaseTableConfig>
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
  getJoin(name: string): JoinObj,
  table(name: string): TableObj | undefined,
  readonly tables: Map<string, TableObj>,
  has(name: string, identity?: unknown): boolean,
  addJoin(config: JoinConfig): void,
  addTable(config: TableConfig | BaseTableConfig, name?: string): void
  query(queryDef: QueryDefObj): QueryObj,
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

export type QueryJoinDefJoinName = {
  joinName: string,
}
export type QueryJoinDefTableName = {
  tableName: string
}

export type queryJoinDefObjSelector = SelectorObj | SelectorObj[]

export type QueryJoinDefObjBase = {
  sel?: queryJoinDefObjSelector
  joins?: QueryJoinDefObj[]
}

export function isQueryJoinDefJoinName(arg: unknown): arg is QueryJoinDefJoinName {
  return !!(
    arg && typeof arg === 'object' &&
    ('joinName' in arg)
  )
}

export function isQueryJoinDefTableName(arg: unknown): arg is QueryJoinDefTableName {
  return !!(
    arg && typeof arg === 'object' &&
    ('tableName' in arg)
  )
}

export type QueryJoinDefObj = QueryJoinDefObjBase & (QueryJoinDefJoinName | QueryJoinDefTableName)

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
  toJSON: TableItemJSON[],
  observable: Observable<TableItemJSON[]>
}

// ------------- RxJS / observable type

export type pojo =  Record<string | symbol, unknown>;
export type listenerType = Partial<Observer<Set<transObj>>> | ((value: Set<transObj>) => void) | undefined;
export type voidFn = () => void;
export type listenerFn = (next: any) => void;
export type mutators = MonoTypeOperatorFunction<any>[];
export type listenerObj = {
  next?: listenerFn;
  error?: listenerFn;
  complete?: voidFn;
};

