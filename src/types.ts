import { transactionSet, transObj } from '@wonderlandlabs/transact/dist/types'
import { collectObj, generalObj } from '@wonderlandlabs/collect/lib/types'
import { MonoTypeOperatorFunction, Observable, Observer } from 'rxjs'

export type scalar = number | string;

export function isScalar(arg: unknown): arg is scalar {
  if (typeof arg === 'string') {
    return true;
  }
  if (typeof arg !== 'number') {
    return false;
  }
  return Number.isFinite(arg) && !(Number.isNaN(arg))
}

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
 * note - 'data' is a raw val; but when added with add/update,
 * it is processed by processData into the proper type, i.e., a val that
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
  addJoin(join: JoinObj): void
  join(identity: unknown, term: JoinTerm): void;
  forEach(en: enumerator): void
  query(queryDef: TableQueryDefObj): QueryObj;
  queryFor(identity: unknown, queryDef: TableQueryDefObj) : QueryObj;

  $set(identity: unknown, record: unknown): void
  $testTable(): unknown
  $joinFromTerm(term: JoinTerm): JoinItem | null
  $clearJoins(): void;
  $joinFromTableName(name: string) : JoinItem | null
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
  readonly tables: Map<string, TableObj>,
  has(name: string, identity?: unknown): boolean,
  addJoin(config: JoinConfig): void,
  addTable(config: TableConfig | BaseTableConfig, name?: string): void
  query(queryDef: QueryDefObj): QueryObj,
}

type BaseJoinDef = {
  table: string,
  compare?(a: unknown, b: unknown): boolean
}

export function isBaseJoinDef(arg: unknown): arg is BaseJoinDef {
  return !!(
    arg
    && typeof arg === 'object'
    && 'table' in arg
    && (('compare' in arg && typeof arg.compare === 'function') || (!('compare' in arg)))
  );
}

export type JoinFieldDef = { field: any }
type JoinIdentityDef = {}

export function isJoinIdentityDef(arg: unknown): arg is JoinIdentityDef {
  return isBaseJoinDef(arg) && !isFieldDef(arg)
}


export function isFieldDef(arg: unknown): arg is JoinFieldDef {
  return isBaseJoinDef(arg)
    && ('field' in arg)
}

export type JoinDef = BaseJoinDef | BaseJoinDef & JoinFieldDef;

export type JoinConfig = {
  from: string | JoinDef
  to: string | JoinDef
  name?: string
  via?: boolean | string
}

export type joinStrategy =
  'field-field'
  | 'field-identity'
  | 'identity-field'
  | 'identity-identity'
  | 'identity-via-identity'

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
export type JoinManagerObj = {
  index: dataMap,
  fromDef: JoinDef,
  table: TableObj,
  isIdentity: boolean
  purgeIndexes(): void,
}
export interface JoinObj {
  name: string
  toTable: TableObj,
  $type: JoinObjType,
  isVia: boolean,
  indexVia(): void,
  toRecordsMap(fromIdentity: unknown): dataMap,
  toRecordsArray(fromIdentity: unknown): unknown[],
  fromRecordsMap(toIdentity: unknown): dataMap,
  fromRecordsArray(fromIdentity: unknown): unknown[],
  purgeIndexes(): void,
  fromIdentities(identity: unknown): unknown[],
  toIdentities(identity: unknown): unknown[],
  fromIdentitiesMap(identities: unknown[]): identityMap,
  toIdentitiesMap(identities: unknown[]): identityMap,
  link(id1: unknown, id2: unknown): void,
  linkMany(fromIdentity: unknown, dataItems: unknown[] | dataMap,
           direction: joinDirection,
           isPairs?: boolean): void
  from: JoinManagerObj,
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

export type TableItemBase = {
  readonly val: unknown,
}

export type tableItemJSONJoinRecord = Record<string, TableItemJSON[]>
export type TableItemJSON = {
  $?: tableItemJSONJoinRecord
  t: string
  id: unknown
} & TableItemBase

export type tableItemJoinMap = Map<string, TableItem[]>;
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

export function isQueryJoinDefJoinName(arg: unknown):arg is QueryJoinDefJoinName {
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


// -------------- general things

export type pojo = { [key: string | symbol]: any };

export interface valuable {
  value: any;
  fast?: boolean;
}


// ------------- RxJS / observable type

export type listenerType = Partial<Observer<Set<transObj>>> | ((value: Set<transObj>) => void) | undefined;
export type voidFn = () => void;
export type listenerFn = (next: any) => void;
export type mutators = MonoTypeOperatorFunction<any>[];
export type listenerObj = {
  next?: listenerFn;
  error?: listenerFn;
  complete?: voidFn;
};

