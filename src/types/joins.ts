import { dataMap, joinDirection } from "./basic";
import { TableItem, TableObj } from './tables'

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

export type JoinItem = {
  join: JoinObj,
  direction: joinDirection
}

export type JoinObjType = 'JoinObj';
export type JoinManagerObj = {
  index: dataMap,
  indexReverse: dataMap,
  def: JoinDef,
  table: TableObj,
  isIdentity: boolean
  purgeIndexes(): void,
  records(identity: unknown): unknown[],
  identities(identity: unknown): unknown[]
}

export interface JoinObj {
  name: string
  $type: JoinObjType,
    isVia: boolean,
    indexVia(): void,
    purgeIndexes(): void,
    link(id1: unknown, id2: unknown): void,
    linkMany(fromIdentity: unknown, dataItems: unknown[] | dataMap,
    direction: joinDirection,
    isPairs?: boolean): void
    from: JoinManagerObj,
    to: JoinManagerObj,
}

export function isJoin(arg: unknown): arg is JoinObj {
  return !!(
    arg
    && typeof arg === 'object'
    && '$type' in arg
    && arg.$type === 'JoinObj'
  );
}
