import { TableObj } from '../types'

export type scalar = number | string;
export type dataMap = Map<any, any>
export type indexMap = Map<unknown, unknown[]>

export type recordGenerator = (table: TableObj) => Generator<any, any, any>
export type creatorFn = (data: unknown, table: TableObj) => unknown
export type mutatorFn = (data: unknown, table: TableObj, current: unknown) => unknown
export type identityFn = (data: unknown) => unknown
export type identityConfigFn = (data: unknown, table: TableObj) => unknown
export type StringKeyRecord = Record<string, unknown>
export type enumerator = (record: unknown, key: unknown) => void
export type joinDirection = 'from' | 'to';
