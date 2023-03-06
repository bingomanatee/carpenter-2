import { joinsMap, TableObj } from './types'

export class TableItemClass {
  constructor(public readonly table: TableObj, public readonly identity: unknown, private _value?: unknown) {
  }

  public get data() {
    return this.table.get(this.identity);
  }

  get value() {
    if (this._value === undefined) {
      return this.data;
    }
    return this._value;
  }

  get exists() {
    return this.table.has(this.identity)
  }

  public joins?: joinsMap

  static toJSON(tc: TableItemClass) {
    return {
      table: tc.table.name,
      value: tc.value,
      identity: tc.identity,
      joins: m2s(tc.joins)
    }
  }
}

function m2s(joins: unknown) {
  if (joins instanceof Map) {
    const out: Record<string, any> = {};
    joins.forEach((item, key) => out[`${key}`] = item.map(TableItemClass.toJSON));
    return out;
  }
  return null;
}
