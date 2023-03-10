import { joinsMap, TableItem, tableItemJoinMap, TableItemJSON, tableItemJSONJoinRecord, TableObj } from './types'

export class TableItemClass implements TableItem {
  constructor(public readonly table: TableObj, public readonly identity: unknown, private _value?: unknown) {
  }

  public get data() {
    return this.table.get(this.identity);
  }

  get val() {
    if (this._value === undefined) {
      return this.data;
    }
    return this._value;
  }

  get exists() {
    return this.table.has(this.identity)
  }

  public joins?: joinsMap

  static toJSON(tc: TableItem): TableItemJSON {

    const base: TableItemJSON = {
      t: tc.table.name,
      val: tc.val,
      id: tc.identity,
    }
    if (tc.joins) {
      base.$ = m2s(tc.joins);
    }
    return base;
  }
}

function m2s(joins: tableItemJoinMap): tableItemJSONJoinRecord {
  const out: Record<string, TableItemJSON[]> = {};
  joins.forEach((item, key) => out[`${key}`] = item.map(TableItemClass.toJSON));
  return out;
}
