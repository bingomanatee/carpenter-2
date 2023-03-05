import { TableObj } from './types'

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
}
