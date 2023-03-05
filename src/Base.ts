import { BaseObj, contextConfig, JoinConfig, JoinObj, QueryDefObj, QueryObj, TableObj } from './types'
import Table from './Table'
import { TransactionSet } from '@wonderlandlabs/transact'
import { transactionSet } from '@wonderlandlabs/transact/dist/types'
import { contextHandlers } from './contextHandlers'
import Join from './Join'
import Query from './Query'

export class Base implements BaseObj {

  constructor(config: contextConfig) {
    if (config.tables) {
      config.tables.forEach((config) => {
        const newTable = new Table(this, config);
        this.tables.set(newTable.name, newTable)
      })
    }

    if (config.joins) {
      config.joins.forEach((joinConfig: JoinConfig) => {
        this.addJoin(joinConfig);
      })
    }
  }

  public readonly joins = new Map<string, JoinObj>()

  private _trans?: transactionSet
  get trans(): transactionSet {
    if (!this._trans) {
      this._trans = new TransactionSet({
        handlers: contextHandlers()
      })
    }
    return this._trans
  }

  private tables: Map<string, TableObj> = new Map()

  table(name: string) {
    return this.tables.get(name)
  }

  has(name: string, identity?: unknown) {
    if (!this.tables.has(name)) {
      return false;
    }

    if (identity === undefined) {
      return true
    }
    return this.table(name)?.has(identity) || false
  }

  addJoin(joinConfig: JoinConfig) {
    const join = new Join(joinConfig, this);
    if (this.joins.has(join.name)) {
      throw new Error(`cannot redefine join ${join.name}`)
    }
    this.joins.set(join.name, join);
    join.fromTable?.addJoin(join, 'from');
    join.toTable?.addJoin(join, 'to');
  }

  query(def: QueryDefObj) : QueryObj {
    return new Query(this, def);
  }
}
