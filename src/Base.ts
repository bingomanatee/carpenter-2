import {
  BaseObj,
  BaseConfig,
  JoinConfig,
  JoinObj,
  QueryDefObj,
  QueryObj,
  TableObj, isTableConfig, TableConfig, BaseTableConfig
} from './types'
import Table from './Table'
import { TransactionSet } from '@wonderlandlabs/transact'
import { transactionSet } from '@wonderlandlabs/transact/dist/types'
import { contextHandlers } from './contextHandlers'
import Join from './Join'
import Query from './Query'
import { c } from '@wonderlandlabs/collect'

export class Base implements BaseObj {

  constructor(config: BaseConfig) {
    if (config.tables) {
      c(config.tables).forEach((config, name) => {
        this.addTable(config, name);
      })
    }

    if (config.joins) {
      c(config.joins).forEach((joinConfig, name) => {
        if (name && typeof name === 'string') {
          this.addJoin({ ...joinConfig, name });
        } else {
          this.addJoin(joinConfig);
        }
      })
    }
  }

  public readonly joins = new Map<string, JoinObj>()

  private _trans?: transactionSet
  get trans(): transactionSet {
    if (!this._trans) {
      this._trans = new TransactionSet({
        handlers: contextHandlers(this)
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

  query(def: QueryDefObj): QueryObj {
    return new Query(this, def);
  }

  public addTable(config: BaseTableConfig | TableConfig, name?: string) {
    if (isTableConfig(config)) {
      this.tables.set(config.name, new Table(this, config));
    } else if (name) {
      this.tables.set(name, new Table(this, { ...config, name: name }));
    } else {
      throw new Error('addTable: no name for table');
    }
  }

}
