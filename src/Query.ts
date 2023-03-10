import {
  BaseObj, identityMap, isQueryJoinDefJoinName, isQueryJoinDefTableName, JoinDef,
  JoinItem,
  JoinObj,
  QueryDefObj,
  QueryJoinDefObj,
  QueryObj,
  SelectorObj,
  TableItem, TableItemJSON,
  TableObj
} from './types'
import { TableItemClass } from './TableItemClass'
import { collectObj } from '@wonderlandlabs/collect/lib/types'
import { c } from '@wonderlandlabs/collect'
import { filter, map, MonoTypeOperatorFunction, Observable, Observer, share, Subscription } from 'rxjs';
import { transObj } from '@wonderlandlabs/transact/dist/types'
import { listenerFactory } from './utils'


export default class Query implements QueryObj {

  constructor(public base: BaseObj, private queryDef: QueryDefObj) {
  }

  get selectors(): SelectorObj[] {
    if (!this.queryDef.sel) {
      return [];
    }
    if (Array.isArray(this.queryDef.sel)) {
      return this.queryDef.sel;
    }
    return [this.queryDef.sel]
  }

  private _value?: TableItem[]
  get value() {
    if (!this._value) {
      const table = this.base.table(this.queryDef.table);
      if (!table) {
        return [];
      }
      if (this.selectors.length) {
        let coll = table.coll.map((_value, identity) => new TableItemClass(table, identity));
        for (const selector of this.selectors) {
          coll = this.applySelector(selector, coll);
        }

        this._value = coll.values;
      } else {
        this._value = table.$coll.keys.map((identity) => new TableItemClass(table, identity))
      }

      if (this.queryDef.joins) {

        this.queryDef.joins?.forEach((joinDef: QueryJoinDefObj) => {
          if (Array.isArray(this._value)) {
            this.applyJoinDef(this._value, joinDef)
          }
        })
      }
    }

    return this._value;
  }

  applyJoinDef(items: TableItem[], joinDef: QueryJoinDefObj) {
    if (!items.length) {
      return;
    }

    let joinItem: JoinItem | null = null
    if (isQueryJoinDefJoinName(joinDef)) {
      joinItem = items[0].table.joins.get(joinDef.joinName) || null;
      if (!joinItem) {
        throw new Error(`cannot retrieve join ${joinDef.joinName} from ${items[0].table.name || '<missing t>'}`);
      }
    } else if (isQueryJoinDefTableName(joinDef)) {
      joinItem = items[0].table.$joinFromTableName(joinDef.tableName)
    }

    if (!joinItem) {
      throw new Error('no join item');
    }

    items?.forEach((ts) => {
      if (!joinItem) return; // typescriptism

      let ids: unknown[];
      let table: TableObj;

      if (joinItem.direction === 'from') {
        table = joinItem.join.toTable
        ids = joinItem.join.toIdentities(ts.identity)
      } else {
        table = joinItem.join.from.table
        ids = joinItem.join.from.identities(ts.identity)
      }

      if (!ids.length) {
        return;
      }

      let joinedItems = ids.map((identity: unknown) => new TableItemClass(table, identity));

      if (joinDef.sel) {
        const selectors = Array.isArray(joinDef.sel) ? joinDef.sel : [joinDef.sel];
        let coll = c(joinedItems).reduce((memo: Map<unknown, TableItem>, item: TableItem) => {
          memo.set(item.identity, item);
        }, new Map());

        selectors.forEach((selector) => {
          coll = this.applySelector(selector, coll);
        });
        joinedItems = coll.values;
      }

      if (joinDef.joins) {
        joinDef.joins.forEach((join: QueryJoinDefObj) => {
          this.applyJoinDef(joinedItems, join);
        });
      }

      if (!ts.joins) {
        ts.joins = new Map();
      }
      if (isQueryJoinDefJoinName(joinDef)) {
        ts.joins.set(joinDef.joinName, joinedItems);
      } else if (isQueryJoinDefTableName(joinDef)) {

      }
    })
  }

  applySelector(selector: SelectorObj, coll: collectObj): collectObj {
    if (typeof selector === 'function') {
      coll.map((item: TableItem) => {
        return new TableItemClass(
          item.table,
          item.identity,
          selector(item)
        );
      });
      return coll;
    } else if ('sort' in selector) {
      const sortFn = (pair1: pair, pair2: pair) => selector.sort(pair1[1], pair2[1]);
      coll.sort(sortFn);
      return coll;
    } else {
      // is a chooser
      const out = new Map<unknown, TableItem>()
      let index = -1;

      for (const [identity, t] of coll.iter) {
        const tItem = t as TableItem
        index += 1;
        if (
          typeof selector.from === 'number'
          && (index < selector.from)
        ) {
          continue;
        }
        if (typeof selector.until === 'number'
          && index >= selector.until) {
          continue;
        }
        if (typeof selector.filter === 'function'
          && !selector.filter(tItem)) {
          continue;
        }
        out.set(identity, tItem);
        if (selector.count && selector.count <= out.size) {
          break;
        }
      }
      return c(out);
    }
  }

  get toJSON() {
    return this.value.map(TableItemClass.toJSON);
  }

  public get observable() : Observable<TableItemJSON[]>{
    //@ts-ignore
    return this.base.trans.pipe(...commitPipes(this));
  }

  subscribe(listener: unknown) {
    return this.observable.subscribe(listenerFactory(listener));
  }
}

export type mutators = MonoTypeOperatorFunction<any>[];
export const commitPipes = (target: QueryObj): mutators =>
   [filter((set: Set<transObj>) => set.size === 0), map(() => target.toJSON), share()]

type pair = [any, TableItem];
