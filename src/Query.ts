import { BaseObj, QueryDefObj, QueryObj, SelectorObj, TableItem, TableObj } from './types'
import { TableItemClass } from './TableItemClass'
import { collectObj } from '@wonderlandlabs/collect/lib/types'
import { c } from '@wonderlandlabs/collect'


export default class Query implements QueryObj{

  constructor(public base: BaseObj, private queryDef: QueryDefObj) {}

  get table(): TableObj | undefined {
    return this.base.table(this.queryDef.table);
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
      const table = this.table;
      if (!table) {
        return [];
      }
      if (this.table && this.selectors.length) {
        const table = this.table;
        let coll = this.table.coll.map((_value, identity) => new TableItemClass(table, identity));
        for (const selector of this.selectors) {
          coll = this.applySelector(selector, coll);
        }
        this._value = coll.values;
      } else {
        this._value = table.$coll.keys.map((identity) => new TableItemClass(table, identity))
      }
    }
    return this._value;
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

      for(const [identity, t] of coll.iter) {
        const tItem = t as TableItem
        index += 1;
        if (
          typeof selector.from === 'number'
          && (index < selector.from)
        ) {
          continue;
        }
        if (typeof selector.until === 'number'
          && index >= selector.until){
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
}

type pair = [any, TableItem];
