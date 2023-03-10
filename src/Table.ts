import { v4 } from 'uuid'
import {
  BaseObj,
  creatorFn,
  dataMap,
  enumerator,
  identityConfigFn, isJoinTermJoinNameBase, isJoinTermTableNameBase, joinDirection, JoinItem, JoinObj, JoinTerm,
  mutatorFn, QueryJoinDefObj, queryJoinDefObjSelector,
  recordGenerator,
  recordTestFn,
  recordTestValue, SelectorObj,
  StringKeyRecord, TableConfig,
  TableItem, tableItemFilter,
  TableObj, TableQueryDefObj,
  tableTestFn
} from './types'
import { c } from '@wonderlandlabs/collect'
import { generalObj } from '@wonderlandlabs/collect/lib/types'
import { TableItemClass } from './TableItemClass'

function identityFn(value: unknown) {
  return value;
}

function falsyFn(): false {
  return false
}

function identifierFn() {
  return v4()
}

function isRecordSet(value: unknown) {
  const coll = c(value)
  return coll.type === 'object' && coll.hasKey('$record')
}

function strTest(value: unknown): false | identityConfigFn {
  if (!value) {
    return false;
  }
  if (typeof value === 'function') {
    return value as identityConfigFn
  }
  if (typeof value === 'string') {
    return (data: unknown) => {
      const obj = data as StringKeyRecord;
      return obj[value]
    }
  }
  throw new Error('identityFromRecord parser (if defined) must be string or function')
}

export default class Table implements TableObj {

  constructor(public base: BaseObj, config: TableConfig) {
    this.name = config.name
    this._onCreate = config.onCreate || identityFn;
    this._onUpdate = config.onUpdate || this._onCreate;
    this._identityFromRecord = strTest(config.identityFromRecord) || identifierFn
    this._testTable = config.testTable || falsyFn;
    this._testRecord = config.testRecord || falsyFn;

    if (config.records) {
      if (Array.isArray(config.records)) {
        config.records.map((data: unknown) => this.processData(data))
          .forEach((record: unknown) => {
            const identity = this.identityFor(record);
            this.$set(identity, record)
          });
      } else {
        c(config.records)
          .map((data, identity) => this.processData(data, identity))
          .forEach((record, identity) => {
            this.$set(record, identity)
          });
      }
    }
  }

  // ---------- filters

  private _identityFromRecord: (data: unknown, table: TableObj) => unknown
  private _onCreate: creatorFn
  private _onUpdate: mutatorFn
  private _testRecord: recordTestFn
  private _testTable: tableTestFn

  processData(data: unknown, identity?: unknown): unknown {
    if (identity && this.has(identity)) {
      return this._onUpdate(data, this, this.get(identity))
    }
    return this._onCreate(data, this);
  }

  private _joins?: Map<string, JoinItem>
  get joins() {
    if (!this._joins) {
      this._joins = new Map();
      this.base.joins.forEach((join) => {
        this.addJoin(join);
      })
    }
    return this._joins;
  }

  $clearJoins() {
    delete this._joins;
  }

  addJoin(join: JoinObj) {
    if (join.from.table.name === this.name) {
      this.joins.set(join.name, { join, direction: 'from' })
    } else if (join.to.table.name === this.name) {
      this.joins.set(join.name, { join, direction: 'to' })
    }
  }

  identityFor(data: unknown): unknown {
    return this._identityFromRecord(data, this)
  }

  public update(identity: unknown, data: unknown, upsert = false) {
    this.trans.do('update', this, identity, data, upsert)
  }

  public updateMany(data: unknown[] | generalObj, restrict?: boolean) {
    return this.trans.do('updateMany', data, this, restrict);
  }

  public setField(identity: unknown, field: unknown, value: unknown) {
    this.trans.do('setField', this, identity, field, value);
  }

  public join(identity: unknown, joinTerm: JoinTerm) {
    this.trans.do('join', this, identity, joinTerm);
  }

  public delete(identity: unknown) {
    this.trans.do('delete', identity, this);
  }

  add(record: unknown, identity?: unknown, replace = false) {
    this.trans.do('add', this, record, identity, replace)
  }

  /**
   *
   * generates a series of records to add or update in the collection
   * either appends new records or replaces entire collection with generated output.
   *
   * note - the generator must yield either
   *    array pairs [record, identityFromRecord]
   *    object records {$record, $identityFromRecord?}
   *    records (any other return val taken as a record)
   *         --- if a record is generated, the t MUST have an identityFromRecord function
   *
   * that means if you want to return a record that _is_ an array
   * you must either wrap it in an array or return it
   * as {$record: myArray, $identityFromRecord: myKey}
   *
   * @param generator *(t) => iter
   * @param replace {boolean} -- whether to add new data over current set
   *                             or COMPLETELY replace collection with generated set
   */
  generate(generator: recordGenerator, replace = false) {
    const coll = this.$coll.cloneEmpty()

    let record: unknown;
    let identity: unknown;
    const iter = generator(this);
    do {
      const { value, done } = iter.next();
      if (!value || done) {
        break;
      }
      if (Array.isArray(value)) {
        record = value[0];
        identity = (value.length > 0) ? value[1] : undefined
      } else if (isRecordSet(value)) {
        record = value.$record;
        identity = value.$identity || undefined
      }
      if (record === undefined && identity === undefined) {
        continue
      }

      if (identity === undefined) {
        identity = this.identityFor(record)
        if (identity === undefined) {
          throw Object.assign(
            new Error('generate -- undefined identityFromRecord for record'),
            { record, generator, table: this.name }
          )
        }
      }

      coll.set(identity, record);
    } while (record)

    this.updateMany(coll.value, replace);
  }

  // ----- informational

  public name: string

  forEach(fn: enumerator) {
    this.$coll.forEach(fn);
  }

  get(identity: unknown) {
    return this.$coll.get(identity)
  }

  getMany(identities: unknown[]): dataMap {
    return identities.reduce((memo: dataMap, identity) => {
      const record = this.get(identity)
      if (record !== undefined) {
        memo.set(identity, record);
      }
      return memo;
    }, new Map())
  }

  getRecords(identities: unknown[]): unknown[] {
    return identities.reduce((memo: unknown[], identity) => {
      const record = this.get(identity)
      if (record !== undefined && !memo.includes(record)) {
        memo.push(record)
      }
      return memo;
    }, [])
  }

  public has(identity: unknown) {
    return this.$records.has(identity)
  }

  get size() {
    return this.$records.size;
  }

  get coll() {
    return this.$coll.clone();
  }

  getItem(identity: unknown): TableItem {
    return new TableItemClass(this, identity)
  }

  query(queryDef: QueryJoinDefObj) {
    return this.base.query({ table: this.name, ...queryDef });
  }

  queryFor(identity: unknown, queryDef: QueryJoinDefObj) {
    const filter: tableItemFilter = (tableItem: TableItem) => tableItem.identity === identity
    const filterSelector: SelectorObj = {filter}
    const { sel: originalSel } = queryDef;
    let sel: queryJoinDefObjSelector = filterSelector;
    if (originalSel) {
      if (Array.isArray(originalSel)) {
        sel = [filterSelector, ...originalSel]
      } else {
        sel = [filterSelector, originalSel]
      }
    }
    return this.base.query({ table: this.name, ...queryDef, sel });
  }

  // ----- internal methods

  private get trans() {
    return this.base.trans
  }

  private readonly _$coll = c(new Map());

  get $records(): dataMap {
    return this.$coll.value
  }

  set $records(records: dataMap) {
    this.$coll.change(records);
  }

  get $coll() {
    return this._$coll;
  }

  $set(identity: unknown, record: unknown) {
    this.$records.set(identity, record)
  }

  $testRecord(record: unknown, identity: unknown): recordTestValue {
    return this._testRecord(record, identity, this);
  }

  $testTable(): unknown {
    return this._testTable(this);
  }

  $joinFromTerm(term: JoinTerm): JoinItem | null {
    if (isJoinTermJoinNameBase(term)) {
      const joinItem = this.joins.get(term.joinName);
      if (joinItem) {
        return joinItem;
      }
    } else if (isJoinTermTableNameBase(term)) {
      // @TODO: detect self-$
      const { tableName } = term;

      return this.$joinFromTableName(tableName);
    }
    return null;
  }

  $joinFromTableName(tableName: string) {
    // requesting a match to the other side of the JoinTerm
    let matches = Array.from(this.joins.values()).filter((joinItem: JoinItem) => {
      return (
        (joinItem.join.from.table.name === this.name
          || joinItem.join.to.table.name === tableName)
        ||
        (joinItem.join.from.table.name === tableName
          || joinItem.join.to.table.name === this.name)
      );
    });

    if (matches.length === 1) {
      return matches[0];
    }
    if (matches.length > 1) {
      throw new Error('multiple $ for ' + tableName + 'from ' + this.name);
    }
    return null;
  }
}
