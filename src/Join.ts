import {
  BaseObj,
  dataMap,
  identityMap,
  isFieldDef,
  isJoinIdentityDef,
  isScalar,
  JoinConfig,
  JoinDef,
  joinDirection,
  JoinFieldDef, JoinManagerObj,
  JoinObj,
  JoinObjType,
  joinStrategy,
  TableObj
} from './types'
import { c } from '@wonderlandlabs/collect'

function recordsForMidKeys(midKeys: unknown[], reverseIndex: dataMap, table: TableObj) {
  const map = new Map();
  midKeys.forEach((midKey) => {
    if (reverseIndex.has(midKey)) {
      const endIdentity = reverseIndex.get(midKey);
      endIdentity.forEach((endId: unknown) => {
        if (!map.has(endId) && table.has(endId)) {
          map.set(endId, table.get(endId));
        }
      })
    }
  });

  return map;
}

function addToIndex(map: dataMap, fromIndex: unknown, toIndex: unknown, isList?: boolean) {
  if (isList && Array.isArray(toIndex)) {
    toIndex.forEach((toItem) => addToIndex(map, fromIndex, toItem));
    return;
  }

  if (!map.has(fromIndex)) {
    map.set(fromIndex, [toIndex]);
  } else {
    const IDs = map.get(fromIndex);
    if (!IDs.includes(toIndex)) {
      IDs.push(toIndex);
    }
  }
}

function identitiesForMidKeys(midKeys: unknown[], reverseIndex: dataMap): unknown[] {
  const matchingIdentities = new Set();
  midKeys.forEach((midKey) => {
    if (reverseIndex.has(midKey)) {
      const endIdentity = reverseIndex.get(midKey);
      endIdentity.forEach((identity: unknown) => matchingIdentities.add(identity));
    }
  });

  return Array.from(matchingIdentities.values());
}

function extractFieldDef(def: JoinFieldDef, record: unknown) {
  const coll = c(record);
  if (coll.family === 'container') {
    try {
      const fromIdentity = coll.get(def.field);
      if (fromIdentity !== undefined) {
        if (Array.isArray(fromIdentity)) {
          return fromIdentity;
        } else {
          return [fromIdentity];
        }
      }
    } catch (_err) {
      // cannot retrieve
    }
  }
  return [];
}

function parseConfig(config: string | JoinDef) {
  if (typeof config === 'string') {
    return {
      table: config,
    }
  } else {
    return config;
  }
}

// @TODO: put from / to relationships into manager classes

export default class Join implements JoinObj {
  constructor(private config: JoinConfig, private base: BaseObj) {
    this.from = new JoinManager(base, this.fromDef, this);
  }

  $type: JoinObjType = 'JoinObj';

  private _name?: string
  get name() {
    if (this.config.name) {
      return this.config.name
    }
    if (!this._name) {
      this._name = [this.fromDef.table, this.toDef.table].sort().join(':');
    }
    return this._name;
  }

  private _fromDef?: JoinDef
  private get fromDef(): JoinDef {
    if (!this._fromDef) {
      this._fromDef = parseConfig(this.config.from);
    }

    return this._fromDef
  }

  private _toDef?: JoinDef
  private get toDef(): JoinDef {
    if (!this._toDef) {
      this._toDef = parseConfig(this.config.to);
    }

    return this._toDef
  }

  purgeIndexes() {
    delete this._toIndex;
    delete this._fromIndexReverse;
    delete this._toIndexReverse;
    this.from.purgeIndexes();
  }

  get isVia() {
    return !!this.config.via;
  }

  get toTable(): TableObj {
    return this.base.table(this.toDef.table) ?? (() => {
      throw(`cannot find table ${this.toDef.table}`)
    })()
  }

  /**
   * fromIndex is a map of the record identityFromRecord to the exposed key
   * that matches an exposed key in toIndex.
   */
  _fromIndexReverse?: dataMap

  get fromIndexReverse(): dataMap {
    if (!this._fromIndexReverse) {
      const coll = new Map();
      this.from.index.forEach((keys, identity) => {
        keys.forEach((key: unknown) => {
          addToIndex(coll, key, identity);
        })
      });

      this._fromIndexReverse = coll
    }

    return this._fromIndexReverse;
  }

  private _toIndex?: dataMap
  private _toIndexReverse?: dataMap
  private _toIsIdentity?: boolean
  private get toIsIdentity() {
    if (typeof this._toIsIdentity !== 'boolean') {
      this._toIsIdentity = isJoinIdentityDef(this.toDef);
    }
    return this._toIsIdentity
  }

  get toIndex(): dataMap {
    if (!this._toIndex) {
      if (this.isVia) {
        this.indexVia();
      } else {
        this._toIndex = this.toTable.$coll.getMap((record, identity) => {
          let out: any[] = [];
          if (this.toIsIdentity) {
            return [identity]
          } else if (isFieldDef(this.toDef)) {
            out = extractFieldDef(this.toDef, record);
          }
          return out;
        });
      }
    }
    return this._toIndex || emptyMap
  }

  get toIndexReverse(): dataMap {
    if (!this._toIndexReverse) {
      const coll = new Map();
      this.toIndex.forEach((keys, identity) => {
        keys.forEach((key: unknown) => {
          addToIndex(coll, key, identity)
        })
      });

      this._toIndexReverse = coll
    }

    return this._toIndexReverse;
  }

  get strategy(): joinStrategy {
    if (this.isVia) {
      return 'identity-via-identity';
    }
    if (isFieldDef(this.fromDef)) {
      if (isFieldDef(this.toDef)) {
        return 'field-field';
      }
      return 'field-identity';
    } else if (isFieldDef(this.toDef)) {
      return 'identity-field'
    }
    return 'identity-identity'
  }

  linkMany(fromIdentity: unknown,
           dataItems: unknown[] | dataMap,
           direction: joinDirection,
           isPairs = false) {
    const table = direction === 'from' ? this.from.table : this.toTable;
    const config = direction === 'from' ? this.fromDef : this.toDef;
    if (!isFieldDef(config)) {
      throw new Error('linkMany must target field type');
    }
    const fieldName = config.field;
    if (direction === 'from') {
      if (this.strategy !== 'identity-field') {
        throw new Error('cannot link many with this linkVia');
      }
    } else {
      if (this.strategy !== 'field-identity') {
        throw new Error('cannot link many with this linkVia');
      }
    }

    this.base.trans.do('withBackedUpTables', [this.from.table.name, this.toTable.name],
      () => {
        if (isPairs) {
          let source = Array.isArray(dataItems) ? new Map(dataItems) : dataItems;
          table.updateMany(c(source).getMap((data: unknown) => c(data).set(fieldName, fromIdentity).value));
        } else if (Array.isArray(dataItems)) {
          dataItems = dataItems.map((data) => {
            return c(data).set(fieldName, fromIdentity).value;
          });
          table.updateMany(dataItems);
        } else {
          throw new Error('bad data');
        }
      });
  }

  link(fromIdentity: unknown, toIdentity: unknown) {
    if (!(this.from.table.has(fromIdentity) && this.toTable.has(toIdentity))) {
      throw new Error(`cannot link ${fromIdentity} and ${toIdentity} -- not in tables`);
    }

    switch (this.strategy) {
      case 'field-identity':
        if (isFieldDef(this.fromDef)) {
          this.from.table.setField(fromIdentity, this.fromDef.field, toIdentity);
        }
        break

      case 'identity-field':
        if (isFieldDef(this.toDef)) {
          this.toTable.setField(toIdentity, this.toDef.field, fromIdentity);
        }
        break;

      case 'identity-via-identity':
        this.linkVia(fromIdentity, toIdentity);
        break;

      case 'identity-identity':
        if (fromIdentity !== toIdentity) {
          throw new Error('cannot link id-id relationships');
        }
        break;

      case 'field-field':
        if (isFieldDef(this.fromDef) && isFieldDef(this.toDef)) {
          const c1 = c(this.from.table.get(fromIdentity));
          const c2 = c(this.toTable.get(toIdentity));
          if (c1.get(this.fromDef.field) !== c2.get(this.toDef.field)) {
            throw new Error('cannot link field-field relationships');
          }
        }
        break;
    }
  }

  private get viaTableName() {
    if (typeof this.config.via === 'string' && this.config.via) {
      return this.config.via;
    }
    return this.name + '$via';
  }

  viaKeys(data: unknown) {
    const toTableName = this.toTable.name;
    const fromTableName = this.from.table.name;
    const coll = c(data);
    const fromId = coll.get(fromTableName);
    const toId = coll.get(toTableName);
    return { fromId, toId };
  }

  private initVia() {
    const self = this;
    this.base.addTable({
      identityFromRecord(data) {
        const { fromId, toId } = self.viaKeys(data);
        return `${fromId}_$via$_${toId}`
      },
      onCreate(data: unknown) {
        if (!(data && typeof data === 'object' && self.from.table.name in data && self.toTable.name in data)) {
          throw new Error(self.viaTableName + ' missing t fields');
        }
        const { fromId, toId } = self.viaKeys(data);

        if (!(isScalar(fromId) && isScalar(toId))) {
          throw new Error('via/intrinsic $ require scalar identities');
        }
        return data;
      }
    }, this.viaTableName);
  }

  private get viaTable() {
    if (!this.config.via) {
      return null;
    }
    if (!this.base.has(this.viaTableName)) {
      this.initVia();
    }
    return this.base.table(this.viaTableName);
  }

  private linkVia(fromIdentity: unknown, toIdentity: unknown) {
    if (!this.base.has(this.viaTableName)) {
      this.initVia();
    }

    const viaIdentity = `${fromIdentity}_$via$_${toIdentity}`;
    if (!this.viaTable?.has(viaIdentity)) {
      const toTableName = this.toTable.name;
      const fromTableName = this.from.table.name;
      this.viaTable?.$set(viaIdentity, { [fromTableName]: fromIdentity, [toTableName]: toIdentity })
    }
  }

  toRecordsMap(fromIdentities: unknown): dataMap {
    return this.from.identities(fromIdentities).reduce((map: dataMap, identity) => {
      map.set(identity, this.toTable.get(identity));
      return map;
    }, new Map()) as dataMap;
  }

  fromIdentitiesMap(toIdentities: unknown[]): identityMap {
    const out = new Map();

    toIdentities.forEach((id) => {
      const identities = this.from.identities(id);
      if (identities.length) {
        out.set(id, identities)
      }
    });

    return out;
  }

  toIdentities(fromIdentity: unknown): unknown[] {
    if (!this.from.index.has(fromIdentity)) {
      return [];
    }
    if (this.toIsIdentity || this.isVia) {
      return this.from.index.get(fromIdentity)
    }
    return identitiesForMidKeys(this.from.index.get(fromIdentity), this.toIndexReverse);
  }

  toIdentitiesMap(fromIdentities: unknown[]): identityMap {
    const out = new Map();

    fromIdentities.forEach((fromIdentity) => {
      const toIds = this.toIdentities(fromIdentity);
      if (toIds.length) {
        out.set(fromIdentity, toIds)
      }
    });

    return out;
  }

  toRecordsArray(fromIdentity: unknown): unknown[] {
    return this.toIdentities(fromIdentity).map((identity: unknown) => this.toTable.get(identity));
  }

  fromRecordsMap(identity: unknown): dataMap {
    let midKeys: unknown[];
    if (isJoinIdentityDef(this.toDef)) {
      if (isJoinIdentityDef(this.fromDef)) {
        if (this.from.table.has(identity)) {
          return new Map([[
            identity, this.from.table.get(identity)
          ]]);
        }
      }
      midKeys = [identity]
      // coerce the reversed map of from into records from the "to" t
    } else {
      midKeys = this.toIndex.get(identity);
    }

    return midKeys.length ? recordsForMidKeys(midKeys, this.fromIndexReverse, this.from.table) : emptyMap
  }

  fromRecordsArray(toIdentity: unknown): unknown[] {
    return this.from.identities(toIdentity).map((identity: unknown) => this.from.table.get(identity));
  }

  public indexVia() {
    this.from.index = new Map();
    this._toIndex = new Map();
    const fromKey = this.from.table.name;
    const toKey = this.toTable.name;
    if (this.viaTable) {
      for (const [_id, keys] of this.viaTable.$coll.iter) {
        const viaItem = keys as Record<string, unknown>;
        const { [fromKey]: fromIndex, [toKey]: toIndex } = viaItem;

        addToIndex(this.from.index, fromIndex, toIndex);
        addToIndex(this._toIndex, toIndex, fromIndex);
      }
    }
  }

  from: JoinManager
}

class JoinManager implements JoinManagerObj {
  constructor(private base: BaseObj, public fromDef: JoinDef, private join: JoinObj) {}

  purgeIndexes() {
    delete this._index;
  }
  private _index?: dataMap
  set index(map: dataMap) {
    this._index = map;
  }

  private _table?: TableObj
  get table(): TableObj {
     if (!this._table) {
       this._table =  this.base.table(this.fromDef.table) ?? (() => {
         throw(`cannot find table ${this.fromDef.table}`)
       })()
     }
     return this._table
  }

  get index(): dataMap {
    if (!this._index) {
      if (this.join.isVia) {
        this.join.indexVia();
      } else {
        this._index = this.table.$coll.getMap((record, identity) => {
          let out: any[] = [];
          if (this.join.from.isIdentity) {
            return [identity]
          } else if (isFieldDef(this.fromDef)) {
            out = extractFieldDef(this.fromDef, record);
          } // else what?
          return out;
        });
      }
    }
    return this._index || emptyMap
  }

  _isIdentity?: boolean
  public get isIdentity() {
    if (typeof this._isIdentity !== 'boolean') {
      this._isIdentity = isJoinIdentityDef(this.fromDef);
    }
    return this._isIdentity
  }


  identities(toIdentity: unknown): unknown[] {

    if (!this.join.toIndex.has(toIdentity)) {
      // console.log('cannot find ', toIdentity, 'in ', this.toTable.name);
      return [];
    }

    if (this.isIdentity || this.join.isVia) {
      return this.join.toIndex.get(toIdentity)
    }
    return identitiesForMidKeys(this.join.toIndex.get(toIdentity), this.join.fromIndexReverse);
  }
}

const emptyMap = new Map()
