import {
  BaseObj,
  dataMap, identityMap,
  isFieldDef,
  isJoinIdentityDef,
  isKeygenDef,
  JoinConfig,
  JoinDef, joinDirection, JoinFieldDef, JoinKeygenDef,
  JoinObj, JoinObjType,
  JoinPair, joinStrategy,
  TableObj
} from './types'
import { collectObj } from '@wonderlandlabs/collect/lib/types'
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

function extractKeyGenDef(def: JoinKeygenDef, record: unknown, identity: unknown) {
  const iter = def.keyGen(record, identity);
  let done = false;
  const out: any[] = [];

  do {
    const { value, done: isDone } = iter.next();
    done = !!isDone;

    if (done) {
      break;
    }

    if (value !== undefined) {
      out.push(value);
    }
    return out;
  } while (!done);

  return out;
}

export default class Join implements JoinObj {

  constructor(private config: JoinConfig, private base: BaseObj) {}

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
    if (typeof this.config.from === 'string') {
      if (!this._fromDef) {
        this._fromDef = {
          table: this.config.from,
          identity: true
        }
      }
      return this._fromDef
    } else {
      return this.config.from
    }
  }

  private _toDef?: JoinDef
  private get toDef(): JoinDef {
    if (typeof this.config.to === 'string') {
      if (!this._toDef) {
        this._toDef = {
          table: this.config.to,
          identity: true
        }
      }
      return this._toDef
    } else {
      return this.config.to
    }
  }

  purgeIndexes() {
    delete this._fromIndex;
    delete this._toIndex;
    delete this._fromIndexReverse;
    delete this._toIndexReverse;
  }

  /**
   * intrinsic is a set of JoinPairs
   * @private
   */
  private _intrinsic?: collectObj
  private get intrinsic() {
    if (!this._intrinsic) {
      this._intrinsic = c(new Set(), {});
    }
    return this._intrinsic;
  }

  get fromTable(): TableObj {
    return this.base.table(this.fromDef.table) ?? (() => {
      throw(`cannot find table ${this.fromDef.table}`)
    })()
  }

  get toTable(): TableObj {
    return this.base.table(this.toDef.table) ?? (() => {
      throw(`cannot find table ${this.toDef.table}`)
    })()
  }

  get toColl() {
    return this.toTable?.$coll || emptyColl
  }

  /**
   * fromIndex is a map of the record identityFromRecord to the exposed key
   * that matches an exposed key in toIndex.
   */
  _fromIndex?: dataMap
  _fromIndexReverse?: dataMap

  get fromIndex(): dataMap {
    if (!this._fromIndex) {
      this._fromIndex = this.fromTable.$coll.getMap((record, identity) => {
        let out: any[] = [];
        if (isJoinIdentityDef(this.fromDef)) {
          return [identity]
        } else if (isKeygenDef(this.fromDef)) {
          out = extractKeyGenDef(this.fromDef, record, this.fromTable);
        } else if (isFieldDef(this.fromDef)) {
          out = extractFieldDef(this.fromDef, record);
        }
        return out;
      });
    }
    return this._fromIndex || emptyMap
  }

  get fromIndexReverse(): dataMap {
    if (!this._fromIndexReverse) {
      const coll = c(new Map());
      this.fromIndex.forEach((keys, identity) => {
        if (Array.isArray(keys)) {
          keys.forEach((key) => {
            if (coll.hasKey(key)) {
              const identities: unknown[] = coll.get(key);
              if (!identities.includes(identity)) {
                identities.push(identity);
              }
            } else {
              coll.set(key, [identity]);
            }
          })
        }
      });
      this._fromIndexReverse = coll.value as dataMap;
    }

    return this._fromIndexReverse;
  }

  _toIndex?: dataMap
  _toIndexReverse?: dataMap

  get toIndex(): dataMap {
    if (!this._toIndex) {
      this._toIndex = this.toTable.$coll.getMap((record, identity) => {
        let out: any[] = [];
        if (isJoinIdentityDef(this.toDef)) {
          return [identity]
        } else if (isKeygenDef(this.toDef)) {
          out = extractKeyGenDef(this.toDef, record, this.toTable);
        } else if (isFieldDef(this.toDef)) {
          out = extractFieldDef(this.toDef, record);
        }
        return out;
      });
    }
    return this._toIndex || emptyMap
  }

  get toIndexReverse(): dataMap {
    if (!this._toIndexReverse) {
      const coll = c(new Map());
      this.toIndex.forEach((keys, identity) => {
        if (Array.isArray(keys)) {
          keys.forEach((key) => {
            if (coll.hasKey(key)) {
              const identities: unknown[] = coll.get(key);
              if (!identities.includes(identity)) {
                identities.push(identity);
              }
            } else {
              coll.set(key, [identity]);
            }
          })
        }
      });

      this._toIndexReverse = coll.value as dataMap;
    }

    return this._toIndexReverse;
  }

  /**
   * for situations in which
   * @param fromIdentity
   * @param toIdentity
   */
  linkVia(fromIdentity: unknown, toIdentity: unknown) {
    if (!this.config.via) {
      throw new Error(`cannot join records for non-intrinsic relationship ${this.name}`);
    }
    const link: JoinPair = { from: fromIdentity, to: toIdentity }
    if (this.intrinsic.hasValue(link)) {
      this.intrinsic.append(link);
      this.purgeIndexes();
    }
  }

  get strategy(): joinStrategy {
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
    const table = direction === 'from' ? this.fromTable : this.toTable;
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

    this.base.trans.do('withBackedUpTables', [this.fromTable.name, this.toTable.name],
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
    if (!(this.fromTable.has(fromIdentity) && this.toTable.has(toIdentity))) {
      throw new Error(`cannot link ${fromIdentity} and ${toIdentity}`);
    }

    switch (this.strategy) {
      case 'field-identity':
        if (isFieldDef(this.fromDef)) {
          this.fromTable.setField(fromIdentity, this.fromDef.field, toIdentity);
        }
        break

      case 'identity-field':
        if (isFieldDef(this.toDef)) {
          this.toTable.setField(toIdentity, this.toDef.field, fromIdentity);
        }
        break;

      case 'identity-identity':
        if (fromIdentity !== toIdentity) {
          throw new Error('cannot link id-id relationships');
        }
        break;

      case 'field-field':
        if (isFieldDef(this.fromDef) && isFieldDef(this.toDef)) {
          const c1 = c(this.fromTable.get(fromIdentity));
          const c2 = c(this.toTable.get(toIdentity));
          if (c1.get(this.fromDef.field) !== c2.get(this.toDef.field)) {
            throw new Error('cannot link field-field relationships');
          }
        }
        break;
    }
  }

  toRecordsFor(identity: unknown): dataMap {

    let midKeys: unknown[];
    if (isJoinIdentityDef(this.fromDef)) {
      if (isJoinIdentityDef(this.toDef)) {
        if (this.toTable.has(identity)) {
          return new Map([[
            identity, this.toTable.get(identity)
          ]]);
        }
      }
      midKeys = [identity]
    } else {
      midKeys = this.fromIndex.get(identity);
    }

    return midKeys.length ? recordsForMidKeys(midKeys, this.toIndexReverse, this.toTable) : emptyMap
  }

  fromIdentities(identity: unknown): unknown[] {
    if (!this.fromIndex.has(identity)) {
      return [];
    }
    const midKeys = this.fromIndex.get(identity);
    let identities = midKeys.map((midId: unknown) => this.toIndexReverse.get(midId) || []).flat();
    const idSet = new Set(identities);
    return Array.from(idSet.values());
  }

  from(identities: unknown[]): identityMap {
    const out = new Map();

    identities.forEach((id) => {
      const identities = this.fromIdentities(id);
      if (identities.length) {
        out.set(id, identities)
      }
    });

    return out;
  }

  toIdentities(identity: unknown): unknown[] {
    if (!this.toIndex.has(identity)) {
      return [];
    }
    const midKeys = this.toIndex.get(identity);
    let identities = midKeys.map((midId: unknown) => this.fromIndexReverse.get(midId) || []).flat();
    const idSet = new Set(identities);
    return Array.from(idSet.values());
  }

  to(identities: unknown[]): identityMap {
    const out = new Map();

    identities.forEach((id) => {
      const toIds = this.toIdentities(id);
      if (toIds.length) {
        out.set(id, toIds)
      }
    });

    return out;
  }

  toRecordsForArray(fromIdentity: unknown): unknown[] {
    const map = this.toRecordsFor(fromIdentity);
    if (!map.size) {
      return [];
    }
    return Array.from(map.values());
  }

  fromRecordsFor(identity: unknown): dataMap {
    let midKeys: unknown[];
    if (isJoinIdentityDef(this.toDef)) {
      if (isJoinIdentityDef(this.fromDef)) {

        if (this.fromTable.has(identity)) {
          return new Map([[
            identity, this.fromTable.get(identity)
          ]]);
        }
      }
      midKeys = [identity]
      // coerce the reversed map of from into records from the "to" table
    } else {
      midKeys = this.toIndex.get(identity);
    }

    return midKeys.length ? recordsForMidKeys(midKeys, this.fromIndexReverse, this.fromTable) : emptyMap
  }

  fromRecordsForArray(toIdentity: unknown): unknown[] {
    const map = this.fromRecordsFor(toIdentity)
    if (!map.size) {
      return [];
    }
    return Array.from(map.values());
  }
}

const emptyMap = new Map()
const emptyColl = c(emptyMap);
