import {
  BaseObj,
  dataMap,
  isFieldDef,
  isJoinIdentityDef,
  isKeygenDef,
  JoinConfig,
  JoinDef, JoinFieldDef, JoinKeygenDef,
  JoinObj,
  JoinPair,
  TableObj
} from './types'
import { collectObj } from '@wonderlandlabs/collect/lib/types'
import { c } from '@wonderlandlabs/collect'

function inverse(midKeys: unknown[], reverseIndex: dataMap, table: TableObj) {
  const map = new Map();
  midKeys.forEach((midKey) => {
    if (!reverseIndex.has(midKey)) {
      return;
    }
    const endIdentity = reverseIndex.get(midKey);

    endIdentity.forEach((endId: unknown) => {
      if (!map.has(endId) && table.has(endId)) {
        map.set(endId, table.get(endId));
      }
    })

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
    if (isDone) {
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

  constructor(private config: JoinConfig, private base: BaseObj) {
  }

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

  get fromColl() {
    return this.fromTable?.$coll || emptyColl
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
   * fromIndex is a map of the record identity to the exposed key
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
  join(fromIdentity: unknown, toIdentity: unknown) {
    if (!this.config.intrinsic) {
      throw new Error(`cannot join records for non-intrinsic relationship ${this.name}`);
    }
    const link: JoinPair = { from: fromIdentity, to: toIdentity }
    if (this.intrinsic.hasValue(link)) {
      this.intrinsic.append(link);
      this.purgeIndexes();
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

    return midKeys.length ? inverse(midKeys, this.toIndexReverse, this.toTable) : emptyMap
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

    return midKeys.length ? inverse(midKeys, this.fromIndexReverse, this.fromTable) : emptyMap
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