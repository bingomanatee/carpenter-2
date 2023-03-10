import {
  BaseObj,
  dataMap, indexMap,
  isFieldDef,
  isJoinIdentityDef,
  JoinDef,
  joinDirection,
  JoinManagerObj,
  JoinObj,
  TableObj
} from './types'
import { addToIndex, emptyMap, extractFieldDef, identitiesForMidKeys, parseConfig } from './joinUtils'

export class JoinManager implements JoinManagerObj {
  constructor(public direction: joinDirection, private base: BaseObj, defSrc: string | JoinDef, private join: JoinObj) {
    this.def = parseConfig(defSrc);
  }

  public def: JoinDef;

  get otherManager() {
    switch (this.direction) {
      case 'from':
        return this.join.to;
      case 'to':
        return this.join.from;

    }
  }

  get otherIndex() {
    return this.otherManager.index
  }

  purgeIndexes() {
    delete this._index;
    delete this._indexReverse;
  }

  private _index?: dataMap
  set index(map: dataMap) {
    this._index = map;
  }

  private _table?: TableObj
  get table(): TableObj {
    if (!this._table) {
      this._table = this.base.table(this.def.table) ?? (() => {
        throw(`cannot find table ${this.def.table}`)
      })()
    }
    return this._table
  }

  get index(): indexMap {
    if (!this._index) {
      if (this.join.isVia) {
        this.join.indexVia();
      } else {
        this._index = this.table.$coll.getMap((record, identity) => {
          let out: any[] = [];
          if (this.isIdentity) {
            return [identity]
          } else if (isFieldDef(this.def)) {
            out = extractFieldDef(this.def, record);
          } // else what?
          return out;
        });
      }
    }
    return this._index || emptyMap
  }

  /**
   * a map of the foreign/mid keys in the other table to the indexes in this one
   */
  _indexReverse?: indexMap
  get indexReverse(): indexMap {
    if (!this._indexReverse) {
      const coll = new Map();
      this.index.forEach((keys, identity) => {
        keys.forEach((key: unknown) => {
          addToIndex(coll, key, identity);
        })
      });

      this._indexReverse = coll
    }

    return this._indexReverse;
  }

  /**
   * reflects whether the records in this collection have a foreign key
   * or are referred to by their identity
   */
  _isIdentity?: boolean
  public get isIdentity() {
    if (typeof this._isIdentity !== 'boolean') {
      this._isIdentity = isJoinIdentityDef(this.def);
    }
    return this._isIdentity
  }

  /**
   * identities in this index that refer to records in the other table
   * @param otherIdentity
   */
  identities(otherIdentity: unknown): unknown[] {

    if (!this.otherIndex.has(otherIdentity)) {
      console.log('no matching ids for ', otherIdentity, 'in', this.table.name);
      return [];
    }

    const midIdentities = this.otherIndex.get(otherIdentity)
    if (this.isIdentity || this.join.isVia) {
      return midIdentities;
    }
    return identitiesForMidKeys(midIdentities, this.indexReverse);
  }

  /**
   * records in this table that match an identity in the other table
   *
   * @param otherIdentity
   */
  records(otherIdentity: unknown): unknown[] {
    const myIds = this.identities(otherIdentity);
    return myIds.map((identity: unknown) => this.table.get(identity));
  }
}
