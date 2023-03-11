import {
  BaseObj,
  dataMap,
  isFieldDef,
  isScalar,
  JoinConfig,
  joinDirection,
  JoinObj,
  JoinObjType,
  joinStrategy, TableItemJSON
} from './types'
import { c } from '@wonderlandlabs/collect'
import { JoinManager } from './JoinManager'
import { addToIndex } from './joinUtils'
import { TableItemClass } from './TableItemClass'


// @TODO: put from / to relationships into manager classes

export default class Join implements JoinObj {
  constructor(private config: JoinConfig, private base: BaseObj) {
    this.from = new JoinManager('from', base, this.config.from, this);
    this.to = new JoinManager('to', base, this.config.to, this);
  }

  $type: JoinObjType = 'JoinObj';

  private _name?: string
  get name() {
    if (this.config.name) {
      return this.config.name
    }
    if (!this._name) {
      this._name = [this.from.def.table, this.to.def.table].sort().join(':');
    }
    return this._name;
  }

  purgeIndexes() {
    this.from.purgeIndexes();
    this.to.purgeIndexes();
  }

  get isVia() {
    return !!this.config.via;
  }

  get strategy(): joinStrategy {
    if (this.isVia) {
      return 'identity-via-identity';
    }
    if (isFieldDef(this.from.def)) {
      if (isFieldDef(this.to.def)) {
        return 'field-field';
      }
      return 'field-identity';
    } else if (isFieldDef(this.to.def)) {
      return 'identity-field'
    }
    return 'identity-identity'
  }

  detach(ref1: TableItemJSON, ref2: TableItemJSON, second?: boolean) {
    if (this.from.table.name === ref1.t && this.to.table.name === ref2.t) {
      this.detachIdentities(ref1.id, ref2.id);
    } else if (!second) {
      this.detach(ref2, ref1, true);
    }
  }

  detachIdentities(fromIdentity: unknown, toIdentity: unknown) {
    if (this.isVia) {
      if (this.viaTable) {
        for (const [identity, data] of this.viaTable.$coll.iter) {
          const coll = c(data);
          if (coll.get(this.fromTableName) === fromIdentity
            && coll.get(this.toTableName) === toIdentity
          ) {
            this.viaTable.delete(identity);
            break;
          }
        }
      } else {
        if (isFieldDef(this.from.def)) {
          this.from.table.setField(fromIdentity, this.from.def.field, undefined);
        }
        if (isFieldDef(this.to.def)) {
          this.to.table.setField(toIdentity, this.to.def.field, undefined);
        }
      }
    }
  }

  private _ftn?: string;
  private _ttn?: string;

  get toTableName() {
    if (!this._ttn) {
      this._ttn = this.to.table.name;
    }
    return this._ttn;
  }

  get fromTableName() {
    if (!this._ftn) {
      this._ftn = this.from.table.name;
    }
    return this._ftn;
  }

  linkManyVia(identity: unknown,
              toIdentities: unknown[],
              otherDirection: joinDirection) {
    const existing: unknown[] = ((otherDirection === 'from') ? this.from.index.get(identity)
      : this.to.index.get(identity)) || []
    const unsetOtherIds = toIdentities.filter((id) => !existing.includes(id));
    const newData = otherDirection === 'from' ?
      unsetOtherIds.map(
        (otherId) => {
          return {
            [this.toTableName]: identity,
            [this.fromTableName]: otherId
          }
        })
      :
      unsetOtherIds.map(
        (otherId) => {
          return {
            [this.toTableName]: otherId,
            [this.fromTableName]: identity
          }
        });
      this.viaTable?.updateMany(newData);
  }

  linkMany(identity: unknown,
           otherIdentities: unknown[],
           otherDirection: joinDirection) {
    const otherTable = otherDirection === 'from' ? this.from.table : this.to.table;
    const otherConfig = otherDirection === 'from' ? this.from.def : this.to.def;
    if (this.isVia) {
      return this.linkManyVia(identity, otherIdentities, otherDirection);
    }
    // only works when remote table is foreign key
    if (!isFieldDef(otherConfig)) {
      throw new Error('linkMany must target field type');
    }
    const fieldName = otherConfig.field;

    const dataRecord = otherIdentities.reduce((memo: dataMap, otherIdentity) => {
      if (!otherTable.has(otherIdentity)) {
        return memo;
      }
      const record = otherTable.get(otherIdentity);
      memo.set(otherIdentity, c(record).set(fieldName, identity).value);
      return memo;
    }, new Map());

    this.base.trans.do('updateMany', otherTable, dataRecord);
  }

  link(fromIdentity: unknown, toIdentity: unknown) {
    if (!(this.from.table.has(fromIdentity) && this.to.table.has(toIdentity))) {
      throw new Error(`cannot link ${fromIdentity} and ${toIdentity} -- not in tables`);
    }

    switch (this.strategy) {
      case 'field-identity':
        if (isFieldDef(this.from.def)) {
          this.from.table.setField(fromIdentity, this.from.def.field, toIdentity);
        }
        break

      case 'identity-field':
        if (isFieldDef(this.to.def)) {
          this.to.table.setField(toIdentity, this.to.def.field, fromIdentity);
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
        if (isFieldDef(this.from.def) && isFieldDef(this.to.def)) {
          const c1 = c(this.from.table.get(fromIdentity));
          const c2 = c(this.to.table.get(toIdentity));
          if (c1.get(this.from.def.field) !== c2.get(this.to.def.field)) {
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
    const toTableName = this.to.table.name;
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
        if (!(data && typeof data === 'object' && self.from.table.name in data && self.to.table.name in data)) {
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
      const toTableName = this.to.table.name;
      const fromTableName = this.from.table.name;
      this.viaTable?.$set(viaIdentity, { [fromTableName]: fromIdentity, [toTableName]: toIdentity })
    }
  }

  public indexVia() {
    const fromMap = new Map();
    const toMap = new Map();
    const fromKey = this.from.table.name;
    const toKey = this.to.table.name;
    if (this.viaTable) {
      for (const [_id, keys] of this.viaTable.$coll.iter) {
        const viaItem = keys as Record<string, unknown>;
        const { [fromKey]: fromIndex, [toKey]: toIndex } = viaItem;

        addToIndex(fromMap, fromIndex, toIndex);
        addToIndex(toMap, toIndex, fromIndex);
      }
    }
    this.from.index = fromMap;
    this.to.index = toMap;
  }

  from: JoinManager
  to: JoinManager
}

