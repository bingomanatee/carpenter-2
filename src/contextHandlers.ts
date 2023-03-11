import { transObj } from '@wonderlandlabs/transact/dist/types'
import {
  BaseObj,
  dataMap,
  isJoinTermDataBase, isJoinTermDataPairsBase,
  isJoinTermDatasBase, isJoinTermIdentityBase,
  isJoinTermIdentityDataBase, JoinObj,
  JoinPart,
  JoinTerm, TableItemJSON,
  TableObj
} from './types'
import { c } from '@wonderlandlabs/collect';

function toError(err: unknown, defaultMessage = 'error') {
  if (err instanceof Error) {
    return err;
  }
  if (typeof err === 'string') {
    return new Error(err);
  }
  return Object.assign(new Error(defaultMessage), { content: err })
}

const restoreRecord = (_err: unknown, trans: transObj) => {
  if (trans.meta.get('changed')) {
    const table: TableObj = trans.meta.get('table');
    const identity: unknown = trans.meta.get('identity');
    if (!trans.meta.get('recordExists')) {
      table.$coll.deleteKey(identity)
    } else {
      table.$set(identity, trans.meta.get('previous'))
    }
    table.purgeIndexes()
  }
  throw _err;
}

/**
 * note - until the 'changed' meta is true, this data will not be restored on error
 * @param trans
 * @param table
 * @param identity
 */
const backupRecord = (trans: transObj, table: TableObj, identity: unknown) => {
  trans.meta.set('table', table)
  const previous = table.get(identity)
  trans.meta.set('identity', identity);
  trans.meta.set('previous', previous);
}

const backUpTable = (trans: transObj, table: TableObj | TableObj[]) => {
  if (!Array.isArray(table)) {
    backUpTable(trans, [table]);
  } else {
    trans.meta.set('backedUpTables', table.map((table) => {
      return {
        table,
        records: c(table.$records).clone().value
      }
    }));
  }
}
const restoreTable = (_err: unknown, trans: transObj) => {
  const backups = trans.meta.get('backedUpTables')
  if (Array.isArray(backups)) {
    backups.forEach(({ table, records }) => {
      table.$records = records;
      table.purgeIndexes();
    });
  }
  throw _err;
}
export const contextHandlers = ((base: BaseObj) => {
    return {
      add: [
        (trans: transObj, table: TableObj, data: unknown, identity?: unknown, replace?: boolean) => {
          let record: unknown;
          if (identity === undefined) {
            record = table.dataToRecord(data);
            identity = table.identityFor(record);
            if (identity === undefined) {
              throw Object.assign(new Error('add: cannot add unidentified record to t ' + table.name),
                {
                  data,
                  table: table.name
                }
              );
            }
          }

          if (table.has(identity) && !replace) {
            throw Object.assign(new Error('add: cannot assert data over existing identityFromRecord unless replace = true'), {
              identity,
              table: table.name
            })
          }
          if (record === undefined) {
            record = table.dataToRecord(data, identity);
          }
          trans.meta.set('recordExists', table.has(identity));
          backupRecord(trans, table, identity)
          trans.transactionSet.do('validateRecord', record, identity, table, trans.meta.get('previous'));
          trans.meta.set('changed', true);
          table.$set(identity, record);
          trans.transactionSet.do('validateTable', table);
          table.purgeIndexes();
        },
        restoreRecord
      ],
      delete: [
        (trans: transObj, identity: unknown, table: TableObj) => {
          if (!table.has(identity)) {
            return;
          }
          backUpTable(trans, table);

          table.$coll.deleteKey(identity);
          trans.transactionSet.do('validateTable', table);
          table.purgeIndexes();
        },
        restoreTable
      ],
      update: [
        (trans: transObj, table: TableObj, identity: unknown, data: unknown, upsert: boolean) => {
          if (!(upsert || table.has(identity))) {
            throw Object.assign(new Error('update: record does not exist'),
              {
                identity,
                table: table.name
              })
          }
          const record = table.dataToRecord(data, identity);
          trans.meta.set('recordExists', table.has(identity));
          backupRecord(trans, table, identity)
          trans.transactionSet.do('validateRecord', record, identity, table, trans.meta.get('previous'));
          trans.meta.set('changed', true);
          table.$set(identity, record);
          trans.transactionSet.do('validateTable', table);
          table.purgeIndexes();

        },
        restoreRecord
      ],
      updateMany: [
        (trans: transObj, data: unknown[] | dataMap, table: TableObj, replace = false) => {
          let recordMap: dataMap // identity - data
          if (Array.isArray(data)) {
            recordMap = new Map();
            data.forEach(data => {
              const record = table.dataToRecord(data);
              const identity = table.identityFor(record);
              recordMap.set(identity, record);
            })
          } else { // is a data map
            recordMap = c(data)
              .getMap((data, identity) => {
                if (data === undefined) {
                  return undefined;
                }
                return table.dataToRecord(data, identity)
              });
          }
          backUpTable(trans, table)
          const definedRecordMap = c(recordMap).filter((value) => value !== undefined).value
          trans.transactionSet.do('validateRecords', definedRecordMap, table);
          if (replace) {
            table.$records = definedRecordMap;
          } else {
            recordMap.forEach((record, identity) => {
              if (record === undefined) {
                table.$coll.deleteKey(identity)
              } else {
                table.$coll.set(identity, record);
              }
            })
          }
          trans.meta.set('changed', true);
          trans.transactionSet.do('validateTable', table);
          table.purgeIndexes();
          return recordMap;
        },
        restoreTable
      ],
      withBackedUpTables: [
        (trans: transObj, tableNames: string | string[], action: () => unknown) => {
          const list = Array.isArray(tableNames) ? tableNames : [tableNames];
          const tables = list.reduce((memo: TableObj[], name: string) => {
            let newTable = base.table(name);
            if (newTable) {
              memo.push(newTable);
            }
            return memo;
          }, []);
          backUpTable(trans, tables);
          action();
        },
        restoreTable,
      ],
      validateRecord(trans: transObj, record: unknown, identity: unknown, table: TableObj) {
        const error = table.$testRecord(record, identity);
        if (error) {
          throw toError(error);
        }
      },
      validateRecords(trans: transObj, records: dataMap, table: TableObj) {
        records.forEach((record, identity) => {
          const error = table.$testRecord(record, identity);
          if (error) {
            throw toError(error);
          }
        });
      },
      validateTable(trans: transObj, table: TableObj) {
        const err = table.$testTable();
        if (err) {
          throw toError(err);
        }
      },

      setField(trans: transObj, table: TableObj, identity: unknown, field: unknown, value: unknown) {
        if (!table.has(identity)) {
          throw new Error(`updateField cannot get identity ${identity}from table ${table.name}`);
        }
        const newColl = c(table.get(identity)).clone().set(field, value);
        trans.transactionSet.do('update', table, identity, newColl.value);
        // @TODO: be more selective
        table.purgeIndexes();
      },

      join(trans: transObj, table: TableObj, identity: unknown, term: JoinTerm) {
        if (!table.has(identity)) {
          throw new Error(`join: no identity ${identity} in ${table.name}`);
        }

        let remoteTable: TableObj
        let joinPart: JoinPart | null
        joinPart = table.$joinFromTerm(term)[0]

        if (!joinPart) {
          throw Object.assign(new Error('cannot linkVia with term'), { term, table, identity });
        }
        trans.transactionSet.do(
          'withBackedUpTables',
          [joinPart.join.from.table.name, joinPart.join.to.table.name],
          () => {
            if (!joinPart) {
              throw Object.assign(new Error('cannot linkVia with term'), { term, table, identity });
            }
            remoteTable = joinPart.direction === 'to' ? joinPart.join.from.table : joinPart.join.to.table
            let remoteIdentity: unknown = undefined;

            if (isJoinTermIdentityDataBase(term)) {
              /**
               * creating a record from data, WITH KNOWN IDENTITY, then joining to it
               */
              remoteIdentity = term.identity;
              remoteTable.add(term.data, term.identity);
            } else if (isJoinTermDataBase(term)) {
              /**
               * creating a record from data, generating identity from data, then joining to it
               */

              remoteIdentity = remoteTable.identityFor(term.data);
              remoteTable.add(term.data, remoteIdentity);
            } else if (isJoinTermIdentityBase(term)) {
              /**
               * getting an existing record and joining to it
               */
              remoteIdentity = term.identity;
              if (!remoteTable.has(term.identity)) {
                throw new Error(`cannot join -- table ${remoteTable.name} has no identity ${isJoinTermIdentityBase}`);
              }
            } else if (isJoinTermDatasBase(term)) {
              joinPart.join.linkMany(identity, term.datas, joinPart.direction);
              joinPart.join.purgeIndexes();
              return;
            } else if (isJoinTermDataPairsBase(term)) {
              joinPart.join.linkMany(identity, term.dataPairs, joinPart.direction, true);
              joinPart.join.purgeIndexes();
              return;
            } else {
              throw new Error('bad term');
            }

            if (joinPart.direction === 'from') {
              joinPart.join.link(identity, remoteIdentity);
            } else {
              joinPart.join.link(remoteIdentity, identity);
            }
            joinPart.join.purgeIndexes();
          });

      },

      detach(trans: transObj, ref1: TableItemJSON, ref2: TableItemJSON, joinName?: string) {
        const t1 = base.table(ref1.t);
        const t2 = base.table(ref2.t);
        if (! (t1 && t2) ) {
          throw new Error('detach: bad tables');
        }
        if (joinName) {
          const join = base.getJoin(joinName);
          if (!join) throw new Error('cannot detach: no join named ' + joinName);
          join.detach(ref1, ref2);
        } else {
          const joins = new Set<JoinObj>();
          t1.joins.forEach((part) => joins.add(part.join));
          t2.joins.forEach((part) => joins.add(part.join));

          joins.forEach((join) => join.detach(ref1, ref2));
        }
      }
    }
  }
)
