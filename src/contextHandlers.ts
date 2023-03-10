import { transObj } from '@wonderlandlabs/transact/dist/types'
import {
  BaseObj,
  dataMap,
  isJoinTermDataBase, isJoinTermDataPairsBase,
  isJoinTermDatasBase, isJoinTermIdentityBase,
  isJoinTermIdentityDataBase,
  JoinItem,
  JoinTerm,
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

const revertRecordChange = (_err: unknown, trans: transObj) => {

  if (trans.meta.get('changed')) {
    const table: TableObj = trans.meta.get('table');
    const identity: unknown = trans.meta.get('identity');
    if (!trans.meta.get('recordExists')) {
      table.$coll.deleteKey(identity)
    } else {
      table.$set(identity, trans.meta.get('previous'))
    }
  }
  throw _err;
}


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
    });
  }
  throw _err;
}
export const contextHandlers = ((base: BaseObj) => {
    let suspended: transObj[] = [];

    function addSuspend(trans: transObj) {
      trans.meta.set('tablesToValidate', new Set());
      trans.meta.set('recordsToValidate', new Set());
      suspended.push(trans);
    }

    return {
      add: [
        (trans: transObj, table: TableObj, data: unknown, identity?: unknown, replace?: boolean) => {
          let record: unknown;
          if (identity === undefined) {
            record = table.processData(data);
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
            record = table.processData(data, identity);
          }
          trans.meta.set('recordExists', table.has(identity));
          backupRecord(trans, table, identity)
          trans.transactionSet.do('validateRecord', record, identity, table, trans.meta.get('previous'));
          table.$set(identity, record);
          trans.meta.set('changed', true);
          trans.transactionSet.do('validateTable', table);
        },
        revertRecordChange
      ],
      delete: [
        (trans: transObj, identity: unknown, table: TableObj) => {
          if (!table.has(identity)) {
            return;
          }
          backUpTable(trans, table);

          table.$coll.deleteKey(identity);
          trans.transactionSet.do('validateTable', table);
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
          const record = table.processData(data, identity);
          trans.meta.set('recordExists', table.has(identity));
          backupRecord(trans, table, identity)
          trans.transactionSet.do('validateRecord', record, identity, table, trans.meta.get('previous'));
          table.$set(identity, record);
          trans.meta.set('changed', true);
          trans.transactionSet.do('validateTable', table);
        },
        revertRecordChange
      ],
      updateMany: [
        (trans: transObj, data: unknown[] | dataMap, table: TableObj, replace = false) => {
          let recordMap: dataMap
          if (Array.isArray(data)) {
            recordMap = new Map();
            data.forEach(data => {
              const record = table.processData(data);
              const identity = table.identityFor(record);
              recordMap.set(identity, record);
            })
          } else {
            recordMap = c(data)
              .getMap((data, identity) => {
                if (data === undefined) {
                  return undefined;
                }
                return table.processData(data, identity)
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
      },

      join: [(trans: transObj, table: TableObj, identity: unknown, term: JoinTerm) => {
        if (!table.has(identity)) {
          throw new Error(`join: no identity ${identity} in ${table.name}`);
        }

        let remoteTable: TableObj
        let joinItem: JoinItem | null

        joinItem = table.$joinFromTerm(term)
        if (!joinItem) {
          throw Object.assign(new Error('cannot linkVia with term'), { term, table, identity });
        }

        remoteTable = joinItem.direction === 'to' ? joinItem.join.from.table : joinItem.join.to.table

        let remoteIdentity: unknown = undefined;

        backUpTable(trans, remoteTable);
        if (isJoinTermIdentityDataBase(term)) {
          remoteIdentity = term.identity;
          remoteTable.add(term.data, term.identity);

          if (joinItem.direction === 'from') {
            joinItem.join.link(identity, remoteIdentity);
          } else {
            joinItem.join.link(remoteIdentity, identity);
          }
        } else if (isJoinTermDataBase(term)) {
          remoteIdentity = remoteTable.identityFor(term.data);
          remoteTable.add(term.data, remoteIdentity);

          if (joinItem.direction === 'from') {
            joinItem.join.link(identity, remoteIdentity);
          } else {
            joinItem.join.link(remoteIdentity, identity);
          }
        } else if (isJoinTermIdentityBase(term)) {
          remoteIdentity = term.identity;
          if (!remoteTable.has(term.identity)) {
            throw new Error(`cannot join -- table ${remoteTable.name} has no identity ${isJoinTermIdentityBase}`);
          }

          if (joinItem.direction === 'from') {
            joinItem.join.link(identity, remoteIdentity);
          } else {
            joinItem.join.link(remoteIdentity, identity);
          }
        } else if (isJoinTermDatasBase(term)) {
          joinItem.join.linkMany(identity, term.datas, joinItem.direction);
        } else if (isJoinTermDataPairsBase(term)) {
          joinItem.join.linkMany(identity, term.dataPairs, joinItem.direction, true);
        } else {
          throw new Error('bad term');
        }
      },
        restoreTable
      ]
    }
  }
)
