import { TableObj, TypedTableObj } from '../types'
import {
  bobAndSue,
  Gender,
  makeBase,
  StateRecord,
  StateTable,
  UsersRecord,
  UserTable,
  westCoast
} from '../fixtures/users'

describe('Table', () => {
  describe('add()', () => {
    it('applies identityFromRecord to records and rejects bad records', () => {

      const base = makeBase();

      const users = base.table('users') as TypedTableObj<number, UsersRecord>
      expect(users).toBeTruthy()
      if (!users) {
        throw new Error('no users t');
      }
      users.add({ name: 'Bob' });
      expect(users.get(1)?.name)
        .toBe('Bob')

      users.add({ name: 'Sue', id: 100 });
      expect(users.get(100)?.name)
        .toBe('Sue')
      expect(() => {
        users.add({ id: 200, name: 1000 });
      }).toThrow(/nonempty string names/);

      expect(users.has(200)).toBeFalsy();
      expect(users.size).toBe(2);
    });

  });
  describe('update()', () => {
    it('updates records', () => {
      const base = makeBase(bobAndSue, westCoast);

      const users = base.table('users') as UserTable

      users.update(2, { age: 30 });
      const sue = users.get(2);
      expect(sue?.name).toBe('Sue');
      expect(sue?.age).toBe(30);


      expect(users.get(1)?.name).toBe('Bob');

    });

    it('resets on error', () => {
      const base = makeBase(bobAndSue);
      const users = base.table('users') as UserTable

      expect(() => users.update(1, { name: 'Fred' }))
        .toThrow(/cannot rename users/);
    })
  });
  describe('updateMany()', () => {
    it('updates existing records', () => {
      const base = makeBase([], westCoast)

      const stateBase = base.table('states') as StateTable
      stateBase.updateMany([{ abbr: 'CA', label: 'Cali' }, { abbr: 'OR', label: 'Ore' }])

      expect(stateBase.$coll.clone()
        .map((state: StateRecord) => {
          return state.label
        })
        .getReduce((memo: Set<string>, label: string) => {
          memo.add(label);
          return memo;
        }, new Set())
      ).toEqual(new Set(['Washington', 'Cali', 'Ore']))
    });

    it('updates exclusively', () => {
      const base = makeBase([], westCoast)

      const stateBase = base.table('states') as StateTable
      stateBase.updateMany([{ abbr: 'CA', label: 'Cali' }, { abbr: 'OR', label: 'Ore' }], true)

      expect(stateBase.$coll.clone()
        .map((state: StateRecord) => {
          return state.label
        })
        .getReduce((memo: Set<string>, label: string) => {
          memo.add(label);
          return memo;
        }, new Set())
      ).toEqual(new Set(['Cali', 'Ore']))
    });

    it('deletes undefined records', () => {
      const base = makeBase(bobAndSue, westCoast)
      const userTable = base.table('users') as UserTable

      const femaleUsers = userTable.coll.map(
        (record: UsersRecord) => record.gender === Gender.female ? record : undefined
      ).value
      userTable.updateMany(femaleUsers);

      expect(userTable.size).toBe(1);
      expect(userTable.has(2)).toBeTruthy();
    })

    it('resets on error', () => {

      const base = makeBase([], westCoast)

      const stateBase = base.table('states')
      expect(() => {
        stateBase?.updateMany([{ abbr: 'CA', label: 'Cali' }, { abbr: 'OR', label: 100 }])
      }).toThrow()

      expect(stateBase?.$coll.clone()
        .map((state: StateRecord) => state.label)
        .getReduce((memo, label) => {
          memo.add(label);
          return memo;
        }, new Set())).toEqual(new Set(['Washington', 'California', 'Oregon']))
    });

    it('returns values', () => {
      const base = makeBase([], westCoast)

      const stateBase = base.table('states') as StateTable
      let output = stateBase.updateMany([{ abbr: 'CA', label: 'Cali' }, { abbr: 'OR', label: 'Ore' }])

      expect(output).toEqual(new Map([
        ['CA', { abbr: 'CA', label: 'Cali' }],
        ['OR', { abbr: 'OR', label: 'Ore' }]
      ]));
    });

  });
  describe('generate', () => {
    it('updates existing records', () => {
      const base = makeBase([], westCoast)
      const userTable = base.table('users') as UserTable
      userTable.generate(function* gen(table: TableObj) {
        for (const [$identity, value] of table.coll.iter) {
          const record = value as UsersRecord
          if (!('age' in record && record.age && record.age >= 18)) {
            yield { $record: { ...record, age: 18 }, $identity }
          }
        }
      })

      userTable.forEach((record: unknown) => {
        const user = record as UsersRecord
        expect(user.age).toBeGreaterThanOrEqual(18)
      });
    });

    it('generates records exclusive output', () => {
      const base = makeBase(bobAndSue, westCoast)
      const statesTable = base.table('states') as StateTable

      statesTable.generate(function* gen(table: TableObj) {
        const userTable = table.base.table('users') as UserTable
        for (const [_id, value] of userTable.coll.iter) {
          const record = value as UsersRecord
          const identity = record.state
          if (identity && table.has(identity)) {
            const state = table.get(identity)
            yield { $record: state, $identity: identity }
          }
        }
      }, true);

      expect(statesTable.has('OR')).toBeTruthy();
      expect(statesTable.has('CA')).toBeTruthy();
      expect(statesTable.has('WA')).not.toBeTruthy();
    });

  });
  describe('delete', () => {

    it('deletes a record', () => {

      const base = makeBase(bobAndSue, [
          ...westCoast,
          { abbr: 'TX', label: 'Texas' },
          { abbr: 'NY', label: 'New York' }
        ]
      )
      const states = base.table('states') as StateTable
      states.delete('TX')
      expect(new Set(states.coll.keys)).toEqual(new Set(['CA', 'WA', 'OR', 'NY']));
    })

    it('reverts on error', () => {
      const base = makeBase(bobAndSue, westCoast)
      const states = base.table('states') as StateTable

      expect(() => states?.delete('CA'))
        .toThrow(/as it is in/)

      expect(new Set(states.coll.keys)).toEqual(new Set(['CA', 'WA', 'OR']));
    });
  });
});
