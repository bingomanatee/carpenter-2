import { BaseObj, TableItem, TypedTableItem } from '../types'
import { Gender, makeBase, manyAddresses, manyPeople, UsersRecord, westCoast, } from '../fixtures/users'
import { TableItemClass } from '../TableItemClass'
import { makeCollegeBase } from '../fixtures/teachers'

const fs = require('fs');
const shouldAllowDeepJoins = JSON.parse(
  fs.readFileSync(__dirname + '/../fixtures/expect/Query/shouldAllowDeepJoins.json').toString()
);

const shouldAllowDeepJoinsOtherDirection = JSON.parse(
  fs.readFileSync(__dirname + '/../fixtures/expect/Query/shouldAllowDeepJoinsOtherDirection.json').toString()
);

const women = (tr: TableItem) => {
  const user = tr.val as UsersRecord;
  return user.gender === Gender.female
}
const oldPeople = (tr: TableItem) => {
  const user = tr.val as UsersRecord;
  return 'age' in user && Number(user.age) > 20
}

describe('Queries', () => {
  let base: BaseObj;
  beforeEach(() => {
    base = makeBase(manyPeople, westCoast, manyAddresses);
  });

  describe('selectors', () => {

    describe('map', () => {

      it('should set the full name', () => {
        const query = base.query({
          table: 'users',
          sel: (tr: TableItem) => {
            const utr = tr as TypedTableItem<UsersRecord, number>
            return ({
              ...utr.val, fullName: [utr.val.name,
                utr.val.last].join(' ')
            })
          }
        });
        const fullNames = query.value.map((tr: TableItem) => {
          const value = tr.val as Record<string, any>
          return value.fullName;
        });
        expect(fullNames).toEqual([
          'Alpha Jones',
          'Beta Smith',
          'Gamma Jones',
          'Delta Smith',
          'Epsilon Smith',
          'Zeta Jones',
          'Eta Smith',
          'Iota Jones',
          'Kappa Smith',
          'Lambda Jones'
        ]);
      });
    });

    describe('chooser', () => {


      describe('filter', () => {
        it('should filter with gender', () => {
          const query = base.query({
            table: 'users',
            sel: {
              filter: women
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual(
            [
              102,
              104,
              108
            ]
          );
        });
        it('should filter with age', () => {
        });
      });
      describe('from', () => {
        it('should return the ids of the earlier items', () => {
          const query = base.query({
            table: 'users',
            sel: {
              from: 3
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual([
            103, 104, 105, 106, 107, 108, 109
          ]);
        });
        it('accepts from with a filter', () => {
          const query = base.query({
            table: 'users',
            sel: {
              filter: women,
              from: 3
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual([
            104, 108,
          ]);
        });
      });

      describe('until', () => {
        it('should return the ids of the first items', () => {
          const query = base.query({
            table: 'users',
            sel: {
              until: 6
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual([
            100, 101, 102, 103, 104, 105,
          ]);
        });

        it('accepts until with a filter', () => {
          const query = base.query({
            table: 'users',
            sel: {
              filter: women,
              until: 6
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual([
            102, 104,
          ]);
        });

        it('accepts from and until', () => {
          const query = base.query({
            table: 'users',
            sel: {
              from: 3,
              until: 6
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual([
            103, 104, 105,
          ]);
        })
      });

      describe('count', () => {
        it('should return the ids of first items', () => {
          const query = base.query({
            table: 'users',
            sel: {
              count: 4
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual([
            100, 101, 102, 103,
          ]);
        });

        it('accepts until with a filter', () => {
          const query = base.query({
            table: 'users',
            sel: {
              filter: women,
              count: 2
            }
          });

          const ids = query.value.map(tr => tr.identity);
          expect(ids).toEqual([
            102, 104,
          ]);
        });
      });
    });

    describe('sorter', () => {
      it('should sort  and limit records', () => {
        const query = base.query({
          table: 'users',
          sel: {
            sort: (t1: TableItem, t2: TableItem) => {
              const u1 = t1.val as UsersRecord;
              const u2 = t2.val as UsersRecord;

              if (u1.gender === u2.gender) {
                return 0;
              }
              return u1.gender === Gender.male ? 1 : -1;
            }
          }
        });

        const users = query.value.map(tr => {
          const user = tr.val as UsersRecord;
          return { id: user.id, gender: user.gender };
        });

        expect(users).toEqual([
          { id: 102, gender: 'female' },
          { id: 104, gender: 'female' },
          { id: 108, gender: 'female' },
          { id: 100, gender: 'male' },
          { id: 101, gender: 'male' },
          { id: 103, gender: 'male' },
          { id: 105, gender: 'male' },
          { id: 106, gender: 'male' },
          { id: 107, gender: 'male' },
          { id: 109, gender: 'male' }
        ]);
      });
    });

    describe('combined selectors', () => {
      it('should sort and limit records', () => {
        const query = base.query({
          table: 'users',
          sel: [{
            sort: (t1: TableItem, t2: TableItem) => {
              const u1 = t1.val as UsersRecord;
              const u2 = t2.val as UsersRecord;

              if (u1.gender === u2.gender) {
                return 0;
              }
              return u1.gender === Gender.male ? 1 : -1;
            }
          }, { count: 6 }]
        });

        const users = query.value.map(tr => {
          const user = tr.val as UsersRecord;
          return { id: user.id, gender: user.gender };
        });

        expect(users).toEqual([
          { id: 102, gender: 'female' },
          { id: 104, gender: 'female' },
          { id: 108, gender: 'female' },
          { id: 100, gender: 'male' },
          { id: 101, gender: 'male' },
          { id: 103, gender: 'male' },
        ]);
      });
      it('should limit and sort records', () => {
        const query = base.query({
          table: 'users',
          sel: [
            { count: 6 },
            {
              sort: (t1: TableItem, t2: TableItem) => {
                const u1 = t1.val as UsersRecord;
                const u2 = t2.val as UsersRecord;

                if (u1.gender === u2.gender) {
                  return 0;
                }
                return u1.gender === Gender.male ? 1 : -1;
              }
            }]
        });

        const users = query.value.map(tr => {
          const user = tr.val as UsersRecord;
          return { id: user.id, gender: user.gender };
        });

        expect(users).toEqual([
            { id: 102, gender: 'female' },
            { id: 104, gender: 'female' },
            { id: 100, gender: 'male' },
            { id: 101, gender: 'male' },
            { id: 103, gender: 'male' },
            { id: 105, gender: 'male' }
          ]
        );
      });
      it('should limit, sort, and flter records', () => {
        const query = base.query({
          table: 'users',
          sel: [
            { count: 6 },
            {
              sort: (t1: TableItem, t2: TableItem) => {
                const u1 = t1.val as UsersRecord;
                const u2 = t2.val as UsersRecord;

                if (u1.gender === u2.gender) {
                  return 0;
                }
                return u1.gender === Gender.male ? 1 : -1;
              }
            },
            {
              filter: oldPeople
            }
          ]
        });

        const users = query.value.map(tr => {
          const user = tr.val as UsersRecord;
          return { id: user.id, gender: user.gender, age: user.age };
        });

        expect(users).toEqual([
          { id: 102, gender: 'female', age: 40 },
          { id: 104, gender: 'female', age: 30 },
          { id: 101, gender: 'male', age: 30 },
          { id: 105, gender: 'male', age: 40 }
        ]);
      });
    });

  });

  describe('joins', () => {
    it('should linkVia related records', () => {
      const query = base.query({
        table: 'users',
        joins: [{ joinName: 'userAddresses' }],
        sel: { count: 4 }
      });

      const values = query.value.map(TableItemClass.toJSON);

      expect(values).toEqual(
        [{
          "table": "users",
          "val": {
            "name": "Alpha",
            "gender": "male",
            "id": 100,
            "last": "Jones",
            "age": 20,
            "state": "CA",
            "address": 210
          },
          "id": 100,
          "$": {
            "userAddresses": [{
              "table": "addresses",
              "val": {
                "id": 210,
                "address": "1000 First Avenue",
                "city": "San Francisco",
                "state": "CA",
                "zip": 11135
              },
              "id": 210
            }]
          }
        }, {
          "table": "users",
          "val": { "name": "Beta", "gender": "male", "id": 101, "last": "Smith", "age": 30, "state": "CA" },
          "id": 101
        }, {
          "table": "users",
          "val": {
            "name": "Gamma",
            "gender": "female",
            "id": 102,
            "last": "Jones",
            "age": 40,
            "state": "OR",
            "address": 290
          },
          "id": 102,
          "$": {
            "userAddresses": [{
              "table": "addresses",
              "val": {
                "id": 290,
                "address": "44 Los Angeles Dr",
                "city": "Simi Valley",
                "state": "CA",
                "zip": 93065
              },
              "id": 290
            }]
          }
        }, {
          "table": "users",
          "val": {
            "name": "Delta",
            "gender": "male",
            "id": 103,
            "last": "Smith",
            "age": 20,
            "state": "CA",
            "address": 280
          },
          "id": 103,
          "$": {
            "userAddresses": [{
              "table": "addresses",
              "val": { "id": 280, "address": "400 4th Ave", "city": "Portland", "state": "OR", "zip": 97205 },
              "id": 280
            }]
          }
        }]);
    });

    it('should linkVia in other direction', () => {
      const query = base.query({
        table: 'states',
        joins: [{ joinName: 'addressStates' }]
      });

      const values = query.value.map(TableItemClass.toJSON);

      expect(values).toEqual(
        [{
          "table": "states",
          "val": { "abbr": "CA", "label": "California" },
          "id": "CA",
          "$": {
            "addressStates": [{
              "table": "addresses",
              "val": {
                "id": 210,
                "address": "1000 First Avenue",
                "city": "San Francisco",
                "state": "CA",
                "zip": 11135
              },
              "id": 210
            }, {
              "table": "addresses",
              "val": {
                "id": 230,
                "address": "1010 Simi Valley Dr",
                "city": "Simi Valley",
                "state": "CA",
                "zip": 93065
              },
              "id": 230
            }, {
              "table": "addresses",
              "val": {
                "id": 240,
                "address": "666 Buffalo Avenue",
                "city": "San Francisco",
                "state": "CA",
                "zip": 11135
              },
              "id": 240
            }, {
              "table": "addresses",
              "val": {
                "id": 260,
                "address": "444 Simi Valley Dr",
                "city": "Simi Valley",
                "state": "CA",
                "zip": 93065
              },
              "id": 260
            }, {
              "table": "addresses",
              "val": {
                "id": 270,
                "address": "1000 Harrison St",
                "city": "San Francisco",
                "state": "CA",
                "zip": 98403
              },
              "id": 270
            }, {
              "table": "addresses",
              "val": {
                "id": 290,
                "address": "44 Los Angeles Dr",
                "city": "Simi Valley",
                "state": "CA",
                "zip": 93065
              },
              "id": 290
            }]
          }
        }, {
          "table": "states",
          "val": { "abbr": "OR", "label": "Oregon" },
          "id": "OR",
          "$": {
            "addressStates": [{
              "table": "addresses",
              "val": { "id": 220, "address": "3317 NE 15th", "city": "Portland", "state": "OR", "zip": 97205 },
              "id": 220
            }, {
              "table": "addresses",
              "val": {
                "id": 250,
                "address": "101 Multnomah Avenue",
                "city": "Portland",
                "state": "OR",
                "zip": 97205
              },
              "id": 250
            }, {
              "table": "addresses",
              "val": { "id": 280, "address": "400 4th Ave", "city": "Portland", "state": "OR", "zip": 97205 },
              "id": 280
            }]
          }
        }, { "table": "states", "val": { "abbr": "WA", "label": "Washington" }, "id": "WA" }]
      );
    });

    it('should allow deep $', () => {
      const query = base.query({
        table: 'users',
        joins: [{ joinName: 'userAddresses', joins: [{ joinName: 'addressStates' }] }],
        sel: { count: 4 }
      });

      const values = query.toJSON
      // console.log('deep $ are ', JSON.stringify(values, undefined, 4));
      expect(values).toEqual(
        shouldAllowDeepJoins
      );
    });

    it('should deep link in other direction', () => {
      const query = base.query({
        table: 'states',
        joins: [{
          joinName: 'addressStates',
          joins: [
            { joinName: 'userAddresses' }
          ]
        }]
      });
      expect(query.toJSON).toEqual(
        shouldAllowDeepJoinsOtherDirection
      );
    });

    describe('link', () => {

      it('should allow you to link in new data', () => {

        const base = makeBase([{ id: 1, gender: Gender.male, name: 'Bob' }], westCoast, []);
        const userTable = base.table('users');
        if (!userTable) {
          throw new Error('no user table');
        }

        userTable.join(1, {
          tableName: 'addresses',
          data: {
            id: 10,
            address: '1000 11th Ave',
            city: 'Seattle',
            state: 'WA',
            zip: 10133
          }
        });

        const userWithAddr = base.query({
          table: 'users',
          joins: [{ joinName: 'userAddresses' }]
        });

        expect(userWithAddr.toJSON).toEqual([{
            "table": "users",
            "val": { "name": "Bob", "gender": "male", "id": 1, "address": 10 },
            "id": 1,
            "$": {
              "userAddresses": [{
                "table": "addresses",
                "val": { "id": 10, "address": "1000 11th Ave", "city": "Seattle", "state": "WA", "zip": 10133 },
                "id": 10
              }]
            }
          }]
        );
      });

      it('should allow via links', () => {

        const base = makeCollegeBase();

        const bobId = btoa('Bob');
        const physId = 'PHYS101';

        base.table('students')?.join(bobId, {
          identity: physId,
          tableName: 'classes'
        });

        const studentClasses = base.table('students')?.query({
          sel: [{ filter: (tableItem) => tableItem.identity === bobId }],
          joins: [{ joinName: 'studentsToClasses' }]
        });

        expect(studentClasses?.toJSON).toEqual([{
            "table": "students",
            "val": { "name": "Bob", "studentId": "Qm9i" },
            "id": "Qm9i",
            "$": {
              "studentsToClasses": [{
                "table": "classes",
                "val": { "name": "Physics", "program": "Science", "id": "PHYS101" },
                "id": "PHYS101"
              }]
            }
          }]
        );
      });
    });
  });
});
