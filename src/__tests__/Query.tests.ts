import { BaseObj, TableItem, TypedTableItem } from '../types'
import { Gender, makeBase, manyPeople, UsersRecord, westCoast, } from '../testFixtures'
const women = (tr: TableItem) => {
  const user = tr.value as UsersRecord;
  return user.gender === Gender.female
}
const oldPeople = (tr: TableItem) => {
  const user = tr.value as UsersRecord;
  return 'age' in user && Number(user.age) > 20
}

describe('Queries', () => {
  let base: BaseObj;
  beforeEach(() => {
    base = makeBase(manyPeople, westCoast);
  });

  describe('map', () => {

    it('should set the full name', () => {
      const query = base.query({
        table: 'users',
        sel: (tr: TableItem) => {
          const utr = tr as TypedTableItem<UsersRecord, number>
          return ({
            ...utr.value, fullName: [utr.value.name,
              utr.value.last].join(' ')
          })
        }
      });
      const fullNames = query.value.map((tr: TableItem) => {
        const value = tr.value as Record<string, any>
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
            const u1 = t1.value as UsersRecord;
            const u2 = t2.value as UsersRecord;

            if (u1.gender === u2.gender) {
              return 0;
            }
            return u1.gender === Gender.male ? 1 : -1;
          }
        }
      });

      const users = query.value.map(tr => {
        const user = tr.value as UsersRecord;
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
            const u1 = t1.value as UsersRecord;
            const u2 = t2.value as UsersRecord;

            if (u1.gender === u2.gender) {
              return 0;
            }
            return u1.gender === Gender.male ? 1 : -1;
          }
        }, { count: 6 }]
      });

      const users = query.value.map(tr => {
        const user = tr.value as UsersRecord;
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
              const u1 = t1.value as UsersRecord;
              const u2 = t2.value as UsersRecord;

              if (u1.gender === u2.gender) {
                return 0;
              }
              return u1.gender === Gender.male ? 1 : -1;
            }
          }]
      });

      const users = query.value.map(tr => {
        const user = tr.value as UsersRecord;
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
              const u1 = t1.value as UsersRecord;
              const u2 = t2.value as UsersRecord;

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
        const user = tr.value as UsersRecord;
        return { id: user.id, gender: user.gender, age: user.age };
      });

      console.log('users', users);
      expect(users).toEqual( [
        { id: 102, gender: 'female', age: 40 },
        { id: 104, gender: 'female', age: 30 },
        { id: 101, gender: 'male', age: 30 },
        { id: 105, gender: 'male', age: 40 }
      ]);
    });
  });
});
