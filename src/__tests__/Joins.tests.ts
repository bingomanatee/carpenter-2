import {
  makeBase,
  addressUsers, westCoast, addresses, AddressRecord, UsersRecord, StateRecord
} from '../fixtures/users'
import { makeCollegeBase } from '../fixtures/teachers'

describe('Table', () => {
  describe('joins', () => {
    const base = makeBase(addressUsers, westCoast, addresses);
    let userAddresses = base.getJoin('userAddresses'); // from users to addresses
    const addressStates = base.getJoin('addressStates'); // from states to addresses

    describe('toRecordsForArray', () => {
      const [bob] = addressUsers;

      it('can get bob\'s address', () => {
        console.log('user index:,', userAddresses.to.index);
        const addresses = userAddresses.to.records(bob.id) as AddressRecord[];
        const [bobAddress, next] = addresses;
        expect(bobAddress.address).toBe('1000 First Avenue');
        expect(next).toBeUndefined();
      });

      it('can get addresses for state', () => {
        const [address1, address2, next] = addressStates.to.records('CA') as AddressRecord[];
        expect(new Set([address1.city, address2.city])).toEqual(
          new Set(['San Francisco', 'Simi Valley'])
        );
        expect(next).toBeUndefined();
      });
    });

    describe('fromRecordsForArray', () => {
      it('should get bob from address', () => {
        const bobsAddress = base.table('addresses')?.get(10);
        if (!bobsAddress) {
          throw new Error('no Bob');
        }

        const users = userAddresses?.from.records(10) as UsersRecord[];
        const [bob, next] = users;
        expect(bob.name).toBe('Bob');
        expect(next).toBeUndefined();
      });

      it('can get state for address id', () => {
        const [state, next] = addressStates.from.records(10) as StateRecord[];
        expect(state.label).toBe('California');
        expect(next).toBeUndefined();
      });
    });

    describe('add via', () => {
      it('should allow you to join in records via', () => {
        const base = makeCollegeBase();

        const bobId = btoa('Bob');
        const physId = 'PHYS101';

        base.table('students')?.join(bobId, {
          identity: physId,
          tableName: 'classes'
        });

        const [[_joinID, join], after] = base.table('studentClasses')?.$records || []
        expect(join).toEqual({ students: 'Qm9i', classes: 'PHYS101' });
        expect(after).toBeUndefined()
      });
    });
  });
});
