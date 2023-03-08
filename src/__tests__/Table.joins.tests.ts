import { isJoin } from '../types'
import {
  makeBase,
  addressUsers, westCoast, addresses, AddressRecord, UsersRecord, StateRecord
} from '../testFixtures'

describe('Table', () => {
  describe('joins', () => {
    const base = makeBase(addressUsers, westCoast, addresses);
    const userAddresses = base.joins.get('userAddresses');
    const addressStates = base.joins.get('addressStates');

    if (!isJoin(userAddresses)) {
      throw new Error('cannot get address linkVia');
    }
    if (!isJoin(addressStates)) {
      throw new Error('cannot get states linkVia');
    }

    describe('toRecordsForArray', () => {
      const [bob] = addressUsers;

      it('can get bob\'s address', () => {
        const addresses = userAddresses.toRecordsForArray(bob.id) as AddressRecord[];
        const [bobAddress, next] = addresses;
        expect(bobAddress.address).toBe('1000 First Avenue');
        expect(next).toBeUndefined();
      });

      it('can get addresses for state', () => {
        const [address1, address2, next] = addressStates.toRecordsForArray('CA') as AddressRecord[];
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

        const users = userAddresses?.fromRecordsForArray(10) as UsersRecord[];
        const [bob, next] = users;
        expect(bob.name).toBe('Bob');
        expect(next).toBeUndefined();
      });

      it('can get state for address id', () => {
        const [state, next] = addressStates.fromRecordsForArray(10) as StateRecord[];
        expect(state.label).toBe('California');
        expect(next).toBeUndefined();
      });
    });
  });
});
