import { BaseObj, TableObj, TypedTableObj } from '../types'
import { Base } from '../Base'
import { c } from '@wonderlandlabs/collect'

export enum Gender {
  male = 'male', female = 'female'
}

const validateUserStates = (base: BaseObj) => {
  const userTable = base.table('users') as UserTable
  const stateTable = base.table('states') as StateTable

  const userStates = userTable.coll.getReduce((memo: Set<string>, user: UsersRecord) => {
    if (user.state) {
      memo.add(user.state);
    }
    return memo
  }, new Set());

  userStates.forEach((state: string) => {
    if (!stateTable.has(state)) {
      throw new Error(`states must contain ${state} as it is in a users data`)
    }
  })
}
export type UsersRecord = {
  name: string,
  last?: string,
  id: number,
  age?: number,
  gender: Gender,
  state?: string,
  address?: number
}
export type StateRecord = { abbr: string, label: string }
type GenericRecord = Record<string, unknown>
export const westCoast = [
  { abbr: 'CA', label: 'California' },
  { abbr: 'OR', label: 'Oregon', },
  { abbr: 'WA', label: 'Washington' }
]
export const bobAndSue = [
  {
    id: 1,
    name: 'Bob',
    state: 'CA',
    gender: Gender.male,
    age: 12
  },
  {
    id: 2,
    name: 'Sue',
    state: 'OR',
    gender: Gender.female,
  }
];
export const manyPeople = [
  {
    id: 100,
    name: 'Alpha',
    last: 'Jones',
    gender: Gender.male,
    age: 20,
    state: 'CA',
    address: 210,
  },
  {
    id: 101,
    name: 'Beta',
    last: 'Smith',
    gender: Gender.male,
    age: 30,
    state: 'CA'
  },
  {
    id: 102,
    name: 'Gamma',
    last: 'Jones',
    gender: Gender.female,
    age: 40,
    state: 'OR',
    address: 290,
  },
  {
    id: 103,
    name: 'Delta',
    last: 'Smith',
    gender: Gender.male,
    age: 20,
    state: 'CA',
    address: 280,
  },
  {
    id: 104,
    name: 'Epsilon',
    last: 'Smith',
    gender: Gender.female,
    age: 30,
    state: 'OR',
    address: 270,
  },
  {
    id: 105,
    name: 'Zeta',
    last: 'Jones',
    gender: Gender.male,
    age: 40,
    state: 'WA',
    address: 260,
  },
  {
    id: 106,
    name: 'Eta',
    last: 'Smith',
    gender: Gender.male,
    age: 25,
    state: 'CA',
    address: 250,
  },
  {
    id: 107,
    name: 'Iota',
    last: 'Jones',
    gender: Gender.male,
    age: 35,
    state: 'WA',
    address: 240,
  },
  {
    id: 108,
    name: 'Kappa',
    last: 'Smith',
    gender: Gender.female,
    age: 45,
    state: 'WA',
    address: 230,
  },
  {
    id: 109,
    name: 'Lambda',
    last: 'Jones',
    gender: Gender.male,
    age: 50,
    state: 'OR',
    address: 220,
  },
];


export const addressUsers = [
  {
    id: 1,
    name: 'Bob',
    gender: Gender.male,
    age: 12,
    address: 10
  },
  {
    id: 2,
    name: 'Sue',
    address: 20,
    gender: Gender.female,
  },
  {
    id: 3,
    name: 'Allen',
    address: 30,
    gender: Gender.male,
  }
]

export const manyAddresses = [
  {
    id: 210,
    address: '1000 First Avenue',
    city: 'San Francisco',
    state: 'CA',
    zip: 11135
  },
  {
    id: 220,
    address: '3317 NE 15th',
    city: 'Portland',
    state: 'OR',
    zip: 97205
  },
  {
    id: 230,
    address: '1010 Simi Valley Dr',
    city: 'Simi Valley',
    state: 'CA',
    zip: 93065
  },
  {
    id: 240,
    address: '666 Buffalo Avenue',
    city: 'San Francisco',
    state: 'CA',
    zip: 11135
  },
  {
    id: 250,
    address: '101 Multnomah Avenue',
    city: 'Portland',
    state: 'OR',
    zip: 97205
  },
  {
    id: 260,
    address: '444 Simi Valley Dr',
    city: 'Simi Valley',
    state: 'CA',
    zip: 93065
  },
  {
    id: 270,
    address: '1000 Harrison St',
    city: 'San Francisco',
    state: 'CA',
    zip: 98403
  },
  {
    id: 280,
    address: '400 4th Ave',
    city: 'Portland',
    state: 'OR',
    zip: 97205
  },
  {
    id: 290,
    address: '44 Los Angeles Dr',
    city: 'Simi Valley',
    state: 'CA',
    zip: 93065
  }
];
export type AddressRecord = {
  id: number,
  address: string,
  address2?: string,
  city: string,
  state: string,
  zip: number | string
}

export function isAddressRecord(argument: unknown): argument is AddressRecord {
  return true;
}

export const addresses: AddressRecord[] = [
  {
    id: 10,
    address: '1000 First Avenue',
    city: 'San Francisco',
    state: 'CA',
    zip: 94103
  },
  {
    id: 20,
    address: '3317 NE 15th',
    city: 'Portland',
    state: 'OR',
    zip: 97205
  },
  {
    id: 30,
    address: '1010 Simi Valley Dr',
    city: 'Simi Valley',
    state: 'CA',
    zip: 93065
  }
];

export type StateTable = TypedTableObj<string, StateRecord>;
export type UserTable = TypedTableObj<number, UsersRecord>
export const makeBase = (
  usersRecords: UsersRecord[] = [],
  states: StateRecord[] = [],
  addresses: AddressRecord[] = []
) => (
  new Base({

    joins: [
      {
        name: 'userAddresses',
        from: {
          table: 'users',
          field: 'address',
        },
        to: 'addresses'
      },
      {
        name: 'addressStates',
        from: 'states',
        to: {
          table: 'addresses',
          field: 'state',
        }
      }
    ],

    tables: [
      {
        name: 'addresses',
        identityFromRecord: 'id',
        onCreate(data): AddressRecord {
          if (!isAddressRecord(data)) {
            throw new Error('val must be data record');
          }
          return data;
        },
        records: addresses
      },
      // users -- since their ID is not a FK for anything else, they can be passed
      // without and ID and auto-generated
      {
        name: 'users',
        identityFromRecord(record, table) {
          const users = record as UsersRecord
          if (users.id) {
            return users.id;
          }
          const currentMax = c(table.$records).getReduce(
            (memo, _value, key) => Math.max(key, memo),
            0);
          return currentMax + 1;
        },
        records: usersRecords,
        onCreate(data, table): UsersRecord {
          const obj = data as GenericRecord
          const id: number = typeof obj.id === 'number' ? obj.id : table.identityFor(obj) as number
          return { name: '', gender: Gender.male, ...obj, id };
        },
        onUpdate(data, table, previous) {
          const previousUsers = previous as UsersRecord
          const obj = data as GenericRecord
          if ('name' in obj && obj.name !== previousUsers.name) {
            throw new Error('cannot rename users record');
          }
          if (obj.id && obj.id !== previousUsers.id) {
            throw new Error('cannot change the identityFromRecord of a record')
          }

          return { ...previousUsers, ...obj }
        },
        testRecord(data) {
          if ((!data) || typeof data !== 'object') {
            return 'users must be objects';
          }
          const record = data as Record<string, unknown>
          if (typeof record.id !== 'number') {
            return 'users identities must be numbers';
          }
          if (!record.name || typeof record.name !== 'string') {
            return 'users must have nonempty string names'
          }
        },
        testTable(table) {
          return validateUserStates(table.base);
        }
      },
      {
        name: 'states',
        identityFromRecord: 'abbr',
        records: states,
        testRecord(data) {
          if ((!data) || typeof data !== 'object') {
            return 'states must be objects';
          }
          const record = data as Record<string, unknown>
          if (typeof record.abbr !== 'string') {
            return 'abbr must be string';
          }
          if (!/^[A-Z]{2}$/.test(record.abbr)) {
            return 'abbr must be 2-character abbreviation';
          }
          if (typeof record.label !== 'string') {
            return 'label must be string';
          }
          if (!/^[A-Z][\w ]+$/.test(record.label)) {
            return 'label must be a title case word';
          }
        },
        onUpdate(data, table, previous) {
          const prevState = previous as StateRecord
          const obj = data as GenericRecord
          if (obj.abbr && obj.abbr !== prevState.abbr) {
            throw new Error('cannot change the identityFromRecord of a record')
          }

          return { ...prevState, ...obj }
        },
        testTable(table: TableObj) {
          const stateLabels = table.$coll.values.map((state: StateRecord) => state.label);
          if (stateLabels.length !== new Set(stateLabels).size) {
            return 'State names must be unique'
          }
          return validateUserStates(table.base);
        }
      }
    ]
  })
)

