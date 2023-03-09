import { Base } from '../Base'

export type Student = {
  studentId: string
  name: string
}

export type BaseClass = {
  name: string,
  teacher?: string,
  program: string,
  level?: string | number
}

export function isClass(arg: unknown): arg is Class {
  return isBaseClass(arg) && 'id' in arg;
}
export function isBaseClass(arg: unknown): arg is BaseClass {
  return !!(arg && typeof arg === 'object' && 'name' in arg && 'program' in arg)
}

export type Class = {
  id: string
} & BaseClass

export type Teacher = {
  employeeId: string,
  name: string,
  program: string
}

export type StudentClass = {
  studentId: string
  classId: string
}

const programs = ['Math', 'Science', 'Art'];

export const students = 'Alice,Bob,Jim,Dan,Elanore'.split(',').map((name, i) => ({
  name,
  studentId: btoa(name)
}));

export const teachers: Teacher[] = 'Victor,Williams,Xavier,Ymera,Zed'.split(',').map((name, i) => ({
  name, employeeId: btoa(name), program: programs[i % programs.length]
}));


const teacherMap = new Map();

teachers.forEach(t => {
  if (!teacherMap.has(t.program)) {
    teacherMap.set(t.program, [t])
  } else {
    teacherMap.get(t.program).push(t);
  }
})

function getTeacherId(program: string) {
  const teacherList: Teacher[] = teacherMap.get(program);
  const nextTeacher = teacherList.shift();
  if (nextTeacher) {
    teachers.push(nextTeacher);
    return nextTeacher.employeeId;
  }
  throw new Error('no teachers for ' + program);
}


export const classes: BaseClass[] = [
  {
    name: 'Algebra',
    program: 'Math'
  }, {
    name: 'Calculus',
    program: 'Math',
    level: 201
  },
  {
    name: 'Physics',
    program: 'Science'
  },
  {
    name: 'Advanced Painting',
    program: 'Art',
    level: 102
  },
  {
    name: 'Sculpture',
    program: 'Art',
  }
];
export const makeCollegeBase = () => {
  return new Base({
    tables: {
      teachers: {
        records: teachers,
        identityFromRecord: 'employeeId',
      },
      students: {
        records: students,
        identityFromRecord: 'studentId',
      },
      classes: {
        records: classes,
        onCreate(data, table) {
          if ( isBaseClass(data)) {
            if (!isClass(data)) {
              return {...data, id: table.identityFor(data)}
            }
            return data;
          }
          throw new Error('must be a base Class');
        },
        identityFromRecord (record) {
          let name = '101'
          if (isBaseClass(record)) {
            let recordName = record.name.split(' ').pop();
            if (!recordName) {
              recordName = record.program
            }
            name = recordName.substring(0, 4).toUpperCase() + (record.level || '101');
          }
          return name;
        }
      }
    },
    joins: {
      teachersToClasses: {
        from: 'teachers',
        to: {
          table: 'classes',
          field: 'teacher'
        }
      },
      studentsToClasses: {
        from: 'students',
        to: 'classes',
        via: 'studentClasses'
      }
    }
  })
}
