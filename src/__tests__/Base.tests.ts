import {Base} from '../index'

describe('Base', () => {
  describe('constructor', () => {
    it('should crete tables that are passed to it', () => {

      const ctx = new Base({
        tables: [
          {name: 'foo'},
          {name: 'bar'}
        ]
      })

      expect(ctx.has('foo')).toBeTruthy()
      expect(ctx.has('bar')).toBeTruthy()
      expect(ctx.has('vey')).toBeFalsy()
    })
  })
})
