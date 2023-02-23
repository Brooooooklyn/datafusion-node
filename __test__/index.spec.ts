import { join } from 'path'
import { fileURLToPath } from 'url'

import test from 'ava'

import { col, SessionContext } from '../index'

// https://github.com/apache/arrow-datafusion/blob/main/datafusion/core/tests/data/customer.csv
const fixture = join(fileURLToPath(import.meta.url), '..', 'example.csv')

test('basic data operator', async (t) => {
  const ctx = new SessionContext()
  const df = await ctx.readCsv(fixture)
  await df
    .filter(col('a').ltEq(col('b')))
    .limit(0, 100)
    .show()
  t.pass()
})
