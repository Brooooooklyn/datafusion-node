# `@napi-rs/datafusion`

![https://github.com/Brooooooklyn/datafusion-node/actions](https://github.com/Brooooooklyn/datafusion-node/workflows/CI/badge.svg)

> Apache DataFusion Node.js binding

# Usage

```ts
import { col, SessionContext } from '@napi-rs/datafusion'

const ctx = new SessionContext()
const df = await ctx.readCsv('./__test__/example.csv')
await df
  .filter(col('a').ltEq(col('b')))
  .limit(0, 100)
  .show()
```
