const { existsSync, readFileSync } = require('fs')
const { join } = require('path')

const { platform, arch } = process

let nativeBinding = null
let localFileExisted = false
let loadError = null

function isMusl() {
  // For Node 10
  if (!process.report || typeof process.report.getReport !== 'function') {
    try {
      const lddPath = require('child_process').execSync('which ldd').toString().trim()
      return readFileSync(lddPath, 'utf8').includes('musl')
    } catch (e) {
      return true
    }
  } else {
    const { glibcVersionRuntime } = process.report.getReport().header
    return !glibcVersionRuntime
  }
}

switch (platform) {
  case 'android':
    switch (arch) {
      case 'arm64':
        localFileExisted = existsSync(join(__dirname, 'datafusion.android-arm64.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.android-arm64.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-android-arm64')
          }
        } catch (e) {
          loadError = e
        }
        break
      case 'arm':
        localFileExisted = existsSync(join(__dirname, 'datafusion.android-arm-eabi.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.android-arm-eabi.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-android-arm-eabi')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(`Unsupported architecture on Android ${arch}`)
    }
    break
  case 'win32':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(join(__dirname, 'datafusion.win32-x64-msvc.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.win32-x64-msvc.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-win32-x64-msvc')
          }
        } catch (e) {
          loadError = e
        }
        break
      case 'ia32':
        localFileExisted = existsSync(join(__dirname, 'datafusion.win32-ia32-msvc.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.win32-ia32-msvc.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-win32-ia32-msvc')
          }
        } catch (e) {
          loadError = e
        }
        break
      case 'arm64':
        localFileExisted = existsSync(join(__dirname, 'datafusion.win32-arm64-msvc.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.win32-arm64-msvc.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-win32-arm64-msvc')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(`Unsupported architecture on Windows: ${arch}`)
    }
    break
  case 'darwin':
    localFileExisted = existsSync(join(__dirname, 'datafusion.darwin-universal.node'))
    try {
      if (localFileExisted) {
        nativeBinding = require('./datafusion.darwin-universal.node')
      } else {
        nativeBinding = require('@napi-rs/datafusion-darwin-universal')
      }
      break
    } catch {}
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(join(__dirname, 'datafusion.darwin-x64.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.darwin-x64.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-darwin-x64')
          }
        } catch (e) {
          loadError = e
        }
        break
      case 'arm64':
        localFileExisted = existsSync(join(__dirname, 'datafusion.darwin-arm64.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.darwin-arm64.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-darwin-arm64')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(`Unsupported architecture on macOS: ${arch}`)
    }
    break
  case 'freebsd':
    if (arch !== 'x64') {
      throw new Error(`Unsupported architecture on FreeBSD: ${arch}`)
    }
    localFileExisted = existsSync(join(__dirname, 'datafusion.freebsd-x64.node'))
    try {
      if (localFileExisted) {
        nativeBinding = require('./datafusion.freebsd-x64.node')
      } else {
        nativeBinding = require('@napi-rs/datafusion-freebsd-x64')
      }
    } catch (e) {
      loadError = e
    }
    break
  case 'linux':
    switch (arch) {
      case 'x64':
        if (isMusl()) {
          localFileExisted = existsSync(join(__dirname, 'datafusion.linux-x64-musl.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./datafusion.linux-x64-musl.node')
            } else {
              nativeBinding = require('@napi-rs/datafusion-linux-x64-musl')
            }
          } catch (e) {
            loadError = e
          }
        } else {
          localFileExisted = existsSync(join(__dirname, 'datafusion.linux-x64-gnu.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./datafusion.linux-x64-gnu.node')
            } else {
              nativeBinding = require('@napi-rs/datafusion-linux-x64-gnu')
            }
          } catch (e) {
            loadError = e
          }
        }
        break
      case 'arm64':
        if (isMusl()) {
          localFileExisted = existsSync(join(__dirname, 'datafusion.linux-arm64-musl.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./datafusion.linux-arm64-musl.node')
            } else {
              nativeBinding = require('@napi-rs/datafusion-linux-arm64-musl')
            }
          } catch (e) {
            loadError = e
          }
        } else {
          localFileExisted = existsSync(join(__dirname, 'datafusion.linux-arm64-gnu.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./datafusion.linux-arm64-gnu.node')
            } else {
              nativeBinding = require('@napi-rs/datafusion-linux-arm64-gnu')
            }
          } catch (e) {
            loadError = e
          }
        }
        break
      case 'arm':
        localFileExisted = existsSync(join(__dirname, 'datafusion.linux-arm-gnueabihf.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./datafusion.linux-arm-gnueabihf.node')
          } else {
            nativeBinding = require('@napi-rs/datafusion-linux-arm-gnueabihf')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(`Unsupported architecture on Linux: ${arch}`)
    }
    break
  default:
    throw new Error(`Unsupported OS: ${platform}, architecture: ${arch}`)
}

if (!nativeBinding) {
  if (loadError) {
    throw loadError
  }
  throw new Error(`Failed to load native binding`)
}

const {
  DataFrame,
  JoinType,
  Expr,
  col,
  binaryExpr,
  and,
  or,
  min,
  max,
  sum,
  avg,
  count,
  countDistinct,
  inList,
  concat,
  concatWs,
  random,
  approxDistinct,
  approxMedian,
  approxPercentileCont,
  approxPercentileContWithWeight,
  groupingSet,
  cube,
  rollup,
  isNull,
  isTrue,
  isNotTrue,
  isFalse,
  isNotFalse,
  isUnknown,
  isNotUnknown,
  Operator,
  SessionContext,
} = nativeBinding

module.exports.DataFrame = DataFrame
module.exports.JoinType = JoinType
module.exports.Expr = Expr
module.exports.col = col
module.exports.binaryExpr = binaryExpr
module.exports.and = and
module.exports.or = or
module.exports.min = min
module.exports.max = max
module.exports.sum = sum
module.exports.avg = avg
module.exports.count = count
module.exports.countDistinct = countDistinct
module.exports.inList = inList
module.exports.concat = concat
module.exports.concatWs = concatWs
module.exports.random = random
module.exports.approxDistinct = approxDistinct
module.exports.approxMedian = approxMedian
module.exports.approxPercentileCont = approxPercentileCont
module.exports.approxPercentileContWithWeight = approxPercentileContWithWeight
module.exports.groupingSet = groupingSet
module.exports.cube = cube
module.exports.rollup = rollup
module.exports.isNull = isNull
module.exports.isTrue = isTrue
module.exports.isNotTrue = isNotTrue
module.exports.isFalse = isFalse
module.exports.isNotFalse = isNotFalse
module.exports.isUnknown = isUnknown
module.exports.isNotUnknown = isNotUnknown
module.exports.Operator = Operator
module.exports.SessionContext = SessionContext
