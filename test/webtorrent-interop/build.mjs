import webpack from 'webpack'
import TerserPlugin from 'terser-webpack-plugin'
import path from 'node:path'
import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'

const require = createRequire(import.meta.url)
const root = path.dirname(fileURLToPath(import.meta.url))
const webtorrentRoot = path.dirname(require.resolve('webtorrent/package.json'))
const config = {
  mode: 'production',
  entry: './client.js',
  context: root,
  target: 'web',
  resolve: {
    aliasFields: ['browser'],
    alias: { path: require.resolve('path-esm') },
    fallback: {
      crypto: require.resolve('crypto-browserify'),
      stream: require.resolve('stream-browserify'),
      buffer: require.resolve('buffer/'),
      process: require.resolve('process/browser'),
      events: require.resolve('events/'),
      util: require.resolve('util/'),
      assert: require.resolve('assert/'),
      os: require.resolve('os-browserify/browser')
    }
  },
  output: { path: root, filename: 'client.bundle.js', iife: true },
  plugins: [
    new webpack.ProvidePlugin({ process: path.join(webtorrentRoot, 'polyfills/process-fast.js') }),
    new webpack.DefinePlugin({ global: 'globalThis' })
  ],
  optimization: { minimize: true, minimizer: [new TerserPlugin({ extractComments: false })] }
}
const compiler = webpack(config)
compiler.run((err, stats) => {
  if (err) { console.error(err.stack || err); process.exitCode = 1; return }
  if (stats.hasErrors()) { console.error(stats.toString({ all: false, errors: true, errorDetails: true })); process.exitCode = 1 }
  else console.log(stats.toString({ all: false, assets: true, timings: true }))
  compiler.close(closeErr => { if (closeErr) { console.error(closeErr); process.exitCode = 1 } })
})
