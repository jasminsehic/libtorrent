import { build } from 'esbuild'
import { nodeModulesPolyfillPlugin } from 'esbuild-plugins-node-modules-polyfill'

await build({
  entryPoints: ['client.js'],
  bundle: true,
  platform: 'browser',
  format: 'iife',
  outfile: 'client.bundle.js',
  banner: { js: 'var process = { env: { NODE_ENV: "production" } };' },
  plugins: [nodeModulesPolyfillPlugin()]
})
