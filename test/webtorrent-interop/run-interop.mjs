import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright'
import parseTorrent from 'parse-torrent'

const root = path.dirname(fileURLToPath(import.meta.url))
const publicDir = root
const torrentPath = process.env.LIBTORRENT_INTEROP_TORRENT
const donePath = process.env.LIBTORRENT_INTEROP_DONE
const tracker = process.env.LIBTORRENT_INTEROP_TRACKER
const logPath = process.env.LIBTORRENT_INTEROP_JS_LOG
if (!torrentPath || !donePath || !tracker || !logPath) throw new Error('Missing LIBTORRENT_INTEROP_* environment variables')

const log = fs.createWriteStream(logPath, { flags: 'w' })
const server = http.createServer((req, res) => {
  const file = req.url === '/client.bundle.js' ? path.join(publicDir, 'client.bundle.js') : null
  if (!file || !fs.existsSync(file)) { res.writeHead(404); res.end('not found'); return }
  res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8', 'cache-control': 'no-store' })
  fs.createReadStream(file).pipe(res)
})
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
const address = server.address()
let browser
try {
  const deadline = Date.now() + 8 * 60 * 1000
  while (!fs.existsSync(torrentPath) && Date.now() < deadline) await new Promise(r => setTimeout(r, 100))
  if (!fs.existsSync(torrentPath)) throw new Error('Timed out waiting for torrent file: ' + torrentPath)
  const parsed = await parseTorrent(fs.readFileSync(torrentPath))
  const magnetURI = 'magnet:?xt=urn:btih:' + parsed.infoHash + '&dn=' + encodeURIComponent(parsed.name || 'interop-test') + '&tr=' + encodeURIComponent(tracker)
  browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] })
  const page = await browser.newPage()
  await page.addInitScript(() => {
    const install = proto => {
      if (!proto) return
      if (typeof proto.toHex !== 'function') {
        Object.defineProperty(proto, 'toHex', {
          configurable: true,
          writable: true,
          value: function toHex() {
            let hex = ''
            for (let i = 0; i < this.length; ++i) hex += this[i].toString(16).padStart(2, '0')
            return hex
          }
        })
      }
      if (typeof proto.toBase64 !== 'function') {
        Object.defineProperty(proto, 'toBase64', {
          configurable: true,
          writable: true,
          value: function toBase64(options = {}) {
            let binary = ''
            for (let i = 0; i < this.length; ++i) binary += String.fromCharCode(this[i])
            let encoded = btoa(binary)
            if (options.alphabet === 'base64url') encoded = encoded.replace(/\\+/g, '-').replace(/\\//g, '_')
            if (options.omitPadding) encoded = encoded.replace(/=+$/, '')
            return encoded
          }
        })
      }
    }
    install(Uint8Array.prototype)
    install(Object.getPrototypeOf(new Uint8Array()))
  })
  page.on('console', msg => log.write('[console:' + msg.type() + '] ' + msg.text() + '\n'))
  page.on('pageerror', err => log.write('[pageerror] ' + (err.stack || err) + '\n'))
  await page.goto('http://127.0.0.1:' + address.port + '/')
  await page.addScriptTag({ url: 'http://127.0.0.1:' + address.port + '/client.bundle.js' })
  await page.evaluate(trackerURL => { window.interopTracker = trackerURL }, tracker)
  const result = await page.evaluate(async magnet => {
    try { return await window.runWebTorrentInterop({ magnetURI: magnet }) }
    catch (e) { return { status: 'FAIL', error: e && (e.stack || String(e)), events: window.interopResult && window.interopResult.events || [] } }
  }, magnetURI)
  log.write(JSON.stringify(result, null, 2) + '\n')
  log.end()
  fs.writeFileSync(donePath, (result.status === 'PASS' ? 'PASS' : 'FAIL') + '\n')
  if (result.status !== 'PASS') process.exitCode = 1
} catch (err) {
  log.write('[runner-error] ' + (err.stack || err) + '\n')
  log.end()
  fs.writeFileSync(donePath, 'FAIL\n')
  process.exitCode = 1
} finally {
  if (browser) await browser.close()
  server.close()
}
