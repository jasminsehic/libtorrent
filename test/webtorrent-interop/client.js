import './polyfills.js'
import WebTorrent from 'webtorrent'

window.runWebTorrentInterop = async function (args) {
  const result = window.interopResult = { status: 'RUNNING', events: [] }
  const record = (name, detail = '') => {
    result.events.push(name + ' ' + detail)
    console.log('[interop] ' + name, detail)
  }
  const client = new WebTorrent()
  try {
    client.on('error', err => record('client-error', err && (err.stack || String(err))))
    const torrent = client.add(args.magnetURI, { announce: [window.interopTracker] })
    torrent.on('warning', err => record('warning', err && (err.message || String(err))))
    torrent.on('error', err => record('torrent-error', err && (err.stack || String(err))))
    torrent.on('wire', wire => {
      record('wire', wire.type || 'unknown')
      wire.on('extended', (ext, payload) => {
        if (ext === 'handshake') record('bep10-handshake', JSON.stringify(payload))
        else if (ext === 'ut_metadata') record('ut_metadata', JSON.stringify(payload))
      })
    })
    await Promise.race([
      new Promise((resolve, reject) => {
        torrent.once('metadata', () => {
          record('metadata', 'name=' + torrent.name + ' infoHash=' + torrent.infoHash)
          resolve()
        })
        torrent.once('error', reject)
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('metadata timeout')), 75000))
    ])
    const file = torrent.files.find(f => f.name === 'interop-test.txt')
    if (!file) throw new Error('Expected interop-test.txt; got ' + torrent.files.map(f => f.name).join(','))
    const data = await new Promise((resolve, reject) => file.getBuffer((err, buffer) => err ? reject(err) : resolve(buffer)))
    const text = data.toString('utf8')
    record('payload', JSON.stringify(text))
    if (text !== 'Test') throw new Error('Payload mismatch: ' + JSON.stringify(text))
    result.status = 'PASS'
    return result
  } catch (err) {
    result.status = 'FAIL'
    result.error = err && (err.stack || String(err))
    record('failure', result.error)
    throw err
  } finally {
    await client.destroy()
  }
}
