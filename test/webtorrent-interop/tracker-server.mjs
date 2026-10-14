import { Server } from 'bittorrent-tracker'

const port = Number(process.env.INTEROP_TRACKER_PORT || 19090)
const host = '127.0.0.1'
const server = new Server({ http: false, udp: false, ws: true, stats: false })
server.on('warning', err => console.error('[tracker-warning]', err.stack || err))
server.on('error', err => { console.error('[tracker-error]', err.stack || err); process.exitCode = 1 })
server.on('listening', () => {
  const addr = server.ws.address()
  console.log('[tracker-listening] ws://' + host + ':' + addr.port)
})
server.listen(port, host)
