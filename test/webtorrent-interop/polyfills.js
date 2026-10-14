// WebTorrent's uint8 utilities use Uint8Array.prototype.toBase64(), which is
// not available in every Chromium version used by Playwright. Implement the
// standard API subset needed by WebTorrent before importing its bundle.
if (typeof Uint8Array.prototype.toBase64 !== 'function') {
  Object.defineProperty(Uint8Array.prototype, 'toBase64', {
    configurable: true,
    writable: true,
    value: function toBase64(options = {}) {
      const alphabet = options.alphabet === 'base64url'
        ? 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
        : 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
      const bytes = this
      let encoded = ''
      const chunkSize = 0x6000
      for (let start = 0; start < bytes.length; start += chunkSize) {
        const end = Math.min(start + chunkSize, bytes.length)
        let binary = ''
        for (let i = start; i < end; ++i) binary += String.fromCharCode(bytes[i])
        encoded += btoa(binary)
      }
      if (options.alphabet === 'base64url') encoded = encoded.replace(/\\+/g, '-').replace(/\\//g, '_')
      if (options.omitPadding) encoded = encoded.replace(/=+$/, '')
      return encoded
    }
  })
}
