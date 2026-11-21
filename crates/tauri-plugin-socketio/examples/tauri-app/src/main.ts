import { attach, connect, SocketIoError } from 'tauri-plugin-socketio-api'

const URL = 'http://localhost:4210' // `just servers` in the repository

function show(p: unknown): string {
  if (typeof p === 'string') return p
  // an Error's `message` is not enumerable: JSON.stringify would drop it
  if (p instanceof SocketIoError) return `${p.kind}: ${p.message}`
  if (p instanceof Error) return p.message
  return JSON.stringify(p)
}

function log(id: string, ...parts: unknown[]): void {
  const el = document.getElementById(id)!
  el.textContent += parts.map(show).join(' ') + '\n'
}

// A client Rust registered as read-only: observe it, emitting is refused.
async function observeCore(): Promise<void> {
  // Rust registers it once its first connect succeeded
  let core
  for (;;) {
    try {
      core = await attach('core', { events: ['tock'] })
      break
    } catch (e) {
      if (!(e instanceof SocketIoError && e.kind === 'UnknownClient')) throw e
      await new Promise((r) => setTimeout(r, 500))
    }
  }
  core.onState((s) => log('core', 'state', s))
  core.on('tock', (n, text) => log('core', 'tock', n, text))
  try {
    await core.emit('tick', 0)
  } catch (e) {
    log('core', 'emit refused:', (e as SocketIoError).kind)
  }
}

// A client of this webview: connect (or, after a reload, take over the registered one).
async function chat(): Promise<void> {
  const socket = await connect('chat', { url: URL, reconnectionDelay: 500 }).catch(async (e) => {
    if (e instanceof SocketIoError && e.kind === 'AlreadyExists') return attach('chat')
    throw e
  })
  socket.onState((s) => log('chat', 'state', s))
  socket.on('echo-back', (...args) => log('chat', 'echo-back', ...args))
  document.getElementById('send')!.onclick = async () => {
    try {
      await socket.emit('echo', 'hello', new Uint8Array([1, 2, 3]))
      log('chat', 'ack:', await socket.emitWithAck('echo', ['with ack'], { timeout: 5000 }))
    } catch (e) {
      // e.g. NotConnected while the server is down
      log('chat', 'send failed:', e)
    }
  }
}

observeCore().catch((e) => log('core', 'error', e))
chat().catch((e) => log('chat', 'error', e))
