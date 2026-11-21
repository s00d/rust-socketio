import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { clearMocks, mockIPC } from '@tauri-apps/api/mocks'
import type { Channel } from '@tauri-apps/api/core'

// @tauri-apps/api's mocks live on `window`; Node has `globalThis` (and `crypto`).
Object.assign(globalThis, { window: globalThis })

const api = await import('../../guest-js/index')
const { SocketIoError, attach, connect, decodeArgs, encodeArgs, list } = api
type Message = import('../../guest-js/index').Message

type Call = { cmd: string; args: Record<string, unknown> }
const connected = { status: 'connected', sid: 's0', recovered: false } as const
let calls: Call[]
let replies: Record<string, (args: Record<string, unknown>) => unknown>

const nextIndex = new WeakMap<Channel<Message>, number>()

/** Delivers `message` the way Rust does: through the channel callback, with the channel's
 * next index (Tauri's `Channel` delivers in index order). */
function send(channel: Channel<Message>, message: Message, _index?: number): void {
  const index = nextIndex.get(channel) ?? 0
  nextIndex.set(channel, index + 1)
  const internals = (window as unknown as { __TAURI_INTERNALS__: { runCallback(id: number, data: unknown): void } })
    .__TAURI_INTERNALS__
  internals.runCallback(channel.id, { message, index })
}

/** Rust starts every subscription with a snapshot of the current state. */
function subscribed(state: Message & { type: 'state' } extends never ? never : any, subscription: number) {
  return (args: Record<string, unknown>) => {
    send(args.channel as Channel<Message>, { type: 'state', state, snapshot: true })
    return { subscription, state }
  }
}

const tick = () => new Promise((r) => setTimeout(r, 0))

function lastChannel(): Channel<Message> {
  const call = [...calls].reverse().find((c) => c.args?.channel)
  return call!.args.channel as Channel<Message>
}

beforeEach(() => {
  calls = []
  replies = {
    'plugin:socketio|connect': subscribed(connected, 7),
    'plugin:socketio|subscribe': subscribed(connected, 9),
    'plugin:socketio|unsubscribe': () => true
  }
  mockIPC((cmd, args) => {
    calls.push({ cmd, args: args as Record<string, unknown> })
    const reply = replies[cmd]
    return reply ? reply(args as Record<string, unknown>) : null
  })
})

afterEach(() => clearMocks())

describe('args encoding', () => {
  it('turns binary anywhere into placeholders + attachments', () => {
    const buf = new Uint8Array([1, 2])
    const wire = encodeArgs([buf, { a: new Uint8Array([3]).buffer, b: [new DataView(new Uint8Array([4, 5]).buffer, 1)] }, 'x'])
    expect(wire).toEqual({
      args: [
        { _placeholder: true, num: 0 },
        { a: { _placeholder: true, num: 1 }, b: [{ _placeholder: true, num: 2 }] },
        'x'
      ],
      binary: [[1, 2], [3], [5]]
    })
    expect(decodeArgs(wire)).toEqual([buf, { a: new Uint8Array([3]), b: [new Uint8Array([5])] }, 'x'])
  })

  it('leaves JSON alone, placeholders included when there is no binary', () => {
    const date = new Date(0)
    expect(encodeArgs([1, null, { d: date }])).toEqual({ args: [1, null, { d: date }] })
    const lookalike = [{ _placeholder: true, num: 0 }]
    expect(decodeArgs({ args: lookalike })).toBe(lookalike)
  })
})

describe('connect', () => {
  it('maps the options and attaches the channel before connecting', async () => {
    const socket = await connect('main', {
      url: 'http://localhost:3000',
      auth: { token: 't' },
      transports: ['websocket'],
      reconnectionAttempts: Infinity,
      reconnectionDelay: 100,
      events: ['chat']
    })
    expect(calls).toHaveLength(1)
    const { cmd, args } = calls[0]
    expect(cmd).toBe('plugin:socketio|connect')
    expect(args.id).toBe('main')
    expect(args.events).toEqual(['chat'])
    expect(args.options).toEqual({
      url: 'http://localhost:3000',
      auth: { token: 't' },
      transports: 'websocket',
      reconnectionAttempts: undefined,
      reconnectionDelay: 100
    })
    expect(socket.clientId).toBe('main')
    // the resolved socket already reflects the snapshot (finding 10)
    expect(socket.connected).toBe(true)
    expect(socket.id).toBe('s0')
  })

  it.each([
    [['polling', 'websocket'], 'any'],
    [['polling'], 'polling'],
    ['websocket-upgrade', 'websocket-upgrade'],
    [undefined, undefined]
  ] as const)('transports %j -> %j', async (transports, wire) => {
    await connect('t', { url: 'http://x', transports: transports as never })
    expect((calls[0].args.options as { transports?: string }).transports).toBe(wire)
  })

  it('rejects with a typed error', async () => {
    replies['plugin:socketio|connect'] = () => {
      throw { kind: 'ConnectRejected', message: 'the server rejected the namespace: no', data: { reason: 'test' } }
    }
    const err = await connect('r', { url: 'http://x' }).catch((e) => e)
    expect(err).toBeInstanceOf(SocketIoError)
    expect(err).toMatchObject({ name: 'SocketIoError', kind: 'ConnectRejected', data: { reason: 'test' } })
    expect(err.message).toContain('rejected')
  })

  it('maps a plain IPC rejection (ACL) to kind Ipc', async () => {
    replies['plugin:socketio|connect'] = () => {
      throw 'socketio.connect not allowed'
    }
    await expect(connect('r', { url: 'http://x' })).rejects.toMatchObject({ kind: 'Ipc', message: 'socketio.connect not allowed' })
  })
})

describe('Socket', () => {
  it('holds early messages until handlers can be added, drops states older than the snapshot', async () => {
    replies['plugin:socketio|connect'] = (args) => {
      // Rust sends these before the invoke resolves
      const channel = args.channel as Channel<Message>
      send(channel, { type: 'state', state: { status: 'connecting', attempt: 0 } }, 0)
      send(channel, { type: 'event', event: 'hello', args: ['first'] }, 1)
      send(channel, { type: 'state', state: { status: 'connected', sid: 's1', recovered: false }, snapshot: true }, 2)
      return { subscription: 1, state: { status: 'connected', sid: 's1', recovered: false } }
    }
    const socket = await connect('early', { url: 'http://x' })
    expect(socket.connected).toBe(true)
    expect(socket.id).toBe('s1')
    const seen: unknown[] = []
    socket.onAny((event, ...args) => seen.push(['any', event, ...args]))
    socket.on('hello', (...args) => seen.push(['hello', ...args]))
    const states: string[] = []
    socket.onState((s) => states.push(s.status))
    await tick()
    expect(seen).toEqual([
      ['any', 'hello', 'first'],
      ['hello', 'first']
    ])
    expect(states).toEqual(['connected'])
    expect(socket.connected).toBe(true)
  })

  it('attach resolves with the current state', async () => {
    replies['plugin:socketio|subscribe'] = subscribed({ status: 'reconnecting', attempt: 2, delay: 100 }, 3)
    const socket = await attach('core')
    expect(socket.lastState).toEqual({ status: 'reconnecting', attempt: 2, delay: 100 })
    expect(socket.connected).toBe(false)
  })

  it('on / once / off / offAny / onState unlisten', async () => {
    const socket = await connect('l', { url: 'http://x' })
    const channel = lastChannel()
    await tick()
    const on = vi.fn()
    const once = vi.fn()
    const any = vi.fn()
    const state = vi.fn()
    socket.on('e', on).once('e', once).onAny(any)
    const unlisten = socket.onState(state)
    send(channel, { type: 'event', event: 'e', args: [1] }, 0)
    send(channel, { type: 'event', event: 'e', args: [2] }, 1)
    expect(on.mock.calls).toEqual([[1], [2]])
    expect(once.mock.calls).toEqual([[1]])
    expect(any.mock.calls).toEqual([
      ['e', 1],
      ['e', 2]
    ])
    socket.off('e', on).offAny()
    unlisten()
    send(channel, { type: 'event', event: 'e', args: [3] }, 2)
    send(channel, { type: 'state', state: { status: 'disconnected', reason: 'transport close' } }, 3)
    expect(on).toHaveBeenCalledTimes(2)
    expect(any).toHaveBeenCalledTimes(2)
    expect(state).not.toHaveBeenCalled()
    expect(socket.lastState).toEqual({ status: 'disconnected', reason: 'transport close' })
    socket.on('e', on).off('e')
    send(channel, { type: 'event', event: 'e', args: [4] }, 4)
    expect(on).toHaveBeenCalledTimes(2)
  })

  it('decodes binary and answers server acks once', async () => {
    const socket = await connect('a', { url: 'http://x' })
    const channel = lastChannel()
    await tick()
    let ack: ((...a: unknown[]) => Promise<void>) | undefined
    socket.on('question', (data, cb) => {
      expect(data).toEqual(new Uint8Array([9]))
      ack = cb
    })
    send(channel, { type: 'event', event: 'question', args: [{ _placeholder: true, num: 0 }], binary: [[9]], ackId: 42 }, 0)
    expect(typeof ack).toBe('function')
    await ack!('yes', new Uint8Array([1]))
    await ack!('again')
    const acks = calls.filter((c) => c.cmd === 'plugin:socketio|ack')
    expect(acks).toEqual([
      {
        cmd: 'plugin:socketio|ack',
        args: { id: 'a', ackId: 42, args: { args: ['yes', { _placeholder: true, num: 0 }], binary: [[1]] } }
      }
    ])
  })

  it('emit and emitWithAck encode args and decode the reply', async () => {
    replies['plugin:socketio|emit_with_ack'] = () => ({ args: ['ok', { _placeholder: true, num: 0 }], binary: [[7]] })
    const socket = await connect('e', { url: 'http://x' })
    await socket.emit('chat', 'hi', new Uint8Array([1]))
    expect(calls.at(-1)).toEqual({
      cmd: 'plugin:socketio|emit',
      args: { id: 'e', event: 'chat', args: { args: ['hi', { _placeholder: true, num: 0 }], binary: [[1]] } }
    })
    await socket.emit('ping')
    expect(calls.at(-1)!.args.args).toEqual({ args: [] })
    const reply = await socket.emitWithAck('sum', [1, 2], { timeout: 500 })
    expect(reply).toEqual(['ok', new Uint8Array([7])])
    expect(calls.at(-1)).toEqual({
      cmd: 'plugin:socketio|emit_with_ack',
      args: { id: 'e', event: 'sum', args: { args: [1, 2] }, timeout: 500 }
    })
    await socket.emitWithAck('x')
    expect(calls.at(-1)!.args.timeout).toBe(10000)
  })

  it('emit errors are typed (read-only client)', async () => {
    replies['plugin:socketio|emit'] = () => {
      throw { kind: 'AccessDenied', message: 'socket.io client `core` is read-only for the webview' }
    }
    const socket = await attach('core')
    await expect(socket.emit('x')).rejects.toMatchObject({ kind: 'AccessDenied' })
  })

  it('setAuth, setHeaders, setQuery and reconnect', async () => {
    replies['plugin:socketio|reconnect'] = () => connected
    const socket = await connect('cfg', { url: 'http://x', ca: '-----BEGIN CERTIFICATE-----' })
    expect((calls[0].args.options as { ca?: string }).ca).toBe('-----BEGIN CERTIFICATE-----')
    await socket.setAuth({ token: 'new' })
    await socket.setHeaders({ authorization: 'Bearer x' })
    await socket.setQuery({ v: '2' })
    expect(await socket.reconnect()).toEqual(connected)
    expect(calls.slice(1)).toEqual([
      { cmd: 'plugin:socketio|set_auth', args: { id: 'cfg', auth: { token: 'new' } } },
      { cmd: 'plugin:socketio|set_headers', args: { id: 'cfg', headers: { authorization: 'Bearer x' } } },
      { cmd: 'plugin:socketio|set_query', args: { id: 'cfg', query: { v: '2' } } },
      { cmd: 'plugin:socketio|reconnect', args: { id: 'cfg' } }
    ])
    replies['plugin:socketio|set_auth'] = () => {
      throw { kind: 'NotConfigurable', message: 'registered from Rust' }
    }
    await expect(socket.setAuth({})).rejects.toMatchObject({ kind: 'NotConfigurable' })
  })

  it('state, linkInfo, close and disconnect', async () => {
    replies['plugin:socketio|state'] = () => ({ status: 'connected', sid: 's', recovered: true })
    replies['plugin:socketio|link_info'] = () => ({ transport: 'websocket' })
    const socket = await attach('core', { events: ['news'] })
    expect(calls[0]).toMatchObject({ cmd: 'plugin:socketio|subscribe', args: { id: 'core', events: ['news'] } })
    expect(await socket.state()).toEqual({ status: 'connected', sid: 's', recovered: true })
    expect(await socket.linkInfo()).toEqual({ transport: 'websocket' })
    const channel = lastChannel()
    const on = vi.fn()
    socket.on('news', on)
    await socket.close()
    await socket.close()
    expect(calls.filter((c) => c.cmd === 'plugin:socketio|unsubscribe')).toEqual([
      { cmd: 'plugin:socketio|unsubscribe', args: { id: 'core', subscription: 9 } }
    ])
    send(channel, { type: 'event', event: 'news', args: [] }, 0)
    expect(on).not.toHaveBeenCalled()

    const own = await connect('own', { url: 'http://x' })
    await tick()
    const reasons: unknown[] = []
    own.on('disconnect', (reason) => reasons.push(reason))
    await own.disconnect()
    expect(calls.at(-1)).toEqual({ cmd: 'plugin:socketio|disconnect', args: { id: 'own' } })
    // the outcome is known here even when the final state message is still on its way
    expect(own.lastState).toEqual({ status: 'closed', reason: 'io client disconnect' })
    expect(own.connected).toBe(false)
    expect(reasons).toEqual(['io client disconnect'])
  })

  it('waits for the snapshot when it arrives after the call resolved', async () => {
    let channel: Channel<Message> | undefined
    replies['plugin:socketio|connect'] = (args) => {
      channel = args.channel as Channel<Message>
      send(channel, { type: 'state', state: { status: 'connecting', attempt: 0 } })
      send(channel, { type: 'state', state: connected })
      return { subscription: 1, state: connected }
    }
    const socket = await connect('late', { url: 'http://x' })
    const states: string[] = []
    socket.onState((st) => states.push(st.status))
    const seen = vi.fn()
    socket.on('big', seen)
    await tick()
    // a large event (sent by fetch) and then the snapshot arrive only now
    send(channel!, { type: 'event', event: 'big', args: ['x'.repeat(10_000)] })
    expect(seen).not.toHaveBeenCalled()
    send(channel!, { type: 'state', state: connected, snapshot: true })
    await tick()
    expect(states).toEqual(['connected'])
    expect(seen).toHaveBeenCalledTimes(1)
  })

  it('a throwing listener stops neither the others nor later messages', async () => {
    // the error is rethrown on its own microtask: capture it instead of failing the run
    const rethrown: unknown[] = []
    const spy = vi.spyOn(globalThis, 'queueMicrotask').mockImplementation((fn) => rethrown.push(fn))
    try {
      const socket = await connect('throw', { url: 'http://x' })
      const channel = lastChannel()
      await tick()
      const after = vi.fn()
      socket.on('e', () => {
        throw new Error('handler bug')
      })
      socket.on('e', after)
      send(channel, { type: 'event', event: 'e', args: [1] })
      send(channel, { type: 'event', event: 'e', args: [2] })
      expect(after.mock.calls).toEqual([[1], [2]])
      expect(rethrown).toHaveLength(2)
    } finally {
      spy.mockRestore()
    }
  })

  it('off removes a once listener; connect / disconnect / connect_error come from states', async () => {
    const socket = await connect('o', { url: 'http://x' })
    const channel = lastChannel()
    await tick()
    const h = vi.fn()
    socket.once('x', h).off('x', h)
    send(channel, { type: 'event', event: 'x', args: [] })
    expect(h).not.toHaveBeenCalled()

    const events: unknown[] = []
    socket.on('connect', () => events.push('connect'))
    socket.on('disconnect', (reason) => events.push(['disconnect', reason]))
    socket.on('connect_error', (e) => events.push(['connect_error', e.kind]))
    send(channel, { type: 'state', state: { status: 'disconnected', reason: 'transport close' } })
    send(channel, {
      type: 'state',
      state: { status: 'reconnecting', attempt: 1, delay: 10, error: { kind: 'Engine', message: 'refused' } }
    })
    send(channel, { type: 'state', state: { status: 'connected', sid: 's2', recovered: false } })
    expect(events).toEqual([['disconnect', 'transport close'], ['connect_error', 'Engine'], 'connect'])
  })

  it('reads Blobs, encodes typed array views at their offset, refuses a raw Blob', async () => {
    const socket = await connect('b', { url: 'http://x' })
    const view = new Int16Array([1, 2, 3]).subarray(1)
    await socket.emit('up', new Blob([new Uint8Array([7, 8])]), { v: view })
    const emit = calls.find((c) => c.cmd === 'plugin:socketio|emit')!
    const bytes = Array.from(new Uint8Array(view.buffer, view.byteOffset, view.byteLength))
    expect(emit.args.args).toEqual({
      args: [{ _placeholder: true, num: 0 }, { v: { _placeholder: true, num: 1 } }],
      binary: [[7, 8], bytes]
    })
    expect(() => encodeArgs([new Blob([])])).toThrow(SocketIoError)
  })

  it('a failed ack answer is logged, not an unhandled rejection', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    replies['plugin:socketio|ack'] = () => {
      throw { kind: 'UnknownAck', message: 'gone' }
    }
    const socket = await connect('ack', { url: 'http://x' })
    const channel = lastChannel()
    await tick()
    let cb: ((...a: unknown[]) => Promise<void>) | undefined
    socket.on('q', (respond) => (cb = respond))
    send(channel, { type: 'event', event: 'q', args: [], ackId: 5 })
    await expect(cb!('late')).resolves.toBeUndefined()
    expect(warn).toHaveBeenCalledTimes(1)
    warn.mockRestore()
  })

  it('transports: [] is refused', async () => {
    await expect(connect('t', { url: 'http://x', transports: [] })).rejects.toMatchObject({ kind: 'InvalidOptions' })
  })
})

it('list', async () => {
  const clients = [{ id: 'core', access: 'readOnly', namespace: '/', state: { status: 'connecting', attempt: 0 } }]
  replies['plugin:socketio|list'] = () => clients
  expect(await list()).toEqual(clients)
  expect(calls[0].cmd).toBe('plugin:socketio|list')
})
