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

/** Delivers `message` the way Rust does: through the channel callback, with its index. */
function send(channel: Channel<Message>, message: Message, index: number): void {
  const internals = (window as unknown as { __TAURI_INTERNALS__: { runCallback(id: number, data: unknown): void } })
    .__TAURI_INTERNALS__
  internals.runCallback(channel.id, { message, index })
}

const tick = () => new Promise((r) => setTimeout(r, 0))

function lastChannel(): Channel<Message> {
  const call = [...calls].reverse().find((c) => c.args?.channel)
  return call!.args.channel as Channel<Message>
}

beforeEach(() => {
  calls = []
  replies = {
    'plugin:socketio|connect': () => ({ subscription: 7, state: connected }),
    'plugin:socketio|subscribe': () => ({ subscription: 9, state: connected }),
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
    replies['plugin:socketio|subscribe'] = () => ({ subscription: 3, state: { status: 'reconnecting', attempt: 2, delay: 100 } })
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
    await own.disconnect()
    expect(calls.at(-1)).toEqual({ cmd: 'plugin:socketio|disconnect', args: { id: 'own' } })
  })
})

it('list', async () => {
  const clients = [{ id: 'core', access: 'readOnly', namespace: '/', state: { status: 'connecting', attempt: 0 } }]
  replies['plugin:socketio|list'] = () => clients
  expect(await list()).toEqual(clients)
  expect(calls[0].cmd).toBe('plugin:socketio|list')
})
