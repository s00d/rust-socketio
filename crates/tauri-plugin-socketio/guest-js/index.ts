// JS API of tauri-plugin-socketio: named Socket.IO clients shared with Rust.
//
//   const socket = await connect('chat', { url: 'https://example.com', auth: { token } })
//   socket.on('message', (text) => console.log(text))
//   await socket.emit('message', 'hi')
//
//   const core = await attach('core') // a client Rust registered (maybe read-only)

import { Channel, invoke } from '@tauri-apps/api/core'

/** socket.io-client's disconnect reasons plus the final ones of socketio-next. */
export type DisconnectReason =
  | 'io server disconnect'
  | 'io client disconnect'
  | 'ping timeout'
  | 'transport close'
  | 'transport error'
  | 'parse error'
  | 'connect rejected'
  | 'reconnect failed'
  | 'client dropped'

/** An error as the plugin sends it. */
export interface ErrorData {
  /** The variant name: `ConnectRejected`, `NotConnected`, `AckTimeout`, `AccessDenied`,
   * `Reserved`, `UrlNotAllowed`, `TooManySubscriptions`, `InvalidOptions`, `NotConfigurable`,
   * `WebsocketClosed`, `HttpStatus`, `ProxyFailed`, ... */
  kind: string
  message: string
  /** `ConnectRejected`: the server's `err.data`; `HttpStatus`: `{ status, message }`;
   * `WebsocketClosed`: `{ code, reason }`. */
  data?: unknown
}

/** The connection lifecycle; delays in milliseconds. `closed` is final. */
export type ConnectionState =
  | { status: 'connecting'; attempt: number }
  | { status: 'connected'; sid: string; recovered: boolean }
  | { status: 'disconnected'; reason: DisconnectReason; error?: ErrorData }
  | { status: 'reconnecting'; attempt: number; delay: number; error?: ErrorData }
  | { status: 'closed'; reason: DisconnectReason; error?: ErrorData }

/** Thrown by every function of this module that talks to the plugin. */
export class SocketIoError extends Error implements ErrorData {
  readonly kind: string
  readonly data?: unknown

  constructor(kind: string, message: string, data?: unknown) {
    super(message)
    this.name = 'SocketIoError'
    this.kind = kind
    this.data = data
  }

  /** A plugin error object, or anything else a rejected invoke gives (kind `Ipc`, e.g. a
   * command the capabilities do not allow). */
  static from(error: unknown): SocketIoError {
    if (error instanceof SocketIoError) return error
    if (typeof error === 'object' && error !== null && 'kind' in error && 'message' in error) {
      const e = error as ErrorData
      return new SocketIoError(String(e.kind), String(e.message), e.data)
    }
    return new SocketIoError('Ipc', error instanceof Error ? error.message : String(error))
  }
}

export type Transport = 'polling' | 'websocket'

/** Options of {@link connect}: socket.io-client names, durations in milliseconds. */
export interface SocketOptions {
  /** `http(s)://` or `ws(s)://`; its path is the namespace (like socket.io-client). */
  url: string
  /** Overrides the namespace of the URL. */
  namespace?: string
  /** The engine.io path, default `/socket.io/`. */
  path?: string
  query?: Record<string, string>
  /** socket.io-client `extraHeaders`. */
  headers?: Record<string, string>
  /** The CONNECT payload (`socket.handshake.auth`); must be an object. */
  auth?: Record<string, unknown>
  /** `['polling', 'websocket']` (default: polling upgraded to websocket), `['websocket']`,
   * `['polling']`, or `'websocket-upgrade'` (polling, then the upgrade must succeed). */
  transports?: Transport[] | 'any' | 'polling' | 'websocket' | 'websocket-upgrade'
  /** One connect attempt (handshake + upgrade + CONNECT), default 20000. */
  timeout?: number
  reconnection?: boolean
  /** Default unlimited (`Infinity`). */
  reconnectionAttempts?: number
  reconnectionDelay?: number
  reconnectionDelayMax?: number
  randomizationFactor?: number
  /** Also reconnect after the server disconnected the socket (default false). */
  reconnectOnDisconnect?: boolean
  /** Queue emits while disconnected (socket.io-client's send buffer; default false: they fail). */
  bufferWhileDisconnected?: boolean
  /** PEM certificates to trust instead of the default roots (a private CA). There is no way
   * to turn certificate checks off. */
  ca?: string
  /** Forward only these events to this webview (state is always forwarded). */
  events?: string[]
}

/** Options of {@link attach}. */
export interface AttachOptions {
  /** Forward only these events to this webview (state is always forwarded). */
  events?: string[]
}

/** A snapshot of the link; times are Unix epoch milliseconds, durations milliseconds. */
export interface LinkInfo {
  sid: string | null
  engineSid: string | null
  transport: Transport | null
  pingInterval: number | null
  pingTimeout: number | null
  maxPayload: number | null
  connectedAt: number | null
  lastPacketAt: number | null
  lastPingAt: number | null
  lastAttemptAt: number | null
  reconnectAttempts: number
  pendingAcks: number
  bufferedEmits: number
  /** Engine.io message packets / bytes sent and received. */
  packetsOut: number
  packetsIn: number
  bytesOut: number
  bytesIn: number
  /** Successful connects (the first one and every reconnect). */
  sessions: number
}

/** A registered client. `readOnly`: this webview may observe it but not emit, ack or
 * disconnect it. */
export interface ClientInfo {
  id: string
  access: 'full' | 'readOnly'
  namespace: string
  state: ConnectionState
}

/** Event arguments on the IPC: JSON plus the attachments that `{_placeholder, num}` stand for. */
interface Subscribed {
  subscription: number
  state: ConnectionState
}

interface WireArgs {
  args: unknown[]
  binary?: number[][]
}

/** What a subscription's channel carries. */
export type Message =
  | { type: 'state'; state: ConnectionState; snapshot?: boolean }
  | ({ type: 'event'; event: string; ackId?: number } & WireArgs)

export type Listener = (...args: any[]) => void
export type AnyListener = (event: string, ...args: any[]) => void
export type StateListener = (state: ConnectionState) => void

async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await invoke<T>(`plugin:socketio|${cmd}`, args)
  } catch (e) {
    throw SocketIoError.from(e)
  }
}

function bytesOf(value: unknown): Uint8Array | undefined {
  if (value instanceof Uint8Array) return value
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  return undefined
}

/** Binary values (`Uint8Array`, `ArrayBuffer`, typed arrays, `DataView`) anywhere in `args`
 * become placeholders plus attachments, as socket.io-parser encodes them. */
export function encodeArgs(args: unknown[]): WireArgs {
  const binary: number[][] = []
  const walk = (value: unknown): unknown => {
    const bytes = bytesOf(value)
    if (bytes) {
      binary.push(Array.from(bytes))
      return { _placeholder: true, num: binary.length - 1 }
    }
    if (Array.isArray(value)) return value.map(walk)
    if (value !== null && typeof value === 'object' && !(value instanceof Date)) {
      const toJSON = (value as { toJSON?: unknown }).toJSON
      if (typeof toJSON === 'function') return value
      return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, walk(v)]))
    }
    return value
  }
  const out = args.map(walk)
  return binary.length ? { args: out, binary } : { args: out }
}

/** The inverse of {@link encodeArgs}: placeholders become `Uint8Array`s. Without attachments
 * nothing is replaced (a JSON object that looks like a placeholder stays JSON). */
export function decodeArgs({ args, binary }: WireArgs): unknown[] {
  if (!binary?.length) return args
  const walk = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(walk)
    if (value !== null && typeof value === 'object') {
      const p = value as { _placeholder?: unknown; num?: unknown }
      if (p._placeholder === true && typeof p.num === 'number' && binary[p.num]) {
        return new Uint8Array(binary[p.num])
      }
      return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, walk(v)]))
    }
    return value
  }
  return args.map(walk)
}

function transportsOption(t: SocketOptions['transports']): string | undefined {
  if (t === undefined || typeof t === 'string') return t
  const polling = t.includes('polling')
  const websocket = t.includes('websocket')
  if (polling && websocket) return 'any'
  if (websocket) return 'websocket'
  if (polling) return 'polling'
  throw new SocketIoError('InvalidOptions', 'transports: expected polling and/or websocket')
}

/**
 * One registered client, seen from this webview: socket.io-client's `Socket` where it makes
 * sense. Events and state transitions arrive in order over a channel.
 *
 * When {@link connect} / {@link attach} resolve, the socket already has the current state
 * (`connected`, `id`). Messages that arrived before are held until the next task, so handlers
 * added right after the `await` see the first events; held states older than that snapshot
 * are dropped.
 */
export class Socket {
  /** The registry id. */
  readonly clientId: string
  private subscription = 0
  private readonly listeners = new Map<string, Set<Listener>>()
  private readonly anyListeners = new Set<AnyListener>()
  private readonly stateListeners = new Set<StateListener>()
  private current: ConnectionState = { status: 'connecting', attempt: 0 }
  private held: Message[] | null = []
  private closed = false
  /** @internal */
  readonly channel = new Channel<Message>()

  /** @internal use {@link connect} or {@link attach} */
  constructor(clientId: string) {
    this.clientId = clientId
    this.channel.onmessage = (m) => (this.held ? this.held.push(m) : this.dispatch(m))
  }

  /** @internal */
  start({ subscription, state }: Subscribed): this {
    this.subscription = subscription
    this.current = state
    setTimeout(() => {
      const held = this.held ?? []
      this.held = null
      // the snapshot is the first state of this subscription: earlier ones are stale
      const at = held.findIndex((m) => m.type === 'state' && m.snapshot)
      held.forEach((m, i) => {
        if (m.type === 'state' && i < at) return
        this.dispatch(m)
      })
    }, 0)
    return this
  }

  /** The last state this webview saw. */
  get lastState(): ConnectionState {
    return this.current
  }

  /** The namespace is joined (as of the last state seen). */
  get connected(): boolean {
    return this.current.status === 'connected'
  }

  /** `socket.id` while connected. */
  get id(): string | undefined {
    return this.current.status === 'connected' ? this.current.sid : undefined
  }

  /** Sends `event`; binary values anywhere in `args` are sent as attachments. Rejects with
   * `NotConnected` while disconnected (unless `bufferWhileDisconnected`), `AccessDenied` for a
   * read-only client. */
  async emit(event: string, ...args: unknown[]): Promise<void> {
    await call('emit', { id: this.clientId, event, args: encodeArgs(args) })
  }

  /** Sends `event` and resolves with the server's ack arguments (rejects with `AckTimeout`
   * after `timeout` ms, default 10000). */
  async emitWithAck(event: string, args: unknown[] = [], { timeout = 10000 }: { timeout?: number } = {}): Promise<unknown[]> {
    const reply = await call<WireArgs>('emit_with_ack', { id: this.clientId, event, args: encodeArgs(args), timeout })
    return decodeArgs(reply)
  }

  /** Handles `event`. When the server asked for an ack, the last argument is a function that
   * answers it (once; `(…args) => Promise<void>`). */
  on(event: string, listener: Listener): this {
    let set = this.listeners.get(event)
    if (!set) this.listeners.set(event, (set = new Set()))
    set.add(listener)
    return this
  }

  /** Handles the next `event` only. */
  once(event: string, listener: Listener): this {
    const wrapper: Listener = (...args) => {
      this.off(event, wrapper)
      listener(...args)
    }
    return this.on(event, wrapper)
  }

  /** Removes `listener`, or every listener of `event`. */
  off(event: string, listener?: Listener): this {
    if (listener) this.listeners.get(event)?.delete(listener)
    else this.listeners.delete(event)
    return this
  }

  /** Sees every event (before its own listeners), as `(event, ...args)`. */
  onAny(listener: AnyListener): this {
    this.anyListeners.add(listener)
    return this
  }

  offAny(listener?: AnyListener): this {
    if (listener) this.anyListeners.delete(listener)
    else this.anyListeners.clear()
    return this
  }

  /** Sees every state transition; returns a function that removes the listener. */
  onState(listener: StateListener): () => void {
    this.stateListeners.add(listener)
    return () => this.stateListeners.delete(listener)
  }

  /** The current state, from the client itself. */
  state(): Promise<ConnectionState> {
    return call('state', { id: this.clientId })
  }

  linkInfo(): Promise<LinkInfo> {
    return call('link_info', { id: this.clientId })
  }

  /** Replaces the CONNECT auth payload from the next connect attempt (a refreshed token). */
  async setAuth(auth: Record<string, unknown>): Promise<void> {
    await call('set_auth', { id: this.clientId, auth })
  }

  /** Replaces the extra headers from the next connect attempt. */
  async setHeaders(headers: Record<string, string>): Promise<void> {
    await call('set_headers', { id: this.clientId, headers })
  }

  /** Replaces the query parameters from the next connect attempt. */
  async setQuery(query: Record<string, string>): Promise<void> {
    await call('set_query', { id: this.clientId, query })
  }

  /** Connects again after the client closed (rejected, reconnect attempts used up, server
   * disconnect); resolves with the new state. A no-op while it is not closed. */
  reconnect(): Promise<ConnectionState> {
    return call('reconnect', { id: this.clientId })
  }

  /** Leaves the namespace, closes the connection and unregisters the client (for every
   * webview and Rust). Rejects with `AccessDenied` for a read-only client. */
  async disconnect(): Promise<void> {
    await call('disconnect', { id: this.clientId })
    this.detach()
  }

  /** Stops receiving events here; the client stays connected. */
  async close(): Promise<void> {
    if (this.closed) return
    this.detach()
    await call('unsubscribe', { id: this.clientId, subscription: this.subscription })
  }

  private detach(): void {
    this.closed = true
    this.channel.onmessage = () => {}
    this.listeners.clear()
    this.anyListeners.clear()
    this.stateListeners.clear()
  }

  private dispatch(m: Message): void {
    if (this.closed) return
    if (m.type === 'state') {
      this.current = m.state
      for (const l of [...this.stateListeners]) l(m.state)
      return
    }
    const args = decodeArgs(m)
    if (m.ackId !== undefined) args.push(this.acker(m.ackId))
    for (const l of [...this.anyListeners]) l(m.event, ...args)
    for (const l of [...(this.listeners.get(m.event) ?? [])]) l(...args)
  }

  private acker(ackId: number): (...args: unknown[]) => Promise<void> {
    let sent = false
    return async (...args: unknown[]) => {
      if (sent) return
      sent = true
      await call('ack', { id: this.clientId, ackId, args: encodeArgs(args) })
    }
  }
}

/**
 * Connects a new client under `id` (fails with `AlreadyExists` when the id is taken, e.g.
 * after a page reload: {@link attach} to it or disconnect it first; `Reserved` for an id Rust
 * keeps; `UrlNotAllowed` outside the plugin's allow-list). Resolves once the server
 * accepted the namespace; rejects with the error of that first attempt (`ConnectRejected`
 * with the server's `data`, `ConnectTimeout`, ...).
 */
export async function connect(id: string, options: SocketOptions): Promise<Socket> {
  const { events, transports, reconnectionAttempts, ...rest } = options
  const socket = new Socket(id)
  const subscribed = await call<Subscribed>('connect', {
    id,
    options: {
      ...rest,
      transports: transportsOption(transports),
      reconnectionAttempts: Number.isFinite(reconnectionAttempts) ? reconnectionAttempts : undefined
    },
    events,
    channel: socket.channel
  })
  return socket.start(subscribed)
}

/** A {@link Socket} for a client that is registered already (by Rust or another webview).
 * The first state message is its current state. */
export async function attach(id: string, options: AttachOptions = {}): Promise<Socket> {
  const socket = new Socket(id)
  const subscribed = await call<Subscribed>('subscribe', { id, events: options.events, channel: socket.channel })
  return socket.start(subscribed)
}

/** Every registered client. */
export function list(): Promise<ClientInfo[]> {
  return call('list')
}
