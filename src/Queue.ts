import Bluebird from 'bluebird'
import { EventEmitter } from 'events'
import crypto from 'crypto'
import connections, { AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager'
import { Channel } from 'amqplib'
import { createRequire } from 'module'

const _require = createRequire(import.meta.url)

export type Options = {
  connectionString?: string
  prefix?: string
}

export interface Job<T> {
  data: T
}

export interface JobOpts {
  timeout?: number
}

type ProcessFnPromise<T> = (job: Job<T>) => Promise<any>
type ProcessFnCallback<T> = (job: Job<T>, done: (err: Error | undefined, result: any) => any) => any
type ProcessFn<T> = ProcessFnPromise<T> | ProcessFnCallback<T>

_require('amqp-connection-manager/lib/ChannelWrapper').default.prototype._runOnce = function(
    fn: (ch: Channel) => any,
) {
  return this.waitForConnect().then(() => fn(this._channel))
}

const DEFAULT_TIMEOUT = 60 * 1000

function formatError(err: any) {
  if (typeof err === 'string') {
    return { message: err }
  }

  if (err instanceof Error) {
    return { message: err.message, stack: err.stack }
  }

  return { message: String(err) }
}

const generateCorrelationId = () => crypto.randomBytes(10).toString('base64')

const DEFAULT_OPTIONS: Partial<Options> = {
  prefix: 'bull',
}

class Queue extends EventEmitter {
  _options: Options
  _name: string
  _conn: AmqpConnectionManager | null
  _chan: ChannelWrapper | null
  _consumeChan: { [key: string]: ChannelWrapper }
  _queuesExist: { [key: string]: boolean }
  _replyHandlers: Map<string, (result: any) => any>
  _replyQueue: Promise<string> | null = null

  constructor(name: string, connectionString: string, options?: Options | string) {
    super()

    if (typeof connectionString === 'object') {
      options = connectionString
    } else {
      // $FlowFixMe
      options = {
        ...DEFAULT_OPTIONS,
        ...((options as Options) || {}),
        connectionString,
      }
    }

    if (!options) {
      throw new Error(`options are required`)
    }

    this._options = options
    this._name = name
    this._setup()
  }

  _resetToInitialState() {
    this._chan = null
    this._conn = null
    this._queuesExist = Object.create(null)
    this._consumeChan = Object.create(null)
    this._replyHandlers = new Map()
    this._replyQueue = null
  }

  async _setup() {
    if (this._conn) {
      return
    }

    this._resetToInitialState()

    // @ts-ignore
    this._conn = connections.connect(this._options.connectionString)
    const conn = this._conn

    this._chan = conn.createChannel()

    conn.on('error', (err: Error) => {
      this.emit('connection:error', err)
    })

    conn.on('close', (err: Error) => {
      this.emit('connection:close', err)
    })
  }

  async _ensureQueueExists(queue: string, channel: any) {
    if (!(queue in this._queuesExist)) {
      async function setup(chan: Channel) {
        await chan.assertQueue(queue)
      }

      // @ts-ignore
      const promise = this._chan._runOnce(setup)
      this._queuesExist[queue] = true
      await promise
    }
  }

  _getQueueName(name: string) {
    return this._options.prefix + '-' + name
  }

  _getPublishOptions() {
    return {
      persistent: true, // TODO
    }
  }

  _ensureConsumeChannelOpen(queue: string) {
    if (!(queue in this._consumeChan)) {
      this._consumeChan[queue] = this._conn!.createChannel()
    }

    return this._consumeChan[queue]
  }

  private static _isPromiseProcessFn<T>(f: ProcessFn<T>): f is ProcessFnPromise<T> {
    return f.length === 1
  }

  async process<T>(name: string, concurrency: number, handler: ProcessFn<T>): Promise<void>
  async process<T>(concurrency: number, handler: ProcessFn<T>): Promise<void>
  async process<T>(name: string, handler: ProcessFn<T>): Promise<void>
  async process<T>(handler: ProcessFn<T>): Promise<void>

  async process<T>() {
    let handler: ProcessFn<T>
    let concurrency: number
    let name: string | undefined

    switch (arguments.length) {
      case 1:
        // (handler)
        handler = arguments[0]
        concurrency = 1
        name = undefined
        break
      case 2:
        handler = arguments[1]

        if (typeof arguments[0] === 'string') {
          // (name, handler)
          name = arguments[0]
          concurrency = 1
        } else {
          // (concurrency, handler)
          concurrency = arguments[0]
          name = undefined
        }
        break
      case 3:
        // (name, concurrency, handler)
        name = arguments[0]
        concurrency = arguments[1]
        handler = arguments[2]
        break
      default:
        throw new Error('invalid call to process()')
    }

    const queue = this._getQueueName(name || this._name)
    const chan = this._ensureConsumeChannelOpen(queue)

    const promiseHandler: ProcessFnPromise<T> = Queue._isPromiseProcessFn(handler)
        ? handler
        : function promiseHandler(job) {
          return new Promise((resolve, reject) => {
            handler(job, (err, result) => {
              if (err) {
                return reject(err)
              }

              return resolve(result)
            })
          })
        }

    await this._ensureQueueExists(queue, chan)

    await chan.addSetup(async (chan: Channel) => {
      await chan.prefetch(concurrency)
      await chan.consume(queue, async (msg) => {
        if (!msg) {
          return
        }

        let data: T = {} as T
        try {
          data = JSON.parse(msg.content.toString())

          const job = {
            data,
          }
          const result = await promiseHandler(job)

          // see if we need to reply
          if (
              typeof result !== 'undefined' &&
              typeof msg.properties === 'object' &&
              typeof msg.properties.replyTo !== 'undefined' &&
              typeof msg.properties.correlationId !== 'undefined'
          ) {
            await chan.sendToQueue(
                msg.properties.replyTo,
                Buffer.from(JSON.stringify(result), 'utf8'),
                {
                  correlationId: msg.properties.correlationId,
                },
            )
          }

          chan.ack(msg)
        } catch (err) {
          chan.nack(msg, false, false)
          const pubChan = this._chan
          // @ts-ignore
          const errors = data['$$errors'] || []
          const newErrors = [...errors, formatError(err)]
          const newData = {
            ...data,
            ['$$errors']: newErrors,
          }

          if (newErrors.length < 3) {
            this.emit('single-failure', err)
            await pubChan!.sendToQueue(
                queue,
                Buffer.from(JSON.stringify(newData), 'utf8'),
                this._getPublishOptions(),
            )
          } else {
            this.emit('failure', err)
            const queue = this._getQueueName('dead-letter-queue')
            await this._ensureQueueExists(queue, pubChan)

            await pubChan!.sendToQueue(
                queue,
                Buffer.from(JSON.stringify(newData), 'utf8'),
                this._getPublishOptions(),
            )
          }
        }
      })
    })
  }

  async _fireJob(name: string | void, data: any, opts?: JobOpts, publishOptions: any = {}) {
    const queue = this._getQueueName(name || this._name)
    const content = Buffer.from(JSON.stringify(data), 'utf8')
    const publishOpts = {
      ...this._getPublishOptions(),
      ...publishOptions,
    }

    await this._sendInternal(queue, content, publishOpts)

    return {
      queue,
    }
  }

  async _sendInternal(queue: string, content: Buffer, opts: object) {
    const chan = this._chan
    await this._ensureQueueExists(queue, chan)
    return chan!.sendToQueue(queue, content, opts)
  }

  async add<T>(name: string, data: T, opts: JobOpts): Promise<any>
  async add<T>(data: T, opts: JobOpts): Promise<any>
  async add<T>(name: string, data: T): Promise<any>
  async add<T>(data: T): Promise<any>

  async add() {
    let name: string | undefined
    let data: any
    let opts: JobOpts | undefined

    switch (arguments.length) {
      case 3:
        name = arguments[0]
        data = arguments[1]
        opts = arguments[2]
        break
      case 2:
        if (typeof arguments[0] === 'string') {
          name = arguments[0]
          data = arguments[1]
        } else {
          data = arguments[0]
          opts = arguments[1]
        }
        break
      case 1:
        data = arguments[0]
        break
      default:
        throw new Error('invalid call to add()')
    }

    await this._fireJob(name, data, opts)
  }

  async _ensureRpcQueueInternal(): Promise<string> {
    if (!this._replyQueue) {
      let _resolve: { (arg0: any): void; (value: string | PromiseLike<string>): void }
      this._replyQueue = new Promise((resolve, reject) => {
        _resolve = resolve
      })

      async function setup(chan: Channel) {
        const q = await chan.assertQueue('', { exclusive: true })
        _resolve(q.queue)
      }

      // @ts-ignore
      await this._chan._runOnce(setup)
    }

    return await this._replyQueue
  }

  async _ensureRpcQueue(): Promise<string> {
    if (!this._replyQueue) {
      const replyQueue = await this._ensureRpcQueueInternal()

      const setup = async (chan: Channel) => {
        await chan.consume(
            replyQueue,
            (msg) => {
              if (!msg) {
                return
              }

              const correlationId = msg.properties.correlationId
              const replyHandler = this._replyHandlers.get(correlationId)

              if (replyHandler) {
                replyHandler(JSON.parse(msg.content.toString()))
                this._replyHandlers.delete(correlationId)
              } else {
                // WARN?
              }
            },
            {
              noAck: true,
            },
        )
      }

      // @ts-ignore
      await this._chan._runOnce(setup)
    }

    return await this._replyQueue!
  }

  async call<T>(name: string, data: any, opts: JobOpts): Promise<T>
  async call<T>(data: any, opts: JobOpts): Promise<T>
  async call<T>(name: string, data: any): Promise<T>
  async call<T>(data: any): Promise<T>

  async call() {
    let name: string | undefined
    let data: any
    let opts: JobOpts | undefined

    switch (arguments.length) {
      case 3:
        name = arguments[0]
        data = arguments[1]
        opts = arguments[2]
        break
      case 2:
        if (typeof arguments[0] === 'string') {
          name = arguments[0]
          data = arguments[1]
        } else {
          data = arguments[0]
          opts = arguments[1]
        }
        break
      case 1:
        data = arguments[0]
        break
      default:
        throw new Error('invalid call to add()')
    }

    const replyTo = await this._ensureRpcQueue()
    const correlationId = generateCorrelationId()

    const { queue } = await this._fireJob(name, data, opts, {
      correlationId,
      replyTo,
    })

    const timeout = (opts && opts.timeout) || DEFAULT_TIMEOUT

    return new Bluebird((resolve, reject) => {
      this._replyHandlers.set(correlationId, resolve)
    })
        .timeout(timeout)
        .catch(Bluebird.TimeoutError, (err) => {
          this._replyHandlers.delete(correlationId)
          return Promise.reject(new Error(`Timeout of ${timeout}ms exceeded`))
        })
  }

  pause() {
    throw new Error(`Not implemented yet`)
  }

  resume() {
    throw new Error(`Not implemented yet`)
  }

  count() {
    throw new Error(`Not implemented yet`)
  }

  empty() {
    throw new Error(`Not implemented yet`)
  }
}

export default Queue