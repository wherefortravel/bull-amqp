// @flow

import amqplib from 'amqplib'
import Bluebird from 'bluebird'
import { EventEmitter } from 'events'
import crypto from 'crypto'
import connections from 'amqp-connection-manager'

export type Options = {
  connectionString: string,
  prefix: string,
}

type Job = {}
type JobOpts = {
  timeout?: number,
}

type Chan = any

type ProcessFnPromise = (job: Job) => Promise<any>
type ProcessFnCallback = (job: Job, done: Function) => any
type ProcessFn = ProcessFnPromise | ProcessFnCallback

const DEFAULT_TIMEOUT = 60 * 1000

function formatError(err: any) {
  if (typeof err === 'string') {
    return { message: err }
  }

  if (err instanceof Error) {
    return { message: err.error, stack: err.stack }
  }

  return { message: String(err) }
}

const generateCorrelationId = () => crypto.randomBytes(10).toString('base64')

type RegisteredConsumer = {
  name: string,
  handler: Function,
  concurrency: number,
}

const DEFAULT_OPTIONS: $Shape<Options> = {
  prefix: 'bull',
}

const runOnceForChannel = (channel: Chan, fn): Promise<any> => {
  return channel.waitForConnect().then(() => fn(channel._channel))
}

class Queue extends EventEmitter {
  _options: Options
  _name: string
  _conn: any
  _chan: Chan
  _consumeChan: { [key: string]: Chan }
  _queuesExist: { [key: string]: boolean }
  _replyHandlers: Map<string, Function>
  _replyQueue: ?Promise<string>

  constructor(name: string, connectionString: string, options?: Options | string) {
    super()

    if (typeof connectionString === 'object') {
      options = connectionString
    } else {
      // $FlowFixMe
      options = {
        ...DEFAULT_OPTIONS,
        ...(options || {}),
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

    this._conn = connections.connect(this._options.connectionString)
    const conn = this._conn

    this._chan = conn.createChannel()

    conn.on('error', (err) => {
      this.emit('connection:error', err)
    })

    conn.on('close', (err) => {
      this.emit('connection:close', err)
    })
  }

  async _ensureQueueExists(queue: string, channel: any) {
    if (!(queue in this._queuesExist)) {
      async function setup(chan) {
        await chan.assertQueue(queue)
      }
      const promise = runOnceForChannel(this._chan, setup)
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
      this._consumeChan[queue] = this._conn.createChannel()
    }

    return this._consumeChan[queue]
  }

  async process(name?: string, concurrency?: number, handler: ProcessFn) {
    switch (arguments.length) {
      case 1:
        // $FlowFixMe
        handler = name
        concurrency = 1
        name = undefined
        break
      case 2: // (string, function) or (string, string) or (number, function) or (number, string)
        // $FlowFixMe
        handler = concurrency
        if (typeof name === 'string') {
          concurrency = 1
        } else {
          concurrency = name
          name = undefined
        }
        break
    }

    const queue = this._getQueueName(name || this._name)
    const chan = this._ensureConsumeChannelOpen(queue)

    const promiseHandler: ProcessFnPromise =
      // $FlowFixMe
      handler.length === 1
        ? handler
        : function promiseHandler(job) {
            return new Bluebird((resolve, reject) => {
              // $FlowFixMe
              handler(job, (err, result) => {
                if (err) {
                  return reject(err)
                }

                return resolve(result)
              })
            })
          }

    await this._ensureQueueExists(queue, chan)

    chan.addSetup(async (chan) => {
      await chan.prefetch(concurrency)
      chan.consume(queue, async (msg) => {
        let data = {}
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
            await chan.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(result), 'utf8'), {
              correlationId: msg.properties.correlationId,
            })
          }

          chan.ack(msg)
        } catch (err) {
          chan.nack(msg, false, false)
          const pubChan = this._chan
          const errors = data['$$errors'] || []
          const newErrors = [...errors, formatError(err)]
          const newData = {
            ...data,
            ['$$errors']: newErrors,
          }

          if (newErrors.length < 3) {
            this.emit('single-failure', err)
            await pubChan.sendToQueue(
              queue,
              new Buffer(JSON.stringify(newData)),
              this._getPublishOptions(),
            )
          } else {
            this.emit('failure', err)
            const queue = this._getQueueName('dead-letter-queue')
            await this._ensureQueueExists(queue, pubChan)

            await pubChan.sendToQueue(
              queue,
              new Buffer(JSON.stringify(newData)),
              this._getPublishOptions(),
            )
          }
        }
      })
    })
  }

  async _fireJob(name: ?string, data: any, opts?: JobOpts, publishOptions: Object = {}) {
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

  async _sendInternal(queue: string, content: Buffer, opts: Object) {
    const chan = this._chan
    await this._ensureQueueExists(queue, chan)
    return chan.sendToQueue(queue, content, opts)
  }

  async add(name?: string, data: any, opts?: JobOpts) {
    if (typeof name !== 'string') {
      opts = data
      data = name
      name = undefined
    }

    await this._fireJob(name, data, opts)
  }

  async _ensureRpcQueueInternal(): Promise<string> {
    if (!this._replyQueue) {
      let _resolve
      this._replyQueue = new Promise((resolve, reject) => {
        _resolve = resolve
      })

      async function setup(chan) {
        const q = await chan.assertQueue('', { exclusive: true })
        _resolve(q.queue)
      }

      await runOnceForChannel(this._chan, setup)
    }

    return await this._replyQueue
  }

  async _ensureRpcQueue(): Promise<string> {
    if (!this._replyQueue) {
      const replyQueue = await this._ensureRpcQueueInternal()
      const replyHandlers = this._replyHandlers
      async function setup(chan) {
        chan.consume(
          replyQueue,
          (msg) => {
            const correlationId = msg.properties.correlationId
            const replyHandler = replyHandlers.get(correlationId)

            if (replyHandler) {
              replyHandler(JSON.parse(msg.content.toString()))
              replyHandlers.delete(correlationId)
            } else {
              // WARN?
            }
          },
          {
            noAck: true,
          },
        )
      }

      await runOnceForChannel(this._chan, setup)
    }

    return await this._replyQueue
  }

  async call(name?: string, data: any, opts?: JobOpts) {
    if (typeof name !== 'string') {
      opts = data
      data = name
      name = undefined
    }
    const replyTo = await this._ensureRpcQueue()
    const correlationId = generateCorrelationId()

    const { queue } = await this._fireJob(name, data, opts, {
      correlationId,
      replyTo,
    })

    const timeout = (opts && opts.timeout) || DEFAULT_TIMEOUT

    return await new Bluebird((resolve, reject) => {
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
