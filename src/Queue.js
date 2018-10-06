// @flow

import amqplib from 'amqplib'
import Bluebird from 'bluebird'
import { EventEmitter } from 'events'
import crypto from 'crypto'

import { connect } from './connections'

export type Options = {
  connectionString: string,
  prefix: string,
}

type Job = {}
type JobOpts = {
  timeout?: number,
}

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

type QueuedMessage = {
  queue: string,
  content: Buffer,
  options: Object,
}

type RegisteredConsumer = {
  name: string,
  handler: Function,
  concurrency: number,
}

const DEFAULT_OPTIONS: $Shape<Options> = {
  prefix: 'bull',
}

class Queue extends EventEmitter {
  _options: Options
  _name: string
  _conn: any // TODO types
  _publishChan: any // TODO types
  _consumeChans: { [key: string]: any }
  _queuesExist: { [key: string]: boolean }
  _replyHandlers: Map<string, Function>
  _replyQueue: any

  _queuedMessages: Array<QueuedMessage>
  _registeredConsumers: { [queue: string]: RegisteredConsumer }

  constructor(
    name: string,
    connectionString: string,
    options?: Options | string,
  ) {
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
    this._resetToInitialState()
  }

  _resetToInitialState() {
    this._publishChan = null
    this._conn = null
    this._consumeChans = Object.create(null)
    this._queuesExist = Object.create(null)
    this._replyHandlers = new Map()
    this._replyQueue = null
    this._queuedMessages = []
    this._registeredConsumers = Object.create(null)
  }

  async _reconnect() {
    // save old state
    const queuedMessages = this._queuedMessages
    const registeredConsumers = this._registeredConsumers

    // purge old state
    this._resetToInitialState()

    try {
      this._conn = await connect(this._options.connectionString)
      this._conn.once('error', (err) => {
        this._reconnect()
      })

      await this._ensureConnection()

      // recover messages
      for (const queuedMessage of queuedMessages) {
        this._sendInternal(
          queuedMessage.queue,
          queuedMessage.content,
          queuedMessage.options,
        )
      }

      // recover consumers
      for (const consumer of Object.values(registeredConsumers)) {
        await this.process(
          consumer.name,
          consumer.concurrency,
          consumer.handler,
        )
      }
    } catch (err) {
      this.emit('reconnect:error', err)
      await Bluebird.delay(5000)
      await this._reconnect()
    }
  }

  async _ensureConnection() {
    if (!this._conn) {
      await this._reconnect()
    }

    return this._conn
  }

  async _ensurePublishChannelOpen() {
    if (!this._publishChan) {
      this._publishChan = (await this._ensureConnection()).createChannel()
    }

    return this._publishChan
  }

  async _ensureConsumeChannelOpen(queue: string) {
    if (!(queue in this._consumeChans)) {
      this._consumeChans[
        queue
      ] = (await this._ensureConnection()).createChannel()
    }

    return this._consumeChans[queue]
  }

  async _ensureQueueExists(queue: string, channel: any) {
    if (!(queue in this._queuesExist)) {
      await channel.assertQueue(queue)
      this._queuesExist[queue] = true
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
    const chan = await this._ensureConsumeChannelOpen(queue)
    await chan.prefetch(concurrency)

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

    this._registeredConsumers[queue] = {
      name,
      concurrency,
      handler: promiseHandler,
    }

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
          chan.sendToQueue(
            msg.properties.replyTo,
            new Buffer(JSON.stringify(result)),
            {
              correlationId: msg.properties.correlationId,
            },
          )
        }

        chan.ack(msg)
      } catch (err) {
        chan.nack(msg, false, false)
        const pubChan = await this._ensurePublishChannelOpen()
        const errors = data['$$errors'] || []
        const newErrors = [...errors, formatError(err)]
        const newData = {
          ...data,
          ['$$errors']: newErrors,
        }

        if (newErrors.length < 3) {
          this.emit('single-failure', err)
          pubChan.sendToQueue(
            queue,
            new Buffer(JSON.stringify(newData)),
            this._getPublishOptions(),
          )
        } else {
          this.emit('failure', err)
          const queue = this._getQueueName('dead-letter-queue')
          await this._ensureQueueExists(queue, pubChan)

          pubChan.sendToQueue(
            queue,
            new Buffer(JSON.stringify(newData)),
            this._getPublishOptions(),
          )
        }
      }
    })
  }

  async _fireJob(
    name: ?string,
    data: any,
    opts?: JobOpts,
    publishOptions: Object = {},
  ) {
    const queue = this._getQueueName(name || this._name)
    const content = new Buffer(JSON.stringify(data))
    const publishOpts = {
      ...this._getPublishOptions(),
      ...publishOptions,
    }

    await this._sendInternal(queue, content, publishOpts)
    await this._ensureConnection()

    return {
      queue,
    }
  }

  async _sendInternal(queue: string, content: Buffer, opts: Object) {
    if (this._conn) {
      const chan = await this._ensurePublishChannelOpen()
      await this._ensureQueueExists(queue, chan)
      chan.sendToQueue(queue, content, opts)
    } else {
      this._queuedMessages.push({
        content,
        queue,
        options: opts,
      })
    }
  }

  async add(name?: string, data: any, opts?: JobOpts) {
    if (typeof name !== 'string') {
      opts = data
      data = name
      name = undefined
    }

    await this._fireJob(name, data, opts)
  }

  async _ensureRpcQueue() {
    if (!this._replyQueue) {
      const chan = await this._ensureConsumeChannelOpen('$$reply')
      const replyQueue = await chan.assertQueue('', { exclusive: true })
      this._replyQueue = replyQueue

      chan.consume(
        replyQueue.queue,
        (msg) => {
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

    return this._replyQueue.queue
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
