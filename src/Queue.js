// @flow

import amqplib from 'amqplib'
import Bluebird from 'bluebird'
import { EventEmitter } from 'events'
import crypto from 'crypto'

import { connect } from './connections'

export type Options = {
  connectionString: string,
  prefix?: string,
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

class Queue extends EventEmitter {
  _options: Options
  _name: string
  _conn: any // TODO types
  _publishChan: any // TODO types
  _consumeChans: { [key: string]: any } = Object.create(null) // TODO types
  _queuesExist: { [key: string]: boolean } = Object.create(null)
  _replyHandlers: Map<string, Function> = new Map()
  _replyQueue: any

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
        ...(options || {}),
        connectionString,
      }
    }

    if (!options) {
      throw new Error(`options are required`)
    }

    this._options = options
    this._name = name
  }

  async _ensureConnection() {
    if (!this._conn) {
      this._conn = await connect(this._options.connectionString)
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
    return (this._options.prefix || 'bull') + '-' + name
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
    const chan = await this._ensurePublishChannelOpen()
    const queue = this._getQueueName(name || this._name)
    await this._ensureQueueExists(queue, chan)
    chan.sendToQueue(queue, new Buffer(JSON.stringify(data)), {
      ...this._getPublishOptions(),
      ...publishOptions,
    })

    return {
      queue,
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
