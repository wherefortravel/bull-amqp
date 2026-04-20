import Bluebird from 'bluebird';
import { EventEmitter } from 'events';
import crypto from 'crypto';
import * as connections from 'amqp-connection-manager';
import type { AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager';
import { setTimeout } from 'timers/promises';
import type { Channel, ConsumeMessage } from 'amqplib';

export interface Options {
  connectionString: string;
  prefix: string;
  minProcessingTimeMs?: number;
  channelArguments?: {
    'x-max-priority'?: number;
  };
}

export interface Job<T = unknown> {
  data: T;
}

export interface JobOpts {
  timeout?: number;
  priority?: number;
}

type ProcessFnPromise<T = unknown, R = unknown> = (job: Job<T>) => Promise<R>;
type ProcessFnCallback<T = unknown, R = unknown> = (
  job: Job<T>,
  done: (err?: Error | null, result?: R) => void
) => void;
type ProcessFn<T = unknown, R = unknown> = ProcessFnPromise<T, R> | ProcessFnCallback<T, R>;

interface PublishOptions {
  persistent?: boolean;
  priority?: number;
  correlationId?: string;
  replyTo?: string;
}

const DEFAULT_TIMEOUT = 60 * 1000;

function formatError(err: unknown): { message: string; stack?: string } {
  if (typeof err === 'string') {
    return { message: err };
  }

  if (err instanceof Error) {
    return { message: err.message, stack: err.stack };
  }

  return { message: String(err) };
}

const generateCorrelationId = (): string => crypto.randomBytes(10).toString('base64');

const DEFAULT_OPTIONS: Partial<Options> = {
  prefix: 'bull',
};

const runOnceForChannel = <T>(channel: ChannelWrapper, fn: (chan: Channel) => Promise<T>): Promise<T> => {
  return channel.waitForConnect().then(() => fn((channel as unknown as { _channel: Channel })._channel));
};

class Queue extends EventEmitter {
  private _options: Options;
  private _name: string;
  private _conn: AmqpConnectionManager | null = null;
  private _chan: ChannelWrapper | null = null;
  private _consumeChan: Record<string, ChannelWrapper> = Object.create(null);
  private _queuesExist: Record<string, boolean> = Object.create(null);
  private _replyHandlers: Map<string, (result: unknown) => void> = new Map();
  private _replyQueue: Promise<string> | null = null;
  private _rpcSetupRegistered: boolean = false;
  private _resolveRpcQueue: ((queue: string) => void) | null = null;

  constructor(name: string, connectionString: string | Options, options?: Options | string);
  constructor(name: string, options: Options);
  constructor(name: string, connectionStringOrOptions: string | Options, options?: Options | string) {
    super();

    let resolvedOptions: Options;

    if (typeof connectionStringOrOptions === 'object') {
      resolvedOptions = {
        ...DEFAULT_OPTIONS,
        ...connectionStringOrOptions,
      } as Options;
    } else {
      resolvedOptions = {
        ...DEFAULT_OPTIONS,
        ...((typeof options === 'object' ? options : {}) || {}),
        connectionString: connectionStringOrOptions,
      } as Options;
    }

    if (!resolvedOptions.connectionString) {
      throw new Error('options are required');
    }

    const minProcessingTimeMs = parseInt(
      process.env.BULL_AMQP_MIN_PROCESSING_TIME_MS || '',
      10
    );

    this._options = resolvedOptions;
    if (!isNaN(minProcessingTimeMs)) {
      this._options.minProcessingTimeMs = minProcessingTimeMs;
    }

    this._name = name;
    this._setup().catch((err) => {
      this.emit('error', err);
    });
  }

  get name(): string {
    return this._name;
  }

  private _resetToInitialState(): void {
    this._chan = null;
    this._conn = null;
    this._queuesExist = Object.create(null);
    this._consumeChan = Object.create(null);
    this._replyHandlers = new Map();
    this._replyQueue = null;
    this._rpcSetupRegistered = false;
    this._resolveRpcQueue = null;
  }

  private async _setup(): Promise<void> {
    if (this._conn) {
      return;
    }

    this._resetToInitialState();

    this._conn = connections.connect(this._options.connectionString);
    const conn = this._conn;

    this._chan = conn.createChannel();

    conn.on('connect', () => {
      this.emit('connection:connected');
    });

    conn.on('connectFailed', ({ err }: { err: Error }) => {
      this.emit('connection:error', err);
    });

    conn.on('disconnect', ({ err }: { err: Error }) => {
      // Invalidate stale reply queue so call() waits for addSetup to recreate it
      // on the new channel rather than publishing with a dead exclusive queue name.
      if (this._rpcSetupRegistered) {
        let resolve!: (queue: string) => void;
        this._replyQueue = new Promise<string>((r) => { resolve = r; });
        this._resolveRpcQueue = resolve;
      }
      this.emit('connection:close', err);
    });
  }

  private async _ensureQueueExists(queue: string, _channel: ChannelWrapper): Promise<void> {
    if (!(queue in this._queuesExist)) {
      const channelArguments =
        this._options && this._options.channelArguments ? this._options.channelArguments : undefined;
      const setup = async (chan: Channel): Promise<void> => {
        if (channelArguments) {
          await chan.assertQueue(queue, { arguments: channelArguments });
        } else {
          await chan.assertQueue(queue);
        }
      };
      const promise = runOnceForChannel(this._chan!, setup);
      this._queuesExist[queue] = true;
      await promise;
    }
  }

  private _getQueueName(name: string): string {
    return this._options.prefix + '-' + name;
  }

  private _getPublishOptions(): PublishOptions {
    return {
      persistent: true,
    };
  }

  private _ensureConsumeChannelOpen(queue: string): ChannelWrapper {
    if (!(queue in this._consumeChan)) {
      this._consumeChan[queue] = this._conn!.createChannel();
    }

    return this._consumeChan[queue];
  }

  // Overloads for process()
  process<T = unknown, R = unknown>(handler: ProcessFn<T, R>): Promise<void>;
  process<T = unknown, R = unknown>(concurrency: number, handler: ProcessFn<T, R>): Promise<void>;
  process<T = unknown, R = unknown>(name: string, handler: ProcessFn<T, R>): Promise<void>;
  process<T = unknown, R = unknown>(name: string, concurrency: number, handler: ProcessFn<T, R>): Promise<void>;
  async process<T = unknown, R = unknown>(
    nameOrConcurrencyOrHandler?: string | number | ProcessFn<T, R>,
    concurrencyOrHandler?: number | ProcessFn<T, R>,
    handlerArg?: ProcessFn<T, R>
  ): Promise<void> {
    let name: string | undefined;
    let concurrency: number;
    let handler: ProcessFn<T, R>;

    const envConcurrency = parseInt(process.env.BULL_AMQP_CONCURRENCY || '', 10);

    switch (arguments.length) {
      case 1:
        handler = nameOrConcurrencyOrHandler as ProcessFn<T, R>;
        concurrency = envConcurrency || 1;
        name = undefined;
        break;
      case 2:
        handler = concurrencyOrHandler as ProcessFn<T, R>;
        if (typeof nameOrConcurrencyOrHandler === 'string') {
          name = nameOrConcurrencyOrHandler;
          concurrency = envConcurrency || 1;
        } else {
          concurrency = envConcurrency || (nameOrConcurrencyOrHandler as number);
          name = undefined;
        }
        break;
      default:
        name = nameOrConcurrencyOrHandler as string;
        concurrency = envConcurrency || (concurrencyOrHandler as number);
        handler = handlerArg as ProcessFn<T, R>;
        break;
    }

    const queue = this._getQueueName(name || this._name);
    const chan = this._ensureConsumeChannelOpen(queue);

    const promiseHandler: ProcessFnPromise<T, R> =
      handler.length === 1
        ? (handler as ProcessFnPromise<T, R>)
        : function promiseHandler(job: Job<T>): Promise<R> {
            return new Bluebird<R>((resolve, reject) => {
              (handler as ProcessFnCallback<T, R>)(job, (err, result) => {
                if (err) {
                  return reject(err);
                }
                return resolve(result as R);
              });
            });
          };

    await this._ensureQueueExists(queue, chan);

    await chan.consume(queue, async (msg: ConsumeMessage) => {
      let data: Record<string, unknown> = {};
      try {
        data = JSON.parse(msg.content.toString());

        const job: Job<T> = {
          data: data as unknown as T,
        };

        const [result] = await Promise.all([
          promiseHandler(job),
          this._options.minProcessingTimeMs !== undefined
            ? setTimeout(this._options.minProcessingTimeMs)
            : Promise.resolve(),
        ]);

        // see if we need to reply
        if (
          typeof result !== 'undefined' &&
          typeof msg.properties === 'object' &&
          typeof msg.properties.replyTo !== 'undefined' &&
          typeof msg.properties.correlationId !== 'undefined'
        ) {
          await chan.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(result), 'utf8'), {
            correlationId: msg.properties.correlationId,
          });
        }

        chan.ack(msg);
      } catch (err) {
        chan.nack(msg, false, false);
        const pubChan = this._chan!;
        const errors = (data['$$errors'] as unknown[]) || [];
        const errorWithFormat = [...errors, formatError(err)];
        const withWithErrors = { ...data, '$$errors': errorWithFormat };

        if (errorWithFormat.length < 3) {
          this.emit('single-failure', err);
          await pubChan.sendToQueue(
            queue,
            Buffer.from(JSON.stringify(withWithErrors)),
            this._getPublishOptions()
          );
        } else {
          this.emit('failure', err);
          const dlqQueue = this._getQueueName('dead-letter-queue');
          await this._ensureQueueExists(dlqQueue, pubChan);

          await pubChan.sendToQueue(
            dlqQueue,
            Buffer.from(JSON.stringify(withWithErrors)),
            this._getPublishOptions()
          );
        }
      }
    }, { prefetch: concurrency });
  }

  private async _fireJob(
    name: string | undefined,
    data: unknown,
    opts?: JobOpts,
    publishOptions: PublishOptions = {}
  ): Promise<{ queue: string }> {
    const queue = this._getQueueName(name || this._name);
    const content = Buffer.from(JSON.stringify(data), 'utf8');
    const publishOpts: PublishOptions = {
      ...this._getPublishOptions(),
      ...publishOptions,
    };
    if (opts && opts.priority) {
      publishOpts.priority = opts.priority;
    }

    await this._sendInternal(queue, content, publishOpts);

    return {
      queue,
    };
  }

  private async _sendInternal(queue: string, content: Buffer, opts: PublishOptions): Promise<boolean> {
    const chan = this._chan!;
    await this._ensureQueueExists(queue, chan);
    return chan.sendToQueue(queue, content, opts);
  }

  // Overloads for add()
  add<T = unknown>(data: T, opts?: JobOpts): Promise<void>;
  add<T = unknown>(name: string, data: T, opts?: JobOpts): Promise<void>;
  async add<T = unknown>(nameOrData: string | T, dataOrOpts?: T | JobOpts, opts?: JobOpts): Promise<void> {
    let name: string | undefined;
    let data: T;
    let resolvedOpts: JobOpts | undefined;

    if (typeof nameOrData !== 'string') {
      resolvedOpts = dataOrOpts as JobOpts | undefined;
      data = nameOrData;
      name = undefined;
    } else {
      name = nameOrData;
      data = dataOrOpts as T;
      resolvedOpts = opts;
    }

    await this._fireJob(name, data, resolvedOpts);
  }

  private async _ensureRpcQueue(): Promise<string> {
    if (!this._replyQueue) {
      const replyHandlers = this._replyHandlers;
      let resolve!: (queue: string) => void;
      this._replyQueue = new Promise<string>((r) => { resolve = r; });
      this._resolveRpcQueue = resolve;

      if (!this._rpcSetupRegistered) {
        this._rpcSetupRegistered = true;

        await this._chan!.addSetup(async (chan: Channel): Promise<void> => {
          const q = await chan.assertQueue('', { exclusive: true });
          // Replace pending/stale promise with resolved one, then unblock any waiters.
          this._replyQueue = Promise.resolve(q.queue);
          if (this._resolveRpcQueue) {
            this._resolveRpcQueue(q.queue);
            this._resolveRpcQueue = null;
          }

          chan.consume(
            q.queue,
            (msg: ConsumeMessage | null) => {
              if (!msg) return;
              const correlationId = msg.properties.correlationId;
              const replyHandler = replyHandlers.get(correlationId);

              if (replyHandler) {
                replyHandler(JSON.parse(msg.content.toString()));
                replyHandlers.delete(correlationId);
              }
            },
            { noAck: true }
          );
        });
      }
    }

    return await this._replyQueue!;
  }

  // Overloads for call()
  call<T = unknown, R = unknown>(data: T, opts?: JobOpts): Promise<R>;
  call<T = unknown, R = unknown>(name: string, data: T, opts?: JobOpts): Promise<R>;
  async call<T = unknown, R = unknown>(
    nameOrData: string | T,
    dataOrOpts?: T | JobOpts,
    opts?: JobOpts
  ): Promise<R> {
    let name: string | undefined;
    let data: T;
    let resolvedOpts: JobOpts | undefined;

    if (typeof nameOrData !== 'string') {
      resolvedOpts = dataOrOpts as JobOpts | undefined;
      data = nameOrData;
      name = undefined;
    } else {
      name = nameOrData;
      data = dataOrOpts as T;
      resolvedOpts = opts;
    }

    const replyTo = await this._ensureRpcQueue();
    const correlationId = generateCorrelationId();

    await this._fireJob(name, data, resolvedOpts, {
      correlationId,
      replyTo,
    });

    const timeout = (resolvedOpts && resolvedOpts.timeout) || DEFAULT_TIMEOUT;

    return await new Bluebird<R>((resolve) => {
      this._replyHandlers.set(correlationId, resolve as (result: unknown) => void);
    })
      .timeout(timeout)
      .catch(Bluebird.TimeoutError, () => {
        this._replyHandlers.delete(correlationId);
        return Promise.reject(new Error(`Timeout of ${timeout}ms exceeded`));
      });
  }

  async stopAcceptingNewJobs(): Promise<void> {
    await Promise.all(Object.values(this._consumeChan).map(
      (ch) => ch.cancelAll())
    );
  }

  pause(): never {
    throw new Error('Not implemented yet');
  }

  resume(): never {
    throw new Error('Not implemented yet');
  }

  count(): never {
    throw new Error('Not implemented yet');
  }

  empty(): never {
    throw new Error('Not implemented yet');
  }

  async close(): Promise<void> {
    // Close all consume channels
    const consumeChannels = Object.values(this._consumeChan);
    for (const chan of consumeChannels) {
      try {
        await chan.close();
      } catch {
        // Ignore errors if channel is already closed
      }
    }

    // Close the main channel
    if (this._chan) {
      try {
        await this._chan.close();
      } catch {
        // Ignore errors if channel is already closed
      }
    }

    // Close the connection
    if (this._conn) {
      try {
        await this._conn.close();
      } catch {
        // Ignore errors if connection is already closed
      }
    }

    // Clear all state
    this._replyHandlers.clear();
    this._resetToInitialState();
  }
}

export default Queue;
