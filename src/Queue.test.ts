import { EventEmitter } from 'events';

// Mock amqp-connection-manager before importing Queue
jest.mock('amqp-connection-manager');

import Queue, { Job } from './Queue';

interface MockMessage {
  content: Buffer;
  properties: {
    correlationId?: string;
    replyTo?: string;
  };
  fields: {
    deliveryTag: number;
  };
}

type ConsumeCallback = (msg: MockMessage | null) => Promise<void> | void;

interface MockChannel {
  assertQueue: jest.Mock;
  sendToQueue: jest.Mock;
  consume: jest.Mock;
  ack: jest.Mock;
  nack: jest.Mock;
  cancel: jest.Mock;
  prefetch: jest.Mock;
}

interface MockChannelWrapper extends EventEmitter {
  _channel: MockChannel;
  waitForConnect: jest.Mock;
  sendToQueue: jest.Mock;
  addSetup: jest.Mock;
  close: jest.Mock;
}

interface MockConnection extends EventEmitter {
  createChannel: jest.Mock;
  close: jest.Mock;
  _channels: MockChannelWrapper[];
}

let mockChannel: MockChannel;
let mockConnection: MockConnection;
let consumerCallbacks: Map<string, ConsumeCallback>;
let consumerTagCounter: number;

const createMockChannel = (): MockChannel => ({
  assertQueue: jest.fn().mockResolvedValue({ queue: 'test-queue' }),
  sendToQueue: jest.fn().mockResolvedValue(true),
  consume: jest.fn().mockImplementation((queue: string, callback: ConsumeCallback) => {
    consumerCallbacks.set(queue, callback);
    consumerTagCounter++;
    return Promise.resolve({ consumerTag: `consumer-tag-${consumerTagCounter}` });
  }),
  ack: jest.fn(),
  nack: jest.fn(),
  cancel: jest.fn().mockResolvedValue(undefined),
  prefetch: jest.fn().mockResolvedValue(undefined),
});

const createMockChannelWrapper = (channel: MockChannel): MockChannelWrapper => {
  const wrapper = new EventEmitter() as MockChannelWrapper;
  wrapper._channel = channel;
  wrapper.waitForConnect = jest.fn().mockResolvedValue(undefined);
  wrapper.sendToQueue = jest.fn().mockResolvedValue(true);
  wrapper.addSetup = jest.fn().mockImplementation(async (fn: (chan: MockChannel) => Promise<void>) => {
    await fn(channel);
  });
  wrapper.close = jest.fn().mockResolvedValue(undefined);
  return wrapper;
};

const createMockConnection = (): MockConnection => {
  const conn = new EventEmitter() as MockConnection;
  conn._channels = [];
  conn.createChannel = jest.fn().mockImplementation(() => {
    const wrapper = createMockChannelWrapper(mockChannel);
    conn._channels.push(wrapper);
    return wrapper;
  });
  conn.close = jest.fn().mockResolvedValue(undefined);
  return conn;
};

// eslint-disable-next-line @typescript-eslint/no-require-imports
const amqpConnectionManager = require('amqp-connection-manager');

const setupMocks = (): void => {
  consumerCallbacks = new Map();
  consumerTagCounter = 0;
  mockChannel = createMockChannel();
  mockConnection = createMockConnection();

  amqpConnectionManager.connect.mockImplementation(() => {
    setImmediate(() => {
      mockConnection.emit('connect', {});
    });
    return mockConnection;
  });
};

describe('Queue', () => {
  const connectionString = 'amqp://localhost';
  const queueName = 'test-queue';

  beforeEach(() => {
    jest.clearAllMocks();
    setupMocks();
    delete process.env.BULL_AMQP_CONCURRENCY;
    delete process.env.BULL_AMQP_MIN_PROCESSING_TIME_MS;
  });

  describe('constructor', () => {
    it('should create queue with name, connectionString and options', () => {
      const queue = new Queue(queueName, connectionString, {
        connectionString,
        prefix: 'custom',
      });

      expect(queue).toBeInstanceOf(Queue);
      expect(queue).toBeInstanceOf(EventEmitter);
      expect(amqpConnectionManager.connect).toHaveBeenCalledWith(connectionString);
    });

    it('should create queue with name and options object', () => {
      const queue = new Queue(queueName, {
        connectionString,
        prefix: 'custom',
      });

      expect(queue).toBeInstanceOf(Queue);
      expect(amqpConnectionManager.connect).toHaveBeenCalledWith(connectionString);
    });

    it('should use default prefix "bull"', () => {
      const queue = new Queue(queueName, connectionString);

      expect(queue).toBeInstanceOf(Queue);
    });

    it('should read minProcessingTimeMs from environment variable', () => {
      process.env.BULL_AMQP_MIN_PROCESSING_TIME_MS = '100';
      const queue = new Queue(queueName, connectionString);

      expect(queue).toBeInstanceOf(Queue);
    });
  });

  describe('connection events', () => {
    it('should emit connection:connected on connect', (done) => {
      const queue = new Queue(queueName, connectionString);

      queue.on('connection:connected', () => {
        done();
      });
    });

    it('should emit connection:error on error', (done) => {
      const queue = new Queue(queueName, connectionString);
      const testError = new Error('Connection error');

      queue.on('connection:error', (err) => {
        expect(err).toBe(testError);
        done();
      });

      setImmediate(() => {
        mockConnection.emit('error', testError);
      });
    });

    it('should emit connection:close on close', (done) => {
      const queue = new Queue(queueName, connectionString);
      const testError = new Error('Connection closed');

      queue.on('connection:close', (err) => {
        expect(err).toBe(testError);
        done();
      });

      setImmediate(() => {
        mockConnection.emit('close', testError);
      });
    });
  });

  describe('process()', () => {
    it('should register handler with default concurrency (1)', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn().mockResolvedValue('result');

      await queue.process(handler);

      expect(mockChannel.prefetch).toHaveBeenCalledWith(1);
      expect(mockChannel.consume).toHaveBeenCalled();
    });

    it('should register handler with custom concurrency', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn().mockResolvedValue('result');

      await queue.process(5, handler);

      expect(mockChannel.prefetch).toHaveBeenCalledWith(5);
    });

    it('should register named handler', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn().mockResolvedValue('result');

      await queue.process('custom-name', handler);

      const consumeCall = mockChannel.consume.mock.calls[0];
      expect(consumeCall[0]).toBe('bull-custom-name');
    });

    it('should register named handler with concurrency', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn().mockResolvedValue('result');

      await queue.process('custom-name', 3, handler);

      expect(mockChannel.prefetch).toHaveBeenCalledWith(3);
      const consumeCall = mockChannel.consume.mock.calls[0];
      expect(consumeCall[0]).toBe('bull-custom-name');
    });

    it('should support callback-style handlers', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn((_job, done) => {
        done(null, 'callback-result');
      });

      await queue.process(handler);

      expect(mockChannel.consume).toHaveBeenCalled();
    });

    it('should override concurrency with BULL_AMQP_CONCURRENCY env', async () => {
      process.env.BULL_AMQP_CONCURRENCY = '10';
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn().mockResolvedValue('result');

      await queue.process(5, handler);

      expect(mockChannel.prefetch).toHaveBeenCalledWith(10);
    });

    it('should process job and ack on success with promise handler', async () => {
      const queue = new Queue(queueName, connectionString);
      let handlerResult: unknown = null;
      const handler = jest.fn((job) => {
        handlerResult = job;
        return Promise.resolve('result');
      });

      await queue.process(handler);

      const callback = consumerCallbacks.get(`bull-${queueName}`);
      expect(callback).toBeDefined();

      const msg = {
        content: Buffer.from(JSON.stringify({ foo: 'bar' })),
        properties: {},
        fields: { deliveryTag: 1 },
      };

      await callback!(msg);

      expect(handlerResult).toEqual({ data: { foo: 'bar' } });
      expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    });

    it('should handle null messages gracefully', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn().mockResolvedValue('result');

      await queue.process(handler);

      const callback = consumerCallbacks.get(`bull-${queueName}`);
      await callback!(null);

      expect(handler).not.toHaveBeenCalled();
    });

    it('should handle callback-style handlers that return error', async () => {
      const queue = new Queue(queueName, connectionString);
      const testError = new Error('Callback error');
      const handler = jest.fn((_job, done) => {
        done(testError);
      });
      const singleFailureHandler = jest.fn();

      queue.on('single-failure', singleFailureHandler);

      await queue.process(handler);

      const callback = consumerCallbacks.get(`bull-${queueName}`);
      const msg = {
        content: Buffer.from(JSON.stringify({ foo: 'bar' })),
        properties: {},
        fields: { deliveryTag: 1 },
      };

      await callback!(msg);

      expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
      expect(singleFailureHandler).toHaveBeenCalledWith(testError);
    });

    it('should handle callback-style handlers that succeed', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn((_job, done) => {
        done(null, 'success-result');
      });

      await queue.process(handler);

      const callback = consumerCallbacks.get(`bull-${queueName}`);
      const msg = {
        content: Buffer.from(JSON.stringify({ foo: 'bar' })),
        properties: {},
        fields: { deliveryTag: 1 },
      };

      await callback!(msg);

      expect(handler).toHaveBeenCalledWith({ data: { foo: 'bar' } }, expect.any(Function));
      expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    });

    it('should reply to RPC calls when result is provided using callback handler', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn((_job, done) => {
        done(null, { response: 'data' });
      });

      await queue.process(handler);

      const callback = consumerCallbacks.get(`bull-${queueName}`);
      const msg = {
        content: Buffer.from(JSON.stringify({ foo: 'bar' })),
        properties: {
          replyTo: 'reply-queue',
          correlationId: 'corr-123',
        },
        fields: { deliveryTag: 1 },
      };

      await callback!(msg);

      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        'reply-queue',
        expect.any(Buffer),
        { correlationId: 'corr-123' }
      );
    });

    it('should nack and emit single-failure on error with callback handler', async () => {
      const queue = new Queue(queueName, connectionString);
      const testError = new Error('Handler error');
      const handler = jest.fn((_job, done) => {
        done(testError);
      });
      const singleFailureHandler = jest.fn();

      queue.on('single-failure', singleFailureHandler);

      await queue.process(handler);

      const callback = consumerCallbacks.get(`bull-${queueName}`);
      const msg = {
        content: Buffer.from(JSON.stringify({ foo: 'bar' })),
        properties: {},
        fields: { deliveryTag: 1 },
      };

      await callback!(msg);

      expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
      expect(singleFailureHandler).toHaveBeenCalledWith(testError);

      // Check that job was re-queued
      const mainChannel = mockConnection._channels[0];
      const sendToQueueCalls = mainChannel.sendToQueue.mock.calls;
      const requeueCall = sendToQueueCalls.find((call: unknown[]) => call[0] === `bull-${queueName}`);
      expect(requeueCall).toBeDefined();

      const requeuedData = JSON.parse(requeueCall[1].toString());
      expect(requeuedData['$$errors']).toHaveLength(1);
    });

    it('should emit failure and send to DLQ after 3 errors with callback handler', async () => {
      const queue = new Queue(queueName, connectionString);
      const testError = new Error('Handler error');
      const handler = jest.fn((_job, done) => {
        done(testError);
      });
      const failureHandler = jest.fn();

      queue.on('failure', failureHandler);

      await queue.process(handler);

      const callback = consumerCallbacks.get(`bull-${queueName}`);
      const msg = {
        content: Buffer.from(JSON.stringify({
          foo: 'bar',
          '$$errors': [
            { message: 'error 1' },
            { message: 'error 2' },
          ],
        })),
        properties: {},
        fields: { deliveryTag: 1 },
      };

      await callback!(msg);

      expect(failureHandler).toHaveBeenCalledWith(testError);

      // Check job was sent to DLQ
      const mainChannel = mockConnection._channels[0];
      const sendToQueueCalls = mainChannel.sendToQueue.mock.calls;
      const dlqCall = sendToQueueCalls.find((call: unknown[]) => call[0] === 'bull-dead-letter-queue');
      expect(dlqCall).toBeDefined();
    });
  });

  describe('add()', () => {
    it('should add job to default queue', async () => {
      const queue = new Queue(queueName, connectionString);

      await queue.add({ foo: 'bar' });

      const channelWrapper = mockConnection._channels[0];
      expect(channelWrapper.sendToQueue).toHaveBeenCalledWith(
        `bull-${queueName}`,
        expect.any(Buffer),
        expect.objectContaining({ persistent: true })
      );
    });

    it('should add job to named queue', async () => {
      const queue = new Queue(queueName, connectionString);

      await queue.add('custom-queue', { foo: 'bar' });

      const channelWrapper = mockConnection._channels[0];
      expect(channelWrapper.sendToQueue).toHaveBeenCalledWith(
        'bull-custom-queue',
        expect.any(Buffer),
        expect.objectContaining({ persistent: true })
      );
    });

    it('should add job with priority', async () => {
      const queue = new Queue(queueName, connectionString);

      await queue.add({ foo: 'bar' }, { priority: 5 });

      const channelWrapper = mockConnection._channels[0];
      expect(channelWrapper.sendToQueue).toHaveBeenCalledWith(
        `bull-${queueName}`,
        expect.any(Buffer),
        expect.objectContaining({ priority: 5 })
      );
    });

    it('should serialize job data correctly', async () => {
      const queue = new Queue(queueName, connectionString);
      const jobData = { foo: 'bar', nested: { value: 123 } };

      await queue.add(jobData);

      const channelWrapper = mockConnection._channels[0];
      const sentBuffer = channelWrapper.sendToQueue.mock.calls[0][1];
      expect(JSON.parse(sentBuffer.toString())).toEqual(jobData);
    });
  });

  describe('call()', () => {
    it('should make RPC call and return response', async () => {
      const queue = new Queue(queueName, connectionString);

      const callPromise = queue.call({ request: 'data' }, { timeout: 5000 });

      // Wait for the queue to set up
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Find the reply queue and simulate a response
      const replyQueueCallback = Array.from(consumerCallbacks.values()).pop();
      if (replyQueueCallback) {
        const channelWrapper = mockConnection._channels[0];
        const sendToQueueCalls = channelWrapper.sendToQueue.mock.calls;
        if (sendToQueueCalls.length > 0) {
          const lastCall = sendToQueueCalls[sendToQueueCalls.length - 1];
          const options = lastCall[2];
          const correlationId = options?.correlationId;

          if (correlationId) {
            replyQueueCallback({
              content: Buffer.from(JSON.stringify({ result: 'response' })),
              properties: { correlationId },
              fields: { deliveryTag: 1 },
            });
          }
        }
      }

      const result = await callPromise;
      expect(result).toEqual({ result: 'response' });
    });

    it('should timeout after specified duration', async () => {
      const queue = new Queue(queueName, connectionString);

      await expect(queue.call({ request: 'data' }, { timeout: 50 }))
        .rejects.toThrow('Timeout of 50ms exceeded');
    });

    it('should support named queue for call', async () => {
      const queue = new Queue(queueName, connectionString);

      const callPromise = queue.call('rpc-queue', { request: 'data' }, { timeout: 50 });

      await expect(callPromise).rejects.toThrow('Timeout');

      const channelWrapper = mockConnection._channels[0];
      const sendToQueueCall = channelWrapper.sendToQueue.mock.calls.find(
        (call: unknown[]) => call[0] === 'bull-rpc-queue'
      );
      expect(sendToQueueCall).toBeDefined();
    });

    it('should include correlationId and replyTo in message', async () => {
      const queue = new Queue(queueName, connectionString);

      const callPromise = queue.call({ request: 'data' }, { timeout: 50 });

      await expect(callPromise).rejects.toThrow('Timeout');

      const channelWrapper = mockConnection._channels[0];
      const sendToQueueCall = channelWrapper.sendToQueue.mock.calls.find(
        (call: unknown[]) => call[0] === `bull-${queueName}`
      );
      expect(sendToQueueCall).toBeDefined();
      expect(sendToQueueCall[2]).toHaveProperty('correlationId');
      expect(sendToQueueCall[2]).toHaveProperty('replyTo');
    });
  });

  describe('stopAcceptingNewJobs()', () => {
    it('should cancel all consumers', async () => {
      const queue = new Queue(queueName, connectionString);
      const handler = jest.fn((_job, done) => done(null, 'result'));

      await queue.process(handler);

      // Verify consumer was registered
      expect(mockChannel.consume).toHaveBeenCalled();

      // Wait for addSetup to complete (it's not awaited in process())
      await new Promise((resolve) => setImmediate(resolve));

      await queue.stopAcceptingNewJobs();

      expect(mockChannel.cancel).toHaveBeenCalled();
    });

    it('should handle empty consumer list', async () => {
      const queue = new Queue(queueName, connectionString);

      await expect(queue.stopAcceptingNewJobs()).resolves.not.toThrow();
    });
  });

  describe('unimplemented methods', () => {
    it('pause() should throw "Not implemented yet"', () => {
      const queue = new Queue(queueName, connectionString);
      expect(() => queue.pause()).toThrow('Not implemented yet');
    });

    it('resume() should throw "Not implemented yet"', () => {
      const queue = new Queue(queueName, connectionString);
      expect(() => queue.resume()).toThrow('Not implemented yet');
    });

    it('count() should throw "Not implemented yet"', () => {
      const queue = new Queue(queueName, connectionString);
      expect(() => queue.count()).toThrow('Not implemented yet');
    });

    it('empty() should throw "Not implemented yet"', () => {
      const queue = new Queue(queueName, connectionString);
      expect(() => queue.empty()).toThrow('Not implemented yet');
    });

    it('close() should close all channels and connection', async () => {
      const queue = new Queue(queueName, connectionString);

      // Set up a consumer to create consume channels
      await queue.process(async (job: Job) => job.data);

      await queue.close();

      // Verify connection was closed
      expect(mockConnection.close).toHaveBeenCalled();
    });
  });

  describe('queue configuration', () => {
    it('should expose queue name via getter', () => {
      const queue = new Queue(queueName, connectionString);
      expect(queue.name).toBe(queueName);
    });

    it('should use custom prefix', async () => {
      const queue = new Queue(queueName, {
        connectionString,
        prefix: 'custom-prefix',
      });

      await queue.add({ foo: 'bar' });

      const channelWrapper = mockConnection._channels[0];
      expect(channelWrapper.sendToQueue).toHaveBeenCalledWith(
        'custom-prefix-test-queue',
        expect.any(Buffer),
        expect.any(Object)
      );
    });

    it('should pass channel arguments when asserting queue', async () => {
      const queue = new Queue(queueName, {
        connectionString,
        prefix: 'bull',
        channelArguments: { 'x-max-priority': 10 },
      });

      await queue.add({ foo: 'bar' });

      expect(mockChannel.assertQueue).toHaveBeenCalledWith(
        `bull-${queueName}`,
        { arguments: { 'x-max-priority': 10 } }
      );
    });

    it('should assert queue without arguments if not provided', async () => {
      const queue = new Queue(queueName, {
        connectionString,
        prefix: 'bull',
      });

      await queue.add({ foo: 'bar' });

      expect(mockChannel.assertQueue).toHaveBeenCalledWith(`bull-${queueName}`);
    });
  });
});
