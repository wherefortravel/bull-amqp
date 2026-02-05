declare module 'amqp-connection-manager' {
  import { EventEmitter } from 'events';
  import { Channel, Options as AmqplibOptions } from 'amqplib';

  export interface AmqpConnectionManager extends EventEmitter {
    createChannel(options?: CreateChannelOpts): ChannelWrapper;
    close(): Promise<void>;
    isConnected(): boolean;
  }

  export interface CreateChannelOpts {
    json?: boolean;
    setup?: (channel: Channel) => Promise<void>;
    name?: string;
  }

  export interface ChannelWrapper extends EventEmitter {
    addSetup(setup: (channel: Channel) => Promise<void>): Promise<void>;
    removeSetup(setup: (channel: Channel) => Promise<void>): Promise<void>;
    waitForConnect(): Promise<void>;
    sendToQueue(queue: string, content: Buffer, options?: AmqplibOptions.Publish): Promise<boolean>;
    ack(message: unknown, allUpTo?: boolean): void;
    nack(message: unknown, allUpTo?: boolean, requeue?: boolean): void;
    close(): Promise<void>;
  }

  export function connect(urls: string | string[], options?: unknown): AmqpConnectionManager;

  export default {
    connect,
  };
}
