import * as connections from 'amqp-connection-manager';

export const getConnectionString = (): string =>
  process.env.AMQP_URL || 'amqp://guest:guest@localhost:5673';

export async function waitForRabbitMQ(maxAttempts = 30): Promise<void> {
  const connectionString = getConnectionString();

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const conn = connections.connect(connectionString);
      const channel = conn.createChannel();

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Connection timeout'));
        }, 2000);

        conn.on('connect', () => {
          clearTimeout(timeout);
          resolve();
        });

        conn.on('connectFailed', (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });

      await channel.close();
      await conn.close();
      return;
    } catch (err) {
      if (attempt === maxAttempts) {
        throw new Error(
          `Failed to connect to RabbitMQ after ${maxAttempts} attempts: ${err}`
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

let queueCounter = 0;
export function uniqueQueueName(base: string): string {
  queueCounter++;
  return `${base}-${Date.now()}-${queueCounter}-${Math.random().toString(36).slice(2, 8)}`;
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
