import { waitForRabbitMQ, getConnectionString } from './helpers';

// Suppress "Channel ended" errors that occur during Jest teardown
// These are expected when Jest force-exits with open AMQP connections
const originalUnhandledRejection = process.listeners('unhandledRejection');
process.removeAllListeners('unhandledRejection');
process.on('unhandledRejection', (reason: unknown) => {
  const message = reason instanceof Error ? reason.message : String(reason);
  if (message.includes('Channel ended') || message.includes('Channel closed')) {
    // Suppress expected teardown errors
    return;
  }
  // Re-emit to original handlers for real errors
  originalUnhandledRejection.forEach((handler) => handler(reason, Promise.reject(reason)));
});

beforeAll(async () => {
  console.log(`Connecting to RabbitMQ at ${getConnectionString()}...`);
  await waitForRabbitMQ();
  console.log('RabbitMQ is ready');
}, 60000);
