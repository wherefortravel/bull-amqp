import { waitForRabbitMQ, getConnectionString } from './helpers';

beforeAll(async () => {
  console.log(`Connecting to RabbitMQ at ${getConnectionString()}...`);
  await waitForRabbitMQ();
  console.log('RabbitMQ is ready');
}, 60000);
