# bull-amqp

An implementation of the bull npm library [https://github.com/OptimalBits/bull] backed by the AMQP protocol, compatible with RabbitMQ

*NOTE: WORK IN PROGRESS - USE AT YOUR OWN RISK*

Currently being used in production at [whereto.com](https://whereto.com "WhereTo.com")

Demo with RPC calling:

```javascript
import Queue from 'bull-amqp'

const queue = new Queue(
  'test',
  process.env.AMQP_CONNECTION_STRING,
  {
    prefix: 'w2',
  },
)

queue.process(1, async (job) => {
  console.log('got job', job.data)

  if (Math.random() > 0.9) { // to illustrate retrying
    throw new Error('oops')
  }

  return {
    hello: true,
    ...job.data,
  }
})

queue.on('error', (err) => {
  console.log('queue caught error', err)
})

setInterval(async () => {
  console.log('response', await queue.call({ job: 'test' }))
}, 5000)

```

# Environment variables

*BULL_AMQP_MIN_PROCESSING_TIME_MS* - sets minimum processing time in milliseconds
*BULL_AMQP_CONCURRENCY* - overrides concurrency value
