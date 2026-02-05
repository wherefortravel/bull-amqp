import Queue from './Queue';

export default Queue;
export { Options, Job, JobOpts } from './Queue';

// CommonJS compatibility - allows `const Queue = require('bull-amqp')`
module.exports = Queue;
module.exports.default = Queue;
