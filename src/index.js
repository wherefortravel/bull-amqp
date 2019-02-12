// @flow

import Queue from './Queue'

require('amqp-connection-manager/lib/ChannelWrapper').default.prototype._runOnce = function(fn) {
  return this.waitForConnect().then(() => fn(this._channel))
}

export default Queue
