'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Queue = require('./Queue');

var _Queue2 = _interopRequireDefault(_Queue);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

require('amqp-connection-manager/lib/ChannelWrapper').default.prototype._runOnce = function (fn) {
  var _this = this;

  return this.waitForConnect().then(function () {
    return fn(_this._channel);
  });
};

exports.default = _Queue2.default;