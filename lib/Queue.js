'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; };

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _amqplib = require('amqplib');

var _amqplib2 = _interopRequireDefault(_amqplib);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

var _events = require('events');

var _crypto = require('crypto');

var _crypto2 = _interopRequireDefault(_crypto);

var _amqpConnectionManager = require('amqp-connection-manager');

var _amqpConnectionManager2 = _interopRequireDefault(_amqpConnectionManager);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var DEFAULT_TIMEOUT = 60 * 1000;

function formatError(err) {
  if (typeof err === 'string') {
    return { message: err };
  }

  if (err instanceof Error) {
    return { message: err.error, stack: err.stack };
  }

  return { message: String(err) };
}

var generateCorrelationId = function generateCorrelationId() {
  return _crypto2.default.randomBytes(10).toString('base64');
};

var DEFAULT_OPTIONS = {
  prefix: 'bull'
};

var runOnceForChannel = function runOnceForChannel(channel, fn) {
  return channel.waitForConnect().then(function () {
    return fn(channel._channel);
  });
};

var Queue = function (_EventEmitter) {
  _inherits(Queue, _EventEmitter);

  function Queue(name, connectionString, options) {
    _classCallCheck(this, Queue);

    var _this = _possibleConstructorReturn(this, (Queue.__proto__ || Object.getPrototypeOf(Queue)).call(this));

    if ((typeof connectionString === 'undefined' ? 'undefined' : _typeof(connectionString)) === 'object') {
      options = connectionString;
    } else {
      // $FlowFixMe
      options = _extends({}, DEFAULT_OPTIONS, options || {}, {
        connectionString: connectionString
      });
    }

    if (!options) {
      throw new Error('options are required');
    }

    _this._options = options;
    _this._name = name;
    _this._setup();
    return _this;
  }

  _createClass(Queue, [{
    key: '_resetToInitialState',
    value: function _resetToInitialState() {
      this._chan = null;
      this._conn = null;
      this._queuesExist = Object.create(null);
      this._consumeChan = Object.create(null);
      this._replyHandlers = new Map();
      this._replyQueue = null;
    }
  }, {
    key: '_setup',
    value: function () {
      var _ref = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee() {
        var _this2 = this;

        var conn;
        return regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                if (!this._conn) {
                  _context.next = 2;
                  break;
                }

                return _context.abrupt('return');

              case 2:

                this._resetToInitialState();

                this._conn = _amqpConnectionManager2.default.connect(this._options.connectionString);
                conn = this._conn;


                this._chan = conn.createChannel();

                conn.on('error', function (err) {
                  _this2.emit('connection:error', err);
                });

                conn.on('close', function (err) {
                  _this2.emit('connection:close', err);
                });

              case 8:
              case 'end':
                return _context.stop();
            }
          }
        }, _callee, this);
      }));

      function _setup() {
        return _ref.apply(this, arguments);
      }

      return _setup;
    }()
  }, {
    key: '_ensureQueueExists',
    value: function () {
      var _ref2 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee3(queue, channel) {
        var setup, promise;
        return regeneratorRuntime.wrap(function _callee3$(_context3) {
          while (1) {
            switch (_context3.prev = _context3.next) {
              case 0:
                if (queue in this._queuesExist) {
                  _context3.next = 6;
                  break;
                }

                setup = function () {
                  var _ref3 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee2(chan) {
                    return regeneratorRuntime.wrap(function _callee2$(_context2) {
                      while (1) {
                        switch (_context2.prev = _context2.next) {
                          case 0:
                            _context2.next = 2;
                            return chan.assertQueue(queue);

                          case 2:
                          case 'end':
                            return _context2.stop();
                        }
                      }
                    }, _callee2, this);
                  }));

                  return function setup(_x3) {
                    return _ref3.apply(this, arguments);
                  };
                }();

                promise = runOnceForChannel(this._chan, setup);

                this._queuesExist[queue] = true;
                _context3.next = 6;
                return promise;

              case 6:
              case 'end':
                return _context3.stop();
            }
          }
        }, _callee3, this);
      }));

      function _ensureQueueExists(_x, _x2) {
        return _ref2.apply(this, arguments);
      }

      return _ensureQueueExists;
    }()
  }, {
    key: '_getQueueName',
    value: function _getQueueName(name) {
      return this._options.prefix + '-' + name;
    }
  }, {
    key: '_getPublishOptions',
    value: function _getPublishOptions() {
      return {
        persistent: true // TODO
      };
    }
  }, {
    key: '_ensureConsumeChannelOpen',
    value: function _ensureConsumeChannelOpen(queue) {
      if (!(queue in this._consumeChan)) {
        this._consumeChan[queue] = this._conn.createChannel();
      }

      return this._consumeChan[queue];
    }
  }, {
    key: 'process',
    value: function () {
      var _ref4 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee6(name, concurrency, handler) {
        var _this3 = this;

        var queue,
            chan,
            promiseHandler,
            _args6 = arguments;
        return regeneratorRuntime.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                _context6.t0 = _args6.length;
                _context6.next = _context6.t0 === 1 ? 3 : _context6.t0 === 2 ? 7 : 10;
                break;

              case 3:
                // $FlowFixMe
                handler = name;
                concurrency = 1;
                name = undefined;
                return _context6.abrupt('break', 10);

              case 7:
                // (string, function) or (string, string) or (number, function) or (number, string)
                // $FlowFixMe
                handler = concurrency;
                if (typeof name === 'string') {
                  concurrency = 1;
                } else {
                  concurrency = name;
                  name = undefined;
                }
                return _context6.abrupt('break', 10);

              case 10:
                queue = this._getQueueName(name || this._name);
                chan = this._ensureConsumeChannelOpen(queue);
                promiseHandler =
                // $FlowFixMe
                handler.length === 1 ? handler : function promiseHandler(job) {
                  return new _bluebird2.default(function (resolve, reject) {
                    // $FlowFixMe
                    handler(job, function (err, result) {
                      if (err) {
                        return reject(err);
                      }

                      return resolve(result);
                    });
                  });
                };
                _context6.next = 15;
                return this._ensureQueueExists(queue, chan);

              case 15:

                chan.addSetup(function () {
                  var _ref5 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee5(chan) {
                    return regeneratorRuntime.wrap(function _callee5$(_context5) {
                      while (1) {
                        switch (_context5.prev = _context5.next) {
                          case 0:
                            _context5.next = 2;
                            return chan.prefetch(concurrency);

                          case 2:
                            chan.consume(queue, function () {
                              var _ref6 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee4(msg) {
                                var data, _job, result, pubChan, errors, newErrors, newData, _queue;

                                return regeneratorRuntime.wrap(function _callee4$(_context4) {
                                  while (1) {
                                    switch (_context4.prev = _context4.next) {
                                      case 0:
                                        data = {};
                                        _context4.prev = 1;

                                        data = JSON.parse(msg.content.toString());

                                        _job = {
                                          data: data
                                        };
                                        _context4.next = 6;
                                        return promiseHandler(_job);

                                      case 6:
                                        result = _context4.sent;

                                        if (!(typeof result !== 'undefined' && _typeof(msg.properties) === 'object' && typeof msg.properties.replyTo !== 'undefined' && typeof msg.properties.correlationId !== 'undefined')) {
                                          _context4.next = 10;
                                          break;
                                        }

                                        _context4.next = 10;
                                        return chan.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(result), 'utf8'), {
                                          correlationId: msg.properties.correlationId
                                        });

                                      case 10:

                                        chan.ack(msg);
                                        _context4.next = 32;
                                        break;

                                      case 13:
                                        _context4.prev = 13;
                                        _context4.t0 = _context4['catch'](1);

                                        chan.nack(msg, false, false);
                                        pubChan = _this3._chan;
                                        errors = data['$$errors'] || [];
                                        newErrors = [].concat(_toConsumableArray(errors), [formatError(_context4.t0)]);
                                        newData = _extends({}, data, _defineProperty({}, '$$errors', newErrors));

                                        if (!(newErrors.length < 3)) {
                                          _context4.next = 26;
                                          break;
                                        }

                                        _this3.emit('single-failure', _context4.t0);
                                        _context4.next = 24;
                                        return pubChan.sendToQueue(queue, new Buffer(JSON.stringify(newData)), _this3._getPublishOptions());

                                      case 24:
                                        _context4.next = 32;
                                        break;

                                      case 26:
                                        _this3.emit('failure', _context4.t0);
                                        _queue = _this3._getQueueName('dead-letter-queue');
                                        _context4.next = 30;
                                        return _this3._ensureQueueExists(_queue, pubChan);

                                      case 30:
                                        _context4.next = 32;
                                        return pubChan.sendToQueue(_queue, new Buffer(JSON.stringify(newData)), _this3._getPublishOptions());

                                      case 32:
                                      case 'end':
                                        return _context4.stop();
                                    }
                                  }
                                }, _callee4, _this3, [[1, 13]]);
                              }));

                              return function (_x8) {
                                return _ref6.apply(this, arguments);
                              };
                            }());

                          case 3:
                          case 'end':
                            return _context5.stop();
                        }
                      }
                    }, _callee5, _this3);
                  }));

                  return function (_x7) {
                    return _ref5.apply(this, arguments);
                  };
                }());

              case 16:
              case 'end':
                return _context6.stop();
            }
          }
        }, _callee6, this);
      }));

      function process(_x4, _x5, _x6) {
        return _ref4.apply(this, arguments);
      }

      return process;
    }()
  }, {
    key: '_fireJob',
    value: function () {
      var _ref7 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee7(name, data, opts) {
        var publishOptions = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : {};
        var queue, content, publishOpts;
        return regeneratorRuntime.wrap(function _callee7$(_context7) {
          while (1) {
            switch (_context7.prev = _context7.next) {
              case 0:
                queue = this._getQueueName(name || this._name);
                content = Buffer.from(JSON.stringify(data), 'utf8');
                publishOpts = _extends({}, this._getPublishOptions(), publishOptions);
                _context7.next = 5;
                return this._sendInternal(queue, content, publishOpts);

              case 5:
                return _context7.abrupt('return', {
                  queue: queue
                });

              case 6:
              case 'end':
                return _context7.stop();
            }
          }
        }, _callee7, this);
      }));

      function _fireJob(_x10, _x11, _x12) {
        return _ref7.apply(this, arguments);
      }

      return _fireJob;
    }()
  }, {
    key: '_sendInternal',
    value: function () {
      var _ref8 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee8(queue, content, opts) {
        var chan;
        return regeneratorRuntime.wrap(function _callee8$(_context8) {
          while (1) {
            switch (_context8.prev = _context8.next) {
              case 0:
                chan = this._chan;
                _context8.next = 3;
                return this._ensureQueueExists(queue, chan);

              case 3:
                return _context8.abrupt('return', chan.sendToQueue(queue, content, opts));

              case 4:
              case 'end':
                return _context8.stop();
            }
          }
        }, _callee8, this);
      }));

      function _sendInternal(_x13, _x14, _x15) {
        return _ref8.apply(this, arguments);
      }

      return _sendInternal;
    }()
  }, {
    key: 'add',
    value: function () {
      var _ref9 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee9(name, data, opts) {
        return regeneratorRuntime.wrap(function _callee9$(_context9) {
          while (1) {
            switch (_context9.prev = _context9.next) {
              case 0:
                if (typeof name !== 'string') {
                  opts = data;
                  data = name;
                  name = undefined;
                }

                _context9.next = 3;
                return this._fireJob(name, data, opts);

              case 3:
              case 'end':
                return _context9.stop();
            }
          }
        }, _callee9, this);
      }));

      function add(_x16, _x17, _x18) {
        return _ref9.apply(this, arguments);
      }

      return add;
    }()
  }, {
    key: '_ensureRpcQueueInternal',
    value: function () {
      var _ref10 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee11() {
        var setup, _resolve;

        return regeneratorRuntime.wrap(function _callee11$(_context11) {
          while (1) {
            switch (_context11.prev = _context11.next) {
              case 0:
                if (this._replyQueue) {
                  _context11.next = 6;
                  break;
                }

                setup = function () {
                  var _ref11 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee10(chan) {
                    var q;
                    return regeneratorRuntime.wrap(function _callee10$(_context10) {
                      while (1) {
                        switch (_context10.prev = _context10.next) {
                          case 0:
                            _context10.next = 2;
                            return chan.assertQueue('', { exclusive: true });

                          case 2:
                            q = _context10.sent;

                            _resolve(q.queue);

                          case 4:
                          case 'end':
                            return _context10.stop();
                        }
                      }
                    }, _callee10, this);
                  }));

                  return function setup(_x19) {
                    return _ref11.apply(this, arguments);
                  };
                }();

                _resolve = void 0;

                this._replyQueue = new Promise(function (resolve, reject) {
                  _resolve = resolve;
                });

                _context11.next = 6;
                return runOnceForChannel(this._chan, setup);

              case 6:
                _context11.next = 8;
                return this._replyQueue;

              case 8:
                return _context11.abrupt('return', _context11.sent);

              case 9:
              case 'end':
                return _context11.stop();
            }
          }
        }, _callee11, this);
      }));

      function _ensureRpcQueueInternal() {
        return _ref10.apply(this, arguments);
      }

      return _ensureRpcQueueInternal;
    }()
  }, {
    key: '_ensureRpcQueue',
    value: function () {
      var _ref12 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee13() {
        var setup, replyQueue, replyHandlers;
        return regeneratorRuntime.wrap(function _callee13$(_context13) {
          while (1) {
            switch (_context13.prev = _context13.next) {
              case 0:
                if (this._replyQueue) {
                  _context13.next = 8;
                  break;
                }

                setup = function () {
                  var _ref13 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee12(chan) {
                    return regeneratorRuntime.wrap(function _callee12$(_context12) {
                      while (1) {
                        switch (_context12.prev = _context12.next) {
                          case 0:
                            chan.consume(replyQueue, function (msg) {
                              var correlationId = msg.properties.correlationId;
                              var replyHandler = replyHandlers.get(correlationId);

                              if (replyHandler) {
                                replyHandler(JSON.parse(msg.content.toString()));
                                replyHandlers.delete(correlationId);
                              } else {
                                // WARN?
                              }
                            }, {
                              noAck: true
                            });

                          case 1:
                          case 'end':
                            return _context12.stop();
                        }
                      }
                    }, _callee12, this);
                  }));

                  return function setup(_x20) {
                    return _ref13.apply(this, arguments);
                  };
                }();

                _context13.next = 4;
                return this._ensureRpcQueueInternal();

              case 4:
                replyQueue = _context13.sent;
                replyHandlers = this._replyHandlers;
                _context13.next = 8;
                return runOnceForChannel(this._chan, setup);

              case 8:
                _context13.next = 10;
                return this._replyQueue;

              case 10:
                return _context13.abrupt('return', _context13.sent);

              case 11:
              case 'end':
                return _context13.stop();
            }
          }
        }, _callee13, this);
      }));

      function _ensureRpcQueue() {
        return _ref12.apply(this, arguments);
      }

      return _ensureRpcQueue;
    }()
  }, {
    key: 'call',
    value: function () {
      var _ref14 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee14(name, data, opts) {
        var _this4 = this;

        var replyTo, correlationId, _ref15, queue, timeout;

        return regeneratorRuntime.wrap(function _callee14$(_context14) {
          while (1) {
            switch (_context14.prev = _context14.next) {
              case 0:
                if (typeof name !== 'string') {
                  opts = data;
                  data = name;
                  name = undefined;
                }
                _context14.next = 3;
                return this._ensureRpcQueue();

              case 3:
                replyTo = _context14.sent;
                correlationId = generateCorrelationId();
                _context14.next = 7;
                return this._fireJob(name, data, opts, {
                  correlationId: correlationId,
                  replyTo: replyTo
                });

              case 7:
                _ref15 = _context14.sent;
                queue = _ref15.queue;
                timeout = opts && opts.timeout || DEFAULT_TIMEOUT;
                _context14.next = 12;
                return new _bluebird2.default(function (resolve, reject) {
                  _this4._replyHandlers.set(correlationId, resolve);
                }).timeout(timeout).catch(_bluebird2.default.TimeoutError, function (err) {
                  _this4._replyHandlers.delete(correlationId);
                  return Promise.reject(new Error('Timeout of ' + timeout + 'ms exceeded'));
                });

              case 12:
                return _context14.abrupt('return', _context14.sent);

              case 13:
              case 'end':
                return _context14.stop();
            }
          }
        }, _callee14, this);
      }));

      function call(_x21, _x22, _x23) {
        return _ref14.apply(this, arguments);
      }

      return call;
    }()
  }, {
    key: 'pause',
    value: function pause() {
      throw new Error('Not implemented yet');
    }
  }, {
    key: 'resume',
    value: function resume() {
      throw new Error('Not implemented yet');
    }
  }, {
    key: 'count',
    value: function count() {
      throw new Error('Not implemented yet');
    }
  }, {
    key: 'empty',
    value: function empty() {
      throw new Error('Not implemented yet');
    }
  }]);

  return Queue;
}(_events.EventEmitter);

exports.default = Queue;