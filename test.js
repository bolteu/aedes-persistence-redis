'use strict'

var test = require('tape').test
var persistence = require('./')
var Redis = require('ioredis')
var mqemitterRedis = require('mqemitter-redis')
var abs = require('aedes-cached-persistence/abstract')
var db = new Redis()

db.on('error', function (e) {
  console.trace(e)
})

db.on('connect', unref)

function unref() {
  this.connector.stream.unref()
}

test('external Redis conn', function (t) {
  t.plan(2)

  var externalRedis = new Redis()
  var emitter = mqemitterRedis()

  db.on('error', function (e) {
    t.notOk(e)
  })

  db.on('connect', function () {
    t.pass('redis connected')
  })
  var instance = persistence({
    conn: externalRedis
  })

  instance.broker = toBroker('1', emitter)

  instance.on('ready', function () {
    t.pass('instance ready')
    externalRedis.disconnect()
    instance.destroy()
    emitter.close()
  })
})

abs({
  test: test,
  buildEmitter: function () {
    const emitter = mqemitterRedis()
    emitter.subConn.on('connect', unref)
    emitter.pubConn.on('connect', unref)

    return emitter
  },
  persistence: function () {
    db.flushall()
    return persistence()
  },
  waitForReady: true
})

function toBroker(id, emitter) {
  return {
    id: id,
    publish: emitter.emit.bind(emitter),
    subscribe: emitter.on.bind(emitter),
    unsubscribe: emitter.removeListener.bind(emitter)
  }
}

test('packet ttl', function (t) {
  t.plan(4)
  db.flushall()
  var emitter = mqemitterRedis()
  var instance = persistence({
    packetTTL: function () {
      return 1
    }
  })
  instance.broker = toBroker('1', emitter)

  var subs = [{
    clientId: 'ttlTest',
    topic: 'hello',
    qos: 1
  }]
  var packet = {
    cmd: 'publish',
    topic: 'hello',
    payload: 'ttl test',
    qos: 1,
    retain: false,
    brokerId: instance.broker.id,
    brokerCounter: 42
  }
  instance.outgoingEnqueueCombi(subs, packet, function enqueued(err, saved) {
    t.notOk(err)
    t.deepEqual(saved, packet)
    setTimeout(function () {
      var offlineStream = instance.outgoingStream({ id: 'ttlTest' })
      offlineStream.on('data', function (offlinePacket) {
        t.notOk(offlinePacket)
      })
      offlineStream.on('end', function () {
        instance.destroy(t.pass.bind(t, 'stop instance'))
        emitter.close(t.pass.bind(t, 'stop emitter'))
      })
    }, 1100)
  })
})

test('outgoingUpdate doesn\'t clear packet ttl', function (t) {
  t.plan(5)
  db.flushall()
  const emitter = mqemitterRedis()
  const instance = persistence({
    packetTTL: function () {
      return 1
    }
  })
  instance.broker = toBroker('1', emitter)

  const client = {
    id: 'ttlTest'
  }
  const subs = [{
    clientId: client.clientId,
    topic: 'hello',
    qos: 1
  }]
  const packet = {
    cmd: 'publish',
    topic: 'hello',
    payload: 'ttl test',
    qos: 1,
    retain: false,
    brokerId: instance.broker.id,
    brokerCounter: 42,
    messageId: 123
  }
  instance.outgoingEnqueueCombi(subs, packet, function enqueued(err, saved) {
    t.notOk(err)
    t.deepEqual(saved, packet)
    instance.outgoingUpdate(client, packet, function updated() {
      setTimeout(function () {
        db.exists('packet:1:42', (_, exists) => {
          t.notOk(exists, 'packet key should have expired')
        })
        instance.destroy(t.pass.bind(t, 'instance dies'))
        emitter.close(t.pass.bind(t, 'emitter dies'))
      }, 1100)
    })
  })
})

test('test unsubscribe', function(t) {
  t.plan(8)
  db.flushall()
  var emitter = mqemitterRedis()
  var instance = persistence()
  instance.broker = toBroker('1', emitter)

  var client = { id: 'remove_sub' }

  function close() {
    instance.destroy(t.pass.bind(t, 'instance dies'))
    emitter.close(t.pass.bind(t, 'emitter dies'))
  }

  instance.addSubscriptions(client, [{topic: 't1', qos: 2}], function (err) {
    t.notOk(err, 'add subs no error')
    instance.subscriptionsByTopic('t1', function (err2, resubs) {
      t.notOk(err2, 'subs by topic no error')
      t.deepEqual(resubs, [{
        clientId: client.id,
        topic: 't1',
        qos: 2
      }])

      instance.removeSubscriptions(client, ['t1'], function (err3) {
        t.notOk(err3, 'subs by topic no error')
        instance.subscriptionsByTopic('t1', function (err4, resubs) {
          t.notOk(err4, 'subs by topic no error')
          t.deepEqual(resubs, [])
          close();
        });
      });

    });
  });
})

test('test re-subscription by qos 0', function (t) {
  t.plan(8)
  db.flushall()
  var emitter = mqemitterRedis()
  var instance = persistence()
  instance.broker = toBroker('1', emitter)

  var client = { id: 'resub_qos0' }

  function close() {
    instance.destroy(t.pass.bind(t, 'instance dies'))
    emitter.close(t.pass.bind(t, 'emitter dies'))
  }

  instance.addSubscriptions(client, [{topic: 't1', qos: 2}], function (err) {
    t.notOk(err, 'add subs no error')
    instance.subscriptionsByTopic('t1', function (err2, resubs) {
      t.notOk(err2, 'subs by topic no error')
      t.deepEqual(resubs, [{
        clientId: client.id,
        topic: 't1',
        qos: 2
      }])

      instance.addSubscriptions(client, [{topic: 't1', qos: 0}], function (err3) {
        t.notOk(err3, 'subs by topic no error')
        instance.subscriptionsByTopic('t1', function (err4, resubs) {
          t.notOk(err4, 'subs by topic no error')
          t.deepEqual(resubs, [])
          close();
        });
      });

    });
  });
});

test('multiple persistences', function (t) {
  t.plan(11)
  db.flushall()
  var emitter = mqemitterRedis()
  var emitter2 = mqemitterRedis()
  var instance = persistence()
  var instance2 = persistence()
  instance.broker = toBroker('1', emitter)
  instance2.broker = toBroker('2', emitter2)

  var client = { id: 'multipleTest' }
  var subs = [{
    topic: 'hello',
    qos: 1
  }, {
    topic: 'matteo',
    qos: 1
  }, {
    topic: 'zeroqos',
    qos: 0
  }]

  function close() {
    instance.destroy(t.pass.bind(t, 'first dies'))
    instance2.destroy(t.pass.bind(t, 'second dies'))
    emitter.close(t.pass.bind(t, 'first emitter dies'))
    emitter2.close(t.pass.bind(t, 'second emitter dies'))
  }

  instance.addSubscriptions(client, subs, function (err) {
    t.notOk(err, 'add subs no error')
    instance2.subscriptionsByTopic('hello', function (err, resubs) {
      t.notOk(err, 'subs by topic no error')
      t.deepEqual(resubs, [{
        clientId: client.id,
        topic: 'hello',
        qos: 1
      }])
      instance2.subscriptionsByTopic('wrongtopic', function (err2, resubs2) {
        t.notOk(err2, 'subs2 by topic no error')
        t.deepEqual(resubs2, []);

        instance2.subscriptionsByTopic('zeroqos', function (err3, resubs3) {
          t.notOk(err3, 'subs3 by topic no error')
          t.deepEqual(resubs3, []);

          close()
        });
      });
    })
  })
})

test('unknown cache key', function (t) {
  t.plan(3)
  db.flushall()
  var emitter = mqemitterRedis()
  var instance = persistence()
  var client = { id: 'unknown_pubrec' }

  instance.broker = toBroker('1', emitter)

  // packet with no brokerId
  var packet = {
    cmd: 'pubrec',
    topic: 'hello',
    qos: 2,
    retain: false
  }

  function close() {
    instance.destroy(t.pass.bind(t, 'instance dies'))
    emitter.close(t.pass.bind(t, 'emitter dies'))
  }

  instance.outgoingUpdate(client, packet, function (err, client, packet) {
    t.equal(err.message, 'unknown key', 'Received unknown PUBREC')
    close()
  })
})

test.onFinish(function () {
  process.exit(0)
})
