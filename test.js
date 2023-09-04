const test = require('tape').test
const persistence = require('./')
const Redis = require('ioredis')
const mqemitterRedis = require('mqemitter-redis')
const abs = require('aedes-cached-persistence/abstract')
const db = new Redis()

db.on('error', e => {
  console.trace(e)
})

db.on('connect', unref)

function unref () {
  this.connector.stream.unref()
}

test('external Redis conn', t => {
  t.plan(2)

  const externalRedis = new Redis()
  const emitter = mqemitterRedis()

  db.on('error', e => {
    t.notOk(e)
  })

  db.on('connect', () => {
    t.pass('redis connected')
  })
  const instance = persistence({
    conn: externalRedis
  })

  instance.broker = toBroker('1', emitter)

  instance.on('ready', () => {
    t.pass('instance ready')
    externalRedis.disconnect()
    instance.destroy()
    emitter.close()
  })
})

abs({
  test,
  buildEmitter () {
    const emitter = mqemitterRedis()
    emitter.subConn.on('connect', unref)
    emitter.pubConn.on('connect', unref)

    return emitter
  },
  persistence () {
    db.flushall()
    return persistence()
  },
  waitForReady: true
})

function toBroker (id, emitter) {
  return {
    id,
    publish: emitter.emit.bind(emitter),
    subscribe: emitter.on.bind(emitter),
    unsubscribe: emitter.removeListener.bind(emitter)
  }
}

test('packet ttl', t => {
  t.plan(4)
  db.flushall()
  const emitter = mqemitterRedis()
  const instance = persistence({
    packetTTL () {
      return 1
    }
  })
  instance.broker = toBroker('1', emitter)

  const subs = [{
    clientId: 'ttlTest',
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
    brokerCounter: 42
  }
  instance.outgoingEnqueueCombi(subs, packet, function enqueued (err, saved) {
    t.notOk(err)
    t.deepEqual(saved, packet)
    setTimeout(() => {
      const offlineStream = instance.outgoingStream({ id: 'ttlTest' })
      offlineStream.on('data', offlinePacket => {
        t.notOk(offlinePacket)
      })
      offlineStream.on('end', () => {
        instance.destroy(t.pass.bind(t, 'stop instance'))
        emitter.close(t.pass.bind(t, 'stop emitter'))
      })
    }, 1100)
  })
})

test('outgoingUpdate doesn\'t clear packet ttl', t => {
  t.plan(5)
  db.flushall()
  const emitter = mqemitterRedis()
  const instance = persistence({
    packetTTL () {
      return 1
    }
  })
  instance.broker = toBroker('1', emitter)

  const client = {
    id: 'ttlTest'
  }
  const subs = [{
    clientId: client.id,
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
  instance.outgoingEnqueueCombi(subs, packet, function enqueued (err, saved) {
    t.notOk(err)
    t.deepEqual(saved, packet)
    instance.outgoingUpdate(client, packet, function updated () {
      setTimeout(() => {
        db.exists('packet:1:42', (_, exists) => {
          t.notOk(exists, 'packet key should have expired')
        })
        instance.destroy(t.pass.bind(t, 'instance dies'))
        emitter.close(t.pass.bind(t, 'emitter dies'))
      }, 1100)
    })
  })
})

test('multiple persistences', t => {
  t.plan(7)
  t.timeoutAfter(60 * 1000)
  db.flushall()
  const emitter = mqemitterRedis()
  const emitter2 = mqemitterRedis()
  const instance = persistence()
  const instance2 = persistence()
  instance.broker = toBroker('1', emitter)
  instance2.broker = toBroker('2', emitter2)

  const client = { id: 'multipleTest' }
  const subs = [{
    topic: 'hello',
    qos: 1
  }, {
    topic: 'hello/#',
    qos: 1
  }, {
    topic: 'matteo',
    qos: 1
  }]

  let gotSubs = false
  let addedSubs = false

  function close () {
    if (gotSubs && addedSubs) {
      instance.destroy(t.pass.bind(t, 'first dies'))
      instance2.destroy(t.pass.bind(t, 'second dies'))
      emitter.close(t.pass.bind(t, 'first emitter dies'))
      emitter2.close(t.pass.bind(t, 'second emitter dies'))
    }
  }

  instance2._waitFor(client, true, 'hello', () => {
    instance2.subscriptionsByTopic('hello', (err, resubs) => {
      t.notOk(err, 'subs by topic no error')
      t.deepEqual(resubs, [{
        clientId: client.id,
        topic: 'hello/#',
        qos: 1,
        rh: undefined,
        rap: undefined,
        nl: undefined
      }, {
        clientId: client.id,
        topic: 'hello',
        qos: 1,
        rh: undefined,
        rap: undefined,
        nl: undefined
      }], 'received correct subs')
      gotSubs = true
      close()
    })
  })

  let ready = false
  let ready2 = false

  function addSubs () {
    if (ready && ready2) {
      instance.addSubscriptions(client, subs, err => {
        t.notOk(err, 'add subs no error')
        addedSubs = true
        close()
      })
    }
  }

  instance.on('ready', () => {
    ready = true
    addSubs()
  })

  instance2.on('ready', () => {
    ready2 = true
    addSubs()
  })
})

test('unknown cache key', t => {
  t.plan(3)
  db.flushall()
  const emitter = mqemitterRedis()
  const instance = persistence()
  const client = { id: 'unknown_pubrec' }

  instance.broker = toBroker('1', emitter)

  // packet with no brokerId
  const packet = {
    cmd: 'pubrec',
    topic: 'hello',
    qos: 2,
    retain: false
  }

  function close () {
    instance.destroy(t.pass.bind(t, 'instance dies'))
    emitter.close(t.pass.bind(t, 'emitter dies'))
  }

  instance.outgoingUpdate(client, packet, (err, client, packet) => {
    t.equal(err.message, 'unknown key', 'Received unknown PUBREC')
    close()
  })
})

test('wills table de-duplicate', t => {
  t.plan(5)
  db.flushall()
  const emitter = mqemitterRedis()
  const instance = persistence()
  const client = { id: 'willsTest' }

  instance.broker = toBroker('1', emitter)

  const packet = {
    cmd: 'publish',
    topic: 'hello',
    payload: 'willsTest',
    qos: 1,
    retain: false,
    brokerId: instance.broker.id,
    brokerCounter: 42,
    messageId: 123
  }

  instance.putWill(client, packet, err => {
    t.notOk(err, 'putWill #1 no error')
    instance.putWill(client, packet, err => {
      t.notOk(err, 'putWill #2 no error')
      let willCount = 0
      const wills = instance.streamWill()
      wills.on('data', function (chunk) {
        willCount++
      })
      wills.on('end', function () {
        t.equal(willCount, 1, 'should only be one will')
        close()
      })
    })
  })

  function close () {
    instance.destroy(t.pass.bind(t, 'instance dies'))
    emitter.close(t.pass.bind(t, 'emitter dies'))
  }
})

test('check storeShared been deleted after time', t => {
  t.plan(10)
  db.flushall()
  const instance = persistence()
  const emitter = mqemitterRedis()
  instance.broker = toBroker('1', emitter)
  const inputTopic = 'some/+/topic'
  instance.storeSharedSubscription(inputTopic, 'someGroup', 'clientId', () => {
    instance.storeSharedSubscription(inputTopic, 'someGroup', 'clientId2', () => {
      db.zrange('sharedtowipe', 0, -1, (err, result) => {
        t.notOk(err, 'zrange #1 no error')
        t.equal(result[0], 'someGroup_some/+/topic@$share/someGroup/$client_clientId/')
        t.equal(result[1], 'someGroup_some/+/topic@$share/someGroup/$client_clientId2/')
        instance.getSharedTopics(inputTopic, (err2, topicResult1) => {
          t.notOk(err2, 'getSharedTopics #1 no error')
          db.zadd('sharedtowipe', (new Date() / 1000), 'someGroup_some/+/topic@$share/someGroup/$client_clientId/', () => {
            setTimeout(() => {
              db.zrange('sharedtowipe', 0, -1, (err3, result) => {
                t.notOk(err3, 'zrange #2 no error')
                t.equal(result.length, 1)
                instance.getSharedTopics(inputTopic, (err4, topicResult2) => {
                  t.notOk(err4, 'getSharedTopics #1 no error')
                  t.equal(topicResult2[0], '$share/someGroup/$client_clientId2/some/+/topic')
                  instance.destroy(t.pass.bind(t, 'instance dies'))
                  emitter.close(t.pass.bind(t, 'stop emitter'))
                })
              })
            }, 2 * 1000)
          })
        })
      })
    })
  })
})

test('check storeShared return to redis after it was somehow deleted', t => {
  t.plan(8)
  db.flushall()
  const instance = persistence()
  const emitter = mqemitterRedis()
  instance.broker = toBroker('1', emitter)
  const inputTopic = 'some/+/topic'
  instance.storeSharedSubscription(inputTopic, 'someGroup', 'clientId', () => {
    instance.getSharedTopics(inputTopic, (err, topicResult1) => {
      t.notOk(err, 'getSharedTopics #1 no error')
      t.equal(topicResult1[0], '$share/someGroup/$client_clientId/some/+/topic')
      // Deleting everything from DB
      db.flushall()
      instance.getSharedTopics(inputTopic, (err2, topicResult2) => {
        t.notOk(err, 'getSharedTopics #2 no error')
        // Should not have any topics
        t.equal(topicResult2.length, 0)
        // Should be restored in 10 seconds
        setTimeout(() => {
          instance.getSharedTopics(inputTopic, (err3, topicResult3) => {
            t.notOk(err3, 'getSharedTopics #3 no error')
            t.equal(topicResult3[0], '$share/someGroup/$client_clientId/some/+/topic')
            instance.destroy(t.pass.bind(t, 'instance dies'))
            emitter.close(t.pass.bind(t, 'stop emitter'))
          })
        }, 11 * 1000)
      })
    })
  })
})

test.onFinish(() => {
  process.exit(0)
})
