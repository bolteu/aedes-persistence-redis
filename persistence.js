const Redis = require('ioredis')
const { Readable } = require('node:stream')
const msgpack = require('msgpack-lite')
const CachedPersistence = require('aedes-cached-persistence')
const Packet = CachedPersistence.Packet
const HLRU = require('hashlru')
const { QlobberTrue, Qlobber } = require('qlobber')
const qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#',
  match_empty_levels: true
}
const CLIENTKEY = 'client:'
const CLIENTSKEY = 'clients'
const WILLSKEY = 'will'
const WILLKEY = 'will:'
const RETAINEDKEY = 'retained'
const ALL_RETAINEDKEYS = `${RETAINEDKEY}:*`
const OUTGOINGKEY = 'outgoing:'
const OUTGOINGIDKEY = 'outgoing-id:'
const INCOMINGKEY = 'incoming:'
const PACKETKEY = 'packet:'
const SHAREDTOPICS = 'sharedtopics'
const SHAREDTOWIPE = 'sharedtowipe'

function clientSubKey (clientId) {
  return `${CLIENTKEY}${encodeURIComponent(clientId)}`
}

function willKey (brokerId, clientId) {
  return `${WILLKEY}${brokerId}:${encodeURIComponent(clientId)}`
}

function outgoingKey (clientId) {
  return `${OUTGOINGKEY}${encodeURIComponent(clientId)}`
}

function outgoingByBrokerKey (clientId, brokerId, brokerCounter) {
  return `${outgoingKey(clientId)}:${brokerId}:${brokerCounter}`
}

function outgoingIdKey (clientId, messageId) {
  return `${OUTGOINGIDKEY}${encodeURIComponent(clientId)}:${messageId}`
}

function incomingKey (clientId, messageId) {
  return `${INCOMINGKEY}${encodeURIComponent(clientId)}:${messageId}`
}

function packetKey (brokerId, brokerCounter) {
  return `${PACKETKEY}${brokerId}:${brokerCounter}`
}

function retainedKey (topic) {
  return `${RETAINEDKEY}:${encodeURIComponent(topic)}`
}

function packetCountKey (brokerId, brokerCounter) {
  return `${PACKETKEY}${brokerId}:${brokerCounter}:offlineCount`
}

async function getDecodedValue (db, listKey, key) {
  const value = await db.getBuffer(key)
  const decoded = value ? msgpack.decode(value) : undefined
  if (!decoded) {
    db.lrem(listKey, 0, key)
  }
  return decoded
}

async function getRetainedKeys (db, hasClusters) {
  if (hasClusters === true) {
    // Get keys of all the masters
    const masters = db.nodes('master')
    const keys = await Promise.all(
      masters.flatMap((node) => node.keys(ALL_RETAINEDKEYS))
    )
    return keys
  }
  return await db.hkeys(RETAINEDKEY)
}

async function getRetainedValueBuffer (db, topic, hasClusters) {
  if (hasClusters === true) {
    return db.getBuffer(retainedKey(topic))
  }
  return db.hgetBuffer(RETAINEDKEY, topic)
}

async function getRetainedValue (db, topic, hasClusters) {
  return msgpack.decode(await getRetainedValueBuffer(db, topic, hasClusters))
}

async function * getRetainedValuesStream (db, topics, hasClusters) {
  for (const topic of topics) {
    const buffer = await getRetainedValueBuffer(db, topic, hasClusters)
    if (buffer && buffer.length > 0) {
      yield msgpack.decode(buffer)
    }
  }
}

async function * createWillStream (db, brokers, maxWills) {
  const results = await db.lrange(WILLSKEY, 0, maxWills)
  for (const key of results) {
    const result = await getDecodedValue(db, WILLSKEY, key)
    if (!brokers || !brokers[key.split(':')[1]]) {
      yield result
    }
  }
}

class NoopLogger {
  info (message, meta) {}
  warning (message, meta) {}
  error (message, meta) {}
}

class RedisPersistence extends CachedPersistence {
  constructor (opts = {}) {
    super(opts)
    this.maxSessionDelivery = opts.maxSessionDelivery || 1000
    this.maxWills = 10000
    this.packetTTL = opts.packetTTL || (() => { return 0 })
    this.subscriptionTimers = {}
    this.sharedCacheRefreshIntervalSec = opts.shared_cache_refresh_interval_sec
    this.sharedSubscriptionRestoreIntervalSec = opts.shared_subscription_restore_interval_sec || 10
    this.sharedCachedQlobber = new Qlobber(qlobberOpts)
    // Map ( topic -> Set( groups ))
    this.cachedSharedTopicsToGroups = new Map()
    // Map ( group -> Set( client_id_topic ))
    this.cachedSharedGroupsClientTopics = new Map()

    this.messageIdCache = HLRU(100000)

    if (opts.cluster && Array.isArray(opts.cluster)) {
      this._db = new Redis.Cluster(opts.cluster)
    } else {
      this._db = opts.conn || new Redis(opts)
    }

    this.hasClusters = !!opts.cluster

    this.logger = opts.logger || new NoopLogger()

    // By default retained values query optimization is disabled (-1)
    this.retainedValuesQueryThreshold = opts.retained_values_query_threshold || -1

    if (this.sharedCacheRefreshIntervalSec) {
      this.once('ready', () => {
        setInterval(() => {
          const start = Date.now()
          this.fillSharedCache((err) => {
            if (err) {
              this.logger.warning('Error while updating shared cache', {
                topics: this.cachedSharedTopicsToGroups.size,
                groups: this.cachedSharedGroupsClientTopics.size,
                duration_ms: Date.now() - start,
                error: err
              })
            } else {
              this.logger.info('Shared cache filled', {
                topics: this.cachedSharedTopicsToGroups.size,
                groups: this.cachedSharedGroupsClientTopics.size,
                duration_ms: Date.now() - start
              })
            }
          })
        }, this.sharedCacheRefreshIntervalSec * 1000)
      })
    }
  }

  /**
   * When using clusters we store it using a compound key instead of an hash
   * to spread the load across the clusters. See issue #85.
   */
  _storeRetainedCluster (packet, cb) {
    if (packet.payload.length === 0) {
      this._db.del(retainedKey(packet.topic), cb)
    } else {
      this._db.set(retainedKey(packet.topic), msgpack.encode(packet), cb)
    }
  }

  _storeRetained (packet, cb) {
    if (packet.payload.length === 0) {
      this._db.hdel(RETAINEDKEY, packet.topic, cb)
    } else {
      this._db.hset(RETAINEDKEY, packet.topic, msgpack.encode(packet), cb)
    }
  }

  storeRetained (packet, cb) {
    if (this.hasClusters) {
      this._storeRetainedCluster(packet, cb)
    } else {
      this._storeRetained(packet, cb)
    }
  }

  async getRetainedKeys () {
    return getRetainedKeys(this._db, this.hasClusters)
  }

  hasWildcard (patterns) {
    return patterns.some((pattern) => pattern.includes('+') || pattern.includes('#'))
  }

  createRetainedStreamCombi (patterns) {
    if (patterns.length <= this.retainedValuesQueryThreshold && !this.hasWildcard(patterns)) {
      return Readable.from(getRetainedValuesStream(this._db, patterns, this.hasClusters))
    }

    const qlobber = new QlobberTrue(qlobberOpts)

    for (const pattern of patterns) {
      qlobber.add(pattern)
    }

    return Readable.from(matchRetained(this._db, qlobber, this.hasClusters))
  }

  createRetainedStream (pattern) {
    return this.createRetainedStreamCombi([pattern])
  }

  addSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }

    const toStore = {}
    let published = 0
    let errored

    for (const sub of subs) {
      toStore[sub.topic] = msgpack.encode(sub)
    }

    this._db.sadd(CLIENTSKEY, client.id, finish)
    this._db.hmsetBuffer(clientSubKey(client.id), toStore, finish)

    this._addedSubscriptions(client, subs, finish)

    function finish (err) {
      errored = err
      published++
      if (published === 3) {
        cb(errored, client)
      }
    }
  }

  removeSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
      return
    }

    const clientSK = clientSubKey(client.id)

    let errored = false
    let outstanding = 0

    function check (err) {
      if (err) {
        if (!errored) {
          errored = true
          cb(err)
        }
      }

      if (errored) {
        return
      }

      outstanding--
      if (outstanding === 0) {
        cb(null, client)
      }
    }

    const that = this
    this._db.hdel(clientSK, subs, function subKeysRemoved (err) {
      if (err) {
        return cb(err)
      }

      outstanding++
      that._db.exists(clientSK, function checkAllSubsRemoved (err, subCount) {
        if (err) {
          return check(err)
        }
        if (subCount === 0) {
          outstanding++
          that._db.del(outgoingKey(client.id), check)
          return that._db.srem(CLIENTSKEY, client.id, check)
        }
        check()
      })

      outstanding++
      that._removedSubscriptions(client, subs.map(toSub), check)
    })
  }

  subscriptionsByClient (client, cb) {
    this._db.hgetallBuffer(clientSubKey(client.id), function returnSubs (err, subs) {
      const toReturn = returnSubsForClient(subs)
      cb(err, toReturn.length > 0 ? toReturn : null, client)
    })
  }

  countOffline (cb) {
    const that = this

    this._db.scard(CLIENTSKEY, function countOfflineClients (err, count) {
      if (err) {
        return cb(err)
      }

      cb(null, that._trie.subscriptionsCount, Number.parseInt(count) || 0)
    })
  }

  subscriptionsByTopic (topic, cb) {
    if (!this.ready) {
      this.once('ready', this.subscriptionsByTopic.bind(this, topic, cb))
      return this
    }

    const result = this._trie.match(topic)

    cb(null, result)
  }

  async _setup () {
    if (this.ready) {
      return
    }

    try {
      const clientIds = await this._db.smembers(CLIENTSKEY)
      for await (const clientId of clientIds) {
        const clientHash = await this._db.hgetallBuffer(clientSubKey(clientId))
        processKeysForClient(clientId, clientHash, this._trie)
      }
      this.ready = true
      this.emit('ready')
    } catch (err) {
      this.emit('error', err)
    }
  }

  outgoingEnqueue (sub, packet, cb) {
    this.outgoingEnqueueCombi([sub], packet, cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (!subs || subs.length === 0) {
      return cb(null, packet)
    }
    let count = 0
    let outstanding = 1
    let errored = false
    const pktKey = packetKey(packet.brokerId, packet.brokerCounter)
    const countKey = packetCountKey(packet.brokerId, packet.brokerCounter)
    const ttl = this.packetTTL(packet)

    const encoded = msgpack.encode(new Packet(packet))

    if (this.hasClusters) {
      // do not do this using `mset`, fails in clusters
      outstanding += 1
      this._db.set(pktKey, encoded, finish)
      this._db.set(countKey, subs.length, finish)
    } else {
      this._db.mset(pktKey, encoded, countKey, subs.length, finish)
    }

    if (ttl > 0) {
      outstanding += 2
      this._db.expire(pktKey, ttl, finish)
      this._db.expire(countKey, ttl, finish)
    }

    for (const sub of subs) {
      const listKey = outgoingKey(sub.clientId)
      this._db.rpush(listKey, pktKey, finish)
    }

    function finish (err) {
      count++
      if (err) {
        errored = err
        return cb(err)
      }
      if (count === (subs.length + outstanding) && !errored) {
        cb(null, packet)
      }
    }
  }

  outgoingUpdate (client, packet, cb) {
    const that = this
    if ('brokerId' in packet && 'messageId' in packet) {
      updateWithClientData(this, client, packet, cb)
    } else {
      augmentWithBrokerData(this, client, packet, function updateClient (err) {
        if (err) { return cb(err, client, packet) }

        updateWithClientData(that, client, packet, cb)
      })
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    const that = this
    const clientListKey = outgoingKey(client.id)
    const messageIdKey = outgoingIdKey(client.id, packet.messageId)

    const clientKey = this.messageIdCache.get(messageIdKey)
    this.messageIdCache.remove(messageIdKey)

    if (!clientKey) {
      return cb(null, packet)
    }

    let count = 0
    let expected = 3
    let errored = false

    // TODO can be cached in case of wildcard deliveries
    this._db.getBuffer(clientKey, function clearMessageId (err, buf) {
      let origPacket
      let pktKey
      let countKey
      if (err) {
        errored = err
        return cb(err)
      }
      if (buf) {
        origPacket = msgpack.decode(buf)
        origPacket.messageId = packet.messageId

        pktKey = packetKey(origPacket.brokerId, origPacket.brokerCounter)
        countKey = packetCountKey(origPacket.brokerId, origPacket.brokerCounter)

        if (clientKey !== pktKey) { // qos=2
          that._db.del(clientKey, finish)
        } else {
          finish()
        }
      } else {
        finish()
      }

      that._db.lrem(clientListKey, 0, pktKey, finish)

      that._db.decr(countKey, (err, remained) => {
        if (err) {
          errored = err
          return cb(err)
        }
        if (remained === 0) {
          if (that.hasClusters) {
            expected++
            // do not remove multiple keys at once, fails in clusters
            that._db.del(pktKey, finish)
            that._db.del(countKey, finish)
          } else {
            that._db.del(pktKey, countKey, finish)
          }
        } else {
          finish()
        }
      })

      function finish (err) {
        count++
        if (err) {
          errored = err
          return cb(err)
        }
        if (count === expected && !errored) {
          cb(err, origPacket)
        }
      }
    })
  }

  outgoingStream (client) {
    const clientListKey = outgoingKey(client.id)
    const db = this._db
    const maxSessionDelivery = this.maxSessionDelivery

    async function * lrangeResult () {
      const results = await db.lrange(clientListKey, 0, maxSessionDelivery)
      for (const key of results) {
        yield getDecodedValue(db, clientListKey, key)
      }
    }

    return Readable.from(lrangeResult())
  }

  incomingStorePacket (client, packet, cb) {
    const key = incomingKey(client.id, packet.messageId)
    const newp = new Packet(packet)
    newp.messageId = packet.messageId
    this._db.set(key, msgpack.encode(newp), cb)
  }

  incomingGetPacket (client, packet, cb) {
    const key = incomingKey(client.id, packet.messageId)
    this._db.getBuffer(key, function decodeBuffer (err, buf) {
      if (err) {
        return cb(err)
      }

      if (!buf) {
        return cb(new Error('no such packet'))
      }

      cb(null, msgpack.decode(buf), client)
    })
  }

  incomingDelPacket (client, packet, cb) {
    const key = incomingKey(client.id, packet.messageId)
    this._db.del(key, cb)
  }

  putWill (client, packet, cb) {
    const key = willKey(this.broker.id, client.id)
    packet.clientId = client.id
    packet.brokerId = this.broker.id
    this._db.lrem(WILLSKEY, 0, key) // Remove duplicates
    this._db.rpush(WILLSKEY, key)
    this._db.setBuffer(key, msgpack.encode(packet), encodeBuffer)

    function encodeBuffer (err) {
      cb(err, client)
    }
  }

  getWill (client, cb) {
    const key = willKey(this.broker.id, client.id)
    this._db.getBuffer(key, function getWillForClient (err, packet) {
      if (err) { return cb(err) }

      let result = null

      if (packet) {
        result = msgpack.decode(packet)
      }

      cb(null, result, client)
    })
  }

  delWill (client, cb) {
    const key = willKey(client.brokerId, client.id)
    let result = null
    const that = this
    this._db.lrem(WILLSKEY, 0, key)
    this._db.getBuffer(key, function getClientWill (err, packet) {
      if (err) { return cb(err) }

      if (packet) {
        result = msgpack.decode(packet)
      }

      that._db.del(key, function deleteWill (err) {
        cb(err, result, client)
      })
    })
  }

  streamWill (brokers) {
    return Readable.from(createWillStream(this._db, brokers, this.maxWills))
  }

  * #getClientIdFromEntries (entries) {
    for (const entry of entries) {
      yield entry.clientId
    }
  }

  getClientList (topic) {
    const entries = this._trie.match(topic, topic)
    return Readable.from(this.#getClientIdFromEntries(entries))
  }

  buildClientSharedTopic (group, clientId) {
    return `$share/${group}/$client_${clientId}/`
  }

  parseSharedTopic (topic) {
    if (!topic || !topic.startsWith('$share/')) return null

    const groupEndIndx = topic.indexOf('/', 7)
    if (groupEndIndx === -1) {
      return null
    }
    const group = topic.substring(7, groupEndIndx)
    const clientIndx = topic.indexOf('/$client_', groupEndIndx)
    if (clientIndx === -1) {
      return {
        group,
        client_id: null,
        topic: topic.substring(8 + group.length)
      }
    }
    const clientEndIndx = topic.indexOf('/', clientIndx + 9)
    const clientId = topic.substring(clientIndx + 9, clientEndIndx)
    const topicItself = topic.substring(clientEndIndx + 1, topic.length)

    return {
      group,
      client_id: clientId,
      topic: topicItself
    }
  }

  clearSharedSubscriptionCache () {
    this.sharedCachedQlobber = new Qlobber(qlobberOpts)
    this.cachedSharedTopicsToGroups = new Map()
    this.cachedSharedGroupsClientTopics = new Map()
  }

  storeSharedSubscriptionToCache (topic, group, clientId) {
    const groupTopic = group + '_' + topic
    const clientTopic = this.buildClientSharedTopic(group, clientId)

    this.sharedCachedQlobber.add(topic, topic)

    const groupsToTopics = this.cachedSharedTopicsToGroups.get(topic)
    if (groupsToTopics) {
      groupsToTopics.add(groupTopic)
    } else {
      this.cachedSharedTopicsToGroups.set(topic, new Set([groupTopic]))
    }

    const groupClients = this.cachedSharedGroupsClientTopics.get(groupTopic)
    if (groupClients) {
      groupClients.add(clientTopic)
    } else {
      this.cachedSharedGroupsClientTopics.set(groupTopic, new Set([clientTopic]))
    }
  }

  storeSharedSubscription (topic, group, clientId, cb) {
    if (this.sharedCacheRefreshIntervalSec) {
      this.storeSharedSubscriptionToCache(topic, group, clientId)
    }
    const clientTopic = this.buildClientSharedTopic(group, clientId)
    const groupTopic = group + '_' + topic
    const pipeline = this._db.multi()
    pipeline.sadd(SHAREDTOPICS, topic)
    pipeline.sadd(topic, groupTopic)
    pipeline.sadd(groupTopic, clientTopic)
    // Adding each shared topic to the list where it will be wiped if it will be not updated within 20 secs.
    pipeline.zadd(SHAREDTOWIPE, (Date.now() / 1000) + 20, `${groupTopic}@${clientTopic}`)
    pipeline.exec((err) => {
      if (err) {
        return cb(err)
      }

      if (!this.subscriptionTimers[clientId]) {
        this.subscriptionTimers[clientId] = {}
      }

      if (this.subscriptionTimers[clientId] && !this.subscriptionTimers[clientId][groupTopic]) {
        // Update all shared topics on Redis each 10 seconds.
        // So if broker and client alive - topic will persist, if not - will be wiped
        this.subscriptionTimers[clientId][groupTopic] = setInterval(() => {
          this.storeSharedSubscription(topic, group, clientId, () => {})
        }, this.sharedSubscriptionRestoreIntervalSec * 1000)
      }

      if (!this._cleanupOldShared) {
        // Protection from leackage of shared subscriptions, in case if client is already dead but topic somehow
        // left on the list
        this._cleanupOldShared = setInterval(() => {
          const luaScript = `
            local sharedtowipe = KEYS[1]
            local sharedTopics = KEYS[2]
            local currentTime = tonumber(ARGV[1])
            local deleted = {}
            local elements = redis.call("zrangebyscore", sharedtowipe, "-inf", currentTime)
            for _, element in ipairs(elements) do
              local parts = {}
              for str in string.gmatch(element, "([^@]+)") do
                table.insert(parts, str)
              end
              local groupTopic, clientTopic = parts[1], parts[2]
              local parts2 = {}
              for str in string.gmatch(groupTopic, "([^_]+)") do
                table.insert(parts2, str)
              end
              local originalTopic = parts2[2]
              redis.call("srem", groupTopic, clientTopic)
              redis.call("zrem", sharedtowipe, element)
              table.insert(deleted, clientTopic)
              local groupCardinality = redis.call("scard", groupTopic)
              if groupCardinality == 0 then
                redis.call("srem", originalTopic, groupTopic)
                local topicCardinality = redis.call("scard", originalTopic)
                if topicCardinality == 0 then
                  redis.call("srem", sharedTopics, originalTopic)
                end
              end
            end

            return deleted
          `
          this._db.eval(luaScript, 2, [SHAREDTOWIPE, SHAREDTOPICS, (Date.now() / 1000)], () => {})
        }, 1 * 1000) // Each second check should we remove some outdated shared subscription or not
      }
      cb(null, clientTopic + topic)
    })
  }

  removeSharedSubscriptionFromCache (topic, group, clientId) {
    const groupTopic = group + '_' + topic
    const clientTopic = this.buildClientSharedTopic(group, clientId)
    const groupClients = this.cachedSharedGroupsClientTopics.get(groupTopic)
    if (groupClients && groupClients.size !== 0) {
      groupClients.delete(clientTopic)
      if (groupClients.size === 0) {
        const groupsToTopics = this.cachedSharedTopicsToGroups.get(topic)
        if (groupsToTopics && groupsToTopics.size !== 0) {
          groupsToTopics.delete(groupTopic)
          if (groupsToTopics.size === 0) {
            this.sharedCachedQlobber.remove(topic)
          }
        }
      }
    }
  }

  removeSharedSubscription (topic, group, clientId, cb) {
    if (this.sharedCacheRefreshIntervalSec) {
      this.removeSharedSubscriptionFromCache(topic, group, clientId)
    }
    const clientTopic = this.buildClientSharedTopic(group, clientId)
    const groupTopic = group + '_' + topic
    const luaScript = `
            local originalTopic = KEYS[1]
            local groupTopic = KEYS[2]
            local clientTopic = KEYS[3]
            local sharedTopics = KEYS[4]
            redis.call("srem", groupTopic, clientTopic)
            local groupCardinality = redis.call("scard", groupTopic)
            if groupCardinality == 0 then
              redis.call("srem", originalTopic, groupTopic)
              local topicCardinality = redis.call("scard", originalTopic)
              if topicCardinality == 0 then
                redis.call("srem", sharedTopics, originalTopic)
              end
            end
        `
    // Remove restoration interval at first
    if (this.subscriptionTimers[clientId] && this.subscriptionTimers[clientId][groupTopic]) {
      clearInterval(this.subscriptionTimers[clientId][groupTopic])
      delete this.subscriptionTimers[clientId][groupTopic]
    }

    this._db.eval(luaScript, 4, [topic, groupTopic, clientTopic, SHAREDTOPICS], cb)
  }

  fillSharedCache (cb) {
    const that = this
    that._db.smembers(SHAREDTOPICS, function (err, sharedTopics) {
      if (err) {
        return cb(err)
      }
      if (!sharedTopics) {
        return cb(null)
      }

      const newSharedCachedQlobber = new Qlobber(qlobberOpts)
      const newCachedSharedTopicsToGroups = new Map()

      const pipeline = that._db.pipeline()
      sharedTopics.forEach((topicFromResult) => {
        newSharedCachedQlobber.add(topicFromResult, topicFromResult)
        pipeline.smembers(topicFromResult)
      })

      // Execute pipeline
      const allGroupTopics = []
      pipeline.exec((err, replies) => {
        if (err) {
          cb(err)
        } else {
          // Map replies to topics
          replies.forEach((reply, index) => {
            if (reply[1] && reply[1].length > 0) {
              const topic = sharedTopics[index]
              newCachedSharedTopicsToGroups.set(topic, new Set(reply[1]))
              reply[1].forEach((clientTopic) => {
                allGroupTopics.push(clientTopic)
              })
            }
          })
          const pipelineForGroups = that._db.pipeline()
          allGroupTopics.forEach((item) => {
            pipelineForGroups.smembers(item)
          })
          pipelineForGroups.exec((err, groupsReplies) => {
            if (err) {
              cb(err)
            } else {
              // Updating shared state only in the very end to make it visible to getSharedTopicsFromCache.
              // Otherwise it could observe inconsistent state.
              that.sharedCachedQlobber = newSharedCachedQlobber
              that.cachedSharedTopicsToGroups = newCachedSharedTopicsToGroups
              that.cachedSharedGroupsClientTopics = new Map()
              groupsReplies.forEach((reply, index) => {
                const groupTopic = allGroupTopics[index]
                const clientTopics = reply[1]
                that.cachedSharedGroupsClientTopics.set(groupTopic, new Set(clientTopics))
              })

              cb(null)
            }
          })
        }
      })
    })
  }

  getSharedTopicsFromCache (topic, cb) {
    const resultTopics = []
    const matches = this.sharedCachedQlobber.match(topic)
    for (const match of matches) {
      const groups = this.cachedSharedTopicsToGroups.get(match) ?? []
      for (const group of groups) {
        const clientTopics = Array.from(this.cachedSharedGroupsClientTopics.get(group))
        const randomTopic = clientTopics[Math.floor(Math.random() * clientTopics.length)]
        resultTopics.push(randomTopic + topic)
      }
    }
    cb(null, resultTopics)
  }

  getSharedTopics (topic, cb) {
    if (this.sharedCacheRefreshIntervalSec) {
      this.getSharedTopicsFromCache(topic, cb)
    } else {
      const luaScript = `
        local inputTopics = ARGV
        local originalTopic = KEYS[1]
        local resultTopics = {}
        for i=1, #inputTopics do
            local groups = redis.call("smembers",inputTopics[i])
            for j=1, #groups do
                local clientTopic = redis.call("srandmember", groups[j])
                table.insert(resultTopics, clientTopic .. originalTopic)
            end
        end

        return resultTopics
      `
      const that = this
      this._db.smembers(SHAREDTOPICS, function (err, sharedTopics) {
        if (err) {
          return cb(err)
        }
        if (!sharedTopics) {
          return cb(null, [])
        }
        const qlobber = new Qlobber(qlobberOpts)
        for (const topicFromResult of sharedTopics) {
          qlobber.add(topicFromResult, topicFromResult)
        }

        const matches = qlobber.match(topic)
        if (!matches) {
          return cb(null, [])
        }

        that._db.eval(luaScript, 1, [topic, ...matches], function (err, clientTopics) {
          if (err) {
            return cb(err)
          }

          cb(null, clientTopics)
        })
      })
    }
  }

  restoreOriginalTopicFromSharedOne (topic) {
    if (topic.startsWith('$share/') && topic.includes('/$client_')) {
      // extracting $share/group/$client_client_id from topic
      const originTopicIndex = topic.indexOf('/', topic.indexOf('/', 7) + 1)
      return topic.substring(originTopicIndex + 1, topic.length)
    }

    return topic
  }

  _buildAugment (listKey) {
    const that = this
    return function decodeAndAugment (key, enc, cb) {
      that._db.getBuffer(key, function decodeMessage (err, result) {
        let decoded
        if (result) {
          decoded = msgpack.decode(result)
        }
        if (err || !decoded) {
          that._db.lrem(listKey, 0, key)
        }
        cb(err, decoded)
      })
    }
  }

  destroy (cb) {
    const that = this
    if (this._cleanupOldShared) {
      clearInterval(this._cleanupOldShared)
    }

    for (const clientId in this.subscriptionTimers) {
      for (const groupTopic in this.subscriptionTimers[clientId]) {
        clearInterval(this.subscriptionTimers[clientId][groupTopic])
        delete this.subscriptionTimers[clientId][groupTopic]
      }
      delete this.subscriptionTimers[clientId]
    }

    CachedPersistence.prototype.destroy.call(this, function disconnect () {
      that._db.disconnect()

      if (cb) {
        that._db.on('end', cb)
      }
    })
  }
}

async function * matchRetained (db, qlobber, hasClusters) {
  for (const key of await getRetainedKeys(db, hasClusters)) {
    const topic = hasClusters ? decodeURIComponent(key.split(':')[1]) : key
    if (qlobber.test(topic)) {
      yield getRetainedValue(db, topic, hasClusters)
    }
  }
}

function toSub (topic) {
  return {
    topic
  }
}

function returnSubsForClient (subs) {
  const subKeys = Object.keys(subs)

  const toReturn = []

  if (subKeys.length === 0) {
    return toReturn
  }

  for (const subKey of subKeys) {
    if (subs[subKey].length === 1) { // version 8x fallback, QoS saved not encoded object
      toReturn.push({
        topic: subKey,
        qos: Number.parseInt(subs[subKey])
      })
    } else {
      toReturn.push(msgpack.decode(subs[subKey]))
    }
  }

  return toReturn
}

function processKeysForClient (clientId, clientHash, trie) {
  const topics = Object.keys(clientHash)
  for (const topic of topics) {
    const sub = msgpack.decode(clientHash[topic])
    sub.clientId = clientId
    trie.add(topic, sub)
  }
}

function updateWithClientData (that, client, packet, cb) {
  const clientListKey = outgoingKey(client.id)
  const messageIdKey = outgoingIdKey(client.id, packet.messageId)
  const pktKey = packetKey(packet.brokerId, packet.brokerCounter)

  const ttl = that.packetTTL(packet)
  if (packet.cmd && packet.cmd !== 'pubrel') { // qos=1
    that.messageIdCache.set(messageIdKey, pktKey)
    if (ttl > 0) {
      return that._db.set(pktKey, msgpack.encode(packet), 'EX', ttl, updatePacket)
    }
    return that._db.set(pktKey, msgpack.encode(packet), updatePacket)
  }

  // qos=2
  const clientUpdateKey = outgoingByBrokerKey(client.id, packet.brokerId, packet.brokerCounter)
  that.messageIdCache.set(messageIdKey, clientUpdateKey)

  let count = 0
  that._db.lrem(clientListKey, 0, pktKey, (err, removed) => {
    if (err) {
      return cb(err)
    }
    if (removed === 1) {
      that._db.rpush(clientListKey, clientUpdateKey, finish)
    } else {
      finish()
    }
  })

  const encoded = msgpack.encode(packet)

  if (ttl > 0) {
    that._db.set(clientUpdateKey, encoded, 'EX', ttl, setPostKey)
  } else {
    that._db.set(clientUpdateKey, encoded, setPostKey)
  }

  function updatePacket (err, result) {
    if (err) {
      return cb(err, client, packet)
    }

    if (result !== 'OK') {
      cb(new Error('no such packet'), client, packet)
    } else {
      cb(null, client, packet)
    }
  }

  function setPostKey (err, result) {
    if (err) {
      return cb(err, client, packet)
    }

    if (result !== 'OK') {
      cb(new Error('no such packet'), client, packet)
    } else {
      finish()
    }
  }

  function finish (err) {
    if (++count === 2) {
      cb(err, client, packet)
    }
  }
}

function augmentWithBrokerData (that, client, packet, cb) {
  const messageIdKey = outgoingIdKey(client.id, packet.messageId)

  const key = that.messageIdCache.get(messageIdKey)
  if (!key) {
    return cb(new Error('unknown key'))
  }
  const tokens = key.split(':')
  packet.brokerId = tokens[tokens.length - 2]
  packet.brokerCounter = tokens[tokens.length - 1]

  cb(null)
}

module.exports = (opts) => new RedisPersistence(opts)
