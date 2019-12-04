package com.github.wangshichun.jedis.internal;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.KeyMergeUtil;
import redis.clients.util.SafeEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wangshichun on 2016/8/8.
 */
public class BinaryJedisCluster implements BasicCommands, BinaryJedisClusterCommands,
        MultiKeyBinaryJedisClusterCommands, JedisClusterBinaryScriptingCommands, Closeable {

    public static final short HASHSLOTS = 16384;
    protected static final int DEFAULT_TIMEOUT = 2000;
    protected static final int DEFAULT_MAX_REDIRECTIONS = 5;

    protected int maxRedirections;

    protected JedisSlotBasedConnectionHandler connectionHandler;

    public BinaryJedisCluster(Set<HostAndPort> nodes, int timeout) {
        this(nodes, timeout, DEFAULT_MAX_REDIRECTIONS, new GenericObjectPoolConfig());
    }

    public BinaryJedisCluster(Set<HostAndPort> nodes) {
        this(nodes, DEFAULT_TIMEOUT);
    }

    public BinaryJedisCluster(Set<HostAndPort> jedisClusterNode, int timeout, int maxRedirections,
                              final GenericObjectPoolConfig poolConfig) {
        this.connectionHandler = new JedisSlotBasedConnectionHandler(jedisClusterNode, poolConfig,
                timeout);
        this.maxRedirections = maxRedirections;
    }

    public BinaryJedisCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout,
                              int soTimeout, int maxRedirections, final GenericObjectPoolConfig poolConfig, String password) {
        this.connectionHandler = new JedisSlotBasedConnectionHandler(jedisClusterNode, poolConfig,
                connectionTimeout, soTimeout, password);
        this.maxRedirections = maxRedirections;
    }

    public void close() throws IOException {
        if (connectionHandler != null) {
            for (JedisPool pool : connectionHandler.getNodes().values()) {
                try {
                    if (pool != null) {
                        pool.destroy();
                    }
                } catch (Exception e) {
                    // pass
                }
            }
        }
    }

    public Map<String, JedisPool> getClusterNodes() {
        return connectionHandler.getNodes();
    }

    public String set(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.set(key, value);
            }
        }.runBinary(false, key);
    }

    public String set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx,
                      final long time) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.set(key, value, nxxx, expx, time);
            }
        }.runBinary(false, key);
    }

    public byte[] get(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.get(key);
            }
        }.runBinary(true, key);
    }

    public Boolean exists(final byte[] key) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxRedirections) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.exists(key);
            }
        }.runBinary(true, key);
    }

    public Long exists(final byte[]... keys) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.exists(keys);
            }
        }.runBinary(true, keys.length, keys);
    }

    public Long persist(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.persist(key);
            }
        }.runBinary(false, key);
    }

    public String type(final byte[] key) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.type(key);
            }
        }.runBinary(true, key);
    }

    public Long expire(final byte[] key, final int seconds) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.expire(key, seconds);
            }
        }.runBinary(false, key);
    }

    public Long pexpire(final byte[] key, final long milliseconds) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.pexpire(key, milliseconds);
            }
        }.runBinary(false, key);
    }

    public Long expireAt(final byte[] key, final long unixTime) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.expireAt(key, unixTime);
            }
        }.runBinary(false, key);
    }

    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.pexpire(key, millisecondsTimestamp);
            }
        }.runBinary(false, key);
    }

    public Long ttl(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.ttl(key);
            }
        }.runBinary(true, key);
    }

    public Boolean setbit(final byte[] key, final long offset, final boolean value) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxRedirections) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.setbit(key, offset, value);
            }
        }.runBinary(false, key);
    }

    public Boolean setbit(final byte[] key, final long offset, final byte[] value) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxRedirections) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.setbit(key, offset, value);
            }
        }.runBinary(false, key);
    }

    public Boolean getbit(final byte[] key, final long offset) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxRedirections) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.getbit(key, offset);
            }
        }.runBinary(true, key);
    }

    public Long setrange(final byte[] key, final long offset, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.setrange(key, offset, value);
            }
        }.runBinary(false, key);
    }


    public byte[] getrange(final byte[] key, final long startOffset, final long endOffset) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.getrange(key, startOffset, endOffset);
            }
        }.runBinary(true, key);
    }


    public byte[] getSet(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.getSet(key, value);
            }
        }.runBinary(false, key);
    }


    public Long setnx(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.setnx(key, value);
            }
        }.runBinary(false, key);
    }

    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.setex(key, seconds, value);
            }
        }.runBinary(false, key);
    }


    public Long decrBy(final byte[] key, final long integer) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.decrBy(key, integer);
            }
        }.runBinary(false, key);
    }


    public Long decr(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.decr(key);
            }
        }.runBinary(false, key);
    }


    public Long incrBy(final byte[] key, final long integer) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.incrBy(key, integer);
            }
        }.runBinary(false, key);
    }


    public Double incrByFloat(final byte[] key, final double value) {
        return new JedisClusterCommand<Double>(connectionHandler, maxRedirections) {
            @Override
            public Double execute(Jedis connection) {
                return connection.incrByFloat(key, value);
            }
        }.runBinary(false, key);
    }

    public Long incr(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.incr(key);
            }
        }.runBinary(false, key);
    }


    public Long append(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.append(key, value);
            }
        }.runBinary(false, key);
    }


    public byte[] substr(final byte[] key, final int start, final int end) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.substr(key, start, end);
            }
        }.runBinary(false, key);
    }


    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hset(key, field, value);
            }
        }.runBinary(false, key);
    }


    public byte[] hget(final byte[] key, final byte[] field) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.hget(key, field);
            }
        }.runBinary(true, key);
    }


    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hsetnx(key, field, value);
            }
        }.runBinary(false, key);
    }


    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.hmset(key, hash);
            }
        }.runBinary(false, key);
    }


    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.hmget(key, fields);
            }
        }.runBinary(true, key);
    }


    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hincrBy(key, field, value);
            }
        }.runBinary(false, key);
    }


    public Double hincrByFloat(final byte[] key, final byte[] field, final double value) {
        return new JedisClusterCommand<Double>(connectionHandler, maxRedirections) {
            @Override
            public Double execute(Jedis connection) {
                return connection.hincrByFloat(key, field, value);
            }
        }.runBinary(false, key);
    }


    public Boolean hexists(final byte[] key, final byte[] field) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxRedirections) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.hexists(key, field);
            }
        }.runBinary(true, key);
    }


    public Long hdel(final byte[] key, final byte[]... field) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hdel(key, field);
            }
        }.runBinary(false, key);
    }


    public Long hlen(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hlen(key);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> hkeys(final byte[] key) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.hkeys(key);
            }
        }.runBinary(true, key);
    }


    public Collection<byte[]> hvals(final byte[] key) {
        return new JedisClusterCommand<Collection<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Collection<byte[]> execute(Jedis connection) {
                return connection.hvals(key);
            }
        }.runBinary(true, key);
    }


    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        return new JedisClusterCommand<Map<byte[], byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Map<byte[], byte[]> execute(Jedis connection) {
                return connection.hgetAll(key);
            }
        }.runBinary(true, key);
    }


    public Long rpush(final byte[] key, final byte[]... args) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.rpush(key, args);
            }
        }.runBinary(false, key);
    }


    public Long lpush(final byte[] key, final byte[]... args) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.lpush(key, args);
            }
        }.runBinary(false, key);
    }


    public Long llen(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.llen(key);
            }
        }.runBinary(true, key);
    }


    public List<byte[]> lrange(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.lrange(key, start, end);
            }
        }.runBinary(true, key);
    }


    public String ltrim(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.ltrim(key, start, end);
            }
        }.runBinary(false, key);
    }


    public byte[] lindex(final byte[] key, final long index) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.lindex(key, index);
            }
        }.runBinary(true, key);
    }


    public String lset(final byte[] key, final long index, final byte[] value) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.lset(key, index, value);
            }
        }.runBinary(false, key);
    }


    public Long lrem(final byte[] key, final long count, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.lrem(key, count, value);
            }
        }.runBinary(false, key);
    }


    public byte[] lpop(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.lpop(key);
            }
        }.runBinary(false, key);
    }


    public byte[] rpop(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.rpop(key);
            }
        }.runBinary(false, key);
    }


    public Long sadd(final byte[] key, final byte[]... member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sadd(key, member);
            }
        }.runBinary(false, key);
    }


    public Set<byte[]> smembers(final byte[] key) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.smembers(key);
            }
        }.runBinary(true, key);
    }


    public Long srem(final byte[] key, final byte[]... member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.srem(key, member);
            }
        }.runBinary(false, key);
    }

    public byte[] spop(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.spop(key);
            }
        }.runBinary(false, key);
    }


    public Set<byte[]> spop(final byte[] key, final long count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.spop(key, count);
            }
        }.runBinary(false, key);
    }


    public Long scard(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.scard(key);
            }
        }.runBinary(true, key);
    }


    public Boolean sismember(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxRedirections) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.sismember(key, member);
            }
        }.runBinary(true, key);
    }


    public byte[] srandmember(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.srandmember(key);
            }
        }.runBinary(true, key);
    }


    public Long strlen(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.strlen(key);
            }
        }.runBinary(true, key);
    }


    public Long zadd(final byte[] key, final double score, final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zadd(key, score, member);
            }
        }.runBinary(false, key);
    }


    public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zadd(key, scoreMembers);
            }
        }.runBinary(false, key);
    }


    public Long zadd(final byte[] key, final double score, final byte[] member,
                     final ZAddParams params) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zadd(key, score, member, params);
            }
        }.runBinary(false, key);
    }


    public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers, final ZAddParams params) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zadd(key, scoreMembers, params);
            }
        }.runBinary(false, key);
    }


    public Set<byte[]> zrange(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrange(key, start, end);
            }
        }.runBinary(true, key);
    }


    public Long zrem(final byte[] key, final byte[]... member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zrem(key, member);
            }
        }.runBinary(false, key);
    }


    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return new JedisClusterCommand<Double>(connectionHandler, maxRedirections) {
            @Override
            public Double execute(Jedis connection) {
                return connection.zincrby(key, score, member);
            }
        }.runBinary(false, key);
    }


    public Double zincrby(final byte[] key, final double score, final byte[] member,
                          final ZIncrByParams params) {
        return new JedisClusterCommand<Double>(connectionHandler, maxRedirections) {
            @Override
            public Double execute(Jedis connection) {
                return connection.zincrby(key, score, member, params);
            }
        }.runBinary(false, key);
    }


    public Long zrank(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zrank(key, member);
            }
        }.runBinary(true, key);
    }

    public Long zrevrank(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zrevrank(key, member);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrange(key, start, end);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrangeWithScores(key, start, end);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrevrangeWithScores(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrevrangeWithScores(key, start, end);
            }
        }.runBinary(true, key);
    }


    public Long zcard(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zcard(key);
            }
        }.runBinary(true, key);
    }


    public Double zscore(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Double>(connectionHandler, maxRedirections) {
            @Override
            public Double execute(Jedis connection) {
                return connection.zscore(key, member);
            }
        }.runBinary(true, key);
    }


    public List<byte[]> sort(final byte[] key) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.sort(key);
            }
        }.runBinary(true, key);
    }


    public List<byte[]> sort(final byte[] key, final SortingParams sortingParameters) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.sort(key, sortingParameters);
            }
        }.runBinary(true, key);
    }


    public Long zcount(final byte[] key, final double min, final double max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zcount(key, min, max);
            }
        }.runBinary(true, key);
    }

    public Long zcount(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zcount(key, min, max);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByScore(key, min, max);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByScore(key, min, max);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrangeByScore(key, max, min);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max,
                                     final int offset, final int count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByScore(key, min, max, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrangeByScore(key, max, min);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max,
                                     final int offset, final int count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByScore(key, min, max, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min,
                                        final int offset, final int count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrangeByScore(key, max, min, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrangeByScoreWithScores(key, min, max);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrevrangeByScoreWithScores(key, max, min);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max,
                                              final int offset, final int count) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min,
                                        final int offset, final int count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrangeByScore(key, max, min, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrangeByScoreWithScores(key, min, max);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrevrangeByScoreWithScores(key, max, min);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max,
                                              final int offset, final int count) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max,
                                                 final double min, final int offset, final int count) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max,
                                                 final byte[] min, final int offset, final int count) {
        return new JedisClusterCommand<Set<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public Set<Tuple> execute(Jedis connection) {
                return connection.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }.runBinary(true, key);
    }


    public Long zremrangeByRank(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zremrangeByRank(key, start, end);
            }
        }.runBinary(false, key);
    }


    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zremrangeByScore(key, start, end);
            }
        }.runBinary(false, key);
    }


    public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zremrangeByScore(key, start, end);
            }
        }.runBinary(false, key);
    }


    public Long linsert(final byte[] key, final Client.LIST_POSITION where, final byte[] pivot,
                        final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.linsert(key, where, pivot, value);
            }
        }.runBinary(false, key);
    }


    public Long lpushx(final byte[] key, final byte[]... arg) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.lpushx(key, arg);
            }
        }.runBinary(false, key);
    }


    public Long rpushx(final byte[] key, final byte[]... arg) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.rpushx(key, arg);
            }
        }.runBinary(false, key);
    }


    public Long del(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.del(key);
            }
        }.runBinary(false, key);
    }


    public byte[] echo(final byte[] arg) {
        // note that it'll be run from arbitary node
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.echo(arg);
            }
        }.runBinary(true, arg);
    }


    public Long bitcount(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.bitcount(key);
            }
        }.runBinary(true, key);
    }


    public Long bitcount(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.bitcount(key, start, end);
            }
        }.runBinary(true, key);
    }


    public Long pfadd(final byte[] key, final byte[]... elements) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.pfadd(key, elements);
            }
        }.runBinary(false, key);
    }


    public long pfcount(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.pfcount(key);
            }
        }.runBinary(true, key);
    }


    public List<byte[]> srandmember(final byte[] key, final int count) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.srandmember(key, count);
            }
        }.runBinary(true, key);
    }


    public Long zlexcount(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zlexcount(key, min, max);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByLex(key, min, max);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max,
                                   final int offset, final int count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByLex(key, min, max, offset, count);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrangeByLex(key, max, min);
            }
        }.runBinary(true, key);
    }


    public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min,
                                      final int offset, final int count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrangeByLex(key, max, min, offset, count);
            }
        }.runBinary(true, key);
    }


    public Long zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zremrangeByLex(key, min, max);
            }
        }.runBinary(false, key);
    }


    public Object eval(final byte[] script, final byte[] keyCount, final byte[]... params) {
        return new JedisClusterCommand<Object>(connectionHandler, maxRedirections) {
            @Override
            public Object execute(Jedis connection) {
                return connection.eval(script, keyCount, params);
            }
        }.runBinary(false, Integer.parseInt(SafeEncoder.encode(keyCount)), params);
    }


    public Object eval(final byte[] script, final int keyCount, final byte[]... params) {
        return new JedisClusterCommand<Object>(connectionHandler, maxRedirections) {
            @Override
            public Object execute(Jedis connection) {
                return connection.eval(script, keyCount, params);
            }
        }.runBinary(false, keyCount, params);
    }


    public Object eval(final byte[] script, final List<byte[]> keys, final List<byte[]> args) {
        return new JedisClusterCommand<Object>(connectionHandler, maxRedirections) {
            @Override
            public Object execute(Jedis connection) {
                return connection.eval(script, keys, args);
            }
        }.runBinary(false, keys.size(), keys.toArray(new byte[keys.size()][]));
    }


    public Object eval(final byte[] script, byte[] key) {
        return new JedisClusterCommand<Object>(connectionHandler, maxRedirections) {
            @Override
            public Object execute(Jedis connection) {
                return connection.eval(script);
            }
        }.runBinary(false, key);
    }


    public Object evalsha(final byte[] script, byte[] key) {
        return new JedisClusterCommand<Object>(connectionHandler, maxRedirections) {
            @Override
            public Object execute(Jedis connection) {
                return connection.evalsha(script);
            }
        }.runBinary(false, key);
    }


    public Object evalsha(final byte[] sha1, final List<byte[]> keys, final List<byte[]> args) {
        return new JedisClusterCommand<Object>(connectionHandler, maxRedirections) {
            @Override
            public Object execute(Jedis connection) {
                return connection.evalsha(sha1, keys, args);
            }
        }.runBinary(false, keys.size(), keys.toArray(new byte[keys.size()][]));
    }


    public Object evalsha(final byte[] sha1, final int keyCount, final byte[]... params) {
        return new JedisClusterCommand<Object>(connectionHandler, maxRedirections) {
            @Override
            public Object execute(Jedis connection) {
                return connection.evalsha(sha1, keyCount, params);
            }
        }.runBinary(false, keyCount, params);
    }


    public List<Long> scriptExists(final byte[] key, final byte[][] sha1) {
        return new JedisClusterCommand<List<Long>>(connectionHandler, maxRedirections) {
            @Override
            public List<Long> execute(Jedis connection) {
                return connection.scriptExists(sha1);
            }
        }.runBinary(false, key);
    }


    public byte[] scriptLoad(final byte[] script, final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.scriptLoad(script);
            }
        }.runBinary(false, key);
    }


    public String scriptFlush(final byte[] key) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.scriptFlush();
            }
        }.runBinary(false, key);
    }


    public String scriptKill(byte[] key) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.scriptKill();
            }
        }.runBinary(false, key);
    }


    public Long del(final byte[]... keys) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.del(keys);
            }
        }.runBinary(false, keys.length, keys);
    }


    public List<byte[]> blpop(final int timeout, final byte[]... keys) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.blpop(timeout, keys);
            }
        }.runBinary(false, keys.length, keys);
    }


    public List<byte[]> brpop(final int timeout, final byte[]... keys) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.brpop(timeout, keys);
            }
        }.runBinary(false, keys.length, keys);
    }


    public List<byte[]> mget(final byte[]... keys) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.mget(keys);
            }
        }.runBinary(true, keys.length, keys);
    }


    public String mset(final byte[]... keysvalues) {
        byte[][] keys = new byte[keysvalues.length / 2][];

        for (int keyIdx = 0; keyIdx < keys.length; keyIdx++) {
            keys[keyIdx] = keysvalues[keyIdx * 2];
        }

        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.mset(keysvalues);
            }
        }.runBinary(false, keys.length, keys);
    }


    public Long msetnx(final byte[]... keysvalues) {
        byte[][] keys = new byte[keysvalues.length / 2][];

        for (int keyIdx = 0; keyIdx < keys.length; keyIdx++) {
            keys[keyIdx] = keysvalues[keyIdx * 2];
        }

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.msetnx(keysvalues);
            }
        }.runBinary(false, keys.length, keys);
    }


    public String rename(final byte[] oldkey, final byte[] newkey) {
        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.rename(oldkey, newkey);
            }
        }.runBinary(false, 2, oldkey, newkey);
    }


    public Long renamenx(final byte[] oldkey, final byte[] newkey) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.renamenx(oldkey, newkey);
            }
        }.runBinary(false, 2, oldkey, newkey);
    }


    public byte[] rpoplpush(final byte[] srckey, final byte[] dstkey) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.rpoplpush(srckey, dstkey);
            }
        }.runBinary(false, 2, srckey, dstkey);
    }


    public Set<byte[]> sdiff(final byte[]... keys) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.sdiff(keys);
            }
        }.runBinary(true, keys.length, keys);
    }


    public Long sdiffstore(final byte[] dstkey, final byte[]... keys) {
        byte[][] wholeKeys = KeyMergeUtil.merge(dstkey, keys);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sdiffstore(dstkey, keys);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }


    public Set<byte[]> sinter(final byte[]... keys) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.sinter(keys);
            }
        }.runBinary(true, keys.length, keys);
    }


    public Long sinterstore(final byte[] dstkey, final byte[]... keys) {
        byte[][] wholeKeys = KeyMergeUtil.merge(dstkey, keys);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sinterstore(dstkey, keys);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }

    public Long smove(final byte[] srckey, final byte[] dstkey, final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.smove(srckey, dstkey, member);
            }
        }.runBinary(false, 2, srckey, dstkey);
    }


    public Long sort(final byte[] key, final SortingParams sortingParameters, final byte[] dstkey) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sort(key, sortingParameters, dstkey);
            }
        }.runBinary(false, 2, key, dstkey);
    }

    public Long sort(final byte[] key, final byte[] dstkey) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sort(key, dstkey);
            }
        }.runBinary(false, 2, key, dstkey);
    }

    public Set<byte[]> sunion(final byte[]... keys) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.sunion(keys);
            }
        }.runBinary(true, keys.length, keys);
    }

    public Long sunionstore(final byte[] dstkey, final byte[]... keys) {
        byte[][] wholeKeys = KeyMergeUtil.merge(dstkey, keys);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sunionstore(dstkey, keys);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }

    public Long zinterstore(final byte[] dstkey, final byte[]... sets) {
        byte[][] wholeKeys = KeyMergeUtil.merge(dstkey, sets);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zinterstore(dstkey, sets);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }

    public Long zinterstore(final byte[] dstkey, final ZParams params, final byte[]... sets) {
        byte[][] wholeKeys = KeyMergeUtil.merge(dstkey, sets);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zinterstore(dstkey, params, sets);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }


    public Long zunionstore(final byte[] dstkey, final byte[]... sets) {
        byte[][] wholeKeys = KeyMergeUtil.merge(dstkey, sets);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zunionstore(dstkey, sets);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }

    public Long zunionstore(final byte[] dstkey, final ZParams params, final byte[]... sets) {
        byte[][] wholeKeys = KeyMergeUtil.merge(dstkey, sets);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zunionstore(dstkey, params, sets);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }


    public byte[] brpoplpush(final byte[] source, final byte[] destination, final int timeout) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxRedirections) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.brpoplpush(source, destination, timeout);
            }
        }.runBinary(false, 2, source, destination);
    }


    public Long publish(final byte[] channel, final byte[] message) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.publish(channel, message);
            }
        }.runWithAnyNode();
    }


    public void subscribe(final BinaryJedisPubSub jedisPubSub, final byte[]... channels) {
        new JedisClusterCommand<Integer>(connectionHandler, maxRedirections) {
            @Override
            public Integer execute(Jedis connection) {
                connection.subscribe(jedisPubSub, channels);
                return 0;
            }
        }.runWithAnyNode();
    }

    public void psubscribe(final BinaryJedisPubSub jedisPubSub, final byte[]... patterns) {
        new JedisClusterCommand<Integer>(connectionHandler, maxRedirections) {
            @Override
            public Integer execute(Jedis connection) {
                connection.subscribe(jedisPubSub, patterns);
                return 0;
            }
        }.runWithAnyNode();
    }
    public Long bitop(final BitOP op, final byte[] destKey, final byte[]... srcKeys) {
        byte[][] wholeKeys = KeyMergeUtil.merge(destKey, srcKeys);

        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.bitop(op, destKey, srcKeys);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }

    public String pfmerge(final byte[] destkey, final byte[]... sourcekeys) {
        byte[][] wholeKeys = KeyMergeUtil.merge(destkey, sourcekeys);

        return new JedisClusterCommand<String>(connectionHandler, maxRedirections) {
            @Override
            public String execute(Jedis connection) {
                return connection.pfmerge(destkey, sourcekeys);
            }
        }.runBinary(false, wholeKeys.length, wholeKeys);
    }

    public Long pfcount(final byte[]... keys) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.pfcount(keys);
            }
        }.runBinary(true, keys.length, keys);
    }

  /*
   * below methods will be removed at 3.0
   */

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String ping() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String quit() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String flushDB() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster and Redis Cluster only uses
     *             db index 0 scheduled to be removed on next major release
     */
    @Deprecated
    public Long dbSize() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster and Redis Cluster only uses
     *             db index 0 scheduled to be removed on next major release
     */
    @Deprecated
    public String select(int index) {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String flushAll() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster and Redis Cluster doesn't
     *             support authorization scheduled to be removed on next major release
     */
    @Deprecated
    public String auth(String password) {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String save() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String bgsave() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String bgrewriteaof() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public Long lastsave() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String shutdown() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String info() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String info(String section) {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String slaveof(String host, int port) {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String slaveofNoOne() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster and Redis Cluster only uses
     *             db index 0 scheduled to be removed on next major release
     */
    @Deprecated
    public Long getDB() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String debug(DebugParams params) {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public String configResetStat() {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    /**
     * @deprecated No key operation doesn't make sense for Redis Cluster scheduled to be removed on
     *             next major release
     */
    @Deprecated
    public Long waitReplicas(int replicas, long timeout) {
        throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }


    public Long geoadd(final byte[] key, final double longitude, final double latitude,
                       final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.geoadd(key, longitude, latitude, member);
            }
        }.runBinary(false, key);
    }


    public Long geoadd(final byte[] key, final Map<byte[], GeoCoordinate> memberCoordinateMap) {
        return new JedisClusterCommand<Long>(connectionHandler, maxRedirections) {
            @Override
            public Long execute(Jedis connection) {
                return connection.geoadd(key, memberCoordinateMap);
            }
        }.runBinary(false, key);
    }


    public Double geodist(final byte[] key, final byte[] member1, final byte[] member2) {
        return new JedisClusterCommand<Double>(connectionHandler, maxRedirections) {
            @Override
            public Double execute(Jedis connection) {
                return connection.geodist(key, member1, member2);
            }
        }.runBinary(true, key);
    }


    public Double geodist(final byte[] key, final byte[] member1, final byte[] member2,
                          final GeoUnit unit) {
        return new JedisClusterCommand<Double>(connectionHandler, maxRedirections) {
            @Override
            public Double execute(Jedis connection) {
                return connection.geodist(key, member1, member2, unit);
            }
        }.runBinary(true, key);
    }


    public List<byte[]> geohash(final byte[] key, final byte[]... members) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.geohash(key, members);
            }
        }.runBinary(true, key);
    }


    public List<GeoCoordinate> geopos(final byte[] key, final byte[]... members) {
        return new JedisClusterCommand<List<GeoCoordinate>>(connectionHandler, maxRedirections) {
            @Override
            public List<GeoCoordinate> execute(Jedis connection) {
                return connection.geopos(key, members);
            }
        }.runBinary(true, key);
    }


    public List<GeoRadiusResponse> georadius(final byte[] key, final double longitude,
                                             final double latitude, final double radius, final GeoUnit unit) {
        return new JedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxRedirections) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadius(key, longitude, latitude, radius, unit);
            }
        }.runBinary(true, key);
    }


    public List<GeoRadiusResponse> georadius(final byte[] key, final double longitude,
                                             final double latitude, final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return new JedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxRedirections) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadius(key, longitude, latitude, radius, unit, param);
            }
        }.runBinary(true, key);
    }


    public List<GeoRadiusResponse> georadiusByMember(final byte[] key, final byte[] member,
                                                     final double radius, final GeoUnit unit) {
        return new JedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxRedirections) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadiusByMember(key, member, radius, unit);
            }
        }.runBinary(true, key);
    }


    public List<GeoRadiusResponse> georadiusByMember(final byte[] key, final byte[] member,
                                                     final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return new JedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxRedirections) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadiusByMember(key, member, radius, unit, param);
            }
        }.runBinary(true, key);
    }


    public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor) {
        return new JedisClusterCommand<ScanResult<Map.Entry<byte[], byte[]>>>(connectionHandler,
                maxRedirections) {
            @Override
            public ScanResult<Map.Entry<byte[], byte[]>> execute(Jedis connection) {
                return connection.hscan(key, cursor);
            }
        }.runBinary(true, key);
    }


    public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor,
                                                       final ScanParams params) {
        return new JedisClusterCommand<ScanResult<Map.Entry<byte[], byte[]>>>(connectionHandler,
                maxRedirections) {
            @Override
            public ScanResult<Map.Entry<byte[], byte[]>> execute(Jedis connection) {
                return connection.hscan(key, cursor, params);
            }
        }.runBinary(true, key);
    }


    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor) {
        return new JedisClusterCommand<ScanResult<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public ScanResult<byte[]> execute(Jedis connection) {
                return connection.sscan(key, cursor);
            }
        }.runBinary(true, key);
    }


    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return new JedisClusterCommand<ScanResult<byte[]>>(connectionHandler, maxRedirections) {
            @Override
            public ScanResult<byte[]> execute(Jedis connection) {
                return connection.sscan(key, cursor, params);
            }
        }.runBinary(true, key);
    }


    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor) {
        return new JedisClusterCommand<ScanResult<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public ScanResult<Tuple> execute(Jedis connection) {
                return connection.zscan(key, cursor);
            }
        }.runBinary(true, key);
    }


    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return new JedisClusterCommand<ScanResult<Tuple>>(connectionHandler, maxRedirections) {
            @Override
            public ScanResult<Tuple> execute(Jedis connection) {
                return connection.zscan(key, cursor, params);
            }
        }.runBinary(true, key);
    }
}