package com.alibaba.jstorm.redis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import com.alibaba.jstorm.common.utils.ResourceUtils;
import com.alibaba.jstorm.redis.common.RedisConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/27 20:42
 */
public abstract class AbstractRedisBolt extends BaseRichBolt {

    private static final long serialVersionUID = -4651952921541523320L;

    private Pool<Jedis> jedisPool;

    private RedisConfig redisConfig;

    public AbstractRedisBolt(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public AbstractRedisBolt(String configPath) {
        this(new RedisConfig(ResourceUtils.readAsProperties(configPath)));
    }

    @Override
    public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
        this.jedisPool = this.getJedisPool(this.redisConfig);
    }

    /**
     * getJedisPool
     * @param redisConfig
     * @return
     */
    protected Pool<Jedis> getJedisPool(RedisConfig redisConfig) {
        return new JedisSentinelPool(redisConfig.getMasterName(), redisConfig.getSentinelSet(), this.generateJedisPoolConfig(this.redisConfig), redisConfig.getTimeout(), redisConfig.getPassword(), redisConfig.getDatabase());
    }

    /**
     * generateJedisPoolConfig
     * @param redisConfig
     * @return
     */
    protected JedisPoolConfig generateJedisPoolConfig(RedisConfig redisConfig) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        if (redisConfig.getMinIdle() != null) {
            jedisPoolConfig.setMinIdle(redisConfig.getMinIdle());
        }
        if (redisConfig.getMaxIdle() != null) {
            jedisPoolConfig.setMaxIdle(redisConfig.getMaxIdle());
        }
        if (redisConfig.getMaxWait() != null) {
            jedisPoolConfig.setMaxWaitMillis(redisConfig.getMaxWait());
        }
        return jedisPoolConfig;
    }

    public Jedis getJedis() {
        return jedisPool.getResource();
    }
}
