package com.alibaba.jstorm.redis.common;

import com.alibaba.jstorm.common.utils.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

/**
 * @author heyc
 * @date 2018/8/28 15:08
 */
public class RedisClient {

    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private static RedisClient defaultClient;

    private transient Pool<Jedis> jedisPool;

    private RedisClient() {
    }

    /**
     * getDefault
     * @return
     */
    public static RedisClient getDefault(RedisConfig redisConfig) {
        if (defaultClient == null) {
            synchronized (RedisClient.class) {
                if (defaultClient == null) {
                    defaultClient = newInstance(redisConfig);
                }
            }
        }
        return defaultClient;
    }

    /**
     * newInstance
     * @param redisConfig
     * @return
     */
    public static RedisClient newInstance(RedisConfig redisConfig) {
        logger.info("create redis client with config: {}", redisConfig);
        RedisClient client = new RedisClient();
        JedisPoolConfig jedisPoolConfig = client.generateJedisPoolConfig(redisConfig);
        client.jedisPool = new JedisSentinelPool(redisConfig.getMasterName(), redisConfig.getSentinelSet(), jedisPoolConfig, redisConfig.getTimeout(), redisConfig.getPassword(), redisConfig.getDatabase());
        return client;
    }

    /**
     * lookupDefaultConfigPath
     * @return
     */
    public static String lookupDefaultConfigPath() {
        if (ResourceUtils.exists("config/redis.yaml")) {
            return "config/redis.yaml";
        }
        if (ResourceUtils.exists("config/redis.properties")) {
            return "config/redis.properties";
        }
        if (ResourceUtils.exists("redis.yaml")) {
            return "redis.yaml";
        }
        if (ResourceUtils.exists("redis.properties")) {
            return "redis.properties";
        }
        if (ResourceUtils.exists("classpath:redis.yaml")) {
            return "classpath:redis.yaml";
        }
        if (ResourceUtils.exists("classpath:redis.properties")) {
            return "classpath:redis.properties";
        }
        return "classpath:default-redis.yaml";
    }

    /**
     * close
     */
    protected void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
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

    /**
     * execute
     * @param command
     * @param <T>
     * @return
     */
    public <T> T execute(RedisCommand<T> command) {
        Jedis jedis = jedisPool.getResource();
        T result = command.doInRedis(jedis);
        jedisPool.returnResource(jedis);
        return result;
    }

}
