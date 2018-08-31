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

    public static volatile boolean initialize;

    private static volatile Pool<Jedis> jedisPool;

    /**
     * 初始化
     * @param configPath
     */
    public static void init(String configPath) {
        if (jedisPool == null) {
            synchronized (RedisClient.class) {
                if (jedisPool == null) {
                    logger.info("initialize redis use config path: {}", configPath);
                    reInit(new RedisConfig(ResourceUtils.readAsProperties(configPath)));
                }
            }
        }
    }

    /**
     * 初始化
     * @param redisConfig
     */
    public static void init(RedisConfig redisConfig) {
        if (jedisPool == null) {
            synchronized (RedisClient.class) {
                if (jedisPool == null) {
                    reInit(redisConfig);
                }
            }
        }
    }

    /**
     * 初始化连接池
     * @param redisConfig
     */
    protected static void reInit(RedisConfig redisConfig) {
        logger.info("初始化 redis 连接池: {}", redisConfig);
        close();
        jedisPool = new JedisSentinelPool(redisConfig.getMasterName(), redisConfig.getSentinelSet(), generateJedisPoolConfig(redisConfig), redisConfig.getTimeout(), redisConfig.getPassword(), redisConfig.getDatabase());
        initialize = true;
    }

    /**
     * close
     */
    protected static void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    /**
     * generateJedisPoolConfig
     * @param redisConfig
     * @return
     */
    protected static JedisPoolConfig generateJedisPoolConfig(RedisConfig redisConfig) {
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
    public static <T> T execute(RedisCommand<T> command) {
        waitForJedisPoolInitialize();
        Jedis jedis = jedisPool.getResource();
        T result = command.doInRedis(jedis);
        jedisPool.returnResource(jedis);
        return result;
    }

    /**
     * 等待jedispool 加载
     */
    protected static void waitForJedisPoolInitialize() {
        while (jedisPool == null || jedisPool.isClosed()) {
            try {
                logger.warn("waitForJedisPoolInitialize ...");
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
    }
}
