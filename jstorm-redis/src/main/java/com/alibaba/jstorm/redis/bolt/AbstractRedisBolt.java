package com.alibaba.jstorm.redis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import com.alibaba.jstorm.common.utils.ResourceUtils;
import com.alibaba.jstorm.redis.common.*;

import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/27 20:42
 */
public abstract class AbstractRedisBolt extends BaseRichBolt {

    private static final long serialVersionUID = -7294334656454951695L;

    private static final RedisSerialize<String> keySerialize = new StringRedisSerializer();

    private static final RedisSerialize<Object> valueSerialize = new ByteArrayRedisSerializer();

    private transient RedisClient redisClient;

    private RedisConfig redisConfig;

    private RedisConfig defaultRedisConfig;

    public AbstractRedisBolt() {
        String configPath = RedisClient.lookupDefaultConfigPath();
        defaultRedisConfig = new RedisConfig(ResourceUtils.readAsProperties(configPath));
    }

    public AbstractRedisBolt(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    @Override
    public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
        if (redisConfig == null) {
            redisClient = RedisClient.getDefault(defaultRedisConfig);
        } else {
            redisClient = RedisClient.newInstance(redisConfig);
        }
    }

    /**
     * doInRedis
     * @param command
     * @param <T>
     * @return
     */
    public <T> T doInRedis(RedisCommand<T> command) {
        return redisClient.execute(command);
    }

    /**
     * serializeKey
     * @param key
     * @return
     */
    public byte[] serializeKey(String key) {
        return keySerialize.serialize(key);
    }

    /**
     * deserializeKey
     * @param bytes
     * @return
     */
    public String deserializeKey(byte[] bytes) {
        return keySerialize.deserialize(bytes);
    }

    /**
     * serializeValue
     * @param value
     * @return
     */
    public byte[] serializeValue(Object value) {
        return valueSerialize.serialize(value);
    }

    /**
     * deserializeValue
     * @param bytes
     * @return
     */
    public Object deserializeValue(byte[] bytes) {
        return valueSerialize.deserialize(bytes);
    }

}
