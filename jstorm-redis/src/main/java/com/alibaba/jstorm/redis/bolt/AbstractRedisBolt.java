package com.alibaba.jstorm.redis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
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

    @Override
    public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
        if (!RedisClient.initialize) {
            RedisClient.init((String)stormConf.getOrDefault("redis.config.path", "classpath:redis.yaml"));
        }
    }

    /**
     * doInRedis
     * @param command
     * @param <T>
     * @return
     */
    public static <T> T doInRedis(RedisCommand<T> command) {
        return RedisClient.execute(command);
    }

    /**
     * serializeKey
     * @param key
     * @return
     */
    public static byte[] serializeKey(String key) {
        return keySerialize.serialize(key);
    }

    /**
     * deserializeKey
     * @param bytes
     * @return
     */
    public static String deserializeKey(byte[] bytes) {
        return keySerialize.deserialize(bytes);
    }

    /**
     * serializeValue
     * @param value
     * @return
     */
    public static byte[] serializeValue(Object value) {
        return valueSerialize.serialize(value);
    }

    /**
     * deserializeValue
     * @param bytes
     * @return
     */
    public static Object deserializeValue(byte[] bytes) {
        return valueSerialize.deserialize(bytes);
    }

}
