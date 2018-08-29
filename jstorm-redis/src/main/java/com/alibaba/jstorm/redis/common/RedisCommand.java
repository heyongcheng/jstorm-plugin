package com.alibaba.jstorm.redis.common;

import redis.clients.jedis.Jedis;

/**
 * @author heyc
 * @date 2018/8/28 17:10
 */
@FunctionalInterface
public interface RedisCommand<T> {

    T doInRedis(Jedis jedis);

}
