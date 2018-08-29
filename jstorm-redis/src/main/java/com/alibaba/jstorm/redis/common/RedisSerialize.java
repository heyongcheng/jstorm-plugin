package com.alibaba.jstorm.redis.common;

/**
 * @author heyc
 * @date 2018/8/29 11:28
 */
public interface RedisSerialize<T> {

    byte[] serialize(T t);

    T deserialize(byte[] bytes);

}
